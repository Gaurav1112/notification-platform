package dev.gaurav.notification.worker.orchestrator;

import dev.gaurav.notification.messaging.consumer.IdempotentConsumer;
import dev.gaurav.notification.messaging.event.NotificationRequestedEvent;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.messaging.topic.Topics;
import dev.gaurav.notification.worker.config.WorkerListenerConfiguration;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * The expander: {@code notification.requested} in, one dispatch event per (recipient, channel) out.
 *
 * <p>The order of the four steps is the design, and every one of them is a failure someone has
 * shipped:
 *
 * <ol>
 *   <li><strong>Idempotent-receiver check by {@code eventId}.</strong> Kafka is at-least-once and
 *       no setting makes it otherwise. A rebalance mid-batch replays the record, and a replayed
 *       expansion is a second copy of an entire campaign.</li>
 *   <li><strong>Preferences, then template, then fan-out — inside one transaction that
 *       commits.</strong></li>
 *   <li><strong>Publish afterwards.</strong> Rows first, events second: a dispatch event that
 *       arrives before its recipient row exists makes the channel worker load nothing, conclude
 *       the message was cancelled, and drop it. The reverse ordering costs at most a re-publish.</li>
 *   <li><strong>Acknowledge last.</strong> The offset is committed after the database commit, so
 *       the worst case is redelivery — which step 1 absorbs — and never a request that Kafka
 *       believes was expanded and PostgreSQL never saw.</li>
 * </ol>
 *
 * <p>There is one window this does not close: a pod killed <em>between</em> the dedup key being
 * written and the transaction committing leaves a key that suppresses the redelivery. That is a
 * deliberate property of the dedup store (failing open on Redis is worse), and the backstop is
 * PostgreSQL: the {@code notification_request} row is still {@code ACCEPTED}, so the reconciler
 * re-drives it. Kafka is transport; the database is the system of record.
 *
 * <p>Publishing is <em>not</em> awaited per event. Blocking on each future would serialise the
 * fan-out of a 10M-recipient campaign at one network round trip per recipient; the producer is
 * configured {@code acks=all} with an idempotent, retrying send, and a genuinely failed publish
 * surfaces as an exception from {@code kafkaTemplate.send} or is recovered by the outbox sweeper.
 */
@Component
public class RequestedEventListener {

    /**
     * Dedup keys are scoped by this, not globally. Several groups read the same topics, and a
     * global key would let whichever group polled first mark the event seen and silently starve
     * every other one — a failure with no error, no lag and no alert.
     */
    public static final String CONSUMER_GROUP = "notification-worker.expander";

    private static final Logger log = LoggerFactory.getLogger(RequestedEventListener.class);

    private final IdempotentConsumer idempotentConsumer;
    private final RequestFanOut fanOut;
    private final NotificationEventPublisher publisher;
    private final Counter duplicates;
    private final Counter suppressed;

    public RequestedEventListener(IdempotentConsumer idempotentConsumer,
                                  RequestFanOut fanOut,
                                  NotificationEventPublisher publisher,
                                  MeterRegistry meters) {
        this.idempotentConsumer = idempotentConsumer;
        this.fanOut = fanOut;
        this.publisher = publisher;
        this.duplicates = Counter.builder("notification.expander.duplicates")
                .description("requested events suppressed as redeliveries")
                .register(meters);
        this.suppressed = Counter.builder("notification.expander.suppressed")
                .description("recipients suppressed by preferences at fan-out")
                .register(meters);
    }

    @KafkaListener(
            id = "expander",
            topics = Topics.REQUESTED,
            groupId = CONSUMER_GROUP,
            containerFactory = WorkerListenerConfiguration.TRANSACTIONAL_FACTORY)
    public void onRequested(NotificationRequestedEvent event, Acknowledgment ack) {
        if (!idempotentConsumer.isFirstSighting(CONSUMER_GROUP, event.eventId())) {
            duplicates.increment();
            ack.acknowledge();
            return;
        }
        try {
            var result = fanOut.expand(event);   // committed when this returns
            result.dispatches().forEach(publisher::publishDispatch);
            suppressed.increment(result.suppressed());
            log.debug("expanded request {} into {} dispatches ({} suppressed, {} deferred)",
                    event.requestId(), result.dispatches().size(), result.suppressed(), result.deferred());
            ack.acknowledge();
        } catch (RuntimeException e) {
            // Without this the redelivery Kafka is about to perform is suppressed by our own dedup
            // entry and the request is silently lost for seven days.
            idempotentConsumer.forget(CONSUMER_GROUP, event.eventId());
            throw e;
        }
    }
}
