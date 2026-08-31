package dev.gaurav.notification.worker.orchestrator;

import dev.gaurav.notification.messaging.consumer.IdempotentConsumer;
import dev.gaurav.notification.messaging.event.NotificationRequestedEvent;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.messaging.producer.PublishBatch;
import dev.gaurav.notification.messaging.topic.Topics;
import dev.gaurav.notification.worker.config.WorkerListenerConfiguration;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;

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
 *   <li><strong>Acknowledge last, and only once every publish is confirmed.</strong> The offset is
 *       committed after the database commit <em>and</em> after the broker has acknowledged every
 *       dispatch event, so the worst case is redelivery — which step 1 absorbs — and never a
 *       request that Kafka believes was expanded and PostgreSQL never saw.</li>
 * </ol>
 *
 * <p>There is one window this does not close: a pod killed <em>between</em> the dedup key being
 * written and the transaction committing leaves a key that suppresses the redelivery. That is a
 * deliberate property of the dedup store (failing open on Redis is worse), and the backstop is
 * PostgreSQL: the {@code notification_request} row is still {@code ACCEPTED}, so the reconciler
 * re-drives it. Kafka is transport; the database is the system of record.
 *
 * <p>Publishing is not awaited <em>per event</em>, and it is not fire-and-forget either. Blocking
 * on each future in turn would serialise the fan-out of a 10M-recipient campaign at one network
 * round trip per recipient; discarding the futures — which this class used to do — commits the
 * offset for a campaign whose dispatch events never left the JVM, because {@code send} reports a
 * broker outage by completing a future exceptionally rather than by throwing. Every send is
 * therefore fired first and the whole batch is confirmed under {@link #PUBLISH_CONFIRM_BUDGET}
 * before the ack. There is no outbox row behind these events: the dispatch event <em>is</em> the
 * expansion, so its future is the only evidence it exists.
 */
@Component
public class RequestedEventListener {

    /**
     * Dedup keys are scoped by this, not globally. Several groups read the same topics, and a
     * global key would let whichever group polled first mark the event seen and silently starve
     * every other one — a failure with no error, no lag and no alert.
     */
    public static final String CONSUMER_GROUP = "notification-worker.expander";

    /**
     * How long one expansion may wait for the broker to acknowledge its whole fan-out.
     *
     * <p>The arithmetic that matters is per <em>poll</em>, not per record. {@code max.poll.records}
     * is 30 and {@code max.poll.interval.ms} is 300 000, so the ceiling this can add between two
     * polls is 30 × 5 s = 150 s — half the interval, with the other half left for the fan-out
     * transactions themselves. Overrun it and the consumer is evicted, the group rebalances and the
     * partitions are reassigned mid-campaign, which trades silent loss for a rebalance storm.
     *
     * <p>The whole fan-out shares this one budget because the sends are concurrently in flight: a
     * batch of 10 000 dispatch events normally confirms in about the time of its slowest single
     * send. A per-event timeout of the same size would be 13 hours.
     */
    private static final Duration PUBLISH_CONFIRM_BUDGET = Duration.ofSeconds(5);

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
            var publishes = new PublishBatch(result.dispatches().size());
            for (var dispatch : result.dispatches()) {
                publishes.add(publisher.publishDispatch(dispatch));
            }
            // Throws if any send did not reach the broker, which skips the ack below and leaves
            // the offset where it is. Redelivery re-expands; step 1 and the row lookups absorb it.
            publishes.awaitAll(PUBLISH_CONFIRM_BUDGET);
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
