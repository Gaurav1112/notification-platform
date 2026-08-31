package dev.gaurav.notification.worker.retry;

import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.messaging.event.RetryScheduledEvent;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.messaging.topic.Topics;
import dev.gaurav.notification.persistence.repository.NotificationRecipientRepository;
import dev.gaurav.notification.worker.config.WorkerListenerConfiguration;
import dev.gaurav.notification.worker.support.Dispatches;
import dev.gaurav.notification.worker.support.PartitionWindow;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The delay tiers: hold a message until its backoff has elapsed, then put it back on its dispatch
 * lane.
 *
 * <p><strong>The wait is a partition pause, never a {@code Thread.sleep}.</strong> Sleeping in a
 * Kafka listener is the mistake this class exists to not make, and it fails in two independent
 * ways:
 *
 * <ol>
 *   <li><em>It holds the partition.</em> The consumer owns it for the whole sleep, so every healthy
 *       message queued behind one recipient stalled on a 30-second backoff waits too.</li>
 *   <li><em>It breaks {@code max.poll.interval.ms}.</em> Kafka measures liveness by the gap between
 *       {@code poll()} calls — 300 s here. The 10-minute and 1-hour tiers obviously exceed it, and
 *       under a provider outage <em>every</em> consumer sleeps at once. The broker declares them
 *       dead, the group rebalances, in-flight partitions are revoked and reassigned, and the whole
 *       group thrashes at the exact moment it must stay stable. KIP-62 moved heartbeats to a
 *       background thread, so the consumer <em>keeps heartbeating and looks perfectly alive</em>
 *       the entire time — which is why this is debugged as a broker problem for hours.
 *       <strong>Raising the interval is not the fix</strong>: it costs failover time, and
 *       worst-case rebalance detection then takes up to twice the interval.</li>
 * </ol>
 *
 * <p>A paused partition still gets polled — the poll simply returns nothing for it — so the poll
 * interval is never breached, the consumer stays in the group, and no rebalance happens. The
 * record is rewound with {@code seek} before the pause so it is re-delivered when the partition
 * resumes; its offset was never committed, so a pod that dies while paused loses nothing.
 *
 * <p>Messages <em>hop</em> tiers rather than waiting in one, because a partition is FIFO: a message
 * waiting an hour at the head of the five-second lane stalls everything behind it. Head-of-line
 * blocking within a tier is therefore bounded by that tier's own delay, which is also what makes it
 * safe to share the five tiers across all three channels.
 *
 * <p>Redelivery here is harmless and deliberately not deduplicated: a duplicate republish produces
 * a dispatch record with the same {@code eventId} and the same attempt number, which the channel
 * worker's attempt-scoped idempotent receiver drops. Adding a Redis round trip to this consumer to
 * re-solve a problem already solved downstream would cost the one property that makes the tiers
 * safe — that they do almost no I/O.
 */
@Component
public class RetryTierListener {

    /** Must match the {@code id} the health gate and the resume sweep look up. */
    public static final String LISTENER_ID = "retry-tiers";

    public static final String CONSUMER_GROUP = "notification-worker.retry";

    private static final Logger log = LoggerFactory.getLogger(RetryTierListener.class);

    private final KafkaListenerEndpointRegistry endpoints;
    private final NotificationRecipientRepository recipients;
    private final NotificationEventPublisher publisher;
    private final Clock clock;
    private final Counter republished;
    private final Counter abandoned;
    private final Map<TopicPartition, Instant> pausedUntil = new ConcurrentHashMap<>();

    public RetryTierListener(KafkaListenerEndpointRegistry endpoints,
                             NotificationRecipientRepository recipients,
                             NotificationEventPublisher publisher,
                             Clock clock,
                             MeterRegistry meters) {
        this.endpoints = endpoints;
        this.recipients = recipients;
        this.publisher = publisher;
        this.clock = clock;
        this.republished = Counter.builder("notification.retry.republished")
                .description("parked messages returned to their dispatch lane")
                .register(meters);
        this.abandoned = Counter.builder("notification.retry.abandoned")
                .description("parked messages dropped because they were cancelled or expired")
                .register(meters);
        meters.gauge("notification.retry.paused_partitions", pausedUntil, Map::size);
    }

    /**
     * All five tiers on one listener.
     *
     * <p>The tier is carried on the event, so five separate methods would differ only in a topic
     * name — and the pause is per <em>partition</em>, which makes the topic split irrelevant to the
     * mechanism. One container also means one place where the pause bookkeeping lives.
     */
    @KafkaListener(
            id = LISTENER_ID,
            topics = {Topics.RETRY_5S, Topics.RETRY_30S, Topics.RETRY_2M, Topics.RETRY_10M,
                    Topics.RETRY_1H},
            groupId = CONSUMER_GROUP,
            containerFactory = WorkerListenerConfiguration.RETRY_TIER_FACTORY)
    public void onParked(RetryScheduledEvent event,
                         @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                         @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                         @Header(KafkaHeaders.OFFSET) long offset,
                         Consumer<?, ?> consumer,
                         Acknowledgment ack) {
        Instant now = clock.instant();
        var partitionRef = new TopicPartition(topic, partition);

        if (!event.isDueAt(now)) {
            park(partitionRef, offset, event.notBefore(), consumer);
            return;   // no acknowledgement: the offset must stay where it is
        }

        if (!stillWanted(event, now)) {
            abandoned.increment();
            ack.acknowledge();
            return;
        }

        publisher.publishDispatch(Dispatches.nextAttempt(event.dispatch(), null));
        republished.increment();
        ack.acknowledge();
    }

    /**
     * Rewinds to this record and pauses the partition until {@code notBefore}.
     *
     * <p>The {@code putIfAbsent} guard is load-bearing. The records already fetched for this
     * partition are still handed to the listener after the pause is requested; without the guard
     * each one would {@code seek} to its own offset, the last seek would win, and every record
     * between the first not-due one and the last would be skipped on resume — a silent loss of
     * exactly the messages the tier was holding. With it, the earliest not-due offset is the one we
     * come back to and the rest are simply re-delivered.
     */
    private void park(TopicPartition partitionRef, long offset, Instant notBefore, Consumer<?, ?> consumer) {
        if (pausedUntil.putIfAbsent(partitionRef, notBefore) != null) {
            return;
        }
        // Safe here and only here: the listener runs on the consumer thread, and a KafkaConsumer is
        // not thread-safe. The pause itself goes through the container, which applies it on the
        // next poll from that same thread.
        consumer.seek(partitionRef, offset);
        container().pausePartition(partitionRef);
        log.debug("paused {} until {}", partitionRef, notBefore);
    }

    /**
     * Resumes partitions whose hold has elapsed.
     *
     * <p>On a timer rather than inside the listener, because a paused partition delivers no records
     * and therefore cannot un-pause itself. 200 ms is well below the shortest tier, so the added
     * latency is noise against a five-second backoff.
     *
     * <p>Resuming a partition that has since been revoked by a rebalance is a no-op on the
     * container, which is why no ownership check is needed here — the entry is simply dropped.
     */
    @Scheduled(fixedDelayString = "${notification.worker.retry-resume-interval-ms:200}")
    public void resumeElapsed() {
        if (pausedUntil.isEmpty()) {
            return;
        }
        Instant now = clock.instant();
        pausedUntil.forEach((partitionRef, notBefore) -> {
            if (!now.isBefore(notBefore)) {
                container().resumePartition(partitionRef);
                pausedUntil.remove(partitionRef);
                log.debug("resumed {}", partitionRef);
            }
        });
    }

    /**
     * Cancelled or expired while it was parked.
     *
     * <p>Expiry is answered from the event alone. Cancellation needs the row, and that read is the
     * one piece of I/O this consumer does: a point lookup pruned to a single daily partition, which
     * is a handful of buffers, against the alternative of spending a provider call and a real SMS
     * on a message the user cancelled twenty minutes ago.
     */
    private boolean stillWanted(RetryScheduledEvent event, Instant now) {
        var dispatch = event.dispatch();
        if (event.outlivesDeadline() || dispatch.isExpiredAt(now)) {
            log.debug("dropping parked message for {}: TTL {} elapsed",
                    dispatch.recipientId(), dispatch.expiresAt());
            return false;
        }
        var window = PartitionWindow.forDispatch(dispatch.notificationCreatedAt());
        var recipient = recipients.findInWindow(dispatch.recipientId(), window.from(), window.to());
        if (recipient.isEmpty()) {
            // The row is gone — retention, or a request that was never committed. Republishing
            // would produce a dispatch the channel worker cannot resolve, three times over.
            log.warn("dropping parked message for {}: no recipient row", dispatch.recipientId());
            return false;
        }
        DeliveryStatus status = recipient.get().getStatus();
        if (status.isTerminal()) {
            log.debug("dropping parked message for {}: already {}", dispatch.recipientId(), status);
            return false;
        }
        return true;
    }

    private MessageListenerContainer container() {
        var container = endpoints.getListenerContainer(LISTENER_ID);
        if (container == null) {
            throw new IllegalStateException("listener container '" + LISTENER_ID + "' is not registered");
        }
        return container;
    }
}
