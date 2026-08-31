package dev.gaurav.notification.messaging.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * Layer 2 of the five-layer idempotency stack: "have I already processed this event id?"
 *
 * <p>Kafka is at-least-once and there is no configuration that makes it otherwise on this path.
 * A rebalance mid-batch, a pod eviction between processing and the offset commit, a DLQ replay —
 * all redeliver records that already had their effects applied. For a platform whose effect is
 * "send an SMS to a real phone", replay without a guard means the user gets two OTPs and stops
 * trusting the product.
 *
 * <p>Backed by a Redis {@code SETNX} with a {@value #DEDUPLICATION_DAYS}-day TTL. Seven days is
 * not arbitrary: {@code notification.requested} retains seven days specifically so a cross-region
 * failover can replay from the mirror, and a dedup window shorter than the replay window would let
 * that replay duplicate every send it touches.
 *
 * <p><strong>Layered degradation: if Redis is down we process anyway.</strong> Failing closed
 * would convert a cache outage into a total delivery outage, which is strictly worse than a small
 * window of duplicates — and we are not undefended in that window. Layer 4 is a
 * {@code UNIQUE (recipient_id, attempt_number)} constraint in PostgreSQL, written <em>before</em>
 * the provider call, and layer 5 is the provider idempotency token on the attempt row. Layers 4
 * and 5 still hold. This layer exists to make the common case cheap, not to be the only defence.
 *
 * <p>Deliberately a plain helper rather than an annotation plus an aspect. An aspect would hide
 * the two things a reader of a consumer most needs to see: that the key is checked at all, and
 * where {@link #forget(String, UUID)} is called on failure. The three-line call site is the
 * documentation.
 *
 * <pre>{@code
 * if (!idempotentConsumer.isFirstSighting(GROUP, event.eventId())) {
 *     ack.acknowledge();                 // already applied; commit and move on
 *     return;
 * }
 * try {
 *     apply(event);
 *     ack.acknowledge();                 // offset committed AFTER the DB commit
 * } catch (RuntimeException e) {
 *     idempotentConsumer.forget(GROUP, event.eventId());   // let the redelivery through
 *     throw e;
 * }
 * }</pre>
 */
@Component
public class IdempotentConsumer {

    /** Matches the seven-day retention on {@code notification.requested}. See the class javadoc. */
    public static final int DEDUPLICATION_DAYS = 7;

    /** The TTL applied to every dedup key. */
    public static final Duration DEDUPLICATION_WINDOW = Duration.ofDays(DEDUPLICATION_DAYS);

    private static final Logger log = LoggerFactory.getLogger(IdempotentConsumer.class);

    private static final String KEY_PREFIX = "notif:seen:";

    private final DeduplicationStore store;

    public IdempotentConsumer(DeduplicationStore store) {
        this.store = store;
    }

    /**
     * Records the sighting and reports whether this consumer group has seen the event before.
     *
     * <p><strong>The key is scoped by consumer group, not global.</strong> Several groups read
     * {@code notification.delivery} — the ledger writer, the status projector, the analytics tap —
     * and each must apply the event exactly once. A global key would let whichever group polled
     * first mark the event seen and silently starve every other group. That failure produces no
     * error, no lag and no alert: the events are consumed, the offsets advance, and one
     * downstream just never gets the data.
     *
     * @return {@code true} if this is the first sighting and the caller should process it
     */
    public boolean isFirstSighting(String consumerGroup, UUID eventId) {
        String key = key(consumerGroup, eventId);
        boolean first = store.putIfAbsent(key, DEDUPLICATION_WINDOW);
        if (!first) {
            log.debug("suppressing redelivery of event {} for group {}", eventId, consumerGroup);
        }
        return first;
    }

    /**
     * Releases the key so the pending Kafka redelivery is allowed to run.
     *
     * <p>Must be called on <em>every</em> failure path after {@link #isFirstSighting}, including
     * the ones that look like they cannot happen. A key left behind by a crashed handler turns
     * at-least-once into at-most-once for that message, and it stays that way for seven days.
     */
    public void forget(String consumerGroup, UUID eventId) {
        store.remove(key(consumerGroup, eventId));
    }

    /** False when the dedup store is unreachable and we are running on layers 4 and 5 alone. */
    public boolean isFullyProtected() {
        return store.isAvailable();
    }

    private static String key(String consumerGroup, UUID eventId) {
        Objects.requireNonNull(consumerGroup, "consumerGroup");
        Objects.requireNonNull(eventId, "eventId");
        return KEY_PREFIX + consumerGroup + ':' + eventId;
    }
}
