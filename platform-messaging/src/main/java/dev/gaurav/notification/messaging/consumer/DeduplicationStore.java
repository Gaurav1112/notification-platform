package dev.gaurav.notification.messaging.consumer;

import java.time.Duration;

/**
 * The set-once store behind {@link IdempotentConsumer}: Redis {@code SET key value NX EX ttl}.
 *
 * <p>This is a port rather than a direct {@code StringRedisTemplate} call for one concrete reason:
 * {@code platform-messaging} does not — and should not — depend on a Redis client to define its
 * consumer contract. The Valkey-backed implementation is wired in by the deployable that has the
 * client on its classpath. It also makes the layered-degradation behaviour testable without
 * running a Redis instance, which matters because the interesting path here is the one where the
 * store is <em>down</em>.
 *
 * <p><strong>Implementations must be atomic.</strong> A {@code GET} followed by a {@code SET} is
 * not this contract: two consumers rebalancing onto the same partition would both see "absent" and
 * both send. Only a single-round-trip {@code SETNX} closes that window.
 *
 * <p>Implementations must not throw. Every method returns a value that lets the caller keep going,
 * because a dedup cache outage must degrade delivery quality, never stop delivery.
 */
public interface DeduplicationStore {

    /**
     * Atomically records the key if it is absent.
     *
     * @return {@code true} if this call inserted it — the first sighting; {@code false} if it was
     *         already there. A store that is unavailable returns {@code true}, because processing
     *         a possible duplicate is a smaller failure than dropping a real message
     */
    boolean putIfAbsent(String key, Duration ttl);

    /**
     * Removes a key.
     *
     * <p>Called when processing failed <em>after</em> the key was recorded. Without this, the
     * redelivery Kafka is about to perform is suppressed by our own dedup entry and the message is
     * silently lost — the classic idempotent-receiver bug, and invisible because both halves look
     * correct in isolation.
     */
    void remove(String key);

    /** False when the backing store is known to be unreachable. Drives the degradation metric. */
    default boolean isAvailable() {
        return true;
    }
}
