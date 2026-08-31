package dev.gaurav.notification.scheduler.config;

import java.time.Duration;

/**
 * The three atomic operations leader election needs from a shared store.
 *
 * <p>This exists as an interface for one reason: <strong>exclusivity is the property that has to
 * be tested, and it cannot be tested against a mock that just returns {@code true}.</strong> A
 * test needs a store that genuinely honours set-if-absent under concurrency so that N threads
 * campaigning at once can be asserted to produce exactly one winner. Isolating the store behind
 * three methods makes that a twenty-line in-memory implementation instead of a broker.
 *
 * <p>Every method is fail-closed: an unreachable store means "you are not the leader". This is the
 * exact inverse of {@code RedisTokenBucketRateLimiter}, which fails open, and the difference is
 * deliberate. Failing open on a rate limit costs some 429s. Failing open on leadership means every
 * scheduler pod believes it is the single writer, which is the situation leader election exists to
 * prevent.
 *
 * @see RedisLeaderLockStore
 */
public interface LeaderLockStore {

    /** Returned by {@link #acquire} when the lock is held by someone else. */
    long NOT_ACQUIRED = 0L;

    /**
     * Takes the lock if it is free and mints a fencing token, atomically.
     *
     * <p>The two steps are one step on purpose. Acquire-then-increment as separate round trips has
     * a window in which two pods can observe the same counter value, and two leaders holding the
     * same token defeats the whole mechanism.
     *
     * @param lockKey  the lease key
     * @param fenceKey the counter key; must hash to the same Redis Cluster slot as {@code lockKey}
     * @param owner    {@link NodeIdentity#instanceId()}, never a bare pod name
     * @param ttl      how long the lock survives without a renewal
     * @return a positive fencing token, or {@link #NOT_ACQUIRED}
     */
    long acquire(String lockKey, String fenceKey, String owner, Duration ttl);

    /**
     * Extends the lock only if this owner still holds it.
     *
     * <p>Compare-and-set, never a bare {@code PEXPIRE}. An unconditional expiry refresh from a pod
     * that lost the lease during a pause would extend the <em>new</em> leader's key, and the new
     * leader would then be evicted by a peer it does not know exists.
     *
     * @return false when the lock expired or was taken; the caller must stop acting as leader
     */
    boolean renew(String lockKey, String owner, Duration ttl);

    /**
     * Releases the lock if this owner still holds it.
     *
     * <p>Called on graceful shutdown so a rolling deploy hands leadership over in milliseconds
     * instead of waiting out the lease. Best-effort: a failure here costs one lease duration of
     * idle hydration, which is why it returns nothing.
     */
    void release(String lockKey, String owner);
}
