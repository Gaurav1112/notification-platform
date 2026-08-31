package dev.gaurav.notification.resilience.ratelimit;

/**
 * A quota check against a named bucket — a tenant, a provider, a recipient.
 *
 * <p>An interface with one method so the dispatch path can be tested without Redis, and so a
 * deployment that has not provisioned Redis can bind a permissive implementation rather than have
 * the platform fail to start.
 *
 * <p>Returns a boolean rather than throwing or blocking. Throwing would make an ordinary,
 * high-frequency, expected outcome cost a stack trace at thousands per second; blocking would hold
 * a Kafka consumer thread and break {@code max.poll.interval.ms}. A refused permit is routed to a
 * retry tier, not waited on.
 *
 * @see RedisTokenBucketRateLimiter
 */
public interface RateLimiter {

    /**
     * @param key     the bucket, e.g. {@code provider:twilio:sms} or {@code tenant:42}
     * @param permits how many tokens this call consumes; batched sends consume one per message so
     *                a 500-message batch cannot slip through a 100/s quota as a single call
     * @return {@code true} if the call may proceed
     */
    boolean tryAcquire(String key, int permits);

    /** Single-permit convenience for the common case. */
    default boolean tryAcquire(String key) {
        return tryAcquire(key, 1);
    }
}
