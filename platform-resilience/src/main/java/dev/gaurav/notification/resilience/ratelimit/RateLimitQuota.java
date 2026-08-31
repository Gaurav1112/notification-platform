package dev.gaurav.notification.resilience.ratelimit;

/**
 * A refill rate and a burst allowance — the two numbers a token bucket needs.
 *
 * <p>They are separate on purpose. Rate alone cannot express "Twilio permits 100 messages per
 * second but tolerates a short burst"; capacity alone is a fixed window, which lets a caller spend
 * the whole allowance in the last millisecond of one window and the whole of the next in the first
 * millisecond of the following one — 2× the quota across a window boundary, which is precisely the
 * spike a provider rate-limits to avoid.
 *
 * @param permitsPerSecond sustained refill rate
 * @param burstCapacity    maximum tokens the bucket holds, i.e. the largest instantaneous burst
 */
public record RateLimitQuota(double permitsPerSecond, int burstCapacity) {

    public RateLimitQuota {
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be positive, got " + permitsPerSecond);
        }
        if (burstCapacity < 1) {
            throw new IllegalArgumentException("burstCapacity must be at least 1, got " + burstCapacity);
        }
    }

    /** A bucket that permits a one-second burst — the sane default when a provider publishes only a rate. */
    public static RateLimitQuota perSecond(int permitsPerSecond) {
        return new RateLimitQuota(permitsPerSecond, permitsPerSecond);
    }
}
