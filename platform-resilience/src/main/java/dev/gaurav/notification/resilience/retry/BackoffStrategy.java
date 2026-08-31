package dev.gaurav.notification.resilience.retry;

import java.time.Duration;
import java.util.Random;

/**
 * How long to wait before the next attempt — and, more importantly, how much of that wait is
 * random.
 *
 * <p><strong>The failure this prevents is the retry stampede.</strong> A provider outage does not
 * fail one message, it fails everything in flight. With 100,000 messages failing inside the same
 * second and a non-jittered backoff of {@code initial × multiplier^attempt}, all 100,000 compute
 * <em>the same</em> delay and therefore retry at the same instant. The provider comes back, is hit
 * by 100,000 simultaneous requests, and falls over again — and because the second failure is also
 * synchronised, the herd stays in lockstep and the outage becomes self-sustaining. The retry
 * traffic, not the original fault, is what keeps the provider down.
 *
 * <p>{@link #FULL} is the fix: {@code random(0, min(max, initial × multiplier^attempt))}. The same
 * 100,000 messages spread uniformly across the whole backoff window, so the recovering provider
 * sees a ramp instead of a wall. Note that {@code delay ± noise} (a symmetric band around the
 * deterministic value) is <em>not</em> enough — it narrows the spike, it does not remove it.
 *
 * <p>The cost of full jitter is that a single message's individual latency gets noisier. That is
 * the right trade: the platform optimises for the recovery time of the whole fleet, not for the
 * p50 of one retry.
 *
 * @see RetryPolicy
 */
public enum BackoffStrategy {

    /**
     * Deterministic exponential backoff, no randomness. Present so the stampede it causes can be
     * demonstrated in a test rather than argued about — not intended for production use against a
     * shared provider.
     */
    NONE {
        @Override
        public Duration computeDelay(int attempt, Duration initial, Duration max,
                                     double multiplier, Random rng) {
            return Duration.ofMillis(ceilingMillis(attempt, initial, max, multiplier));
        }
    },

    /**
     * Half deterministic, half random: {@code base/2 + random(0, base/2)}. Halves the spike rather
     * than flattening it. Useful when a caller genuinely needs a minimum wait — for example a
     * provider that penalises probes arriving too soon after a 5xx.
     */
    EQUAL {
        @Override
        public Duration computeDelay(int attempt, Duration initial, Duration max,
                                     double multiplier, Random rng) {
            long half = ceilingMillis(attempt, initial, max, multiplier) / 2;
            return Duration.ofMillis(half + nextLongBounded(rng, half));
        }
    },

    /**
     * {@code random(0, min(max, initial × multiplier^attempt))}. The platform default, and the
     * only strategy that actually spreads a synchronised fleet uniformly across the window.
     */
    FULL {
        @Override
        public Duration computeDelay(int attempt, Duration initial, Duration max,
                                     double multiplier, Random rng) {
            return Duration.ofMillis(nextLongBounded(rng, ceilingMillis(attempt, initial, max, multiplier)));
        }
    },

    /**
     * {@code min(max, random(initial, previousCeiling × 3))} — AWS's "decorrelated jitter". It
     * climbs faster than full jitter and never drops back to near-zero, which shortens total
     * recovery time when the dependency is only lightly overloaded.
     *
     * <p>The canonical formulation feeds back the <em>actual</em> previous sleep. This platform
     * dispatches retries through Kafka tiers rather than a sleeping loop (see {@link RetryTier}),
     * so no per-message previous value survives between attempts; the deterministic ceiling of
     * {@code attempt - 1} is used in its place. The distribution is slightly tighter than true
     * decorrelated jitter, and it is still unsynchronised, which is the property that matters.
     */
    DECORRELATED {
        @Override
        public Duration computeDelay(int attempt, Duration initial, Duration max,
                                     double multiplier, Random rng) {
            long floor = Math.max(0, initial.toMillis());
            long previous = ceilingMillis(Math.max(0, attempt - 1), initial, max, multiplier);
            long ceiling = Math.min(max.toMillis(), saturatingTriple(previous));
            if (ceiling <= floor) {
                return Duration.ofMillis(ceiling);
            }
            return Duration.ofMillis(floor + nextLongBounded(rng, ceiling - floor));
        }
    };

    /**
     * The delay before attempt number {@code attempt + 1}.
     *
     * @param attempt    zero-based index of the retry being scheduled; {@code 0} is the first retry
     *                   and yields a window of {@code initial}
     * @param initial    the first window
     * @param max        hard ceiling on the window, applied before jitter
     * @param multiplier growth factor per attempt, typically {@code 2.0}
     * @param rng        source of randomness; inject a seeded {@link Random} to make a test
     *                   deterministic, never {@code null}
     * @return a non-negative delay, never longer than {@code max}
     */
    public abstract Duration computeDelay(int attempt, Duration initial, Duration max,
                                          double multiplier, Random rng);

    /**
     * {@code min(max, initial × multiplier^attempt)} in milliseconds.
     *
     * <p>The exponentiation is done in {@code double} and clamped there. Computing it in
     * {@code long} overflows into a negative value somewhere around attempt 40 with a 5 s initial
     * delay, and a negative bound makes {@link Random#nextLong(long)} throw — a retry storm would
     * turn into an exception storm inside the retry path itself.
     */
    static long ceilingMillis(int attempt, Duration initial, Duration max, double multiplier) {
        long maxMillis = Math.max(0, max.toMillis());
        if (attempt < 0 || initial.isNegative() || initial.isZero()) {
            return 0;
        }
        double scaled = initial.toMillis() * Math.pow(multiplier, attempt);
        if (Double.isNaN(scaled)) {
            return maxMillis;
        }
        return (long) Math.min(scaled, (double) maxMillis);
    }

    /** {@link Random#nextLong(long)} rejects a bound of zero; a zero window is a legal outcome. */
    private static long nextLongBounded(Random rng, long boundExclusive) {
        return boundExclusive <= 0 ? 0L : rng.nextLong(boundExclusive);
    }

    private static long saturatingTriple(long value) {
        return value > Long.MAX_VALUE / 3 ? Long.MAX_VALUE : value * 3;
    }
}
