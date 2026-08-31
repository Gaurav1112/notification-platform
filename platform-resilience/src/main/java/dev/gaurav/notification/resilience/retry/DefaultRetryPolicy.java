package dev.gaurav.notification.resilience.retry;

import dev.gaurav.notification.domain.enums.FailureType;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;

/**
 * The platform default: five attempts, full-jitter exponential backoff from 5 s to 1 h, bounded by
 * a 72-minute deadline that matches the sum of the {@link RetryTier} ladder.
 *
 * <p>A record because a retry policy is a value — two policies with the same numbers are the same
 * policy, and there is nothing to mutate. It carries its own {@link Random} so tests can seed it;
 * production passes a per-instance {@code Random}, never a shared {@code Math.random()}.
 *
 * <p><strong>{@code Retry-After} is honoured as a floor, not as a replacement.</strong> The delay
 * is {@code max(jittered, retryAfter)}. Taking the header verbatim would put every throttled
 * message back on the wire at exactly the same instant — the provider told all of them the same
 * number — which is the stampede {@link BackoffStrategy#FULL} exists to prevent. Taking only the
 * jittered value would ignore an explicit instruction from the provider and earn a longer ban.
 *
 * @param maxAttempts   total attempts including the first
 * @param initialDelay  the first backoff window
 * @param maxDelay      ceiling on the window before jitter
 * @param multiplier    growth per attempt
 * @param strategy      how much of the window is randomised
 * @param totalDeadline wall-clock ceiling on the whole sequence
 * @param rng           randomness source; seed it in tests
 */
public record DefaultRetryPolicy(int maxAttempts,
                                 Duration initialDelay,
                                 Duration maxDelay,
                                 double multiplier,
                                 BackoffStrategy strategy,
                                 Duration totalDeadline,
                                 Random rng) implements RetryPolicy {

    public DefaultRetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1, got " + maxAttempts);
        }
        if (initialDelay.isNegative() || maxDelay.isNegative() || totalDeadline.isNegative()) {
            throw new IllegalArgumentException("durations must not be negative");
        }
        if (maxDelay.compareTo(initialDelay) < 0) {
            throw new IllegalArgumentException("maxDelay must be >= initialDelay");
        }
        if (multiplier < 1.0) {
            // A multiplier below 1 shrinks the window on every attempt, which turns backoff into
            // a tight retry loop against a provider that is already failing.
            throw new IllegalArgumentException("multiplier must be >= 1.0, got " + multiplier);
        }
        if (strategy == null || rng == null) {
            throw new IllegalArgumentException("strategy and rng are required");
        }
    }

    /**
     * 5 attempts, 5 s → 1 h with full jitter, 72-minute deadline.
     *
     * <p>The numbers are the {@link RetryTier} ladder (5 s + 30 s + 2 m + 10 m + 1 h). Keeping the
     * policy and the tier topics on the same budget means a message can never be scheduled into a
     * tier that outlives its own deadline.
     */
    public static DefaultRetryPolicy platformDefault(Random rng) {
        return new DefaultRetryPolicy(5, Duration.ofSeconds(5), Duration.ofHours(1), 2.0,
                BackoffStrategy.FULL, Duration.ofMinutes(72), rng);
    }

    /** For a channel where a second send is worse than no send at all — SMS one-time passcodes. */
    public static DefaultRetryPolicy noRetry() {
        return new DefaultRetryPolicy(1, Duration.ZERO, Duration.ZERO, 1.0,
                BackoffStrategy.NONE, Duration.ZERO, new Random());
    }

    @Override
    public boolean isRetryable(FailureType type) {
        return type.isRetryable();
    }

    @Override
    public Optional<Duration> nextDelay(int attempt, FailureType type, Optional<Duration> retryAfter) {
        if (!isRetryable(type) || attempt >= maxAttempts) {
            return Optional.empty();
        }
        // attempt is 1-based (one call has already failed); the backoff exponent is 0-based, so the
        // first retry waits initialDelay rather than initialDelay × multiplier.
        int exponent = Math.max(0, attempt - 1);
        Duration jittered = strategy.computeDelay(exponent, initialDelay, maxDelay, multiplier, rng);
        return Optional.of(retryAfter.filter(after -> after.compareTo(jittered) > 0).orElse(jittered));
    }

    /** The tier topic this delay should be published to. */
    public Optional<RetryTier> nextTier(int attempt, FailureType type, Optional<Duration> retryAfter) {
        return nextDelay(attempt, type, retryAfter).map(RetryTier::nearestFor);
    }
}
