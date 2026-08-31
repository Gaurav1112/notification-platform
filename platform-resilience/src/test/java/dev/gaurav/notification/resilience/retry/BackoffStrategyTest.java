package dev.gaurav.notification.resilience.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The thundering-herd tests. {@link #fullJitterSpreadsASynchronisedFleet()} and
 * {@link #noJitterMakesEveryCallerRetryAtTheSameInstant()} are a matched pair: the second
 * demonstrates the outage, the first demonstrates the fix.
 */
class BackoffStrategyTest {

    private static final Duration INITIAL = Duration.ofSeconds(5);
    private static final Duration MAX = Duration.ofHours(1);
    private static final double MULTIPLIER = 2.0;

    /** attempt 3 → ceiling of 5 s × 2³ = 40 s, comfortably under the 1 h cap. */
    private static final int ATTEMPT = 3;
    private static final long CEILING_MILLIS = 40_000;

    private final Random rng = new Random(20260831L);

    @Test
    @DisplayName("100k messages failing at the same instant do not all retry at the same instant")
    void fullJitterSpreadsASynchronisedFleet() {
        // One sample stands in for one of the 100k messages that failed in the same second.
        // Bucketing at 500 ms gives 80 possible buckets across the 40 s window.
        Set<Long> buckets = new HashSet<>();
        for (int i = 0; i < 1_000; i++) {
            long millis = BackoffStrategy.FULL
                    .computeDelay(ATTEMPT, INITIAL, MAX, MULTIPLIER, rng).toMillis();
            buckets.add(millis / 500);
        }

        assertThat(buckets)
                .as("full jitter must spread the fleet across the whole backoff window; "
                        + "a narrow spread still re-kills the provider on recovery")
                .hasSizeGreaterThanOrEqualTo(20);
    }

    @Test
    @DisplayName("without jitter all 100k retries land on one millisecond — the outage this fixes")
    void noJitterMakesEveryCallerRetryAtTheSameInstant() {
        Set<Duration> distinct = new HashSet<>();
        for (int i = 0; i < 1_000; i++) {
            distinct.add(BackoffStrategy.NONE.computeDelay(ATTEMPT, INITIAL, MAX, MULTIPLIER, rng));
        }

        assertThat(distinct)
                .as("deterministic backoff gives every caller the identical delay")
                .containsExactly(Duration.ofMillis(CEILING_MILLIS));
    }

    @Test
    @DisplayName("a jittered delay never exceeds the ceiling it was drawn from")
    void fullJitterStaysInsideTheWindow() {
        for (int i = 0; i < 1_000; i++) {
            Duration delay = BackoffStrategy.FULL.computeDelay(ATTEMPT, INITIAL, MAX, MULTIPLIER, rng);
            assertThat(delay).isBetween(Duration.ZERO, Duration.ofMillis(CEILING_MILLIS));
        }
    }

    @Test
    @DisplayName("equal jitter guarantees a minimum wait, so a provider is never probed immediately")
    void equalJitterKeepsAFloor() {
        for (int i = 0; i < 500; i++) {
            Duration delay = BackoffStrategy.EQUAL.computeDelay(ATTEMPT, INITIAL, MAX, MULTIPLIER, rng);
            assertThat(delay)
                    .isBetween(Duration.ofMillis(CEILING_MILLIS / 2), Duration.ofMillis(CEILING_MILLIS));
        }
    }

    @Test
    @DisplayName("decorrelated jitter never drops below the initial delay")
    void decorrelatedJitterKeepsClimbing() {
        for (int i = 0; i < 500; i++) {
            Duration delay = BackoffStrategy.DECORRELATED
                    .computeDelay(ATTEMPT, INITIAL, MAX, MULTIPLIER, rng);
            assertThat(delay).isBetween(INITIAL, MAX);
        }
    }

    @Test
    @DisplayName("a high attempt count overflows to a negative bound and throws, unless it is clamped")
    void deepAttemptCountsDoNotOverflowIntoAnException() {
        // 5 s × 2^60 in long arithmetic wraps negative, and Random.nextLong(negative) throws —
        // turning a retry storm into an exception storm inside the retry path itself.
        for (BackoffStrategy strategy : BackoffStrategy.values()) {
            Duration delay = strategy.computeDelay(60, INITIAL, MAX, MULTIPLIER, rng);
            assertThat(delay)
                    .as("%s must clamp rather than overflow", strategy)
                    .isBetween(Duration.ZERO, MAX);
        }
    }

    @Test
    @DisplayName("the exponential ceiling is capped at max, so backoff cannot outlive the TTL")
    void ceilingIsCappedAtMax() {
        assertThat(BackoffStrategy.NONE.computeDelay(20, INITIAL, MAX, MULTIPLIER, rng)).isEqualTo(MAX);
    }

    @Test
    @DisplayName("the first retry waits the initial delay, not the initial delay times the multiplier")
    void firstRetryUsesTheInitialWindow() {
        assertThat(BackoffStrategy.NONE.computeDelay(0, INITIAL, MAX, MULTIPLIER, rng)).isEqualTo(INITIAL);
    }
}
