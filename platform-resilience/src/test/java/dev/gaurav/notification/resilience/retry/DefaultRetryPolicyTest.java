package dev.gaurav.notification.resilience.retry;

import dev.gaurav.notification.domain.enums.FailureType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class DefaultRetryPolicyTest {

    private final DefaultRetryPolicy policy = DefaultRetryPolicy.platformDefault(new Random(7L));

    @Test
    @DisplayName("an invalid phone number is not retried five times just because attempts remain")
    void permanentFailuresAreNeverRetried() {
        assertThat(policy.nextDelay(1, FailureType.INVALID_RECIPIENT, Optional.empty())).isEmpty();
        assertThat(policy.nextDelay(1, FailureType.TEMPLATE_ERROR, Optional.empty())).isEmpty();
    }

    @Test
    @DisplayName("the fifth failure exhausts the budget and the message goes to the DLQ, not round again")
    void attemptBudgetIsRespected() {
        assertThat(policy.nextDelay(4, FailureType.PROVIDER_5XX, Optional.empty())).isPresent();
        assertThat(policy.nextDelay(5, FailureType.PROVIDER_5XX, Optional.empty())).isEmpty();
    }

    @Test
    @DisplayName("Retry-After is a floor, so a long provider ban is honoured rather than ignored")
    void retryAfterRaisesTheDelay() {
        Duration retryAfter = Duration.ofMinutes(30);

        Optional<Duration> delay = policy.nextDelay(1, FailureType.RATE_LIMITED, Optional.of(retryAfter));

        assertThat(delay).contains(retryAfter);
    }

    @Test
    @DisplayName("a short Retry-After does not collapse jitter and re-synchronise the throttled fleet")
    void retryAfterDoesNotReplaceJitter() {
        // The provider returns the same Retry-After to everyone. Obeying it verbatim would send
        // every throttled message back at the identical instant.
        Duration tiny = Duration.ofMillis(1);
        var distinct = new java.util.HashSet<Duration>();

        for (int i = 0; i < 200; i++) {
            distinct.add(policy.nextDelay(3, FailureType.RATE_LIMITED, Optional.of(tiny)).orElseThrow());
        }

        assertThat(distinct).hasSizeGreaterThan(50);
        assertThat(distinct).allSatisfy(d -> assertThat(d).isGreaterThanOrEqualTo(tiny));
    }

    @Test
    @DisplayName("the first retry waits at most the initial window, not the full exponential ceiling")
    void firstRetryUsesTheInitialWindow() {
        Duration delay = policy.nextDelay(1, FailureType.TRANSIENT_NETWORK, Optional.empty()).orElseThrow();

        assertThat(delay).isBetween(Duration.ZERO, Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("an SMS OTP policy never schedules a second send, because a duplicate is unrecoverable")
    void noRetryPolicyNeverRetries() {
        assertThat(DefaultRetryPolicy.noRetry().nextDelay(1, FailureType.PROVIDER_5XX, Optional.empty()))
                .isEmpty();
    }

    @Test
    @DisplayName("a multiplier below 1 would shrink the window into a tight loop and is refused")
    void rejectsShrinkingBackoff() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultRetryPolicy(
                3, Duration.ofSeconds(5), Duration.ofMinutes(1), 0.5,
                BackoffStrategy.FULL, Duration.ofMinutes(10), new Random()));
    }

    @Test
    @DisplayName("a computed backoff is routed to a tier that is never shorter than the backoff")
    void tierRoundsUp() {
        var slowPolicy = new DefaultRetryPolicy(5, Duration.ofSeconds(17), Duration.ofHours(1), 2.0,
                BackoffStrategy.NONE, Duration.ofMinutes(72), new Random(1L));

        assertThat(slowPolicy.nextTier(1, FailureType.PROVIDER_5XX, Optional.empty()))
                .contains(RetryTier.T30S);
    }
}
