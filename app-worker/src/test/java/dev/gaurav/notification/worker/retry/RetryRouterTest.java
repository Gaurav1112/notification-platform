package dev.gaurav.notification.worker.retry;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.resilience.retry.DefaultRetryPolicy;
import dev.gaurav.notification.resilience.retry.RetryBudget;
import dev.gaurav.notification.resilience.retry.RetryTier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The retry matrix, one production failure per test name.
 *
 * <p>Every case here is a decision that is wrong in a specific, expensive way if it flips: an
 * invalid number retried five times, a revoked credential dropped instead of failed over, a timeout
 * blind-retried into a duplicate one-time passcode.
 */
class RetryRouterTest {

    private static final Instant NOW = Instant.parse("2026-08-31T12:00:00Z");
    private static final Instant WELL_INSIDE_TTL = NOW.plus(Duration.ofHours(4));

    private final RetryRouter router = new RetryRouter(
            DefaultRetryPolicy.platformDefault(new Random(7L)), RetryBudget.tenPercent());

    // ---- transient ------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(value = FailureType.class,
            names = {"TRANSIENT_NETWORK", "PROVIDER_5XX"})
    @DisplayName("a socket reset is retried on the same provider rather than lost")
    void transientFailuresRetryTheSameProvider(FailureType type) {
        var decision = router.decide(context(Channel.EMAIL, type, 1));

        assertThat(decision).isInstanceOfSatisfying(RetryDecision.RetrySameProvider.class, retry -> {
            assertThat(retry.failureType()).isEqualTo(type);
            // Full jitter draws from [0, window), so zero is a legal — and desirable — delay.
            assertThat(retry.delay().isNegative()).isFalse();
        });
    }

    @Test
    @DisplayName("the backoff is rounded up to a tier, never down — rounding down discards the backoff")
    void backoffRoundsUpToATier() {
        var decision = router.decide(context(Channel.EMAIL, FailureType.TRANSIENT_NETWORK, 1));

        var retry = (RetryDecision.RetrySameProvider) decision;
        assertThat(retry.tier().delay()).isGreaterThanOrEqualTo(retry.delay());
    }

    @Test
    @DisplayName("a 30-minute Retry-After parks the message in the 1h tier, not the 5s one")
    void retryAfterIsHonouredAsAFloor() {
        var context = new RetryContext(Channel.EMAIL, FailureType.RATE_LIMITED, false, 1,
                Optional.of(Duration.ofMinutes(30)), false, NOW, WELL_INSIDE_TTL);

        var decision = router.decide(context);

        assertThat(decision).isInstanceOfSatisfying(RetryDecision.RetrySameProvider.class, retry ->
                assertThat(retry.tier()).isEqualTo(RetryTier.T1H));
    }

    // ---- permanent ------------------------------------------------------------------------

    @Test
    @DisplayName("an invalid phone number is failed permanently, not retried five times")
    void invalidRecipientIsNeverRetried() {
        var decision = router.decide(context(Channel.SMS, FailureType.INVALID_RECIPIENT, 1));

        assertThat(decision).isInstanceOfSatisfying(RetryDecision.PermanentFailure.class, failure -> {
            assertThat(failure.failureType()).isEqualTo(FailureType.INVALID_RECIPIENT);
            assertThat(failure.suppressAddress())
                    .as("an invalid address must be suppressed, or the next campaign sends to it again")
                    .isTrue();
        });
    }

    @ParameterizedTest
    @EnumSource(value = FailureType.class,
            names = {"UNSUBSCRIBED", "DEVICE_UNREGISTERED", "CONTENT_REJECTED"})
    @DisplayName("a STOP reply, a dead token and a spam rejection all suppress the address")
    void suppressingFailuresAreSuppressed(FailureType type) {
        var decision = router.decide(context(Channel.PUSH, type, 1));

        assertThat(decision).isInstanceOfSatisfying(RetryDecision.PermanentFailure.class, failure ->
                assertThat(failure.suppressAddress()).isTrue());
    }

    @ParameterizedTest
    @EnumSource(value = FailureType.class,
            names = {"TEMPLATE_ERROR", "PAYLOAD_TOO_LARGE", "PERMANENT_UNKNOWN"})
    @DisplayName("our own bugs fail the message without blaming the address")
    void ourBugsDoNotSuppressTheAddress(FailureType type) {
        var decision = router.decide(context(Channel.EMAIL, type, 1));

        assertThat(decision).isInstanceOfSatisfying(RetryDecision.PermanentFailure.class, failure ->
                assertThat(failure.suppressAddress()).isFalse());
    }

    // ---- failover -------------------------------------------------------------------------

    @Test
    @DisplayName("a revoked API key fails over immediately instead of waiting out five tiers")
    void authFailureFailsOverImmediately() {
        var decision = router.decide(withAlternative(Channel.SMS, FailureType.AUTH_FAILURE, 1));

        assertThat(decision).isInstanceOfSatisfying(RetryDecision.FailoverNow.class, failover -> {
            assertThat(failover.failureType()).isEqualTo(FailureType.AUTH_FAILURE);
            assertThat(failover.pageOnCall())
                    .as("running on the secondary because our credentials are dead is an incident")
                    .isTrue();
        });
    }

    @Test
    @DisplayName("a spent provider quota moves traffic to the secondary and pages")
    void quotaExceededFailsOver() {
        var decision = router.decide(withAlternative(Channel.EMAIL, FailureType.QUOTA_EXCEEDED, 1));

        assertThat(decision).isInstanceOfSatisfying(RetryDecision.FailoverNow.class, failover ->
                assertThat(failover.pageOnCall()).isTrue());
    }

    @Test
    @DisplayName("a 429 fails over without paging — throttling is normal, revoked credentials are not")
    void rateLimitedFailsOverWithoutPaging() {
        var decision = router.decide(withAlternative(Channel.EMAIL, FailureType.RATE_LIMITED, 1));

        assertThat(decision).isInstanceOfSatisfying(RetryDecision.FailoverNow.class, failover ->
                assertThat(failover.pageOnCall()).isFalse());
    }

    @Test
    @DisplayName("a revoked key on push, where there is nothing to fail over to, fails permanently")
    void authFailureWithoutAnAlternativeIsPermanent() {
        var decision = router.decide(context(Channel.PUSH, FailureType.AUTH_FAILURE, 1));

        assertThat(decision).isInstanceOf(RetryDecision.PermanentFailure.class);
    }

    // ---- indeterminate --------------------------------------------------------------------

    @Test
    @DisplayName("an SMS timeout is left UNKNOWN for reconciliation and is NOT blind-retried")
    void smsTimeoutIsNeverBlindRetried() {
        var decision = router.decide(context(Channel.SMS, FailureType.PROVIDER_TIMEOUT, 1));

        assertThat(decision).isInstanceOfSatisfying(RetryDecision.AwaitReconciliation.class,
                await -> assertThat(await.failureType()).isEqualTo(FailureType.PROVIDER_TIMEOUT));
    }

    @Test
    @DisplayName("an SMS call that timed out is not retried even when the failure type looks retryable")
    void smsIndeterminateFlagAloneStopsTheRetry() {
        var context = new RetryContext(Channel.SMS, FailureType.TRANSIENT_NETWORK, true, 1,
                Optional.empty(), true, NOW, WELL_INSIDE_TTL);

        assertThat(router.decide(context)).isInstanceOf(RetryDecision.AwaitReconciliation.class);
    }

    @Test
    @DisplayName("an email timeout IS retried, because SES can be reconciled by our own tag")
    void emailTimeoutIsRetried() {
        var decision = router.decide(context(Channel.EMAIL, FailureType.PROVIDER_TIMEOUT, 1));

        assertThat(decision).isInstanceOf(RetryDecision.RetrySameProvider.class);
    }

    // ---- deadlines and budget --------------------------------------------------------------

    @Test
    @DisplayName("a message whose TTL has already elapsed is expired, not failed and not retried")
    void alreadyExpiredMessagesAreExpired() {
        var context = new RetryContext(Channel.EMAIL, FailureType.TRANSIENT_NETWORK, false, 1,
                Optional.empty(), true, NOW, NOW.minusSeconds(1));

        assertThat(router.decide(context)).isInstanceOf(RetryDecision.Expired.class);
    }

    @Test
    @DisplayName("a retry that would land after the TTL is expired now, not scheduled and wasted")
    void retriesThatOutliveTheTtlAreExpired() {
        // The first backoff is drawn from [0, 5s); a 1-second TTL cannot accommodate the ladder.
        var context = new RetryContext(Channel.EMAIL, FailureType.PROVIDER_5XX, false, 4,
                Optional.empty(), true, NOW, NOW.plusMillis(1));

        assertThat(router.decide(context)).isInstanceOf(RetryDecision.Expired.class);
    }

    @Test
    @DisplayName("the sixth attempt is dead-lettered rather than looping round the tiers forever")
    void exhaustedAttemptsDeadLetter() {
        var decision = router.decide(context(Channel.EMAIL, FailureType.PROVIDER_5XX, 5));

        assertThat(decision).isInstanceOfSatisfying(RetryDecision.DeadLetter.class, dead ->
                assertThat(dead.detail()).contains("attempts exhausted"));
    }

    @Test
    @DisplayName("an exhausted retry budget dead-letters instead of amplifying load on a sick provider")
    void exhaustedBudgetDeadLetters() {
        var starved = new RetryRouter(
                DefaultRetryPolicy.platformDefault(new Random(7L)), new RetryBudget(0.0, 1));
        // Spend the single token the bucket starts with.
        assertThat(starved.decide(context(Channel.EMAIL, FailureType.PROVIDER_5XX, 1)))
                .isInstanceOf(RetryDecision.RetrySameProvider.class);

        var decision = starved.decide(context(Channel.EMAIL, FailureType.PROVIDER_5XX, 1));

        assertThat(decision).isInstanceOfSatisfying(RetryDecision.DeadLetter.class, dead ->
                assertThat(dead.detail()).contains("budget exhausted"));
    }

    @Test
    @DisplayName("a permanent failure never spends a retry token — the budget is for retries only")
    void permanentFailuresDoNotSpendBudget() {
        long before = router.availableRetries();

        router.decide(context(Channel.SMS, FailureType.INVALID_RECIPIENT, 1));

        assertThat(router.availableRetries()).isEqualTo(before);
    }

    // ---- the whole matrix -------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(FailureType.class)
    @DisplayName("every FailureType produces a decision — none falls through to a catch-all")
    void everyFailureTypeIsClassified(FailureType type) {
        assertThat(router.decide(context(Channel.EMAIL, type, 1))).isNotNull();
        assertThat(router.decide(withAlternative(Channel.SMS, type, 1))).isNotNull();
    }

    @ParameterizedTest
    @EnumSource(FailureType.class)
    @DisplayName("no non-retryable failure is ever scheduled onto a retry tier")
    void nonRetryableFailuresNeverReachATier(FailureType type) {
        var decision = router.decide(context(Channel.EMAIL, type, 1));

        if (!type.isRetryable()) {
            assertThat(decision).isNotInstanceOf(RetryDecision.RetrySameProvider.class);
        }
    }

    private static RetryContext context(Channel channel, FailureType type, int attempt) {
        return new RetryContext(channel, type, false, attempt, Optional.empty(), false,
                NOW, WELL_INSIDE_TTL);
    }

    private static RetryContext withAlternative(Channel channel, FailureType type, int attempt) {
        return new RetryContext(channel, type, false, attempt, Optional.empty(), true,
                NOW, WELL_INSIDE_TTL);
    }
}
