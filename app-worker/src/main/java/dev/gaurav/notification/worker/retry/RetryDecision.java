package dev.gaurav.notification.worker.retry;

import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.resilience.retry.RetryTier;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * What to do with a send that did not succeed. Six outcomes, and the reason each one is a separate
 * case rather than a boolean is that they produce six different rows, statuses and metrics.
 *
 * <p>Sealed so that a new outcome cannot be added without every consumer being made to look at it.
 * The alternative — a {@code retry} flag plus a {@code delay} plus a nullable tier — is how
 * "permanent failure" and "budget exhausted" end up sharing a code path and then sharing a
 * dashboard, at which point nobody can tell a broken provider from a broken phone number.
 */
public sealed interface RetryDecision {

    /** Human-readable, and the value of the {@code reason} metric tag. */
    String reason();

    /**
     * Try the same provider again after {@code delay}, parked on {@code tier}.
     *
     * @param delay the jittered backoff the policy computed; the tier is the smallest lane that is
     *              not <em>shorter</em> than it, because rounding down would discard the backoff
     */
    record RetrySameProvider(RetryTier tier, Duration delay, FailureType failureType)
            implements RetryDecision {

        public RetrySameProvider {
            Objects.requireNonNull(tier, "tier");
            Objects.requireNonNull(delay, "delay");
            Objects.requireNonNull(failureType, "failureType");
        }

        @Override
        public String reason() {
            return "retry_same_provider";
        }
    }

    /**
     * Send it again now, through a different provider.
     *
     * <p>Reached for {@code AUTH_FAILURE}, {@code QUOTA_EXCEEDED} and {@code RATE_LIMITED}: all
     * three are properties of <em>our account with this vendor</em>, not of the message. Waiting
     * helps none of them, and retrying the same provider is guaranteed to fail identically.
     * {@code AUTH_FAILURE} and {@code QUOTA_EXCEEDED} additionally page on-call — the failover
     * keeps traffic flowing, but running on the secondary indefinitely is an incident.
     */
    record FailoverNow(FailureType failureType, boolean pageOnCall) implements RetryDecision {

        public FailoverNow {
            Objects.requireNonNull(failureType, "failureType");
        }

        @Override
        public String reason() {
            return "failover_now";
        }
    }

    /**
     * The message will never be deliverable to this address on any provider.
     *
     * @param suppressAddress when true the address is added to the suppression list — an invalid
     *                        number, a dead push token or an explicit STOP. Retrying these burns
     *                        budget forever and, for {@code UNSUBSCRIBED}, is a compliance breach
     */
    record PermanentFailure(FailureType failureType, boolean suppressAddress)
            implements RetryDecision {

        public PermanentFailure {
            Objects.requireNonNull(failureType, "failureType");
        }

        @Override
        public String reason() {
            return "permanent_failure";
        }
    }

    /**
     * The provider may or may not have delivered it, and we must not guess.
     *
     * <p>Produced for an indeterminate outcome on a channel whose duplicate cost is higher than
     * its loss cost — SMS. Twilio offers no client idempotency key and no reconciliation by our
     * own reference, so a retry is an unrecoverable duplicate: real money, a confused user and a
     * carrier spam signal. A lost OTP, by contrast, is recovered by the user pressing "resend".
     * The attempt row stays {@code UNKNOWN} and the delivery webhook or the reconciler resolves it.
     */
    record AwaitReconciliation(FailureType failureType) implements RetryDecision {

        public AwaitReconciliation {
            Objects.requireNonNull(failureType, "failureType");
        }

        @Override
        public String reason() {
            return "await_reconciliation";
        }
    }

    /**
     * The TTL elapsed, or the next retry would land after it.
     *
     * <p>Distinct from a failure because it is not one: a 40-minute-old one-time passcode is worse
     * than no passcode, and delivering it late is the failure. Reported as {@code EXPIRED}, which
     * ranks below {@code QUEUED} precisely so it can only describe a message that never went out.
     */
    record Expired(Instant expiresAt) implements RetryDecision {

        public Expired {
            Objects.requireNonNull(expiresAt, "expiresAt");
        }

        @Override
        public String reason() {
            return "expired";
        }
    }

    /**
     * Stop, park it on the dead-letter topic, and <strong>commit the offset</strong>.
     *
     * <p>Reached when the attempt ladder is spent or the retry budget is empty. Budget exhaustion
     * is the system working, not an error: retries are capped at ~10% of successful traffic so a
     * failing provider is never handed more load than when it was healthy.
     */
    record DeadLetter(FailureType failureType, String detail) implements RetryDecision {

        public DeadLetter {
            Objects.requireNonNull(detail, "detail");
        }

        @Override
        public String reason() {
            return "dead_letter";
        }
    }
}
