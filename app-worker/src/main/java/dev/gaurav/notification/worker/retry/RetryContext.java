package dev.gaurav.notification.worker.retry;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Everything {@link RetryRouter} is allowed to look at.
 *
 * <p>Passing this instead of seven parameters keeps the router a <strong>pure function</strong>:
 * no clock, no registry, no database, no Kafka. That is what makes the whole retry policy — the
 * part most likely to be wrong and hardest to reproduce in production — testable with literals,
 * including the cases nobody can trigger on demand, like "the budget is empty and the deadline is
 * three seconds away".
 *
 * @param channel                     decides the semantics of an indeterminate outcome:
 *                                    at-most-once for SMS, at-least-once plus reconciliation for
 *                                    email and push. It is a cost asymmetry, not a preference
 * @param failureType                 the classification; the single input to retry-vs-fail
 * @param outcomeIndeterminate        true when the provider call ended without an answer. Passed
 *                                    explicitly rather than derived from {@code failureType} alone
 *                                    because a decorator can time a call out and report a type
 *                                    that does not itself imply indeterminacy
 * @param attemptNumber               1-based count of provider attempts already made and failed
 * @param retryAfter                  a provider-supplied {@code Retry-After}, honoured as a floor
 * @param alternativeProviderAvailable whether failing over is actually possible. Push has exactly
 *                                    one route to a device; a decision to fail over that cannot be
 *                                    carried out would silently become a drop
 * @param now                         evaluation time, supplied by the caller so tests have a clock
 * @param expiresAt                   the message TTL. A retry that lands after it is a provider
 *                                    call spent delivering something the user no longer wants
 */
public record RetryContext(Channel channel,
                           FailureType failureType,
                           boolean outcomeIndeterminate,
                           int attemptNumber,
                           Optional<Duration> retryAfter,
                           boolean alternativeProviderAvailable,
                           Instant now,
                           Instant expiresAt) {

    public RetryContext {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(failureType, "failureType");
        Objects.requireNonNull(now, "now");
        Objects.requireNonNull(expiresAt, "expiresAt");
        retryAfter = retryAfter == null ? Optional.empty() : retryAfter;
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber is 1-based, got " + attemptNumber);
        }
    }

    /** True when the failure itself means we cannot know whether the message was delivered. */
    public boolean isIndeterminate() {
        return outcomeIndeterminate || failureType.isOutcomeIndeterminate();
    }

    /**
     * Whether a retry scheduled {@code delay} from now would still be inside the TTL.
     *
     * <p>Checked against the delay rather than against {@code now}, because the message that
     * matters is the one that is alive at decision time and dead by the time the tier fires.
     */
    public boolean fitsBefore(Duration delay) {
        return now.plus(delay).isBefore(expiresAt);
    }
}
