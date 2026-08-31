package dev.gaurav.notification.resilience.retry;

import dev.gaurav.notification.domain.enums.FailureType;

import java.time.Duration;
import java.util.Optional;

/**
 * The single place that answers "should this failed send be tried again, and when".
 *
 * <p>An interface rather than a static helper because retry limits are a per-tenant, per-channel
 * business decision: a bulk campaign to a cheap SMS route and a login OTP to a premium one do not
 * deserve the same budget. Making it a strategy also means a worker can be handed a
 * no-retry policy in a test without touching configuration.
 *
 * <p>{@link #nextDelay} returns {@link Optional#empty()} rather than a sentinel duration. Retry
 * exhaustion is a normal outcome — the caller routes the message to the DLQ — and an empty optional
 * makes the compiler ask about that branch. A {@code Duration.ZERO} or {@code -1} sentinel gets
 * passed straight into a scheduler by the first caller who forgets to check.
 */
public interface RetryPolicy {

    /**
     * Whether retrying the <em>same</em> provider could plausibly succeed. Delegates the judgement
     * to {@link FailureType} so there is exactly one classification table in the system.
     */
    boolean isRetryable(FailureType type);

    /** Total attempts allowed, including the first. {@code 1} means "never retry". */
    int maxAttempts();

    /**
     * The wait before the next attempt, or empty when there must not be one.
     *
     * @param attempt    how many attempts have already been made and failed; {@code 1} after the
     *                   first failure
     * @param type       the classified failure
     * @param retryAfter a provider-supplied {@code Retry-After}, honoured as a floor
     * @return empty when the failure is not retryable or the attempt budget is spent
     */
    Optional<Duration> nextDelay(int attempt, FailureType type, Optional<Duration> retryAfter);

    /**
     * The wall-clock ceiling on the whole retry sequence, measured from the first attempt.
     *
     * <p>Separate from {@link #maxAttempts()} on purpose: a message can burn its attempts quickly
     * on fast failures, or slowly against a provider honouring long {@code Retry-After} headers.
     * Only a deadline bounds the second case, and without one a message can outlive its own
     * {@code TrafficClass} TTL and be delivered long after it stopped being useful.
     */
    Duration totalDeadline();
}
