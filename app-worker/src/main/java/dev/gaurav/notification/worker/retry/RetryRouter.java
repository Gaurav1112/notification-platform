package dev.gaurav.notification.worker.retry;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.resilience.retry.RetryBudget;
import dev.gaurav.notification.resilience.retry.RetryPolicy;
import dev.gaurav.notification.resilience.retry.RetryTier;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * The one place that answers "what happens to this failed send".
 *
 * <p>Every alternative to having this class is a {@code catch (Exception e) { retry(); }} spread
 * across three channel workers, which is the most common bug in this class of system: retrying an
 * invalid phone number five times burns budget and helps nobody, while not retrying a socket reset
 * loses a message that would have worked. The two are indistinguishable unless something
 * classifies first.
 *
 * <p><strong>The order of the checks is the policy.</strong> Each one is placed where it is
 * because moving it up or down changes a real outcome:
 *
 * <ol>
 *   <li><strong>Indeterminate, first.</strong> Before anything else, because an unknown outcome is
 *       not a failure and must not be fed into a failure ladder. For SMS this ends the decision:
 *       no reconciliation exists on the dominant vendor, so a retry is an unrecoverable duplicate.</li>
 *   <li><strong>Expired, second.</strong> A dead message is not worth classifying, failing over or
 *       spending a budget token on.</li>
 *   <li><strong>Failover, third — above the retryable check.</strong> {@code AUTH_FAILURE} is
 *       <em>not retryable</em>; if the retryable test came first it would be classified permanent
 *       and the message dropped, when a healthy secondary was sitting right there. Revoked
 *       credentials are a property of our account with one vendor, not of the message.</li>
 *   <li><strong>Permanent, fourth.</strong> Anything not retryable and with nowhere to fail over to.</li>
 *   <li><strong>Budget, fifth.</strong> Checked before the attempt ladder so a token is not spent
 *       on a message the ladder was going to refuse anyway.</li>
 *   <li><strong>The ladder, last.</strong> Attempts exhausted means the dead-letter topic.</li>
 * </ol>
 *
 * <p>Stateless apart from the budget, which is a per-JVM token bucket, so one instance serves
 * every channel worker thread.
 */
@Component
public class RetryRouter {

    private final RetryPolicy policy;
    private final RetryBudget budget;

    public RetryRouter(RetryPolicy policy, RetryBudget budget) {
        this.policy = policy;
        this.budget = budget;
    }

    /**
     * Decides, and — when the decision is to try again — <strong>spends a retry token</strong>.
     *
     * <p>The side effect is deliberate and belongs here rather than at the call site. A budget
     * that is consulted in one place and debited in another drifts the moment a caller returns
     * early, and a retry budget that over-reports its remaining capacity is worse than no budget:
     * it is the amplification guard that fails exactly when the amplification starts.
     */
    public RetryDecision decide(RetryContext ctx) {
        var type = ctx.failureType();

        // 1. Unknown outcome. Never a failure, and for SMS never a retry.
        if (ctx.isIndeterminate() && ctx.channel() == Channel.SMS) {
            return new RetryDecision.AwaitReconciliation(type);
        }

        // 2. Already dead. Nothing below this line can produce a useful delivery.
        if (!ctx.now().isBefore(ctx.expiresAt())) {
            return new RetryDecision.Expired(ctx.expiresAt());
        }

        // 3. Our account with this vendor is broken, not the message. Above the retryable check
        //    on purpose — AUTH_FAILURE and QUOTA_EXCEEDED are not retryable but are failoverable.
        if (type.shouldFailoverImmediately() && ctx.alternativeProviderAvailable()) {
            boolean page = !type.isRetryable();   // AUTH_FAILURE, QUOTA_EXCEEDED
            return new RetryDecision.FailoverNow(type, page);
        }

        // 4. Nothing will ever deliver this.
        if (!type.isRetryable()) {
            return new RetryDecision.PermanentFailure(type, type.shouldSuppressAddress());
        }

        // 5. The retry would land after the TTL. Compute the delay first so the deadline is
        //    checked against where the retry actually lands, not against now.
        var delay = policy.nextDelay(ctx.attemptNumber(), type, ctx.retryAfter());
        if (delay.isEmpty()) {
            return new RetryDecision.DeadLetter(type,
                    "retry attempts exhausted after " + ctx.attemptNumber());
        }
        if (!ctx.fitsBefore(delay.get())) {
            return new RetryDecision.Expired(ctx.expiresAt());
        }

        // 6. Amplification guard. Refusal here is the system working: a failing provider must
        //    never be handed more load than when it was healthy.
        if (!budget.tryAcquire()) {
            return new RetryDecision.DeadLetter(type, "retry budget exhausted");
        }

        Duration backoff = delay.get();
        return new RetryDecision.RetrySameProvider(RetryTier.nearestFor(backoff), backoff, type);
    }

    /**
     * Deposits a fraction of a token. Call once per successful send, first attempt or not.
     *
     * <p>Without deposits the bucket only ever drains and retries switch off permanently after the
     * first incident, which looks exactly like the platform having stopped retrying — a silent,
     * global degradation with no configuration change to point at.
     */
    public void recordSuccess() {
        budget.recordSuccess();
    }

    /** Whole retries still affordable. Exported as a gauge; a sustained zero is an outage. */
    public long availableRetries() {
        return budget.availableRetries();
    }
}
