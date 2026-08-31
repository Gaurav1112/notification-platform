package dev.gaurav.notification.resilience.retry;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * A per-JVM token bucket that caps retries at a fixed fraction — 10% by default — of successful
 * calls, so that a failing provider is never handed <em>more</em> load than when it was healthy.
 *
 * <p><strong>The failure this prevents is retry amplification.</strong> Backoff and jitter fix
 * <em>when</em> retries land; neither fixes <em>how many</em> there are. AWS's own analysis of the
 * pattern is the standard citation: in a five-layer call stack where every layer retries three
 * times, a single user request becomes up to 3^5 = 243 calls at the bottom of the stack. Every
 * layer is individually behaving reasonably, and the dependency that was merely degraded is now
 * receiving 243× its normal traffic and cannot possibly recover. The retries, not the original
 * fault, become the outage.
 *
 * <p>The counterweight is that retries buy far less than intuition suggests. Segment published
 * measurements from their delivery pipeline showing only about <strong>1.5%</strong> of deliveries
 * eventually succeeded on a retry. Spending unbounded capacity chasing a 1.5% recovery rate — while
 * that spend is exactly what prevents the other 98.5% from recovering — is a bad trade at any
 * scale, and a catastrophic one at 3,900 provider calls per second.
 *
 * <p>So retries are treated as a budget, not a right. Every successful call deposits
 * {@code retryRatio} of a token; every retry withdraws one. The bucket is capped, so a long quiet
 * period cannot accumulate a reserve large enough to fund a stampede later. When the provider is
 * healthy, successes vastly outnumber retries and the budget is never felt. When it fails, deposits
 * stop, the bucket drains in bounded time, and retries switch off automatically — no operator, no
 * feature flag, no deploy.
 *
 * <p>Deliberately per-JVM and in-memory. A Redis-backed shared budget would put a network round
 * trip on the failure path, which is the path most likely to already be slow, and would make the
 * retry decision depend on the availability of yet another dependency. Per-pod is a good enough
 * approximation because pods see statistically identical traffic.
 *
 * <p>Thread-safe. Called from every channel worker thread on every failure.
 */
public final class RetryBudget {

    /**
     * Tokens are held as thousandths so a 10% deposit is exact integer arithmetic. Accumulating a
     * {@code double} across billions of compare-and-set operations drifts; a long does not.
     */
    private static final long MILLI_TOKEN = 1_000L;

    private final long capacityMilliTokens;
    private final long depositMilliTokens;
    private final AtomicLong milliTokens;
    private final LongAdder throttled = new LongAdder();

    /**
     * @param retryRatio retries permitted per successful call; {@code 0.1} allows retries to be at
     *                   most ~10% of total traffic once the initial allowance is spent
     * @param capacity   maximum retries that can be banked, which bounds the size of the burst
     *                   allowed at the very start of an incident
     */
    public RetryBudget(double retryRatio, int capacity) {
        if (retryRatio < 0 || retryRatio > 1) {
            throw new IllegalArgumentException("retryRatio must be in [0,1], got " + retryRatio);
        }
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be at least 1, got " + capacity);
        }
        this.capacityMilliTokens = (long) capacity * MILLI_TOKEN;
        this.depositMilliTokens = Math.round(retryRatio * MILLI_TOKEN);
        // Start full: a JVM that has just rolled has recorded no successes yet, and refusing every
        // retry for the first few seconds after each deploy would be its own small outage.
        this.milliTokens = new AtomicLong(capacityMilliTokens);
    }

    /** 10% of calls, with a 100-retry burst allowance. The platform default. */
    public static RetryBudget tenPercent() {
        return new RetryBudget(0.10, 100);
    }

    /**
     * Withdraws one retry from the budget.
     *
     * @return {@code false} when the budget is spent, in which case the caller must fail the
     *         message to the DLQ instead of retrying — being refused here is the system working,
     *         not an error
     */
    public boolean tryAcquire() {
        while (true) {
            long current = milliTokens.get();
            if (current < MILLI_TOKEN) {
                throttled.increment();
                return false;
            }
            if (milliTokens.compareAndSet(current, current - MILLI_TOKEN)) {
                return true;
            }
            // Lost the race to another worker thread; re-read and decide again. Never spin on a
            // stale value — that is how two threads both spend the last token.
        }
    }

    /**
     * Deposits {@code retryRatio} of a token. Call once per call that succeeded, whether it
     * succeeded on the first attempt or on a retry.
     */
    public void recordSuccess() {
        milliTokens.accumulateAndGet(depositMilliTokens,
                (current, deposit) -> Math.min(capacityMilliTokens, current + deposit));
    }

    /** Whole retries currently affordable. Exported as a gauge; a sustained zero means an outage. */
    public long availableRetries() {
        return milliTokens.get() / MILLI_TOKEN;
    }

    /** Retries refused since startup. Exported as a counter; a rising value is the budget biting. */
    public long throttledCount() {
        return throttled.sum();
    }
}
