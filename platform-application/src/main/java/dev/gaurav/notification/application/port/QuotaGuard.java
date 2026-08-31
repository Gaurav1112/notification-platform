package dev.gaurav.notification.application.port;

/**
 * Per-tenant admission control on the accept path.
 *
 * <p>Its job is to stop one tenant's runaway loop from consuming the queue depth, provider spend
 * and worker capacity that every other tenant is paying for. The real implementation is a Valkey
 * token bucket, because the counter has to be shared across every API pod — a per-pod counter
 * multiplies the effective limit by the replica count, which is the same as having no limit during
 * an autoscale event.
 *
 * <p><strong>Fail-open is the contract.</strong> An implementation that cannot reach Valkey must
 * throw rather than return {@code false}, and the caller admits the request. Returning {@code false}
 * on an infrastructure fault would make a cache outage look like every tenant hitting their limit
 * simultaneously — turning a degraded dependency into a total outage of the send API.
 */
public interface QuotaGuard {

    /**
     * @param permits one per recipient, so a 10M-recipient campaign is charged as 10M rather than
     *                as one request. Charging per request lets a single call bypass the limit
     *                entirely
     * @return {@code false} when the tenant is over budget — the caller answers {@code 429}
     * @throws RuntimeException when the limiter itself is unavailable; the caller fails open
     */
    boolean tryConsume(String tenantId, int permits);
}
