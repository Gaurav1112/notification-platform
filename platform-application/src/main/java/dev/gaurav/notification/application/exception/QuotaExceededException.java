package dev.gaurav.notification.application.exception;

/**
 * The tenant is over its send budget. {@code 429}.
 *
 * <p>Thrown only when the limiter <em>answered</em> and the answer was no. A limiter that cannot be
 * reached must never produce this: the accept path fails open, because a Valkey outage that looks
 * like every tenant hitting their limit at once converts a degraded cache into a total outage of
 * the send API.
 *
 * @see dev.gaurav.notification.application.port.QuotaGuard
 */
public final class QuotaExceededException extends ApplicationException {

    private final String tenantId;
    private final int requestedPermits;

    public QuotaExceededException(String tenantId, int requestedPermits) {
        super("rate-limited",
                "Tenant send quota exceeded",
                429,
                "Tenant '" + tenantId + "' has no budget for " + requestedPermits + " more recipients.");
        this.tenantId = tenantId;
        this.requestedPermits = requestedPermits;
    }

    public String tenantId() {
        return tenantId;
    }

    /** Lets the API compute a meaningful {@code Retry-After} rather than a constant. */
    public int requestedPermits() {
        return requestedPermits;
    }
}
