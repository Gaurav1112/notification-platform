package dev.gaurav.notification.application.exception;

/**
 * A concurrent request already holds this {@code Idempotency-Key}. {@code 409}.
 *
 * <p>Distinct from {@link IdempotencyConflictException} because the client action is opposite:
 * a conflict is permanent and must never be retried, this one resolves on its own within the lock
 * window. Collapsing both into one status is how a client ends up retrying a conflict forever, or
 * giving up on a request that was about to succeed.
 *
 * <p>The alternative — blocking until the first request finishes — would hold an HTTP thread for
 * the duration of someone else's database transaction, which is how a slow accept becomes a
 * thread-pool exhaustion across the whole API fleet.
 */
public final class RequestInProgressException extends ApplicationException {

    private final String idempotencyKey;

    public RequestInProgressException(String idempotencyKey) {
        super("request-in-progress",
                "A concurrent request holds this idempotency key",
                409,
                "Key '" + idempotencyKey + "' is being processed. Retry shortly.");
        this.idempotencyKey = idempotencyKey;
    }

    public String idempotencyKey() {
        return idempotencyKey;
    }
}
