package dev.gaurav.notification.application.exception;

/**
 * The same {@code Idempotency-Key} arrived with a different request body. {@code 409}.
 *
 * <p>This is the check most implementations skip, and skipping it is worse than having no
 * idempotency at all: the caller gets a {@code 202} referencing a notification they did not ask
 * for, believes their new request succeeded, and never retries it. The message is simply never
 * sent, and nothing anywhere is red.
 */
public final class IdempotencyConflictException extends ApplicationException {

    private final String idempotencyKey;

    public IdempotencyConflictException(String idempotencyKey) {
        super("idempotency-key-reused",
                "Idempotency key reused with a different payload",
                409,
                "Key '" + idempotencyKey + "' was already used with a different request body.");
        this.idempotencyKey = idempotencyKey;
    }

    /** Echoed in the problem detail so the caller can find the offending call site. */
    public String idempotencyKey() {
        return idempotencyKey;
    }
}
