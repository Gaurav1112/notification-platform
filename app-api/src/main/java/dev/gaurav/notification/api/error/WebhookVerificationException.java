package dev.gaurav.notification.api.error;

/**
 * An inbound provider webhook failed one of the verification gates.
 *
 * <p>Always renders as {@code 401 unauthenticated}, and always with the same body regardless of
 * which gate failed. <strong>The reason is deliberately not disclosed.</strong> Telling a caller
 * "signature mismatch" versus "timestamp too old" versus "unknown provider" hands them a free
 * oracle for probing the verifier — and the legitimate provider never sees any of these messages,
 * because a legitimate provider's webhooks verify.
 *
 * <p>The {@code reason} is carried on the exception for the audit record and the
 * {@code webhook_signature_invalid_total} metric label, which are internal.
 */
public class WebhookVerificationException extends ApiException {

    /** Which gate rejected the request. Internal only — never serialised into the response. */
    public enum Reason {
        UNKNOWN_PROVIDER,
        MISSING_SIGNATURE,
        MISSING_TIMESTAMP,
        MALFORMED_TIMESTAMP,
        TIMESTAMP_OUTSIDE_WINDOW,
        SIGNATURE_MISMATCH,
        SOURCE_IP_NOT_ALLOWED
    }

    private final transient Reason reason;

    public WebhookVerificationException(Reason reason) {
        super(ProblemType.UNAUTHENTICATED, "Webhook signature verification failed.");
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
