package dev.gaurav.notification.application.exception;

/**
 * Base of the sealed hierarchy of business failures, each already carrying the RFC 9457
 * {@code type} slug and status the API will render.
 *
 * <p>The status lives on the exception rather than in a mapping table in {@code app-api} because
 * the two drift: a new failure gets thrown, nobody adds the table entry, and the client sees a
 * {@code 500} for something that was a perfectly ordinary {@code 409}. Here they cannot disagree.
 *
 * <p>Stack traces are deliberately <em>not</em> suppressed. These are rare per-request events, not
 * control flow in a hot loop, and the trace is what tells an on-call engineer which of four call
 * sites produced the conflict.
 */
public abstract sealed class ApplicationException extends RuntimeException
        permits AlreadyDispatchedException,
                IdempotencyConflictException,
                NotificationNotFoundException,
                QuotaExceededException,
                RequestInProgressException,
                ValidationException {

    /** The documented namespace. Clients match on the full URI, so it must never move silently. */
    public static final String TYPE_BASE = "https://docs.notification-platform.dev/errors/";

    private final String typeSlug;
    private final String title;
    private final int status;

    protected ApplicationException(String typeSlug, String title, int status, String detail) {
        super(detail);
        this.typeSlug = typeSlug;
        this.title = title;
        this.status = status;
    }

    /** The stable slug from {@code docs/API.md}, e.g. {@code idempotency-key-reused}. */
    public final String typeSlug() {
        return typeSlug;
    }

    /** The full RFC 9457 {@code type} URI. */
    public final String typeUri() {
        return TYPE_BASE + typeSlug;
    }

    /** Short, human-readable summary — stable for a given {@link #typeSlug()}. */
    public final String title() {
        return title;
    }

    /** The HTTP status this failure maps to. */
    public final int status() {
        return status;
    }
}
