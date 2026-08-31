package dev.gaurav.notification.api.error;

import org.springframework.http.HttpStatus;

import java.net.URI;

/**
 * The complete, closed catalogue of machine-readable error identifiers this API can emit.
 *
 * <p><strong>The failure this prevents:</strong> clients string-matching English prose. Without a
 * stable {@code type} URI a caller who needs to distinguish "your schedule was invalid" from "your
 * idempotency key was reused" has exactly one tool — {@code detail.contains("idempotency")} — and
 * the day someone improves the wording, their retry logic silently changes behaviour. A closed enum
 * means the identifier is part of the compiled artefact and a reviewer can diff it.
 *
 * <p>Status and slug are paired here rather than at each throw site so the table in
 * {@code docs/API.md} has exactly one implementation. Two 400s and three 409s exist deliberately:
 * the status code is not specific enough to act on, which is the entire argument for RFC 9457.
 */
public enum ProblemType {

    /** Schema violation, malformed recipient, missing template variable. */
    VALIDATION_FAILED("validation-failed", HttpStatus.BAD_REQUEST, "Request validation failed"),

    /**
     * {@code sendAt} in the past, beyond the one-year horizon, or — the one that matters —
     * carrying no UTC offset. "9 a.m." in an unstated zone is how an OTP arrives at 3 a.m.
     */
    SCHEDULE_INVALID("schedule-invalid", HttpStatus.BAD_REQUEST, "Schedule is not valid"),

    /** Missing, expired or unverifiable credential. Also an inbound webhook that failed HMAC. */
    UNAUTHENTICATED("unauthenticated", HttpStatus.UNAUTHORIZED, "Authentication required"),

    /** The token is genuine; it simply does not carry the scope this endpoint needs. */
    INSUFFICIENT_SCOPE("insufficient-scope", HttpStatus.FORBIDDEN, "Insufficient scope"),

    /**
     * Unknown id — <strong>and also the answer for a cross-tenant read.</strong> See
     * {@link NotificationNotFoundException} for why this must never be a 403.
     */
    NOTIFICATION_NOT_FOUND("notification-not-found", HttpStatus.NOT_FOUND, "Notification not found"),

    /** Same {@code Idempotency-Key}, different request fingerprint. The check most designs skip. */
    IDEMPOTENCY_KEY_REUSED("idempotency-key-reused", HttpStatus.CONFLICT,
            "Idempotency key reused with a different payload"),

    /** A concurrent request already holds the key and has not yet stored its response. */
    REQUEST_IN_PROGRESS("request-in-progress", HttpStatus.CONFLICT, "A request with this key is in progress"),

    /** Cancel or reschedule arrived after the notification was claimed or dispatched. */
    ALREADY_DISPATCHED("already-dispatched", HttpStatus.CONFLICT, "Notification has already been dispatched"),

    /** Body over 256 KB. The fix is {@code recipients.kind = S3_MANIFEST}, not a bigger limit. */
    PAYLOAD_TOO_LARGE("payload-too-large", HttpStatus.CONTENT_TOO_LARGE, "Request body is too large"),

    /**
     * Syntactically fine, semantically empty: every recipient was opted out, capped or suppressed.
     * A 202 here would tell the caller something was sent when nothing was.
     */
    ALL_RECIPIENTS_SUPPRESSED("all-recipients-suppressed", HttpStatus.UNPROCESSABLE_CONTENT,
            "Every recipient was suppressed"),

    /** Tenant or endpoint quota exhausted. Always accompanied by {@code Retry-After}. */
    RATE_LIMITED("rate-limited", HttpStatus.TOO_MANY_REQUESTS, "Rate limit exceeded"),

    /** The catch-all. Carries a {@code traceId} and nothing else — never a stack trace. */
    INTERNAL_ERROR("internal-error", HttpStatus.INTERNAL_SERVER_ERROR, "Internal error"),

    /** Load shedding. BULK sheds before TRANSACTIONAL; CRITICAL sheds last, if ever. */
    SERVICE_DEGRADED("service-degraded", HttpStatus.SERVICE_UNAVAILABLE, "Service is shedding load");

    /**
     * Documentation root. A {@code type} URI is an identifier first and a link second, but making
     * it resolve is what stops the next team inventing their own parallel error vocabulary.
     */
    private static final String DOC_ROOT = "https://docs.notification-platform.dev/errors/";

    private final String slug;
    private final HttpStatus status;
    private final String title;

    ProblemType(String slug, HttpStatus status, String title) {
        this.slug = slug;
        this.status = status;
        this.title = title;
    }

    /** The kebab-case identifier used in {@code docs/API.md} and in the {@code type} URI. */
    public String slug() {
        return slug;
    }

    public HttpStatus status() {
        return status;
    }

    public String title() {
        return title;
    }

    /** Absolute, stable, and safe to branch on. */
    public URI typeUri() {
        return URI.create(DOC_ROOT + slug);
    }
}
