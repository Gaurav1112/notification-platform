package dev.gaurav.notification.api.error;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Any failure the API is willing to describe to a caller, carrying its own RFC 9457 identity.
 *
 * <p><strong>The failure this prevents:</strong> a controller that knows the HTTP status. Spread
 * {@code throw new ResponseStatusException(CONFLICT, "…")} across six controllers and the error
 * contract becomes an emergent property of the codebase rather than a document — the table in
 * {@code docs/API.md} drifts, and nobody notices because no test asserts on prose. Attaching a
 * {@link ProblemType} to the exception means the status, title and {@code type} URI are decided in
 * one enum and rendered in one advice.
 *
 * <p>Deliberately unchecked. These are transport-boundary conditions raised deep inside a use case;
 * threading a checked exception up through the call stack would only produce wrappers.
 *
 * <p><strong>The message is caller-facing.</strong> Never put a provider credential, a recipient
 * address or a stack trace in it — {@code detail} is echoed verbatim into the response body.
 */
public class ApiException extends RuntimeException {

    private final transient ProblemType type;
    private final transient List<FieldViolation> violations;
    private final transient Duration retryAfter;

    public ApiException(ProblemType type, String detail) {
        this(type, detail, List.of(), null);
    }

    public ApiException(ProblemType type, String detail, List<FieldViolation> violations) {
        this(type, detail, violations, null);
    }

    public ApiException(ProblemType type, String detail, List<FieldViolation> violations, Duration retryAfter) {
        super(detail);
        this.type = Objects.requireNonNull(type, "type");
        this.violations = List.copyOf(Objects.requireNonNull(violations, "violations"));
        this.retryAfter = retryAfter;
    }

    public ProblemType type() {
        return type;
    }

    /** Field-level detail for the {@code errors} array. Empty for problems with no field to blame. */
    public List<FieldViolation> violations() {
        return violations;
    }

    /**
     * Present only where the caller can act on it. A {@code 429} without {@code Retry-After} is an
     * invitation to hot-loop, which is how a rate limit becomes an outage.
     */
    public Optional<Duration> retryAfter() {
        return Optional.ofNullable(retryAfter);
    }

    /**
     * Stack traces cost roughly a microsecond each to fill in, which is irrelevant once — and very
     * relevant when a tenant is being rate limited ten thousand times a second. These carry no
     * debugging value the {@code traceId} does not already carry.
     */
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    // --- factories for problems nobody needs to catch by type -----------------------------------

    public static ApiException validationFailed(String detail, List<FieldViolation> violations) {
        return new ApiException(ProblemType.VALIDATION_FAILED, detail, violations);
    }

    public static ApiException unauthenticated(String detail) {
        return new ApiException(ProblemType.UNAUTHENTICATED, detail);
    }

    public static ApiException insufficientScope(String requiredScope) {
        return new ApiException(ProblemType.INSUFFICIENT_SCOPE,
                "This token does not carry the '%s' scope.".formatted(requiredScope));
    }

    public static ApiException payloadTooLarge(long bytes, long limitBytes) {
        return new ApiException(ProblemType.PAYLOAD_TOO_LARGE,
                "Body was %d bytes; the limit is %d. Use recipients.kind = S3_MANIFEST for large audiences."
                        .formatted(bytes, limitBytes));
    }

    public static ApiException allRecipientsSuppressed(int recipientCount) {
        return new ApiException(ProblemType.ALL_RECIPIENTS_SUPPRESSED,
                "All %d recipients were suppressed; nothing was queued.".formatted(recipientCount));
    }

    public static ApiException rateLimited(String detail, Duration retryAfter) {
        return new ApiException(ProblemType.RATE_LIMITED, detail, List.of(), retryAfter);
    }

    public static ApiException serviceDegraded(String detail, Duration retryAfter) {
        return new ApiException(ProblemType.SERVICE_DEGRADED, detail, List.of(), retryAfter);
    }
}
