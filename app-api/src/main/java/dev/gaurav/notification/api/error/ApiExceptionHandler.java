package dev.gaurav.notification.api.error;

import dev.gaurav.notification.api.filter.RequestIdFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Locale;

/**
 * The single place that turns a throwable into an RFC 9457 {@code application/problem+json} body.
 *
 * <p><strong>The failure this prevents, and it is not hypothetical:</strong> a stack trace in a
 * response body. An unhandled exception rendered by a default error page leaks class names, the
 * ORM in use, SQL fragments, file paths and occasionally a connection string — a free architecture
 * diagram for anybody probing the API. Every path through this class ends in a hand-built
 * {@link ProblemDetail}; nothing derives its body from the exception's own type or message unless
 * that exception was explicitly written to be caller-facing.
 *
 * <p>The 500 handler is the one that matters. It logs the throwable with the {@code traceId} and
 * returns a body containing that id and nothing else, because the id is the only thing a caller can
 * usefully do with an internal error.
 *
 * <p><strong>Content negotiation is not consulted.</strong> Errors are always
 * {@code application/problem+json}, even when the caller asked for something else — a client that
 * cannot parse the error body can still read the status code, whereas a 406 in place of the real
 * error tells them nothing at all.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    /** Extension member names, fixed by {@code docs/API.md}. */
    private static final String TRACE_ID = "traceId";
    private static final String ERRORS = "errors";

    // --- caller-facing problems ------------------------------------------------------------------

    /**
     * Covers every {@link ApiException} subtype in one handler.
     *
     * <p>Deliberately not one handler per subclass: the status, title and type URI already live on
     * {@link ProblemType}, so a per-subclass handler could only repeat them — and would be the
     * thing somebody forgets to add when they introduce a new exception, producing a silent 500
     * for a condition that was fully modelled.
     */
    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ProblemDetail> handleApiException(ApiException e, HttpServletRequest request) {
        if (e.type().status().is5xxServerError()) {
            log.error("api error {} traceId={}", e.type().slug(), RequestIdFilter.traceIdOf(request), e);
        } else {
            log.debug("api error {} traceId={} detail={}", e.type().slug(),
                    RequestIdFilter.traceIdOf(request), e.getMessage());
        }
        return respond(e.type(), e.getMessage(), e.violations(), request,
                e.retryAfter().orElse(null));
    }

    // --- framework validation --------------------------------------------------------------------

    /**
     * Bean-validation failures on {@code @Valid @RequestBody}.
     *
     * <p>Every violation is reported, not just the first. A client fixing a payload one 400 at a
     * time across a slow feedback loop is a worse experience than a slightly longer error body, and
     * the {@code errors} array is what makes a form-field mapping possible on their side.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ProblemDetail> handleBodyValidation(MethodArgumentNotValidException e,
                                                              HttpServletRequest request) {
        var violations = e.getBindingResult().getAllErrors().stream()
                .map(error -> error instanceof FieldError fieldError
                        ? FieldViolation.of(fieldError.getField(), codeOf(fieldError.getCode()))
                        : FieldViolation.of(error.getObjectName(), codeOf(error.getCode())))
                .toList();
        return respond(ProblemType.VALIDATION_FAILED,
                "The request body failed validation; see errors for the offending fields.",
                violations, request, null);
    }

    /** Bean-validation failures on path variables and query parameters. */
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ProblemDetail> handleParameterValidation(ConstraintViolationException e,
                                                                   HttpServletRequest request) {
        var violations = e.getConstraintViolations().stream()
                .map(ApiExceptionHandler::toViolation)
                .toList();
        return respond(ProblemType.VALIDATION_FAILED, "One or more parameters failed validation.",
                violations, request, null);
    }

    /**
     * Malformed JSON, or a value that cannot be bound — an unknown enum constant, a string where a
     * number was expected.
     *
     * <p><strong>The exception message is not echoed.</strong> Jackson's parse errors quote the
     * offending input and name internal classes and field paths; that is debugging output, not an
     * API contract, and it has a habit of containing the caller's own payload verbatim, which may
     * be a recipient's phone number.
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ProblemDetail> handleUnreadableBody(HttpMessageNotReadableException e,
                                                              HttpServletRequest request) {
        log.debug("unreadable request body traceId={}", RequestIdFilter.traceIdOf(request), e);
        return respond(ProblemType.VALIDATION_FAILED,
                "The request body could not be parsed as JSON matching this endpoint's schema.",
                List.of(FieldViolation.of("body", "MALFORMED_JSON")), request, null);
    }

    /** A path variable or query parameter of the wrong type — most often a malformed UUID. */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ProblemDetail> handleTypeMismatch(MethodArgumentTypeMismatchException e,
                                                            HttpServletRequest request) {
        return respond(ProblemType.VALIDATION_FAILED,
                "Parameter '%s' is not a valid %s.".formatted(e.getName(), simpleRequiredType(e)),
                List.of(FieldViolation.of(e.getName(), "TYPE_MISMATCH")), request, null);
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<ProblemDetail> handleMissingHeader(MissingRequestHeaderException e,
                                                             HttpServletRequest request) {
        return respond(ProblemType.VALIDATION_FAILED,
                "Required header '%s' is missing.".formatted(e.getHeaderName()),
                List.of(FieldViolation.of(e.getHeaderName(), "MISSING_HEADER")), request, null);
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ProblemDetail> handleMissingParameter(MissingServletRequestParameterException e,
                                                                HttpServletRequest request) {
        return respond(ProblemType.VALIDATION_FAILED,
                "Required parameter '%s' is missing.".formatted(e.getParameterName()),
                List.of(FieldViolation.of(e.getParameterName(), "MISSING_PARAMETER")), request, null);
    }

    /**
     * Body over the container limit.
     *
     * <p>The detail names the remedy rather than the limit, because a caller hitting this is
     * usually trying to inline a large audience and the answer is a manifest, not a bigger request.
     */
    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<ProblemDetail> handleTooLarge(MaxUploadSizeExceededException e,
                                                        HttpServletRequest request) {
        return respond(ProblemType.PAYLOAD_TOO_LARGE,
                "The request body exceeds the 256 KB limit. Use recipients.kind = S3_MANIFEST for large audiences.",
                List.of(), request, null);
    }

    // --- routing -----------------------------------------------------------------------------------

    /**
     * A path with no handler.
     *
     * <p>Reuses {@code notification-not-found} rather than inventing an undocumented type. The
     * catalogue in {@code docs/API.md} is the contract; adding a type here that is not in the table
     * is exactly the drift the closed enum exists to prevent.
     */
    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ProblemDetail> handleNoResource(NoResourceFoundException e,
                                                          HttpServletRequest request) {
        return respond(ProblemType.NOTIFICATION_NOT_FOUND, "No resource at this path.", List.of(), request, null);
    }

    /** Right path, wrong verb. Carries {@code Allow}, which is what a client needs to correct it. */
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ProblemDetail> handleMethodNotSupported(HttpRequestMethodNotSupportedException e,
                                                                  HttpServletRequest request) {
        var problem = problem(ProblemType.VALIDATION_FAILED,
                "%s is not supported on this path.".formatted(e.getMethod()), List.of(), request);
        problem.setStatus(HttpStatus.METHOD_NOT_ALLOWED.value());
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_PROBLEM_JSON);
        if (e.getSupportedHttpMethods() != null) {
            headers.setAllow(e.getSupportedHttpMethods());
        }
        return new ResponseEntity<>(problem, headers, HttpStatus.METHOD_NOT_ALLOWED);
    }

    // --- security ------------------------------------------------------------------------------------

    @ExceptionHandler(AuthenticationException.class)
    public ResponseEntity<ProblemDetail> handleAuthentication(AuthenticationException e,
                                                              HttpServletRequest request) {
        // The reason is logged, never returned: distinguishing "expired" from "bad signature" from
        // "unknown issuer" in the response body is a free probing oracle.
        log.debug("authentication failed traceId={}", RequestIdFilter.traceIdOf(request), e);
        return respond(ProblemType.UNAUTHENTICATED, "A valid bearer token is required.", List.of(), request, null);
    }

    /**
     * A genuine token without the required scope.
     *
     * <p>A 403 is correct here and <em>only</em> here: it says something about the token, not about
     * whether a particular resource exists. The moment the answer would reveal the existence of
     * another tenant's row it must be a 404 instead — see {@link NotificationNotFoundException}.
     */
    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<ProblemDetail> handleAccessDenied(AccessDeniedException e,
                                                            HttpServletRequest request) {
        return respond(ProblemType.INSUFFICIENT_SCOPE, "This token does not carry the required scope.",
                List.of(), request, null);
    }

    // --- last resort ---------------------------------------------------------------------------------

    /**
     * Everything unanticipated.
     *
     * <p>Logged in full, returned as nothing. The {@code traceId} in the body is the join key
     * between what the caller saw and what the log holds — which is the entire point of putting it
     * in every error response, and the reason a support conversation does not require the caller to
     * describe their request from memory.
     */
    @ExceptionHandler(Throwable.class)
    public ResponseEntity<ProblemDetail> handleUnexpected(Throwable e, HttpServletRequest request) {
        var traceId = RequestIdFilter.traceIdOf(request);
        log.error("unhandled exception traceId={} path={}", traceId, request.getRequestURI(), e);
        return respond(ProblemType.INTERNAL_ERROR,
                "The request could not be completed. Quote traceId %s when reporting this.".formatted(traceId),
                List.of(), request, null);
    }

    // --- rendering -------------------------------------------------------------------------------------

    private ResponseEntity<ProblemDetail> respond(ProblemType type, String detail,
                                                  List<FieldViolation> violations,
                                                  HttpServletRequest request, Duration retryAfter) {
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_PROBLEM_JSON);
        if (retryAfter != null) {
            // Seconds, not an HTTP-date. Both are legal; only one is unambiguous when the client's
            // clock is wrong, and a client whose clock is wrong is exactly who is retrying badly.
            headers.set(HttpHeaders.RETRY_AFTER, Long.toString(Math.max(1, retryAfter.toSeconds())));
        }
        return new ResponseEntity<>(problem(type, detail, violations, request), headers, type.status());
    }

    private ProblemDetail problem(ProblemType type, String detail, List<FieldViolation> violations,
                                  HttpServletRequest request) {
        var problem = ProblemDetail.forStatusAndDetail(type.status(), detail);
        problem.setType(type.typeUri());
        problem.setTitle(type.title());
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty(TRACE_ID, RequestIdFilter.traceIdOf(request));
        if (!violations.isEmpty()) {
            problem.setProperty(ERRORS, violations);
        }
        return problem;
    }

    /** {@code NotBlank} → {@code NOT_BLANK}. Null-safe: an anonymous constraint yields INVALID. */
    private static String codeOf(String constraintCode) {
        if (constraintCode == null || constraintCode.isBlank()) {
            return "INVALID";
        }
        return constraintCode
                .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                .toUpperCase(Locale.ROOT);
    }

    private static FieldViolation toViolation(ConstraintViolation<?> violation) {
        var path = violation.getPropertyPath().toString();
        var constraint = violation.getConstraintDescriptor().getAnnotation().annotationType().getSimpleName();
        return FieldViolation.of(path, codeOf(constraint));
    }

    private static String simpleRequiredType(MethodArgumentTypeMismatchException e) {
        var required = e.getRequiredType();
        return required == null ? "value" : required.getSimpleName();
    }
}
