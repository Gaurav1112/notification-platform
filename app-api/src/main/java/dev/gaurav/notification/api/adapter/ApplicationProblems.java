package dev.gaurav.notification.api.adapter;

import dev.gaurav.notification.api.error.ApiException;
import dev.gaurav.notification.api.error.FieldViolation;
import dev.gaurav.notification.api.error.IdempotencyKeyReusedException;
import dev.gaurav.notification.api.error.NotificationNotFoundException;
import dev.gaurav.notification.api.error.ProblemType;
import dev.gaurav.notification.api.error.RequestInProgressException;
import dev.gaurav.notification.application.exception.AlreadyDispatchedException;
import dev.gaurav.notification.application.exception.ApplicationException;
import dev.gaurav.notification.application.exception.IdempotencyConflictException;
import dev.gaurav.notification.application.exception.QuotaExceededException;
import dev.gaurav.notification.application.exception.ValidationException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Turns an {@link ApplicationException} into the {@link ApiException} the RFC 9457 advice already
 * knows how to render.
 *
 * <p><strong>This translation is the anti-corruption layer, and it is why the controllers do not
 * catch application exceptions directly.</strong> Both hierarchies carry a status and a type slug,
 * and they agree today — but they are owned by different modules, and the moment one changes
 * without the other, "they agree" stops being true silently. Doing the mapping in one place means a
 * divergence shows up in a diff.
 *
 * <p>{@code ApplicationException} is a sealed hierarchy, and the intent recorded in its package
 * javadoc is that this mapping be an exhaustive {@code switch} so that adding a business failure
 * without handling it is a compile error. Pattern matching for {@code switch} is a Java 21 feature
 * and this module targets 17, so the chain below is {@code instanceof} instead and the
 * exhaustiveness check is a throw rather than the compiler. TODO(phase-3): convert to a pattern
 * switch when the toolchain moves to 21 — the throw is a runtime backstop and the compiler is a
 * better one.
 */
final class ApplicationProblems {

    /**
     * What a rate-limited caller is told to wait.
     *
     * <p>A {@code 429} with no {@code Retry-After} is an invitation to hot-loop, which is how a
     * rate limit becomes an outage. One second is the token bucket's refill granularity; anything
     * longer would be a guess dressed up as a promise.
     *
     * <p>TODO(phase-8): the bucket knows when the next permit is due. Surfacing that through
     * {@code QuotaGuard} would make this the real answer rather than a floor.
     */
    private static final Duration QUOTA_RETRY_AFTER = Duration.ofSeconds(1);

    private ApplicationProblems() {
    }

    /** @param now used only to date the idempotency conflict message; injected so it is testable */
    static ApiException translate(ApplicationException e, Instant now) {
        if (e instanceof IdempotencyConflictException conflict) {
            return new IdempotencyKeyReusedException(conflict.idempotencyKey(), now);
        }
        if (e instanceof dev.gaurav.notification.application.exception.RequestInProgressException inProgress) {
            return new RequestInProgressException(inProgress.idempotencyKey());
        }
        if (e instanceof QuotaExceededException quota) {
            return ApiException.rateLimited(quota.getMessage(), QUOTA_RETRY_AFTER);
        }
        if (e instanceof dev.gaurav.notification.application.exception.NotificationNotFoundException notFound) {
            return new NotificationNotFoundException(notFound.notificationId());
        }
        if (e instanceof AlreadyDispatchedException dispatched) {
            // Deliberately not api.error.AlreadyDispatchedException: its constructor takes the
            // current DeliveryStatus in order to name it in the message, and the application layer
            // does not report which status refused the transition. Inventing one would put a
            // status in front of the caller that the row may never have held.
            return new ApiException(ProblemType.ALREADY_DISPATCHED, dispatched.getMessage());
        }
        if (e instanceof ValidationException validation) {
            return validationProblem(validation);
        }
        throw new IllegalStateException(
                "unmapped ApplicationException subtype: " + e.getClass().getName(), e);
    }

    /**
     * {@code ValidationException} carries two slugs — a field problem and a time problem — and they
     * map to two different problem types even though both are 400s. The caller's remedy differs:
     * one is a serialiser bug, the other is a clock or timezone bug, and those get routed to
     * different people.
     */
    private static ApiException validationProblem(ValidationException validation) {
        var type = "schedule-invalid".equals(validation.typeSlug())
                ? ProblemType.SCHEDULE_INVALID
                : ProblemType.VALIDATION_FAILED;
        List<FieldViolation> violations = validation.errors().stream()
                .map(error -> FieldViolation.of(error.field(), error.code()))
                .toList();
        return new ApiException(type, validation.getMessage(), violations);
    }
}
