package dev.gaurav.notification.application.exception;

import java.util.List;
import java.util.Objects;

/**
 * The request could not be accepted because it was not well-formed. {@code 400}.
 *
 * <p>Carries a machine-readable {@code errors} array rather than one prose sentence, because the
 * caller is a service, not a person: "validation failed" gives a client no way to fix itself, and
 * whoever is integrating ends up reading our source to find out which field was wrong.
 *
 * <p>Two slugs share this type. {@code validation-failed} is a schema or field problem;
 * {@code schedule-invalid} is a time problem — a past {@code sendAt}, one beyond the one-year
 * horizon, or one with no UTC offset. They are separated because a client's remediation differs:
 * one is a code fix, the other is usually a clock or timezone bug.
 */
public final class ValidationException extends ApplicationException {

    /** Field-level detail, so a client can point at the offending property. */
    public record FieldError(String field, String code) {

        public FieldError {
            Objects.requireNonNull(field, "field");
            Objects.requireNonNull(code, "code");
        }
    }

    private final List<FieldError> errors;

    private ValidationException(String typeSlug, String title, String detail, List<FieldError> errors) {
        super(typeSlug, title, 400, detail);
        this.errors = List.copyOf(errors);
    }

    /** A schema or field problem: {@code validation-failed}. */
    public static ValidationException invalidField(String field, String code, String detail) {
        return new ValidationException("validation-failed", "Request validation failed", detail,
                List.of(new FieldError(field, code)));
    }

    /** Several field problems reported together, so the client fixes them in one round trip. */
    public static ValidationException invalidFields(String detail, List<FieldError> errors) {
        return new ValidationException("validation-failed", "Request validation failed", detail, errors);
    }

    /** A time problem: {@code schedule-invalid}. */
    public static ValidationException invalidSchedule(String detail) {
        return new ValidationException("schedule-invalid", "Schedule is not valid", detail,
                List.of(new FieldError("schedule.sendAt", "SCHEDULE_INVALID")));
    }

    /** Never empty — an error body with no errors tells the client nothing. */
    public List<FieldError> errors() {
        return errors;
    }
}
