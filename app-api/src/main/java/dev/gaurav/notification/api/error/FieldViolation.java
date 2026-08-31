package dev.gaurav.notification.api.error;

import java.util.Objects;

/**
 * One entry in the RFC 9457 {@code errors} array: which field, and a code the caller can branch on.
 *
 * <p><strong>Why {@code code} is not the validation message:</strong> "must not be blank" is a
 * sentence written for a human debugging a curl. A client that needs to highlight the offending
 * form field programmatically cannot use it, and a localised build would change it. The pair
 * {@code (field, code)} is the machine contract; {@code detail} on the enclosing problem carries
 * the prose.
 *
 * @param field dotted JSON path into the request body, e.g. {@code schedule.sendAt}
 * @param code  screaming-snake identifier, e.g. {@code MISSING_OFFSET}
 */
public record FieldViolation(String field, String code) {

    public FieldViolation {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(code, "code");
    }

    public static FieldViolation of(String field, String code) {
        return new FieldViolation(field, code);
    }
}
