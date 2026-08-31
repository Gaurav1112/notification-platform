package dev.gaurav.notification.api.filter;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a handler that creates work and therefore requires an {@code Idempotency-Key} header.
 *
 * <p><strong>Why an annotation rather than a URL pattern in a filter.</strong> A pattern like
 * "every POST under {@code /v1}" is wrong in both directions: it would demand a key from
 * {@code POST /v1/webhooks/{code}}, where the provider has no idea what one is and would be told
 * 400 forever, and it would silently stop covering the next work-creating endpoint somebody adds
 * under a different prefix. Marking the method makes the requirement visible where the handler is
 * written and impossible to acquire by accident.
 *
 * @see IdempotencyKeyInterceptor
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Idempotent {
}
