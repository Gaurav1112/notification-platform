/**
 * The database-side retry backstop.
 *
 * <p>Consuming the five tiered retry topics belongs to {@code app-worker}. What lives here is the
 * sweep for rows whose {@code next_attempt_at} has passed and for which no tier record survives —
 * a topic that aged out, an offset reset, a worker that died between writing the row and producing
 * the event. In a healthy system it finds nothing.
 */
package dev.gaurav.notification.scheduler.retry;
