package dev.gaurav.notification.api.error;

import java.util.UUID;

/**
 * The notification does not exist <em>as far as this caller is concerned</em>.
 *
 * <p><strong>This is the only exception raised for a cross-tenant read, and it must never become a
 * 403.</strong> The distinction is not pedantry:
 *
 * <pre>
 *   404 for id X  →  "no such notification"          (learned: nothing)
 *   403 for id X  →  "exists, but not yours"         (learned: it exists)
 * </pre>
 *
 * <p>A 403 turns the id space into an oracle. UUIDv7 ids are time-ordered, so an attacker holding
 * one valid id of their own can generate neighbouring candidates and, one 403 at a time, enumerate
 * how many notifications a competitor sent and when — volume, cadence and campaign timing, all
 * without reading a single body. That is a real disclosure built entirely out of status codes.
 *
 * <p>The corresponding rule on the write side is that tenant scoping happens in the repository
 * predicate, not in a controller {@code if}: a query that cannot return another tenant's row cannot
 * accidentally answer 403, because it never learns the row exists either.
 */
public class NotificationNotFoundException extends ApiException {

    public NotificationNotFoundException(UUID notificationId) {
        super(ProblemType.NOTIFICATION_NOT_FOUND, "No notification with id '%s'.".formatted(notificationId));
    }
}
