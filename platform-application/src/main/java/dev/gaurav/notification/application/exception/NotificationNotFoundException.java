package dev.gaurav.notification.application.exception;

import java.util.UUID;

/**
 * No notification with this id is visible to this tenant. {@code 404}.
 *
 * <p><strong>Also thrown for a cross-tenant read.</strong> A {@code 403} would confirm the id
 * exists, which turns the endpoint into an oracle: an attacker enumerating UUIDs learns which ones
 * are real and how many notifications a competitor sent. The tenant filter is part of the query,
 * not a check after it, so there is no code path where the row is loaded and then rejected.
 */
public final class NotificationNotFoundException extends ApplicationException {

    private final UUID notificationId;

    public NotificationNotFoundException(UUID notificationId) {
        super("notification-not-found",
                "Notification not found",
                404,
                "No notification " + notificationId + " is visible to this tenant.");
        this.notificationId = notificationId;
    }

    public UUID notificationId() {
        return notificationId;
    }
}
