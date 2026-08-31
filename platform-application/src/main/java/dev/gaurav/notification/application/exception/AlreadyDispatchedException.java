package dev.gaurav.notification.application.exception;

import java.util.UUID;

/**
 * Cancel or reschedule arrived after the notification was already claimed or dispatched.
 * {@code 409}.
 *
 * <p>The honest answer, and the reason it is not a {@code 200}: once a worker has the message there
 * is no transactional way to recall it — the provider call cannot enlist in our transaction. A
 * best-effort tombstone is written to Valkey and workers check it immediately before the provider
 * call, which closes most of the window but not all of it. Returning {@code 200} here would promise
 * a guarantee the physics of the system cannot deliver.
 */
public final class AlreadyDispatchedException extends ApplicationException {

    private final UUID notificationId;

    public AlreadyDispatchedException(UUID notificationId) {
        super("already-dispatched",
                "Notification already dispatched",
                409,
                "Notification " + notificationId + " was already claimed or dispatched and cannot be cancelled.");
        this.notificationId = notificationId;
    }

    public UUID notificationId() {
        return notificationId;
    }
}
