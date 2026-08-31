package dev.gaurav.notification.api.error;

import dev.gaurav.notification.domain.enums.DeliveryStatus;

import java.util.UUID;

/**
 * Cancel or reschedule arrived after the notification was claimed or dispatched.
 *
 * <p><strong>Why this is a 409 and not a best-effort 200:</strong> the honest answer to "cancel it"
 * once a worker holds the row is "probably not". A tombstone is written to Valkey and workers check
 * it immediately before the provider call, so a cancel landing in that window usually wins — but
 * "usually" is not a guarantee, and a 200 would be read as one. Somebody would then build a
 * recall-the-email feature on top of it.
 *
 * <p>The underlying rule is enforced by rank rather than by a branch: {@code CANCELLED} is 28 and
 * {@code QUEUED} is 30, so {@link DeliveryStatus#transitionTo} rejects the transition on its own.
 * This exception is that rejection, surfaced.
 */
public class AlreadyDispatchedException extends ApiException {

    public AlreadyDispatchedException(UUID notificationId, DeliveryStatus current) {
        super(ProblemType.ALREADY_DISPATCHED,
                "Notification '%s' is %s; it can no longer be cancelled or rescheduled."
                        .formatted(notificationId, current));
    }
}
