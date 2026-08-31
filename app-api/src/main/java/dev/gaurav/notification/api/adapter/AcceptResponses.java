package dev.gaurav.notification.api.adapter;

import dev.gaurav.notification.api.dto.AcceptResponse;
import dev.gaurav.notification.application.result.AcceptResult;

import java.util.List;
import java.util.UUID;

/**
 * The one mapping from the accept use case's read model to the {@code 202} wire body.
 *
 * <p>It is a separate class from either caller because two of them need it and they must agree
 * exactly: {@link AcceptResponseSerializer} writes the bytes that will be stored for replay, and
 * {@link NotificationCommandAdapter} builds the object returned on the first call. If those two
 * produced different shapes, a retry would receive a different body from the original for the same
 * idempotency key — the failure the header exists to prevent, and one that only appears on the
 * second request.
 */
final class AcceptResponses {

    private AcceptResponses() {
    }

    static AcceptResponse from(AcceptResult result) {
        var notifications = result.notifications().stream()
                .map(n -> AcceptResponse.AcceptedNotification.accepted(n.id(), n.channel()))
                .toList();
        return new AcceptResponse(
                result.requestId(),
                AcceptResponse.ACCEPTED,
                result.acceptedAt(),
                result.recipientCount(),
                notifications,
                new AcceptResponse.Links(statusPath(notifications)));
    }

    /**
     * The status link points at a notification, not at the request envelope.
     *
     * <p>The envelope has no status resource — {@code GET /v1/notifications/{id}} resolves a
     * {@code notification}, not a {@code notification_request} — so linking to the request id would
     * hand every client a URL that 404s. One request fans out to one notification per channel; the
     * first is the one a single-channel caller means, and a multi-channel caller reads the
     * {@code notifications} array.
     */
    private static String statusPath(List<AcceptResponse.AcceptedNotification> notifications) {
        UUID first = notifications.isEmpty() ? null : notifications.get(0).id();
        return "/v1/notifications/" + first;
    }
}
