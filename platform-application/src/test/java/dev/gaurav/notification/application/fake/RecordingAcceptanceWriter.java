package dev.gaurav.notification.application.fake;

import dev.gaurav.notification.application.command.SendNotificationCommand;
import dev.gaurav.notification.application.port.AcceptanceWriter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Stands in for the accept transaction and counts how many times it ran.
 *
 * <p>The count is the assertion that matters: a replayed idempotency key must produce zero
 * additional invocations, because each one would be another committed notification.
 */
public final class RecordingAcceptanceWriter implements AcceptanceWriter {

    private final List<UUID> persistedRequestIds = new ArrayList<>();

    @Override
    public AcceptedRecords persist(
            SendNotificationCommand command, UUID requestId, Instant acceptedAt, Instant expiresAt) {
        persistedRequestIds.add(requestId);
        var notifications = command.channels().stream()
                .map(channel -> new PersistedNotification(UUID.randomUUID(), acceptedAt, channel))
                .toList();
        return new AcceptedRecords(requestId, acceptedAt, notifications, command.recipientCount());
    }

    /** How many accept transactions committed. */
    public int persistCount() {
        return persistedRequestIds.size();
    }

    public List<UUID> persistedRequestIds() {
        return List.copyOf(persistedRequestIds);
    }
}
