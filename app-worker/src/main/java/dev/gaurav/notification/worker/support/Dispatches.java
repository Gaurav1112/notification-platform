package dev.gaurav.notification.worker.support;

import dev.gaurav.notification.messaging.event.NotificationDispatchEvent;

/**
 * Copies a dispatch event forward to its next attempt.
 *
 * <p>A record has no wither, and hand-rolling a 23-argument copy at each of the two call sites —
 * the failover path in the channel worker and the republish path in the retry tier — is how one of
 * them ends up dropping {@code notificationCreatedAt} and every status write for that message
 * starts scanning all 90 partitions. One copy, one place to be wrong.
 *
 * <p><strong>The {@code eventId} is deliberately unchanged.</strong> A retry is the same logical
 * send, and the id identifies the event, not the attempt of it. Consumers scope their
 * deduplication by attempt number for exactly this reason.
 */
public final class Dispatches {

    private Dispatches() {
    }

    /**
     * @param preferredProvider pins the next attempt to a specific provider, for failover; null
     *                          leaves the choice to the router
     */
    public static NotificationDispatchEvent nextAttempt(NotificationDispatchEvent event,
                                                        String preferredProvider) {
        return new NotificationDispatchEvent(
                event.eventId(), event.occurredAt(), event.tenantId(), event.traceparent(),
                event.notificationId(), event.notificationCreatedAt(), event.recipientId(),
                event.requestId(), event.channel(), event.trafficClass(), event.priority(),
                event.addressCipher(), event.addressHint(), event.dekRef(), event.subject(),
                event.bodyCipher(), event.bodyRef(), event.templateCode(),
                event.idempotencyToken(), preferredProvider, event.attemptNumber() + 1,
                event.expiresAt(), event.attributes());
    }
}
