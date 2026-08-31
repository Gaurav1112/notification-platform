package dev.gaurav.notification.messaging.event;

import dev.gaurav.notification.domain.enums.AttemptState;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.domain.enums.SuppressionReason;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * One observation about the fate of one message. Emitted by workers, by provider webhooks and by
 * the reconciler, all onto the same spine.
 *
 * <p>These arrive <strong>out of order and more than once</strong>, always. A {@code DELIVERED}
 * webhook routinely beats our own {@code SENT} write because the provider's callback path is
 * shorter than our database commit; providers re-fire webhooks on any non-2xx; and Kafka replays
 * on rebalance. So this record is deliberately an <em>observation</em>, not a command: the
 * projector feeds it to {@link DeliveryStatus#transitionTo(DeliveryStatus)} and the monotonic
 * SQL guard, and a rejected transition is a normal outcome recorded as {@code applied = false},
 * not an error.
 *
 * @param version    a monotonically increasing counter for this recipient. Required because
 *                   {@code notification.status} is a compacted topic and <strong>compaction keeps
 *                   the record with the highest offset, not the latest state</strong> — a late
 *                   {@code SENT} produced after {@code DELIVERED} survives compaction and would
 *                   regress the projection. Never rely on compaction for correctness
 * @param notificationCreatedAt partition key of the {@code notification} row, so the monotonic
 *                   UPDATE prunes to one daily partition (measured: 8 buffers, 0.041 ms)
 * @param occurredAt the provider's event time where available, ours otherwise. Ordering by the
 *                   Kafka produce time instead collapses every replayed event to "now"
 * @param attemptState {@code UNKNOWN} is the one that matters — it means the provider may or may
 *                   not have delivered, and the per-channel policy (at-most-once for SMS,
 *                   at-least-once plus reconciliation for email) decides what happens next
 * @param costMicros null unless the provider reported a price for this attempt
 */
public record DeliveryStatusEvent(
        UUID eventId,
        Instant occurredAt,
        long tenantId,
        String traceparent,
        UUID notificationId,
        Instant notificationCreatedAt,
        UUID recipientId,
        Channel channel,
        DeliveryStatus status,
        long version,
        int attemptNumber,
        AttemptState attemptState,
        String providerCode,
        String providerMessageId,
        FailureType failureType,
        String failureCode,
        String failureDetail,
        SuppressionReason suppressionReason,
        Long costMicros,
        Long latencyMillis) implements NotificationEvent {

    public DeliveryStatusEvent {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(notificationId, "notificationId");
        Objects.requireNonNull(recipientId, "recipientId");
        Objects.requireNonNull(status, "status");
        if (version < 0) {
            throw new IllegalArgumentException("version must be non-negative, got " + version);
        }
        // A send failure with no classification forces the retry decision into a catch-all
        // branch, which is how an invalid phone number gets retried five times.
        if ((status == DeliveryStatus.SEND_FAILED || status == DeliveryStatus.FAILED)
                && failureType == null) {
            throw new IllegalArgumentException(status + " requires a failureType");
        }
        if (status == DeliveryStatus.SUPPRESSED && suppressionReason == null) {
            throw new IllegalArgumentException("SUPPRESSED requires a suppressionReason");
        }
    }

    /**
     * Whether this observation is newer than what the projector already holds.
     *
     * <p>Strictly greater, not greater-or-equal: a redelivered event carries the same version and
     * must be dropped, otherwise every rebalance double-counts the delivery metrics.
     */
    public boolean supersedes(long currentVersion) {
        return version > currentVersion;
    }

    @Override
    public String eventType() {
        return TYPE_DELIVERY_STATUS;
    }
}
