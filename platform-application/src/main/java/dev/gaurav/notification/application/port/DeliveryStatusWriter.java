package dev.gaurav.notification.application.port;

import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.FailureType;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Applies a delivery-status event through the monotonic guard, and records the ones the guard
 * rejected.
 *
 * <p>The guard is a single conditional {@code UPDATE} — no {@code SELECT}-then-{@code UPDATE}, no
 * optimistic-lock retry loop, no application-level sequencing:
 *
 * <pre>{@code
 * UPDATE notif.notification_recipient
 *    SET status = $1, status_rank = $2 ...
 *  WHERE id = $3 AND created_at >= $4 AND created_at < $5   -- partition pruning
 *    AND status_rank < $2                                   -- monotonic
 *    AND NOT EXISTS (terminal)
 * }</pre>
 *
 * <p><strong>Zero rows updated is a success, not an error.</strong> It is the expected answer for a
 * duplicated webhook, a Kafka redelivery, or a late {@code SENT} arriving after {@code DELIVERED} —
 * all of which happen routinely at volume. That single property is what lets every consumer on this
 * path be at-least-once and makes DLQ replay harmless.
 *
 * @see DeliveryStatus#transitionTo(DeliveryStatus)
 */
public interface DeliveryStatusWriter {

    /**
     * @return rows updated: {@code 1} when applied, {@code 0} when the guard refused it as stale,
     *         duplicate or illegal
     */
    int applyStatus(StatusTransition transition);

    /**
     * Appends the refused event to {@code notification_event} with {@code applied = false}.
     *
     * <p>These rows are the most valuable debugging artefact in the system: the webhooks we
     * correctly ignored. Dropping a refused event instead of recording it makes "we never got the
     * bounce" and "we got it and correctly ignored it as stale" indistinguishable six weeks later.
     *
     * @param reason why the guard refused, e.g. {@code STALE_RANK} or {@code TERMINAL}
     */
    void recordUnappliedEvent(StatusTransition transition, String reason);

    /**
     * One provider-reported state change.
     *
     * @param recipientCreatedAt partition key of the {@code notification_recipient} row; without it
     *                           every status write scans every daily partition
     * @param occurredAt         when the provider says it happened, not when we read it. Webhooks
     *                           arrive out of order, so receipt time is the wrong clock
     * @param dedupHash          identifies the source event, so a redelivery of the same webhook
     *                           does not append a second unapplied row for the same fact
     * @param failureType        {@code null} on a success path
     */
    record StatusTransition(
            String tenantId,
            UUID notificationId,
            Instant notificationCreatedAt,
            UUID recipientId,
            Instant recipientCreatedAt,
            DeliveryStatus proposed,
            Instant occurredAt,
            String providerCode,
            FailureType failureType,
            String errorDetail,
            byte[] dedupHash) {

        public StatusTransition {
            Objects.requireNonNull(tenantId, "tenantId");
            Objects.requireNonNull(notificationId, "notificationId");
            Objects.requireNonNull(recipientId, "recipientId");
            Objects.requireNonNull(recipientCreatedAt, "recipientCreatedAt");
            Objects.requireNonNull(proposed, "proposed");
            Objects.requireNonNull(occurredAt, "occurredAt");
            if (dedupHash == null || dedupHash.length == 0) {
                throw new IllegalArgumentException(
                        "without a dedup hash a redelivered webhook appends a duplicate event row");
            }
            dedupHash = dedupHash.clone();
        }

        @Override
        public byte[] dedupHash() {
            return dedupHash.clone();
        }
    }
}
