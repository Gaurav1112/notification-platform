package dev.gaurav.notification.application.result;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.TrafficClass;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * The aggregate answer to {@code GET /v1/notifications/{id}}.
 *
 * <p>{@link Counts} is read from the denormalised counters on the {@code notification} row, not
 * computed by aggregating {@code notification_recipient}. For a 10M-recipient campaign that
 * aggregation is a partition-wide scan per status poll, and clients poll — the counters exist
 * precisely so a status read stays a single index probe.
 *
 * @param status        the notification-level status. For a fan-out this is the coarse state of the
 *                      parent, which is why {@link #aggregateStatus()} exists
 * @param dispatchedAt  {@code null} until the first recipient leaves for a provider
 * @param completedAt   {@code null} until every recipient reached a terminal state
 */
public record NotificationStatusView(
        UUID id,
        UUID requestId,
        Channel channel,
        TrafficClass trafficClass,
        DeliveryStatus status,
        Instant createdAt,
        Instant scheduledAt,
        Instant dispatchedAt,
        Instant completedAt,
        Counts counts) {

    public NotificationStatusView {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(createdAt, "createdAt");
        counts = counts == null ? Counts.empty() : counts;
    }

    /**
     * The word a dashboard shows for a fan-out.
     *
     * <p>Derived rather than stored, because it is a function of the counters and a stored copy is
     * one more thing that can drift from them. {@code PARTIALLY_COMPLETED} is a real and common
     * outcome — a campaign where some recipients bounced is neither a success nor a failure, and
     * flattening it to either one is how a broken segment goes unnoticed for a week.
     */
    public String aggregateStatus() {
        if (counts.total() == 0) {
            return status.name();
        }
        int settled = counts.delivered() + counts.failed() + counts.suppressed();
        if (settled < counts.total()) {
            return "IN_PROGRESS";
        }
        if (counts.delivered() == counts.total()) {
            return "COMPLETED";
        }
        if (counts.delivered() == 0) {
            return "FAILED";
        }
        return "PARTIALLY_COMPLETED";
    }

    /**
     * Recipient tallies.
     *
     * @param pending everything not yet settled — kept explicit rather than derived, so a client
     *                that only renders the five numbers cannot show a total that fails to add up
     */
    public record Counts(int total, int delivered, int failed, int suppressed, int pending) {

        public static Counts empty() {
            return new Counts(0, 0, 0, 0, 0);
        }

        public Counts {
            if (total < 0 || delivered < 0 || failed < 0 || suppressed < 0 || pending < 0) {
                throw new IllegalArgumentException("recipient counts cannot be negative");
            }
        }
    }
}
