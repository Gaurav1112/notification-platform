package dev.gaurav.notification.scheduler.due;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.TrafficClass;

/**
 * One row of {@code notif.scheduled_notification}: a piece of work that becomes dispatchable at
 * {@link #dueAt}.
 *
 * <p>{@link #payload} carries the fully-formed {@code NotificationDispatchEvent} as JSON, written
 * when the row was created. Carrying the whole event rather than a pointer is what keeps the claim
 * path free of joins: at 74,600 rows/s a lookup per row would put the dispatch fan-out back on the
 * database that the Redis near-horizon exists to keep out of the loop.
 *
 * <p>{@link #claimCount} is not the number of times the row has been claimed — it is the number of
 * times a claim <em>ended in an expired lease</em>. See {@link LeaseReaper} for why that
 * distinction is what makes it a poison-pill detector rather than a retry counter.
 *
 * @param shard      {@code 0..255}. Fixed at write time and never recomputed, because it decides
 *                   which pod owns the row and a moving shard is a row two pods can claim
 * @param claimedBy  the lease holder's instance id, or {@code null} when the row is not CLAIMED.
 *                   The schema enforces the biconditional
 *                   {@code CHECK ((state = 'CLAIMED') = (claimed_by IS NOT NULL))}, which makes
 *                   "CLAIMED with no owner" unrepresentable and catches a buggy reclaim before it
 *                   double-sends
 */
public record ScheduledWork(UUID id,
                            Instant dueAt,
                            int shard,
                            long tenantId,
                            UUID notificationId,
                            Instant notificationCreatedAt,
                            UUID recipientId,
                            Channel channel,
                            TrafficClass trafficClass,
                            int claimCount,
                            String claimedBy,
                            Instant claimExpiresAt,
                            String payload) {

    public ScheduledWork {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(dueAt, "dueAt");
        if (shard < 0 || shard >= ShardAssignment.SHARD_COUNT) {
            throw new IllegalArgumentException(
                    "shard %d is outside 0..%d".formatted(shard, ShardAssignment.SHARD_COUNT - 1));
        }
    }

    /** The lane this work belongs on, e.g. {@code notification.dispatch.sms.bulk}. */
    public String dispatchTopic() {
        return trafficClass.dispatchTopic(channel);
    }
}
