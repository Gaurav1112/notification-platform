package dev.gaurav.notification.worker.support;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * The {@code [from, to)} bound every read against a partitioned table has to carry.
 *
 * <p>{@code notification}, {@code notification_recipient}, {@code delivery_attempt} and
 * {@code notification_event} are all RANGE-partitioned by day. A query without a bound on the
 * partition column is not an error — it is a scan of every partition, which at 335 partitions
 * turns a 7-buffer point lookup into a sequential read of the whole table. Nothing fails, the
 * query just gets slower every day the platform stays up.
 *
 * <p>Existing as a type rather than as two loose {@code Instant} arguments removes the two ways
 * that bound gets written wrong: passing them in the wrong order (which silently matches nothing),
 * and passing a notification's window to a query over a differently-partitioned table.
 *
 * @param from inclusive lower bound on the partition column
 * @param to   exclusive upper bound
 */
public record PartitionWindow(Instant from, Instant to) {

    public PartitionWindow {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        if (!from.isBefore(to)) {
            throw new IllegalArgumentException("window must be non-empty: " + from + " .. " + to);
        }
    }

    /** The single UTC day that contains {@code anchor}. The tightest bound, and the common case. */
    public static PartitionWindow day(Instant anchor) {
        Instant start = Objects.requireNonNull(anchor, "anchor").truncatedTo(ChronoUnit.DAYS);
        return new PartitionWindow(start, start.plus(1, ChronoUnit.DAYS));
    }

    /**
     * {@code days} whole UTC days starting with the one containing {@code anchor}.
     *
     * <p>Needed wherever the anchor and the target row have different partition columns. A
     * dispatch event carries {@code notificationCreatedAt}, but the {@code notification_recipient}
     * row it points at was written during fan-out, which can land on the next day; and a
     * {@code delivery_attempt} for a message retried across the 1-hour tier can land a day after
     * the recipient row. A one-day window would miss those rows and the worker would treat a live
     * message as if it had never existed.
     */
    public static PartitionWindow days(Instant anchor, int days) {
        if (days < 1) {
            throw new IllegalArgumentException("days must be at least 1, got " + days);
        }
        Instant start = Objects.requireNonNull(anchor, "anchor").truncatedTo(ChronoUnit.DAYS);
        return new PartitionWindow(start, start.plus(days, ChronoUnit.DAYS));
    }

    /**
     * The window a worker uses to find rows related to a dispatch: the notification's day plus the
     * two after it. Covers fan-out crossing midnight and the full {@link
     * dev.gaurav.notification.messaging.topic.RetryTier} budget (1 h 12 m) landing on the next day.
     */
    public static PartitionWindow forDispatch(Instant notificationCreatedAt) {
        return days(notificationCreatedAt, 3);
    }

    /** How wide the scan is. Exported so a widening window shows up as a metric, not as latency. */
    public Duration span() {
        return Duration.between(from, to);
    }
}
