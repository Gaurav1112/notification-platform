package dev.gaurav.notification.adapter.persistence;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * The half-open {@code [from, to)} bounds these adapters put on every partitioned read.
 *
 * <p>The repositories all demand a window and refuse to guess one — see the package javadoc in
 * {@link dev.gaurav.notification.persistence.repository}. This class is where the adapter layer's
 * choices of width live, so that "how far back do we look for a notification by id" is answered
 * once rather than differently at five call sites.
 *
 * <p>Two widths, for two different reasons:
 *
 * <ul>
 *   <li>{@link #dayOf} — used whenever the row's own {@code created_at} is already known. One
 *       partition, one index probe. This is the fast path and the one that should be used wherever
 *       the caller can supply the timestamp.</li>
 *   <li>{@link #lookbackFrom} — used for a lookup by id alone, where nothing tells us which day the
 *       row landed in. It spans {@link #LOOKUP_LOOKBACK}, so the planner prunes to that many daily
 *       partitions instead of every partition that exists. It is a bounded scan, not a free one.
 *       <strong>TODO(phase-3):</strong> the API should carry the notification's {@code created_at}
 *       in the opaque resource identifier so this widening is unnecessary.</li>
 * </ul>
 */
final class PartitionWindows {

    /**
     * How far back a lookup by id alone will search.
     *
     * <p>Matches the retention the design assumes for the hot tables. Shorter would make an
     * in-retention notification return 404, which is a worse failure than a bounded scan; longer
     * would probe partitions that no longer exist.
     */
    static final Duration LOOKUP_LOOKBACK = Duration.ofDays(90);

    /** Idempotency records expire after 24 hours, and the table is partitioned hourly. */
    static final Duration IDEMPOTENCY_RETENTION = Duration.ofHours(24);

    private PartitionWindows() {
    }

    /** Inclusive lower bound of the UTC day containing {@code instant}. */
    static Instant dayStart(Instant instant) {
        return instant.truncatedTo(ChronoUnit.DAYS);
    }

    /** Exclusive upper bound of the UTC day containing {@code instant}. */
    static Instant dayEnd(Instant instant) {
        return dayStart(instant).plus(Duration.ofDays(1));
    }

    /** Lower bound for a lookup by id alone, relative to {@code now}. */
    static Instant lookbackFrom(Instant now) {
        return dayStart(now.minus(LOOKUP_LOOKBACK));
    }

    /**
     * Upper bound for a lookup by id alone.
     *
     * <p>Tomorrow rather than {@code now}: a row written by a pod whose clock is a few seconds
     * ahead, or one created in the last microsecond of the day, must still be findable.
     */
    static Instant lookbackTo(Instant now) {
        return dayEnd(now);
    }

    /** The hourly partition slot an idempotency claim belongs to. */
    static Instant hourSlot(Instant instant) {
        return instant.truncatedTo(ChronoUnit.HOURS);
    }
}
