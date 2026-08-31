package dev.gaurav.notification.scheduler.due;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Spreads a scheduled instant deterministically over a five-minute window.
 *
 * <p><strong>The number this fixes.</strong> Humans schedule on the hour. Not approximately — the
 * API receives {@code 09:00:00Z}, {@code 10:00:00Z}, {@code 00:00:00Z}, and a campaign builder UI
 * that offers a time picker in 15-minute increments guarantees it. Measured against the platform's
 * projected 25 million scheduled sends a day, roughly 5% of a day's scheduled volume lands inside a
 * single minute:
 *
 * <pre>
 *   25,000,000 × 5% = 1,250,000 in 60 s  =  20,833 dispatches/s
 *   spread over 300 s                    =   4,167 dispatches/s
 * </pre>
 *
 * <p>20,833/s is five times the platform's steady-state ceiling and would be absorbed by scaling
 * the entire fleet for one minute an hour. 4,167/s is ordinary traffic. The difference costs one
 * modulo.
 *
 * <p><strong>Deterministic, and that is the whole design.</strong> A random offset would move on
 * every restart, so a hydrator that crashes and re-scans the same rows would push different scores
 * into Redis, a row could be dispatched twice at two different instants, and no two runs of a load
 * test would agree. Hashing the row's own id gives the same offset forever: hydration is
 * idempotent, restarts are invisible, and a support question — "why did this land at 09:03:47?" —
 * has an answer that can be recomputed by hand.
 *
 * <p>Not applied to {@link dev.gaurav.notification.domain.enums.TrafficClass#CRITICAL}: that lane's
 * dispatch objective is five seconds, and five minutes of deliberate spread would breach it by two
 * orders of magnitude. See {@link DueScanHydrator}, which is the only caller.
 */
public final class ScheduleJitter {

    /**
     * Five minutes. A constant, not configuration: the offset is baked into scores already written
     * to Redis and into every reasoning-by-hand about when a row will fire. Changing it moves rows
     * that were already scheduled, which is exactly the non-determinism this class exists to
     * prevent.
     */
    public static final int WINDOW_SECONDS = 300;

    private ScheduleJitter() {
    }

    /**
     * The scheduled instant, pushed forward by this row's stable offset.
     *
     * <p>Forward only. Jittering backwards would dispatch before the time the caller asked for,
     * which for a scheduled send is a correctness bug, not a smoothing tactic — an embargoed press
     * release or a scheduled statement must never go early.
     */
    public static Instant apply(UUID id, Instant dueAt) {
        Objects.requireNonNull(dueAt, "dueAt");
        return dueAt.plusSeconds(offsetSeconds(id));
    }

    /**
     * This row's offset in {@code [0, 300)}.
     *
     * <p>{@link Math#floorMod} rather than {@code %}: {@code UUID.hashCode()} is a 32-bit xor-fold
     * and is negative for about half of all UUIDs, and {@code -7 % 300} is {@code -7} in Java.
     * The remainder operator here would silently pull those rows <em>earlier</em> than their
     * scheduled time — the one thing {@link #apply} promises never to do.
     */
    public static int offsetSeconds(UUID id) {
        Objects.requireNonNull(id, "id");
        return Math.floorMod(id.hashCode(), WINDOW_SECONDS);
    }
}
