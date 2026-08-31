package dev.gaurav.notification.application.command;

import dev.gaurav.notification.application.exception.ValidationException;
import dev.gaurav.notification.domain.enums.ScheduleType;

import java.time.Duration;
import java.time.Instant;

/**
 * When the caller wants this sent.
 *
 * <p>{@code sendAt} is an {@link Instant}, which is the whole point: the API rejects
 * {@code "2026-09-01T09:00:00"} with no offset as a {@code 400} rather than guessing a zone.
 * Guessing is how an OTP scheduled for 9 a.m. arrives at 3 a.m.
 *
 * <p>Both bounds are checked here rather than downstream because neither is recoverable later. A
 * past {@code sendAt} is claimed by the due scanner on its very next pass and fires immediately —
 * a "send this tomorrow" typo becomes a send right now. A {@code sendAt} years out sits in a
 * partitioned table that retention will drop out from under it, so the send silently never happens.
 */
public record Schedule(ScheduleType type, Instant sendAt) {

    /**
     * One year. Beyond this the schedule row outlives the partitions retention will drop, so the
     * send would disappear rather than fire late — a silent failure, which is the worst kind.
     */
    public static final Duration MAX_HORIZON = Duration.ofDays(365);

    /**
     * Small tolerance for clock skew between the caller and us. Without it a client whose clock is
     * two seconds fast gets a spurious {@code 400} on a perfectly reasonable "in five seconds".
     */
    private static final Duration PAST_TOLERANCE = Duration.ofSeconds(30);

    public Schedule {
        if (type == null) {
            throw ValidationException.invalidField("schedule.type", "REQUIRED", "schedule type is required");
        }
        var now = Instant.now();
        switch (type) {
            case IMMEDIATE -> {
                // Accepting a sendAt here would silently ignore it, and the caller would only find
                // out when the message arrived hours early.
                if (sendAt != null) {
                    throw ValidationException.invalidSchedule(
                            "IMMEDIATE carries no sendAt; use SCHEDULED if the time matters");
                }
            }
            case SCHEDULED, RECURRING -> {
                if (sendAt == null) {
                    throw ValidationException.invalidSchedule(
                            type + " requires sendAt with an explicit UTC offset");
                }
                if (sendAt.isBefore(now.minus(PAST_TOLERANCE))) {
                    throw ValidationException.invalidSchedule(
                            "sendAt " + sendAt + " is in the past; the due scanner would fire it immediately");
                }
                if (sendAt.isAfter(now.plus(MAX_HORIZON))) {
                    throw ValidationException.invalidSchedule(
                            "sendAt " + sendAt + " is beyond the " + MAX_HORIZON.toDays() + "-day horizon");
                }
            }
        }
    }

    /** Send as soon as a worker picks it up. */
    public static Schedule immediate() {
        return new Schedule(ScheduleType.IMMEDIATE, null);
    }

    /** Send once, at {@code sendAt}. */
    public static Schedule at(Instant sendAt) {
        return new Schedule(ScheduleType.SCHEDULED, sendAt);
    }

    /** True when the request goes to the hydrator rather than straight onto a dispatch topic. */
    public boolean isDeferred() {
        return type != ScheduleType.IMMEDIATE;
    }
}
