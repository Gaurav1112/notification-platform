package dev.gaurav.notification.api.dto;

import dev.gaurav.notification.api.error.ScheduleInvalidException;
import dev.gaurav.notification.domain.enums.ScheduleType;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Optional;

/**
 * When to send.
 *
 * <p><strong>{@code sendAt} is a {@link String}, on purpose.</strong> See {@link Rfc3339}: binding
 * it to {@code OffsetDateTime} makes "a bare local time is a 400" depend on a Jackson leniency flag
 * rather than on code, and the failure mode of getting that wrong is a batch that shifts by hours
 * without anything logging a warning.
 *
 * <p>{@code timezone} is carried separately from the offset and is not redundant with it. An offset
 * fixes an instant; an IANA zone is what a {@code RECURRING} schedule needs in order to keep
 * meaning "09:00 local" across a daylight-saving boundary. A cron expression evaluated against a
 * fixed offset drifts by an hour twice a year.
 *
 * @param type     IMMEDIATE, SCHEDULED or RECURRING
 * @param sendAt   RFC 3339 with offset; required for SCHEDULED, forbidden otherwise
 * @param cron     quartz-style expression; required for RECURRING
 * @param timezone IANA zone for a RECURRING schedule, e.g. {@code Asia/Kolkata}
 */
public record ScheduleRequest(

        @NotNull
        ScheduleType type,

        @Size(max = 64)
        String sendAt,

        @Size(max = 128)
        String cron,

        @Size(max = 64)
        String timezone
) {

    /** IMMEDIATE with no other field set — the shape used when {@code schedule} is omitted entirely. */
    public static ScheduleRequest immediate() {
        return new ScheduleRequest(ScheduleType.IMMEDIATE, null, null, null);
    }

    /**
     * Resolves {@code sendAt} against the offset, past and horizon rules.
     *
     * <p>Kept out of bean validation because the three failures need distinct codes
     * ({@code MISSING_OFFSET}, {@code SEND_AT_IN_PAST}, {@code BEYOND_HORIZON}) and the
     * {@code schedule-invalid} problem type, none of which a {@code @Pattern} can express.
     *
     * @param now injected so "in the past" is testable without sleeping
     * @throws ScheduleInvalidException when the combination of type and sendAt is not sendable
     */
    public Optional<Instant> resolveSendAt(Instant now) {
        var effectiveType = type == null ? ScheduleType.IMMEDIATE : type;
        return switch (effectiveType) {
            case IMMEDIATE, RECURRING -> {
                if (sendAt != null && !sendAt.isBlank()) {
                    throw ScheduleInvalidException.notScheduled("schedule.sendAt");
                }
                yield Optional.empty();
            }
            case SCHEDULED -> {
                if (sendAt == null || sendAt.isBlank()) {
                    throw ScheduleInvalidException.notScheduled("schedule.sendAt");
                }
                yield Optional.of(Rfc3339.requireWithinHorizon("schedule.sendAt", sendAt, now));
            }
        };
    }
}
