package dev.gaurav.notification.api.dto;

import dev.gaurav.notification.api.error.ScheduleInvalidException;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

/**
 * Parses caller-supplied timestamps under one rule: <strong>an offset is mandatory.</strong>
 *
 * <p><strong>Why this is not left to Jackson.</strong> Binding straight to {@link OffsetDateTime}
 * looks equivalent and is not: Jackson's leniency around a missing offset is a configuration flag,
 * and the failure mode when somebody flips it — or when a future Boot upgrade changes the default —
 * is not a compile error or a test failure. It is a scheduled batch that silently shifts by five
 * and a half hours. Keeping the wire type a {@code String} and parsing here makes the rule code
 * that a test can point at, and lets the rejection carry {@code MISSING_OFFSET} instead of
 * Jackson's "cannot deserialize" prose.
 *
 * <p>The horizon check exists because {@code notification_schedule} is partitioned to one year
 * ahead. A {@code sendAt} in 2039 has no partition to land in, and the DEFAULT partition is a
 * safety net with an alarm on it, never a destination.
 */
public final class Rfc3339 {

    /**
     * One year. Matches the partition horizon, and is also roughly the point beyond which a
     * scheduled send is a data-retention question rather than a notification.
     */
    public static final Duration SCHEDULE_HORIZON = Duration.ofDays(365);

    /**
     * Regex used only for the field-level {@code @Pattern} hint in the OpenAPI document and for
     * fast rejection. The authoritative check is {@link #requireOffsetInstant}, because a regex
     * that also validates day-of-month is unreadable and wrong.
     */
    public static final String OFFSET_PATTERN =
            "^\\d{4}-\\d{2}-\\d{2}[Tt]\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?([Zz]|[+-]\\d{2}:\\d{2})$";

    private Rfc3339() {
    }

    /**
     * @throws ScheduleInvalidException if the value is unparseable or carries no offset — both are
     *                                  {@code 400 schedule-invalid}, never a 500
     */
    public static Instant requireOffsetInstant(String field, String value) {
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException e) {
            // OffsetDateTime.parse rejects a bare local time, which is exactly the case worth
            // naming. Anything else malformed lands here too; MISSING_OFFSET is the common cause
            // and the detail string carries the offending value either way.
            throw ScheduleInvalidException.missingOffset(field, value);
        }
    }

    /** Rejects the past and the far future. {@code now} is injected so the rule is testable. */
    public static Instant requireWithinHorizon(String field, String value, Instant now) {
        var parsed = requireOffsetInstant(field, value);
        if (parsed.isBefore(now)) {
            throw ScheduleInvalidException.inThePast(field, value);
        }
        if (parsed.isAfter(now.plus(SCHEDULE_HORIZON))) {
            throw ScheduleInvalidException.beyondHorizon(field, value);
        }
        return parsed;
    }
}
