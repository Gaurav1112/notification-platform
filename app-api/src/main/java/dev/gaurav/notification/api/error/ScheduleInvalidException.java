package dev.gaurav.notification.api.error;

import java.util.List;

/**
 * {@code sendAt} was in the past, beyond the one-year horizon, or carried no UTC offset.
 *
 * <p>Kept distinct from {@link ProblemType#VALIDATION_FAILED} even though both are 400s, because
 * the caller's remedy is different: a schema error means "fix your serialiser", a schedule error
 * means "fix your clock or your timezone handling", and those get routed to different people.
 *
 * <p><strong>The missing-offset case is the one that earns the separate type.</strong>
 * {@code "2026-09-01T09:00:00"} parses cleanly in almost every language and means nothing: 9 a.m.
 * in the sender's zone, the recipient's zone, or the server's, and the server's is UTC in
 * production and Asia/Kolkata on the laptop where it was tested. Defaulting is how a quiet-hours
 * respecting platform sends at 3 a.m. Rejecting is the only safe reading.
 */
public class ScheduleInvalidException extends ApiException {

    private ScheduleInvalidException(String detail, String field, String code) {
        super(ProblemType.SCHEDULE_INVALID, detail, List.of(FieldViolation.of(field, code)));
    }

    public static ScheduleInvalidException missingOffset(String field, String value) {
        return new ScheduleInvalidException(
                "'%s' has no UTC offset. Send an RFC 3339 timestamp such as 2026-09-01T09:00:00+05:30."
                        .formatted(value),
                field, "MISSING_OFFSET");
    }

    public static ScheduleInvalidException inThePast(String field, String value) {
        return new ScheduleInvalidException("'%s' is in the past.".formatted(value), field, "SEND_AT_IN_PAST");
    }

    public static ScheduleInvalidException beyondHorizon(String field, String value) {
        return new ScheduleInvalidException(
                "'%s' is more than one year ahead; the schedule table is partitioned to that horizon."
                        .formatted(value),
                field, "BEYOND_HORIZON");
    }

    public static ScheduleInvalidException notScheduled(String field) {
        return new ScheduleInvalidException(
                "sendAt is required when schedule.type is SCHEDULED and must be absent otherwise.",
                field, "SEND_AT_REQUIRED");
    }
}
