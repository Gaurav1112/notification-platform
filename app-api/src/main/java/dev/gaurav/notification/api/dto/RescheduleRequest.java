package dev.gaurav.notification.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.Instant;

/**
 * Body of {@code PATCH /v1/notifications/{id}/schedule}.
 *
 * <p>Same offset rule as {@link ScheduleRequest#sendAt()} and for the same reason — see
 * {@link Rfc3339}. A reschedule is the request most likely to be issued by a human through a
 * console, which is exactly where an unzoned local time gets typed.
 *
 * <p>Worth knowing about the implementation on the other side: this is a {@code DELETE} plus an
 * {@code INSERT} on the schedule row, not an {@code UPDATE}. {@code send_at} is the partition key,
 * and PostgreSQL implements a partition-key update as a silent DELETE+INSERT at roughly triple the
 * WAL cost — that cost should be visible in the code rather than hidden behind an {@code UPDATE}
 * that looks free.
 */
public record RescheduleRequest(

        @NotBlank
        @Size(max = 64)
        String sendAt
) {

    /**
     * @param now injected so the past-check is testable
     * @throws dev.gaurav.notification.api.error.ScheduleInvalidException if unzoned, past or
     *                                                                    beyond the one-year horizon
     */
    public Instant resolveSendAt(Instant now) {
        return Rfc3339.requireWithinHorizon("sendAt", sendAt, now);
    }
}
