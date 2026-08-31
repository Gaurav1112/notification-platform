package dev.gaurav.notification.application.result;

import dev.gaurav.notification.domain.enums.AttemptState;
import dev.gaurav.notification.domain.enums.FailureType;

import java.time.Instant;
import java.util.Objects;

/**
 * One row of {@code GET /v1/notifications/{id}/attempts} — what we tried, against whom, and what
 * came back.
 *
 * <p>This endpoint exists because "why didn't my customer get the SMS?" is unanswerable from a
 * status field alone. The attempt history shows the failover: primary timed out at 5001 ms,
 * secondary accepted in 212 ms. Without it, support escalates to engineering and engineering reads
 * logs.
 *
 * <p>{@link AttemptState#UNKNOWN} is a first-class, documented outcome, not a bug in the read
 * model. The provider timed out <em>after</em> we handed over the message, so it may or may not
 * have been delivered; it resolves later by webhook or reconciliation. Rendering it as
 * {@code FAILED} would be a lie that invites a blind retry and a duplicate OTP.
 *
 * @param providerCode      which provider, so a bad one is visible per-message and not only in a
 *                          dashboard
 * @param latencyMs         {@code null} while the attempt is still {@code PENDING}
 * @param costMicros        {@code null} unless the provider reported it
 * @param retryScheduledAt  when the next attempt is due; {@code null} if there will not be one
 */
public record DeliveryAttemptView(
        int attemptNumber,
        String providerCode,
        AttemptState state,
        FailureType failureType,
        String errorCode,
        String providerMessageId,
        Integer latencyMs,
        Long costMicros,
        Instant startedAt,
        Instant respondedAt,
        Instant retryScheduledAt) {

    public DeliveryAttemptView {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(startedAt, "startedAt");
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber is 1-based, got " + attemptNumber);
        }
        if (latencyMs != null && latencyMs < 0) {
            throw new IllegalArgumentException("negative latency means the clocks disagree");
        }
    }

    /**
     * True when this attempt tells us nothing about whether the message arrived.
     *
     * <p>Surfaced as a method so callers branch on the meaning rather than on
     * {@code state == UNKNOWN} at four different call sites, one of which will eventually be
     * written as {@code state != SUCCEEDED} and start retrying indeterminate sends.
     */
    public boolean isIndeterminate() {
        return state == AttemptState.UNKNOWN
                || (failureType != null && failureType.isOutcomeIndeterminate());
    }
}
