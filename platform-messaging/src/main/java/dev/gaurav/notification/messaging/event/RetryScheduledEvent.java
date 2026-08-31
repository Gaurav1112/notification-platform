package dev.gaurav.notification.messaging.event;

import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.messaging.topic.RetryTier;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * A dispatch that failed transiently and is parked on a delay tier until {@link #notBefore}.
 *
 * <p>The whole original {@link NotificationDispatchEvent} is carried, not a pointer to it. A
 * pointer would force the tier consumer to read PostgreSQL for every parked message, which turns
 * the one consumer in the system with <em>no external I/O</em> into one with a database dependency
 * — and that is precisely what makes it safe to share the tiers across all three channels.
 *
 * <p>The nested dispatch event keeps its original {@code eventId}. The retry is the same logical
 * send, so the idempotent receiver downstream must still recognise it; a fresh id there would let
 * a replayed retry produce a second SMS.
 *
 * @param tier          which delay tier this is parked on; also names the topic
 * @param notBefore     {@code occurredAt + tier.delay()}. The consumer pauses the partition until
 *                      this instant rather than sleeping the thread — a sleep would blow through
 *                      {@code max.poll.interval.ms} and trigger a rebalance storm
 * @param attemptNumber 1-based count of provider attempts already made
 * @param failureType   why the previous attempt failed. Carried so the tier consumer can abandon a
 *                      message whose classification turned permanent between tiers, instead of
 *                      burning the remaining budget on a number that will never accept
 * @param dispatch      the work to redo, verbatim
 */
public record RetryScheduledEvent(
        UUID eventId,
        Instant occurredAt,
        long tenantId,
        String traceparent,
        RetryTier tier,
        Instant notBefore,
        int attemptNumber,
        FailureType failureType,
        NotificationDispatchEvent dispatch) implements NotificationEvent {

    public RetryScheduledEvent {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(tier, "tier");
        Objects.requireNonNull(notBefore, "notBefore");
        Objects.requireNonNull(failureType, "failureType");
        Objects.requireNonNull(dispatch, "dispatch");
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber is 1-based, got " + attemptNumber);
        }
        // Scheduling a retry for a permanent failure spends the whole 72-minute budget on a
        // message that was dead on the first attempt.
        if (!failureType.isRetryable()) {
            throw new IllegalArgumentException(failureType + " is not retryable; dead-letter it");
        }
    }

    /** True when the tier delay has elapsed and the message may be republished. */
    public boolean isDueAt(Instant now) {
        return !now.isBefore(notBefore);
    }

    /**
     * True when the retry would land after the message's own TTL.
     *
     * <p>Checked before republishing, because a retry that completes after the notification has
     * expired costs a provider call and delivers a message the user no longer wants.
     */
    public boolean outlivesDeadline() {
        return notBefore.isAfter(dispatch.expiresAt());
    }

    @Override
    public String eventType() {
        return TYPE_RETRY_SCHEDULED;
    }
}
