package dev.gaurav.notification.messaging.event;

import dev.gaurav.notification.domain.enums.FailureType;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * A record that failed three in-place attempts and has been parked, so its partition can move on.
 *
 * <p><strong>The poison-pill signature:</strong> offsets commit only after successful processing,
 * so a consumer that keeps throwing seeks back to the same offset forever. Throughput for that
 * partition is exactly zero while the consumer heartbeats, reports healthy and shows no error rate
 * at the group level. With {@code hash(tenant) % partitions}, a deterministic subset of tenants
 * receives nothing while every dashboard is green. Publishing this event and
 * <strong>committing the offset</strong> is the only thing that breaks that loop.
 *
 * <p>Everything needed to triage without the original topic is carried here, because by the time
 * anyone looks the source record may be past its 3-day retention.
 *
 * @param sourceTopic     where it came from, so replay knows the shape of the payload
 * @param sourcePartition original partition — for correlating with a lag alarm
 * @param sourceOffset    original offset — the coordinate for a manual seek if replay is rejected
 * @param sourceKey       the original partition key, so a replay lands on the same key
 * @param payload         the original record value, verbatim and unparsed. Kept as a raw string
 *                        because a schema-invalid message is exactly the case where re-parsing
 *                        into a typed record throws again
 * @param replayAttempts  how many times this has already been replayed. Every replayed message
 *                        carries this with a ceiling, because naive bulk replay of a deterministic
 *                        poison recreates the identical storm that produced it
 * @param failureType     classification where we managed to derive one; null for a payload we
 *                        could not even deserialise
 * @param tenantId        {@link NotificationEvent#UNKNOWN_TENANT} when the payload was unparseable
 */
public record DeadLetterEvent(
        UUID eventId,
        Instant occurredAt,
        long tenantId,
        String traceparent,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        String sourceKey,
        String consumerGroup,
        UUID notificationId,
        UUID recipientId,
        FailureType failureType,
        String exceptionClass,
        String exceptionMessage,
        String stackTrace,
        int attemptCount,
        int replayAttempts,
        String payload) implements NotificationEvent {

    /** A message that has been replayed this many times is a bug report, not a retry candidate. */
    public static final int MAX_REPLAY_ATTEMPTS = 3;

    public DeadLetterEvent {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(sourceTopic, "sourceTopic");
        if (sourceOffset < 0) {
            throw new IllegalArgumentException("sourceOffset must be non-negative");
        }
    }

    /** False once the ceiling is reached — replaying past it recreates the original storm. */
    public boolean isReplayable() {
        return replayAttempts < MAX_REPLAY_ATTEMPTS;
    }

    @Override
    public String eventType() {
        return TYPE_DEAD_LETTER;
    }
}
