package dev.gaurav.notification.messaging.event;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.Priority;
import dev.gaurav.notification.domain.enums.ScheduleType;
import dev.gaurav.notification.domain.enums.TrafficClass;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * "We accepted this request and committed it." Produced from the transactional outbox, consumed by
 * the expander.
 *
 * <p>This event is a <em>pointer to committed state</em>, not the state itself. The row is already
 * in PostgreSQL when the sweeper publishes, so losing the message costs a re-publish, never a
 * request. That is the whole reason the accept path can answer 202 in single-digit milliseconds
 * without a dual write.
 *
 * <p><strong>Recipients are never inlined at campaign scale.</strong> A 10M-recipient blast would
 * be a ~600 MB Kafka record; the broker's default {@code max.message.bytes} is 1 MB and raising it
 * globally to accommodate one workload degrades every other topic's latency. Above
 * {@link #MAX_INLINE_RECIPIENTS} the list is written to S3 and only {@link #recipientRef} travels
 * — the Claim Check pattern.
 *
 * @param requestId       the {@code notification_request} primary key; the expander's anchor
 * @param requestCreatedAt partition key of that row — carried so the expander can prune to a single
 *                        daily partition instead of scanning all of them
 * @param idempotencyKey  the caller's {@code Idempotency-Key}, scoped by tenant. Nullable: it is
 *                        optional on the API, and the partition key falls back to the requestId
 * @param recipientRef    S3 URI of the recipient manifest, or {@code null} when inlined
 * @param recipientIds    inlined user references, empty when {@link #recipientRef} is set
 * @param recipientCount  the authoritative count in both cases, so the expander can report
 *                        progress without reading S3 first
 * @param scheduledAt     required when {@link #scheduleType} is {@code SCHEDULED}
 * @param expiresAt       hard TTL from {@link TrafficClass#defaultTtl()}; the expander drops the
 *                        request rather than fanning out a message that is already worthless
 */
public record NotificationRequestedEvent(
        UUID eventId,
        Instant occurredAt,
        long tenantId,
        String traceparent,
        UUID requestId,
        Instant requestCreatedAt,
        String idempotencyKey,
        Set<Channel> channels,
        TrafficClass trafficClass,
        Priority priority,
        ScheduleType scheduleType,
        Instant scheduledAt,
        Instant expiresAt,
        String templateCode,
        String templateLocale,
        Map<String, String> templateData,
        String recipientRef,
        List<String> recipientIds,
        int recipientCount) implements NotificationEvent {

    /** Above this the recipient list goes to S3. Chosen to stay far below a 1 MB record. */
    public static final int MAX_INLINE_RECIPIENTS = 500;

    public NotificationRequestedEvent {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(trafficClass, "trafficClass");
        Objects.requireNonNull(scheduleType, "scheduleType");
        Objects.requireNonNull(expiresAt, "expiresAt");
        channels = channels == null ? Set.of() : Set.copyOf(channels);
        templateData = templateData == null ? Map.of() : Map.copyOf(templateData);
        recipientIds = recipientIds == null ? List.of() : List.copyOf(recipientIds);
        priority = priority == null ? Priority.P2_NORMAL : priority;
        if (channels.isEmpty()) {
            throw new IllegalArgumentException("a request with no channel can never be delivered");
        }
        // A SCHEDULED request with no time is unschedulable and would sit in the hydrator forever.
        if (scheduleType == ScheduleType.SCHEDULED && scheduledAt == null) {
            throw new IllegalArgumentException("SCHEDULED requires scheduledAt");
        }
        // Both set means two sources of truth for the recipient list, and the expander would have
        // to guess which one wins. Neither set means nothing to send to.
        if ((recipientRef == null) == recipientIds.isEmpty()) {
            throw new IllegalArgumentException(
                    "exactly one of recipientRef (claim check) or recipientIds must be present");
        }
    }

    @Override
    public String eventType() {
        return TYPE_REQUESTED;
    }

    /** True when the recipient list lives in S3 rather than in this record. */
    public boolean isClaimCheck() {
        return recipientRef != null;
    }
}
