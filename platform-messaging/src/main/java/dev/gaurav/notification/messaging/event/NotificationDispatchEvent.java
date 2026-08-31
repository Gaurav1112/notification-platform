package dev.gaurav.notification.messaging.event;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.Priority;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.messaging.topic.Topics;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * "Send this one message to this one recipient." The unit of work for a channel worker.
 *
 * <p>One event per (recipient, channel), which is what makes the FIFO contract —
 * {@code (tenantId, recipientId, channel)} — expressible as a Kafka key.
 *
 * <p><strong>The address travels as ciphertext, never plaintext.</strong> Broker-level KMS
 * encryption protects the log files on disk; it does nothing for the 3-day retention window during
 * which anyone with topic read access, any mirroring tool, and every DLQ dump can read the record.
 * The address is AES-GCM encrypted under a per-user DEK, so a Kafka read grants nothing, and GDPR
 * erasure is a key destruction rather than a topic rewrite (which Kafka cannot do). Logging and
 * alerting use {@link #addressHint} — {@code g***@example.com} — which is the only form safe to
 * put in a log line.
 *
 * @param notificationCreatedAt the partition key of the {@code notification} row. Carried so the
 *                              worker's status UPDATE prunes to one daily partition; without it
 *                              every status write scans every partition
 * @param addressCipher  Base64 AES-GCM ciphertext of the destination (E.164, email, device token)
 * @param dekRef         reference to the per-user data encryption key, resolved at send time
 * @param bodyCipher     Base64 ciphertext of the rendered body, or {@code null} when the body is
 *                       too large and lives in S3
 * @param bodyRef        S3 URI of the rendered body (Claim Check), or {@code null} when inlined
 * @param idempotencyToken the token recorded on the attempt row before the network call, and sent
 *                       to the provider where one is supported. Stable across retries of the same
 *                       logical send — regenerating it per attempt would defeat both layers
 * @param attemptNumber  1-based. Drives retry-tier selection and the attempt-row unique constraint
 * @param expiresAt      the worker drops the message rather than sending it late. A 40-minute-old
 *                       OTP is worse than no OTP: the user has already requested another
 */
public record NotificationDispatchEvent(
        UUID eventId,
        Instant occurredAt,
        long tenantId,
        String traceparent,
        UUID notificationId,
        Instant notificationCreatedAt,
        UUID recipientId,
        UUID requestId,
        Channel channel,
        TrafficClass trafficClass,
        Priority priority,
        String addressCipher,
        String addressHint,
        String dekRef,
        String subject,
        String bodyCipher,
        String bodyRef,
        String templateCode,
        String idempotencyToken,
        String preferredProvider,
        int attemptNumber,
        Instant expiresAt,
        Map<String, String> attributes) implements NotificationEvent {

    public NotificationDispatchEvent {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(notificationId, "notificationId");
        Objects.requireNonNull(recipientId, "recipientId");
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(trafficClass, "trafficClass");
        Objects.requireNonNull(addressCipher, "addressCipher");
        Objects.requireNonNull(idempotencyToken, "idempotencyToken");
        Objects.requireNonNull(expiresAt, "expiresAt");
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        priority = priority == null ? Priority.P2_NORMAL : priority;
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber is 1-based, got " + attemptNumber);
        }
        // Two bodies means the worker has to pick one, and the two will eventually disagree.
        if (bodyCipher != null && bodyRef != null) {
            throw new IllegalArgumentException("body is either inline or a claim check, not both");
        }
    }

    /** {@code notification.dispatch.sms.tx} — the lane this message belongs on. */
    public String topic() {
        return Topics.dispatchTopic(channel, trafficClass);
    }

    /** True when the TTL has elapsed and the worker must drop rather than send. */
    public boolean isExpiredAt(Instant now) {
        return !now.isBefore(expiresAt);
    }

    @Override
    public String eventType() {
        return TYPE_DISPATCH;
    }
}
