package dev.gaurav.notification.application.port;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.ScheduleType;
import dev.gaurav.notification.domain.enums.TrafficClass;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Publishes the fast-path notice that state already committed to PostgreSQL is ready to be worked
 * on. The adapter maps each notice onto the corresponding {@code platform-messaging} event.
 *
 * <p><strong>This is a hint, not a hand-off.</strong> The row is durable before any method here is
 * called, so a failed publish costs latency — the outbox sweeper picks it up within ~2 s — and
 * never a message. Callers therefore treat every method as best-effort and must not let a broker
 * outage turn a committed accept into a {@code 500}; the client would retry with the same key and
 * get a replay of a response we never sent.
 *
 * <p>No topic and no partition key appears in this interface. Both are decisions that belong next
 * to their reasoning in {@code NotificationEventPublisher}: a topic typed at a call site becomes a
 * topic nobody consumes, and a key chosen at a call site is how one path drops {@code recipientId}
 * and builds a hot partition.
 */
public interface EventPublisher {

    /** "A request was accepted." Consumed by the expander. */
    void publishRequested(RequestedNotice notice);

    /** "Send this one message to this one recipient." Consumed by a channel worker. */
    void publishDispatch(DispatchNotice notice);

    /** "This recipient's delivery state moved." Consumed by the status processor. */
    void publishStatus(StatusNotice notice);

    /**
     * @param requestCreatedAt the partition key of the {@code notification_request} row, carried so
     *                         the expander prunes to one daily partition instead of scanning all of
     *                         them
     * @param idempotencyKey   nullable; the partition key falls back to {@code requestId}
     * @param expiresAt        hard TTL, so the expander can drop a request that is already
     *                         worthless rather than fan it out
     */
    record RequestedNotice(
            String tenantId,
            UUID requestId,
            Instant requestCreatedAt,
            String idempotencyKey,
            Set<Channel> channels,
            TrafficClass trafficClass,
            ScheduleType scheduleType,
            Instant scheduledAt,
            Instant expiresAt,
            int recipientCount,
            String traceparent) {

        public RequestedNotice {
            Objects.requireNonNull(tenantId, "tenantId");
            Objects.requireNonNull(requestId, "requestId");
            Objects.requireNonNull(requestCreatedAt, "requestCreatedAt");
            Objects.requireNonNull(trafficClass, "trafficClass");
            Objects.requireNonNull(scheduleType, "scheduleType");
            Objects.requireNonNull(expiresAt, "expiresAt");
            channels = channels == null ? Set.of() : Set.copyOf(channels);
            if (channels.isEmpty()) {
                throw new IllegalArgumentException("a request with no channel can never be delivered");
            }
        }
    }

    /**
     * @param notificationCreatedAt partition key of the {@code notification} row; without it every
     *                              status write from the worker scans every daily partition
     * @param attemptNumber         1-based, and stable across a redelivery of the same logical send
     */
    record DispatchNotice(
            String tenantId,
            UUID notificationId,
            Instant notificationCreatedAt,
            UUID recipientId,
            Channel channel,
            TrafficClass trafficClass,
            int attemptNumber,
            Instant expiresAt,
            String traceparent) {

        public DispatchNotice {
            Objects.requireNonNull(tenantId, "tenantId");
            Objects.requireNonNull(notificationId, "notificationId");
            Objects.requireNonNull(recipientId, "recipientId");
            Objects.requireNonNull(channel, "channel");
            Objects.requireNonNull(trafficClass, "trafficClass");
            Objects.requireNonNull(expiresAt, "expiresAt");
            if (attemptNumber < 1) {
                throw new IllegalArgumentException("attemptNumber is 1-based, got " + attemptNumber);
            }
        }
    }

    /**
     * @param occurredAt when the provider says it happened, not when we read it. Ordering by
     *                   receipt time is wrong: webhooks routinely arrive out of order
     */
    record StatusNotice(
            String tenantId,
            UUID notificationId,
            UUID recipientId,
            DeliveryStatus status,
            Instant occurredAt,
            String providerCode) {

        public StatusNotice {
            Objects.requireNonNull(tenantId, "tenantId");
            Objects.requireNonNull(notificationId, "notificationId");
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(occurredAt, "occurredAt");
        }
    }
}
