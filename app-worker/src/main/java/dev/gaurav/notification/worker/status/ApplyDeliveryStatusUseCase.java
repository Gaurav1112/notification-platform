package dev.gaurav.notification.worker.status;

import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.persistence.entity.NotificationEvent.EventSource;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Applies one observation about the fate of one message, and reports whether it stuck.
 *
 * <p>Every status change in the platform goes through here — the worker's own {@code SENDING} and
 * {@code SENT}, the provider's {@code DELIVERED} webhook, the reconciler's late verdict — because
 * they all face the same problem: <strong>observations arrive out of order and more than once,
 * always</strong>. A {@code DELIVERED} webhook routinely beats our own {@code SENT} write, because
 * the provider's callback path is shorter than our database commit. Providers re-fire callbacks on
 * any non-2xx. Kafka replays on rebalance.
 *
 * <p>So this is a command that may legitimately do nothing, and
 * {@link Result#applied()} {@code == false} is a normal outcome rather than an error. Modelling it
 * as a return value instead of an exception is what lets consumers be at-least-once, webhooks be
 * freely retried, and DLQ replay be harmless.
 *
 * <p>A port living in the worker because {@code platform-application} does not exist yet.
 * TODO(phase-9): move the interface there, unchanged, once it does; the implementation is already
 * free of Kafka and of the worker's own types.
 */
public interface ApplyDeliveryStatusUseCase {

    /**
     * One observation.
     *
     * @param notificationCreatedAt partition bound. Without it the monotonic {@code UPDATE} scans
     *                              every daily partition instead of probing one index
     * @param status                the proposed new status; the guard decides whether it wins
     * @param occurredAt            the <em>provider's</em> event time where available. Ordering by
     *                              the Kafka produce time collapses every replayed event to "now"
     *                              and lets a stale status overwrite a fresh one
     * @param dedupSeed             stable identity of this signal. Hashed into
     *                              {@code notification_event.dedup_hash}, which carries a unique
     *                              index, so a provider retrying its callback five times yields
     *                              one row
     * @param providerId            the {@code notif.provider} surrogate key, or null when the
     *                              observation did not come from a provider
     */
    record Command(long tenantId,
                   UUID notificationId,
                   Instant notificationCreatedAt,
                   UUID recipientId,
                   DeliveryStatus status,
                   Instant occurredAt,
                   EventSource source,
                   Short providerId,
                   String dedupSeed) {

        public Command {
            Objects.requireNonNull(notificationId, "notificationId");
            Objects.requireNonNull(notificationCreatedAt, "notificationCreatedAt");
            Objects.requireNonNull(recipientId, "recipientId");
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(occurredAt, "occurredAt");
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(dedupSeed, "dedupSeed");
        }
    }

    /**
     * @param applied   true when the guard accepted the transition
     * @param duplicate true when this exact signal had already been recorded. Distinguished from a
     *                  merely stale event because they mean different things operationally: a
     *                  duplicate is a provider retrying its webhook, a stale one is reordering.
     *                  Collapsing them hides a provider that has started double-firing everything
     */
    record Result(boolean applied, boolean duplicate) {

        /** The guard moved the row. */
        public static Result accepted() {
            return new Result(true, false);
        }

        /** The guard refused it as stale or illegal — expected traffic, not an error. */
        public static Result stale() {
            return new Result(false, false);
        }

        /** This exact signal was already recorded; the provider re-fired its callback. */
        public static Result alreadySeen() {
            return new Result(false, true);
        }
    }

    Result apply(Command command);
}
