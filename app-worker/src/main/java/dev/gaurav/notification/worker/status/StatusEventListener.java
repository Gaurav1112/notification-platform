package dev.gaurav.notification.worker.status;

import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.domain.enums.SuppressionReason;
import dev.gaurav.notification.messaging.consumer.IdempotentConsumer;
import dev.gaurav.notification.messaging.event.DeliveryStatusEvent;
import dev.gaurav.notification.messaging.topic.Topics;
import dev.gaurav.notification.persistence.entity.NotificationEvent.EventSource;
import dev.gaurav.notification.worker.config.WorkerListenerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Projects provider callbacks and reconciler verdicts onto the notification and recipient rows.
 *
 * <p>Subscribes to {@code notification.status}, which is <strong>compacted</strong>. Compaction
 * keeps the record with the highest offset per key, <em>not</em> the one with the latest state — a
 * late {@code SENT} produced after {@code DELIVERED} survives compaction untouched. Nothing about
 * the topic makes the projection correct; the monotonic guard does, and the compaction is only a
 * storage optimisation. Any consumer that trusts the topic instead will regress statuses.
 *
 * <p><strong>Zero rows affected is not an error.</strong> It means the observation was stale,
 * duplicated or illegal, which at volume is ordinary traffic: a provider re-fires its webhook on
 * any non-2xx, and a {@code DELIVERED} callback routinely overtakes our own {@code SENT} write. The
 * event is still recorded, with {@code applied = false}, and a counter moves. That pair — record it,
 * count it, do not raise — is what lets the consumer be at-least-once and webhook replay be
 * harmless. An exception here would instead retry the record three times and dead-letter a
 * perfectly normal duplicate.
 *
 * <p>The second job is the one that protects sender reputation: a hard bounce, a complaint or a
 * {@code STOP} must reach the suppression list. These signals are asynchronous and arrive on a
 * different path from the send, so if they are only applied to the row for that one message the
 * next campaign mails the same dead address again.
 */
@Component
public class StatusEventListener {

    /** Scoped per group: several groups read this topic and each must apply the event once. */
    public static final String CONSUMER_GROUP = "notification-worker.status";

    private static final Logger log = LoggerFactory.getLogger(StatusEventListener.class);

    private final IdempotentConsumer idempotentConsumer;
    private final ApplyDeliveryStatusUseCase applyStatus;
    private final SuppressionWriter suppressions;

    public StatusEventListener(IdempotentConsumer idempotentConsumer,
                               ApplyDeliveryStatusUseCase applyStatus,
                               SuppressionWriter suppressions) {
        this.idempotentConsumer = idempotentConsumer;
        this.applyStatus = applyStatus;
        this.suppressions = suppressions;
    }

    @KafkaListener(
            id = "status-projector",
            topics = Topics.STATUS,
            groupId = CONSUMER_GROUP,
            containerFactory = WorkerListenerConfiguration.TRANSACTIONAL_FACTORY)
    public void onStatus(DeliveryStatusEvent event, Acknowledgment ack) {
        if (!idempotentConsumer.isFirstSighting(CONSUMER_GROUP, event.eventId())) {
            ack.acknowledge();
            return;
        }
        try {
            var result = applyStatus.apply(new ApplyDeliveryStatusUseCase.Command(
                    event.tenantId(),
                    event.notificationId(),
                    event.notificationCreatedAt(),
                    event.recipientId(),
                    event.status(),
                    event.occurredAt(),
                    sourceOf(event),
                    null,
                    dedupSeed(event)));

            // Suppression is applied on the signal, not on whether the transition won. A duplicate
            // BOUNCED whose transition was rejected still means the address is dead, and skipping
            // it here because "we already knew" is how an address that bounced during a rebalance
            // never reaches the list at all.
            suppressionReasonFor(event).ifPresent(reason ->
                    suppressions.suppress(event.tenantId(), event.recipientId(), event.channel(), reason));

            if (!result.applied()) {
                log.debug("status {} for recipient {} not applied (duplicate={})",
                        event.status(), event.recipientId(), result.duplicate());
            }
            ack.acknowledge();
        } catch (RuntimeException e) {
            idempotentConsumer.forget(CONSUMER_GROUP, event.eventId());
            throw e;
        }
    }

    /**
     * Which suppressions this observation implies.
     *
     * <p>Driven off {@link FailureType#shouldSuppressAddress()} where a failure type is present, so
     * the classification table stays the only place that knows, and off the terminal engagement
     * statuses otherwise — a bounce or a complaint carries no failure type, it <em>is</em> the
     * signal.
     */
    private static Optional<SuppressionReason> suppressionReasonFor(DeliveryStatusEvent event) {
        if (event.status() == DeliveryStatus.BOUNCED) {
            return Optional.of(SuppressionReason.HARD_BOUNCE);
        }
        if (event.status() == DeliveryStatus.COMPLAINED) {
            return Optional.of(SuppressionReason.SPAM_COMPLAINT);
        }
        var failure = event.failureType();
        if (failure == null || !failure.shouldSuppressAddress()) {
            return Optional.empty();
        }
        return Optional.of(switch (failure) {
            case UNSUBSCRIBED -> SuppressionReason.USER_OPTED_OUT;
            case DEVICE_UNREGISTERED, INVALID_RECIPIENT -> SuppressionReason.INVALID_ADDRESS;
            // CONTENT_REJECTED and anything added later: the address itself is not proven bad, but
            // the classification says stop sending to it. Recorded generically rather than
            // guessing a reason the tenant would then see on a report.
            default -> SuppressionReason.HARD_BOUNCE;
        });
    }

    /**
     * Where the observation came from, which is what makes the event log triageable.
     *
     * <p>{@code providerCode} present means a vendor callback; absent means we produced it — the
     * reconciler or a worker. Losing that distinction turns "the provider never told us" and "we
     * never asked" into the same row.
     */
    private static EventSource sourceOf(DeliveryStatusEvent event) {
        return event.providerCode() == null ? EventSource.SYSTEM : EventSource.PROVIDER;
    }

    /**
     * The identity of the signal, not of the message.
     *
     * <p>Includes the version and the event time, so a provider re-firing the identical callback
     * collapses to one row while a genuinely new observation about the same recipient does not.
     */
    private static String dedupSeed(DeliveryStatusEvent event) {
        return String.join("|",
                event.recipientId().toString(),
                event.status().name(),
                Long.toString(event.version()),
                event.occurredAt().toString(),
                String.valueOf(event.providerMessageId()));
    }
}
