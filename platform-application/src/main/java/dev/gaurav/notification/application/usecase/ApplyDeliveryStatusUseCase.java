package dev.gaurav.notification.application.usecase;

import dev.gaurav.notification.application.port.DeliveryStatusWriter;
import dev.gaurav.notification.application.port.DeliveryStatusWriter.StatusTransition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Objects;

/**
 * Applies one provider-reported state change through the monotonic guard.
 *
 * <p>Every event on this path is at-least-once and out-of-order by construction: Kafka redelivers
 * after a rebalance, providers retry webhooks they think we missed, and a {@code SENT} confirmation
 * routinely arrives after the {@code DELIVERED} receipt it precedes. The guard is a single
 * conditional {@code UPDATE} that only moves a recipient forward and never past a terminal state,
 * so all three cases collapse into "zero rows updated".
 *
 * <p><strong>Zero rows is not a failure.</strong> It is the correct, expected outcome, and treating
 * it as an error is how a duplicate webhook becomes a DLQ entry and an on-call page. The event is
 * appended to {@code notification_event} with {@code applied = false} instead — those rows are the
 * most useful debugging artefact in the system, because they are the difference between "the bounce
 * never reached us" and "it reached us and we correctly ignored it as stale".
 *
 * <p>Deliberately no in-memory pre-check against the current status before the write. Reading the
 * row first and deciding in Java re-introduces the race the {@code WHERE} clause exists to remove:
 * two status processors would both read {@code SENT}, both decide {@code DELIVERED} is legal, and
 * both write. The database is the only place the check and the write can be one operation.
 */
@Service
public class ApplyDeliveryStatusUseCase {

    private static final Logger log = LoggerFactory.getLogger(ApplyDeliveryStatusUseCase.class);

    /** The guard refused because a higher-ranked status is already recorded, or it is terminal. */
    static final String REASON_NOT_MONOTONIC = "NOT_MONOTONIC_OR_TERMINAL";

    private final DeliveryStatusWriter deliveryStatusWriter;

    public ApplyDeliveryStatusUseCase(DeliveryStatusWriter deliveryStatusWriter) {
        this.deliveryStatusWriter = Objects.requireNonNull(deliveryStatusWriter, "deliveryStatusWriter");
    }

    /**
     * @return {@code true} when the transition moved the recipient forward, {@code false} when the
     *         guard refused it. Callers commit their Kafka offset either way — a refused event has
     *         been fully accounted for, and re-consuming it would refuse it again forever
     */
    public boolean apply(StatusTransition transition) {
        int updated = deliveryStatusWriter.applyStatus(transition);
        if (updated > 0) {
            return true;
        }
        deliveryStatusWriter.recordUnappliedEvent(transition, REASON_NOT_MONOTONIC);
        log.debug("dropped stale or duplicate status recipient={} proposed={} occurredAt={}",
                transition.recipientId(), transition.proposed(), transition.occurredAt());
        return false;
    }
}
