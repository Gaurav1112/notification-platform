package dev.gaurav.notification.application.fake;

import dev.gaurav.notification.application.port.DeliveryStatusWriter;
import dev.gaurav.notification.domain.enums.DeliveryStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * An in-memory monotonic guard, applying the same rule as the real {@code WHERE} clause:
 * only forward, never past a terminal status.
 *
 * <p>Implemented with {@link DeliveryStatus#transitionTo} rather than a rank comparison written out
 * again here, so the fake cannot drift from the state machine it is standing in for.
 */
public final class FakeDeliveryStatusWriter implements DeliveryStatusWriter {

    /** An event the guard refused, and why. */
    public record Unapplied(StatusTransition transition, String reason) {}

    private final Map<UUID, DeliveryStatus> current = new HashMap<>();
    private final List<Unapplied> unapplied = new ArrayList<>();

    /** Seeds the status a recipient is already at. */
    public FakeDeliveryStatusWriter startingAt(UUID recipientId, DeliveryStatus status) {
        current.put(recipientId, status);
        return this;
    }

    @Override
    public int applyStatus(StatusTransition transition) {
        var existing = current.get(transition.recipientId());
        if (existing == null) {
            current.put(transition.recipientId(), transition.proposed());
            return 1;
        }
        var next = existing.transitionTo(transition.proposed());
        if (next.isEmpty()) {
            return 0;
        }
        current.put(transition.recipientId(), next.get());
        return 1;
    }

    @Override
    public void recordUnappliedEvent(StatusTransition transition, String reason) {
        unapplied.add(new Unapplied(transition, reason));
    }

    public DeliveryStatus statusOf(UUID recipientId) {
        return current.get(recipientId);
    }

    public List<Unapplied> unapplied() {
        return List.copyOf(unapplied);
    }
}
