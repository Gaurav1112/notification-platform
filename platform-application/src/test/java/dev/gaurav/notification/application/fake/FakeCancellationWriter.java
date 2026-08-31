package dev.gaurav.notification.application.fake;

import dev.gaurav.notification.application.port.CancellationWriter;

import java.time.Instant;
import java.util.UUID;

/** Returns whichever of the three real outcomes of the conditional {@code UPDATE} a test needs. */
public final class FakeCancellationWriter implements CancellationWriter {

    private CancelOutcome outcome = CancelOutcome.CANCELLED;
    private int calls;

    public static FakeCancellationWriter cancelling() {
        return new FakeCancellationWriter();
    }

    /** Zero rows: the rank guard refused because the notification is already in flight. */
    public FakeCancellationWriter alreadyDispatched() {
        this.outcome = CancelOutcome.ALREADY_DISPATCHED;
        return this;
    }

    public FakeCancellationWriter notFound() {
        this.outcome = CancelOutcome.NOT_FOUND;
        return this;
    }

    @Override
    public CancelOutcome cancel(String tenantId, UUID notificationId, Instant now) {
        calls++;
        return outcome;
    }

    public int calls() {
        return calls;
    }
}
