package dev.gaurav.notification.application.usecase;

import dev.gaurav.notification.application.exception.AlreadyDispatchedException;
import dev.gaurav.notification.application.exception.NotificationNotFoundException;
import dev.gaurav.notification.application.port.CancellationWriter;
import dev.gaurav.notification.application.port.DispatchTombstoneStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.Objects;
import java.util.UUID;

/**
 * {@code POST /v1/notifications/{id}/cancel}.
 *
 * <p>Cancellation is a race against a worker, and this use case is honest about which side won.
 * The durable half is one conditional {@code UPDATE} whose {@code WHERE} clause carries the rank
 * guard: {@code CANCELLED} is rank 28 and {@code QUEUED} is 30, so "you cannot cancel something
 * already queued" is enforced by ordering rather than by an {@code if} a future change can drop.
 * Both interleavings are safe — if the cancel commits first it is terminal and the racing
 * {@code QUEUED} is refused instead.
 *
 * <p>The Valkey tombstone written afterwards is a best-effort narrowing of the remaining window:
 * a worker that has already claimed the message re-checks it immediately before the provider call.
 * It cannot close the window, because the provider call cannot enlist in our transaction — which is
 * why the caller still gets {@code 409 already-dispatched} rather than a promise we cannot keep.
 *
 * <p>A tombstone write that fails is logged and swallowed. The cancellation is already durable; it
 * would be perverse to fail a committed cancel because a cache was slow.
 */
@Service
public class CancelNotificationUseCase {

    private static final Logger log = LoggerFactory.getLogger(CancelNotificationUseCase.class);

    private final CancellationWriter cancellationWriter;
    private final DispatchTombstoneStore tombstoneStore;
    private final Clock clock;

    public CancelNotificationUseCase(
            CancellationWriter cancellationWriter, DispatchTombstoneStore tombstoneStore, Clock clock) {
        this.cancellationWriter = Objects.requireNonNull(cancellationWriter, "cancellationWriter");
        this.tombstoneStore = Objects.requireNonNull(tombstoneStore, "tombstoneStore");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * @throws AlreadyDispatchedException    the rank guard refused: already claimed or dispatched
     * @throws NotificationNotFoundException absent, or owned by another tenant
     */
    public void cancel(String tenantId, UUID notificationId) {
        var outcome = cancellationWriter.cancel(tenantId, notificationId, clock.instant());
        switch (outcome) {
            case NOT_FOUND -> throw new NotificationNotFoundException(notificationId);
            case ALREADY_DISPATCHED -> throw new AlreadyDispatchedException(notificationId);
            case CANCELLED -> writeTombstone(tenantId, notificationId);
        }
    }

    private void writeTombstone(String tenantId, UUID notificationId) {
        try {
            tombstoneStore.tombstone(tenantId, notificationId);
        } catch (RuntimeException e) {
            log.warn("tombstone write failed for notification={}; cancellation is already durable",
                    notificationId, e);
        }
    }
}
