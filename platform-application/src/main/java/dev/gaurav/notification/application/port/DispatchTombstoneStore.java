package dev.gaurav.notification.application.port;

import java.util.UUID;

/**
 * A best-effort "do not send this" marker that workers check immediately before the provider call.
 *
 * <p>It exists to shrink, not close, the cancel race. Between a worker claiming a message and
 * calling the provider there is a window measured in tens of milliseconds during which a cancel can
 * still land; a Valkey read at the last possible moment catches most of those. It cannot catch all
 * of them, because the provider call cannot enlist in our transaction — which is why the API still
 * answers {@code 409 already-dispatched} rather than pretending the cancel was guaranteed.
 *
 * <p>Deliberately a separate port from {@link CancellationWriter}. One writes the system of record,
 * the other writes a cache; folding them together would invite an implementation that treats a
 * Valkey timeout as a failed cancellation, when the durable {@code CANCELLED} row has already made
 * the cancel real.
 */
public interface DispatchTombstoneStore {

    /**
     * Marks a notification as cancelled for the workers.
     *
     * <p>Implementations must set a TTL — an unbounded tombstone set grows by the cancellation rate
     * forever, and nothing ever reads an entry older than the notification's own TTL.
     *
     * @throws RuntimeException on a cache fault; the caller logs and continues, because the
     *                          durable cancellation has already committed
     */
    void tombstone(String tenantId, UUID notificationId);
}
