package dev.gaurav.notification.adapter.persistence;

import dev.gaurav.notification.application.port.CancellationWriter;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.persistence.entity.NotificationEntity;
import dev.gaurav.notification.persistence.repository.NotificationRepository;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Bridges {@link CancellationWriter} onto the monotonic guard in {@link NotificationRepository}.
 *
 * <p>The seam exists so the cancel use case can distinguish the three outcomes it must report —
 * cancelled, already dispatched, not found — without knowing that "already dispatched" is expressed
 * as {@code status_rank < 28} in a {@code WHERE} clause. The rank ordering is what makes the answer
 * trustworthy: {@code CANCELLED} is 28 and {@code QUEUED} is 30, so a cancel racing a worker's
 * claim is decided by the database in one statement, and both interleavings are safe.
 *
 * <p><strong>Two statements, and the first one is not a check-then-act.</strong> The lookup exists
 * only to learn the row's {@code created_at}, which is the partition key the conditional
 * {@code UPDATE} needs in order to prune; the cancel decision itself is made entirely by that
 * {@code UPDATE}. If a worker claims the row between the two statements the update simply returns
 * zero and the caller is told {@code ALREADY_DISPATCHED}, which is the truth.
 *
 * <p>A cross-tenant id is indistinguishable from an unknown one because the tenant is a predicate
 * in both statements, not a check on a result. The lookup narrows by tenant and so does the
 * {@code UPDATE}: scoping only the lookup would leave the write addressable by id alone, and the
 * write is the irreversible half — a cancel that lands on another tenant's notification cannot be
 * undone, because the guard is monotonic and {@code CANCELLED} outranks what came before it.
 *
 * <p>Note that one request fans out to one {@code notification} per channel, so cancelling the
 * envelope means cancelling each of them. This port is addressed by notification id, so it cancels
 * exactly the one row the caller named.
 */
@Component
public class JpaCancellationWriter implements CancellationWriter {

    private final NotificationRepository notifications;
    private final TenantDirectory tenants;

    public JpaCancellationWriter(NotificationRepository notifications, TenantDirectory tenants) {
        this.notifications = Objects.requireNonNull(notifications, "notifications");
        this.tenants = Objects.requireNonNull(tenants, "tenants");
    }

    @Override
    public CancelOutcome cancel(String tenantId, UUID notificationId, Instant now) {
        var tenant = tenants.internalIdOf(tenantId);
        if (tenant.isEmpty()) {
            return CancelOutcome.NOT_FOUND;
        }
        var found = notifications.findInWindow(notificationId, tenant.get(),
                PartitionWindows.lookbackFrom(now), PartitionWindows.lookbackTo(now));
        if (found.isEmpty()) {
            return CancelOutcome.NOT_FOUND;
        }
        return applyCancel(found.get(), tenant.get(), now);
    }

    private CancelOutcome applyCancel(NotificationEntity notification, long tenant, Instant now) {
        int updated = notifications.applyStatusTransition(
                notification.getId(),
                tenant,
                PartitionWindows.dayStart(notification.getCreatedAt()),
                PartitionWindows.dayEnd(notification.getCreatedAt()),
                DeliveryStatus.CANCELLED.name(),
                (short) DeliveryStatus.CANCELLED.rank(),
                now);
        // Zero rows is not an error here: the guard refused because the notification is already
        // in flight or already terminal, which is exactly the 409 the caller must be told about.
        return updated > 0 ? CancelOutcome.CANCELLED : CancelOutcome.ALREADY_DISPATCHED;
    }
}
