package dev.gaurav.notification.application.port;

import dev.gaurav.notification.application.result.DeliveryAttemptView;
import dev.gaurav.notification.application.result.NotificationStatusView;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * The read side. Projections only — nothing here returns an entity or an open persistence context.
 *
 * <p>Every method takes {@code tenantId} and every implementation must put it in the
 * {@code WHERE} clause rather than checking it after the fetch. A post-fetch check is a code path
 * away from being forgotten, and the failure mode is one tenant reading another's traffic. Absence
 * and cross-tenant access are the same answer — an empty {@link Optional}, rendered as {@code 404}
 * — so the endpoint cannot be used to test whether an id exists.
 */
public interface NotificationQuery {

    /** Aggregate status, from the denormalised counters rather than a scan over recipients. */
    Optional<NotificationStatusView> findStatus(String tenantId, UUID notificationId);

    /**
     * Attempt history, newest first.
     *
     * @param limit hard-capped by the caller. Unbounded here would let one poisoned notification
     *              with 64 attempts across 40 recipients return a page nobody can render
     */
    List<DeliveryAttemptView> findAttempts(String tenantId, UUID notificationId, int limit);
}
