package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.persistence.entity.NotificationEvent;
import jakarta.persistence.QueryHint;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

/**
 * Reads over the append-only event log. Partitioned by {@code occurred_at}, so the window
 * parameters are named after that column — it is the <em>provider's</em> clock, not ours, and a
 * caller who passes our {@code recorded_at} window will miss events from a skewed vendor.
 */
public interface NotificationEventRepository extends JpaRepository<NotificationEvent, UUID> {

    /**
     * The full signal history for one notification. Served by {@code ne_notification_ix}.
     *
     * <p>Tenant-scoped: these rows carry provider codes, error details and the raw attribute blob
     * from a vendor callback. A notification id is a UUID the caller supplies, so without the
     * predicate one guessed id returns another tenant's forensic trail.
     */
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    @Query("""
            SELECT e FROM NotificationEvent e
             WHERE e.notificationId = :notificationId
               AND e.tenantId = :tenantId
               AND e.occurredAt >= :occurredAtFrom
               AND e.occurredAt <  :occurredAtTo
             ORDER BY e.occurredAt DESC
            """)
    List<NotificationEvent> findByNotification(@Param("notificationId") UUID notificationId,
                                               @Param("tenantId") Long tenantId,
                                               @Param("occurredAtFrom") Instant occurredAtFrom,
                                               @Param("occurredAtTo") Instant occurredAtTo);

    /**
     * The events the monotonic guard rejected.
     *
     * <p>The single most useful query in the system when a notification looks stuck. Everything
     * else in the platform can only tell you what the current status is; this tells you what
     * later signals arrived and were correctly discarded, which is the difference between "we
     * never heard back" and "we heard back three times and every one was stale".
     */
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    @Query("""
            SELECT e FROM NotificationEvent e
             WHERE e.notificationId = :notificationId
               AND e.tenantId = :tenantId
               AND e.applied = false
               AND e.occurredAt >= :occurredAtFrom
               AND e.occurredAt <  :occurredAtTo
             ORDER BY e.occurredAt DESC
            """)
    List<NotificationEvent> findUnappliedByNotification(@Param("notificationId") UUID notificationId,
                                                        @Param("tenantId") Long tenantId,
                                                        @Param("occurredAtFrom") Instant occurredAtFrom,
                                                        @Param("occurredAtTo") Instant occurredAtTo);

    /**
     * Whether this exact signal has been seen before.
     *
     * <p>Advisory only. The unique index on {@code (dedup_hash, occurred_at)} is the real
     * defence — this check races by construction, and code that treats a {@code false} here as
     * permission to insert without handling the constraint violation will drop events under
     * concurrent webhook delivery.
     *
     * <p><strong>Deliberately not tenant-scoped, and this is the interesting one.</strong>
     * {@code ne_dedup_uk} is on {@code (dedup_hash, occurred_at)} with no tenant column, so the
     * constraint this probe exists to stay ahead of is global. Narrowing the probe to one tenant
     * while the index stays global would make the two disagree: the probe answers "no such event",
     * the caller inserts, and the global index rejects it — turning a quiet duplicate into an
     * exception on the webhook path, which is exactly the case this method was added to avoid.
     * It returns a {@code boolean} about a SHA-256 digest and never a row, so nothing tenant-owned
     * crosses the boundary. Allowlisted in {@code TenantScopedQueryArchTest}; if the index ever
     * gains {@code tenant_id}, this predicate must gain it in the same migration.
     */
    @Query("""
            SELECT count(e) > 0 FROM NotificationEvent e
             WHERE e.dedupHash = :dedupHash
               AND e.occurredAt >= :occurredAtFrom
               AND e.occurredAt <  :occurredAtTo
            """)
    boolean existsByDedupHashInWindow(@Param("dedupHash") byte[] dedupHash,
                                      @Param("occurredAtFrom") Instant occurredAtFrom,
                                      @Param("occurredAtTo") Instant occurredAtTo);
}
