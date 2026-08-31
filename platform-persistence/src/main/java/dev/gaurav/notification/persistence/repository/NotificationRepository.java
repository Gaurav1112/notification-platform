package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.persistence.entity.NotificationEntity;
import jakarta.persistence.QueryHint;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reads and status writes for {@link NotificationEntity}.
 *
 * <p>The inherited {@code findById}, {@code delete} and {@code findAll} methods work but will
 * scan every partition; prefer the window-bounded methods declared here. See the package javadoc.
 */
public interface NotificationRepository extends JpaRepository<NotificationEntity, UUID> {

    /**
     * The monotonic guard: the single most important statement in the codebase.
     *
     * <p>One atomic {@code UPDATE} that applies a status transition only if it is legal. There is
     * no {@code SELECT}-then-{@code UPDATE}, no optimistic-lock retry loop and no
     * application-level sequencing, because all three of those have a window in which a second
     * writer interleaves. Instead the {@code WHERE} clause rejects the three things that actually
     * happen in production:
     *
     * <ul>
     *   <li>{@code created_at} between the bounds — partition pruning. Without it this is a scan
     *       of every partition in the retention window.</li>
     *   <li>{@code status_rank < :newRank} — rejects an event that is stale (a late {@code SENT}
     *       arriving after {@code DELIVERED}) and, because the comparison is strict, also rejects
     *       an exact duplicate (the same {@code DELIVERED} webhook delivered twice).</li>
     *   <li>the {@code NOT EXISTS} anti-join against {@code delivery_status.is_terminal} — nothing
     *       may leave a terminal state. Note that {@code DELIVERED} is deliberately not terminal,
     *       so a hard {@code BOUNCED} still applies after it.</li>
     * </ul>
     *
     * <p>{@code status_at} uses {@code greatest(...)} rather than assignment so that an
     * out-of-order-but-still-advancing event cannot move the timestamp backwards.
     *
     * <p><strong>A return of {@code 0} is not an error.</strong> It is the expected outcome for a
     * duplicated or reordered event, and it happens routinely at volume. The caller records a
     * {@code notification_event} row with {@code applied = false} and moves on. This property is
     * what lets every Kafka consumer be at-least-once and makes DLQ replay harmless.
     *
     * @param id            the notification
     * @param createdAtFrom inclusive lower bound on {@code created_at}, for partition pruning
     * @param createdAtTo   exclusive upper bound on {@code created_at}
     * @param newStatus     the proposed status code
     * @param newRank       the proposed status' rank, from {@code DeliveryStatus.rank()}
     * @param occurredAt    when the event happened according to its source
     * @return 1 if applied, 0 if the event was stale, duplicated or illegal
     */
    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE notif.notification n
               SET status      = :newStatus,
                   status_rank = :newRank,
                   status_at   = greatest(n.status_at, :occurredAt),
                   updated_at  = now()
             WHERE n.id          = :id
               AND n.created_at >= :createdAtFrom
               AND n.created_at  < :createdAtTo
               AND n.status_rank < :newRank
               AND NOT EXISTS (SELECT 1
                                 FROM notif.delivery_status d
                                WHERE d.code = n.status
                                  AND d.is_terminal)
            """, nativeQuery = true)
    int applyStatusTransition(@Param("id") UUID id,
                              @Param("createdAtFrom") Instant createdAtFrom,
                              @Param("createdAtTo") Instant createdAtTo,
                              @Param("newStatus") String newStatus,
                              @Param("newRank") short newRank,
                              @Param("occurredAt") Instant occurredAt);

    /** Point lookup, pruned. Served by {@code n_id_uk} — 7 buffers when the window is one day. */
    @Query("""
            SELECT n FROM NotificationEntity n
             WHERE n.id = :id
               AND n.createdAt >= :createdAtFrom
               AND n.createdAt <  :createdAtTo
            """)
    Optional<NotificationEntity> findInWindow(@Param("id") UUID id,
                                              @Param("createdAtFrom") Instant createdAtFrom,
                                              @Param("createdAtTo") Instant createdAtTo);

    /** Tenant-scoped point lookup. Cross-tenant reads must return empty, not another tenant's row. */
    @Query("""
            SELECT n FROM NotificationEntity n
             WHERE n.id = :id
               AND n.tenantId = :tenantId
               AND n.createdAt >= :createdAtFrom
               AND n.createdAt <  :createdAtTo
            """)
    Optional<NotificationEntity> findInWindowForTenant(@Param("id") UUID id,
                                                       @Param("tenantId") Long tenantId,
                                                       @Param("createdAtFrom") Instant createdAtFrom,
                                                       @Param("createdAtTo") Instant createdAtTo);

    /** The fan-out of one request. Served by {@code n_request_ix}. */
    @Query("""
            SELECT n FROM NotificationEntity n
             WHERE n.requestId = :requestId
               AND n.createdAt >= :createdAtFrom
               AND n.createdAt <  :createdAtTo
             ORDER BY n.createdAt
            """)
    List<NotificationEntity> findByRequest(@Param("requestId") UUID requestId,
                                           @Param("createdAtFrom") Instant createdAtFrom,
                                           @Param("createdAtTo") Instant createdAtTo);

    /**
     * Notifications whose TTL has elapsed while they were still in flight.
     *
     * <p>Read-only hint because this feeds a reaper that never mutates through the entity — it
     * emits an expiry event and lets the guard do the write. Without the hint Hibernate keeps
     * every loaded row in the persistence context for dirty checking, which on a large sweep is
     * pure garbage.
     */
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    @Query("""
            SELECT n FROM NotificationEntity n
             WHERE n.expiresAt < :now
               AND n.statusRank < :terminalRankFloor
               AND n.createdAt >= :createdAtFrom
               AND n.createdAt <  :createdAtTo
             ORDER BY n.expiresAt
            """)
    List<NotificationEntity> findExpiredInWindow(@Param("now") Instant now,
                                                 @Param("terminalRankFloor") short terminalRankFloor,
                                                 @Param("createdAtFrom") Instant createdAtFrom,
                                                 @Param("createdAtTo") Instant createdAtTo);
}
