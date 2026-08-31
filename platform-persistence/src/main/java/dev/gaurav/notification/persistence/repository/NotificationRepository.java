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
 *
 * <p>{@link #findExpiredInWindow} is the one method here that deliberately sweeps across tenants;
 * it is named in the allowlist inside {@code TenantScopedQueryArchTest} together with the reason.
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
     *   <li>{@code tenant_id = :tenantId} — the isolation boundary. A notification id is a bare
     *       UUID that appears in an API response and on every Kafka event, so it is an identifier
     *       an outside caller can hold. Without this predicate, a cancel or a status callback
     *       naming another tenant's notification <em>succeeds</em>, and because the guard is
     *       monotonic the write cannot be undone: a forged terminal status permanently stops that
     *       notification, and the real provider callback is then refused as post-terminal.</li>
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
               AND n.tenant_id    = :tenantId
               AND n.created_at >= :createdAtFrom
               AND n.created_at  < :createdAtTo
               AND n.status_rank < :newRank
               AND NOT EXISTS (SELECT 1
                                 FROM notif.delivery_status d
                                WHERE d.code = n.status
                                  AND d.is_terminal)
            """, nativeQuery = true)
    int applyStatusTransition(@Param("id") UUID id,
                              @Param("tenantId") Long tenantId,
                              @Param("createdAtFrom") Instant createdAtFrom,
                              @Param("createdAtTo") Instant createdAtTo,
                              @Param("newStatus") String newStatus,
                              @Param("newRank") short newRank,
                              @Param("occurredAt") Instant occurredAt);

    /**
     * Point lookup, pruned and tenant-scoped. Served by {@code n_id_uk} — 7 buffers when the window
     * is one day.
     *
     * <p>There used to be two of these: this one without a tenant and a {@code findInWindowForTenant}
     * with one. That pair is the bug, not the fix — a repository that offers a safe and an unsafe
     * version of the same lookup will be called with the unsafe one, and the call site that does it
     * looks exactly like the call site that does not. There is one lookup now and the tenant is not
     * optional.
     *
     * <p>Tenant is a predicate rather than a check on the result, so an unknown id and another
     * tenant's id produce the same empty answer and the endpoint above cannot be used to test
     * whether a notification exists.
     */
    @Query("""
            SELECT n FROM NotificationEntity n
             WHERE n.id = :id
               AND n.tenantId = :tenantId
               AND n.createdAt >= :createdAtFrom
               AND n.createdAt <  :createdAtTo
            """)
    Optional<NotificationEntity> findInWindow(@Param("id") UUID id,
                                              @Param("tenantId") Long tenantId,
                                              @Param("createdAtFrom") Instant createdAtFrom,
                                              @Param("createdAtTo") Instant createdAtTo);

    /**
     * The fan-out of one request. Served by {@code n_request_ix}.
     *
     * <p>Tenant-scoped, and this one is a write hazard rather than a read leak. Its caller,
     * {@code RequestFanOut}, does not merely read these rows — it <em>adopts</em> them, keyed by
     * channel, and then hangs a campaign's recipient rows off whatever it found. Unscoped, a
     * request id that collided with another tenant's would make the expander attach one tenant's
     * audience, ciphertext and all, to the other tenant's notification row, and advance that row's
     * status while it did it. The tenant predicate is what makes a wrong-tenant request expand
     * into fresh rows of its own instead of into someone else's campaign.
     *
     * <p>The caller is an internal Kafka consumer today, but "no external caller" is a property of
     * the current wiring rather than of this query, and adoption is the kind of write that nobody
     * would notice going wrong.
     */
    @Query("""
            SELECT n FROM NotificationEntity n
             WHERE n.requestId = :requestId
               AND n.tenantId = :tenantId
               AND n.createdAt >= :createdAtFrom
               AND n.createdAt <  :createdAtTo
             ORDER BY n.createdAt
            """)
    List<NotificationEntity> findByRequest(@Param("requestId") UUID requestId,
                                           @Param("tenantId") Long tenantId,
                                           @Param("createdAtFrom") Instant createdAtFrom,
                                           @Param("createdAtTo") Instant createdAtTo);

    /**
     * Notifications whose TTL has elapsed while they were still in flight.
     *
     * <p><strong>Deliberately cross-tenant.</strong> A TTL is our promise, not a tenant's request:
     * the reaper runs on a timer with no caller to scope it by, and a per-tenant variant would
     * leave every tenant nobody enumerated with notifications stuck in flight forever. Allowlisted
     * in {@code TenantScopedQueryArchTest}.
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
