package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.persistence.entity.NotificationRecipient;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reads and status writes for the per-address rows the workers operate on.
 *
 * <p>Partitioned daily by {@code created_at}; every finder takes a window. See the package
 * javadoc for why the derived shortcuts are not used, and for why every finder here also takes a
 * {@code tenantId}.
 *
 * <p>The two sweeps — {@link #findDueForRetry} and {@link #findStaleInFlight} — are the deliberate
 * exceptions. They are infrastructure that has to see the whole table, and they are named in the
 * allowlist inside {@code TenantScopedQueryArchTest}.
 */
public interface NotificationRecipientRepository extends JpaRepository<NotificationRecipient, UUID> {

    /**
     * Point lookup, pruned to one partition and scoped to one tenant.
     *
     * <p>The recipient id is a bare UUID that travels on a Kafka dispatch event and comes back on a
     * provider webhook, so it is an identifier an attacker can supply. Without
     * {@code tenantId} in the predicate this method hands the caller another tenant's row —
     * ciphertext, address hint, delivery state and all — on nothing more than a guessed id, and
     * every caller becomes responsible for remembering to compare the tenant afterwards. Making it
     * a predicate rather than a post-fetch check also collapses "not yours" and "does not exist"
     * into the same empty answer, so the endpoint above cannot be used as an existence oracle.
     */
    @Query("""
            SELECT r FROM NotificationRecipient r
             WHERE r.id = :id
               AND r.tenantId = :tenantId
               AND r.createdAt >= :createdAtFrom
               AND r.createdAt <  :createdAtTo
            """)
    Optional<NotificationRecipient> findInWindow(@Param("id") UUID id,
                                                 @Param("tenantId") Long tenantId,
                                                 @Param("createdAtFrom") Instant createdAtFrom,
                                                 @Param("createdAtTo") Instant createdAtTo);

    /**
     * Served by {@code nrec_notification_ix}.
     *
     * <p>{@code notificationId} alone is not a tenant boundary: one guessed notification id would
     * otherwise enumerate that notification's entire audience. The tenant predicate is what makes
     * a wrong-tenant walk return nothing instead of a recipient list.
     */
    @Query("""
            SELECT r FROM NotificationRecipient r
             WHERE r.notificationId = :notificationId
               AND r.tenantId = :tenantId
               AND r.createdAt >= :createdAtFrom
               AND r.createdAt <  :createdAtTo
             ORDER BY r.createdAt
            """)
    List<NotificationRecipient> findByNotification(@Param("notificationId") UUID notificationId,
                                                   @Param("tenantId") Long tenantId,
                                                   @Param("createdAtFrom") Instant createdAtFrom,
                                                   @Param("createdAtTo") Instant createdAtTo);

    /**
     * Resolves a provider callback to the row it is about. Served by {@code nrec_provider_msg_ix}.
     *
     * <p>Returns a list rather than an {@code Optional} deliberately: {@code provider_message_id}
     * is the vendor's identifier, and nothing stops two vendors — or one vendor across a contract
     * migration — from issuing the same string. A signature promising uniqueness we do not
     * control would turn that into an exception on the webhook path.
     *
     * <p>That same non-uniqueness is exactly why the tenant predicate matters here more than
     * anywhere else on this interface. The input is a string chosen by an outside party, arriving
     * on a public webhook endpoint; two tenants sharing one vendor account can legitimately collide
     * on it. Unscoped, a forged callback carrying another tenant's message id resolves to that
     * tenant's recipient row and drives a status transition on it.
     */
    @Query("""
            SELECT r FROM NotificationRecipient r
             WHERE r.providerMessageId = :providerMessageId
               AND r.tenantId = :tenantId
               AND r.createdAt >= :createdAtFrom
               AND r.createdAt <  :createdAtTo
            """)
    List<NotificationRecipient> findByProviderMessageId(@Param("providerMessageId") String providerMessageId,
                                                        @Param("tenantId") Long tenantId,
                                                        @Param("createdAtFrom") Instant createdAtFrom,
                                                        @Param("createdAtTo") Instant createdAtTo);

    /**
     * Retry candidates whose backoff has elapsed.
     *
     * <p><strong>Deliberately cross-tenant.</strong> This is the scheduler's backstop sweep, not a
     * tenant data access: it runs on a timer with no request and therefore no tenant, and a
     * per-tenant variant would have to iterate every tenant on every sweep and would miss the
     * tenants it did not know to ask about. Allowlisted in {@code TenantScopedQueryArchTest}.
     *
     * <p>Native, and the status list is written as literals rather than bound parameters, because
     * the index this has to hit is <em>partial</em>:
     * {@code nrec_retry_ix … WHERE status IN ('SEND_FAILED','QUEUED') AND next_attempt_at IS NOT NULL}.
     * The planner can only use a partial index when it can prove the query predicate implies the
     * index predicate, and it cannot prove that about {@code status IN ($1,$2)} — the values are
     * not known at plan time. Bind them and the query silently falls back to a sequential scan of
     * the partition, which is exactly the case the partial index was built for.
     *
     * <p>{@code next_attempt_at > :notBefore} is not redundant with the day window either.
     * Without a lower bound the index scan has no start point and degrades linearly with backlog
     * depth: the deeper the backlog, the slower the query meant to drain it.
     */
    @Query(value = """
            SELECT *
              FROM notif.notification_recipient r
             WHERE r.status IN ('SEND_FAILED','QUEUED')
               AND r.next_attempt_at IS NOT NULL
               AND r.next_attempt_at >  :notBefore
               AND r.next_attempt_at <= :dueBy
               AND r.created_at >= :createdAtFrom
               AND r.created_at <  :createdAtTo
             ORDER BY r.next_attempt_at
             LIMIT :maxRows
            """, nativeQuery = true)
    List<NotificationRecipient> findDueForRetry(@Param("notBefore") Instant notBefore,
                                                @Param("dueBy") Instant dueBy,
                                                @Param("createdAtFrom") Instant createdAtFrom,
                                                @Param("createdAtTo") Instant createdAtTo,
                                                @Param("maxRows") int maxRows);

    /**
     * Rows a worker claimed and never resolved. Served by the partial index
     * {@code nrec_inflight_ix … WHERE status IN ('CLAIMED','SENDING')}, so the statuses are
     * literals here for the same reason as in {@link #findDueForRetry}.
     *
     * <p>These are the leases of workers that died between claiming and reporting. Nothing else
     * will ever move them, so without this sweep they stay {@code CLAIMED} forever and the
     * recipient simply never hears from us.
     *
     * <p><strong>Deliberately cross-tenant</strong>, for the same reason as {@link #findDueForRetry}:
     * a dead worker's leases do not belong to whoever happens to be making a request. Allowlisted
     * in {@code TenantScopedQueryArchTest}.
     */
    @Query(value = """
            SELECT *
              FROM notif.notification_recipient r
             WHERE r.status IN ('CLAIMED','SENDING')
               AND r.last_status_at < :staleBefore
               AND r.created_at >= :createdAtFrom
               AND r.created_at <  :createdAtTo
             ORDER BY r.last_status_at
             LIMIT :maxRows
            """, nativeQuery = true)
    List<NotificationRecipient> findStaleInFlight(@Param("staleBefore") Instant staleBefore,
                                                  @Param("createdAtFrom") Instant createdAtFrom,
                                                  @Param("createdAtTo") Instant createdAtTo,
                                                  @Param("maxRows") int maxRows);

    /**
     * The monotonic guard for a recipient row — the same shape as
     * {@link NotificationRepository#applyStatusTransition}, and for the same reason: provider
     * webhooks arrive out of order and Kafka is at-least-once, so the transition has to be one
     * atomic statement that rejects stale, duplicate and post-terminal events in its
     * {@code WHERE} clause.
     *
     * <p><strong>{@code tenant_id} is part of that clause and not an afterthought.</strong> This is
     * a write reachable from a public webhook: the caller supplies a recipient id and a status, and
     * without the tenant predicate any tenant can mark any other tenant's message
     * {@code DELIVERED}, {@code BOUNCED} or {@code FAILED}. Because the guard is monotonic, that
     * write is <em>irreversible</em> — a forged {@code BOUNCED} is terminal and nothing will ever
     * move the row again, so the real provider callback is refused and the recipient is quietly
     * abandoned. A read leak is bad; this one silently destroys another tenant's delivery.
     *
     * <p>{@code last_status_at} moves with {@code greatest(...)} so a reordered event cannot drag
     * the in-flight sweep's clock backwards and make a live row look abandoned.
     *
     * <p>Returning {@code 0} is the normal outcome for a duplicated callback, not an error — and
     * now also for a cross-tenant one, which is the correct answer for it too.
     *
     * @return 1 if applied, 0 if the event was stale, duplicated, illegal or from another tenant
     */
    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE notif.notification_recipient r
               SET status         = :newStatus,
                   status_rank    = :newRank,
                   last_status_at = greatest(r.last_status_at, :occurredAt)
             WHERE r.id          = :id
               AND r.tenant_id    = :tenantId
               AND r.created_at >= :createdAtFrom
               AND r.created_at  < :createdAtTo
               AND r.status_rank < :newRank
               AND NOT EXISTS (SELECT 1
                                 FROM notif.delivery_status d
                                WHERE d.code = r.status
                                  AND d.is_terminal)
            """, nativeQuery = true)
    int applyStatusTransition(@Param("id") UUID id,
                              @Param("tenantId") Long tenantId,
                              @Param("createdAtFrom") Instant createdAtFrom,
                              @Param("createdAtTo") Instant createdAtTo,
                              @Param("newStatus") String newStatus,
                              @Param("newRank") short newRank,
                              @Param("occurredAt") Instant occurredAt);
}
