package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.persistence.entity.DeliveryAttempt;
import jakarta.persistence.QueryHint;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

/**
 * Reads over the append-only attempt log. Partitioned daily by {@code attempted_at}, so the
 * window parameters here are named {@code attemptedAtFrom} / {@code attemptedAtTo} rather than
 * {@code createdAt*} — the column really is different, and hiding that would invite a caller to
 * pass the notification's window and quietly miss attempts that crossed midnight.
 *
 * <p>Every row here is billing evidence: which vendor was used, what it answered, and
 * {@code cost_micros}. So the two finders that return rows are tenant-scoped, and the two that
 * aggregate for operations are the deliberate, allowlisted exceptions — see
 * {@code TenantScopedQueryArchTest}.
 */
public interface DeliveryAttemptRepository extends JpaRepository<DeliveryAttempt, Long> {

    /**
     * Attempt history for one address, newest first. Served by {@code da_recipient_ix}.
     *
     * <p>Tenant-scoped because this is the ledger behind the per-notification attempt view: it
     * carries the vendor we used, what it charged and what it said when it failed. A recipient id
     * is a guessable UUID that has already left this system on a Kafka event, so without the
     * predicate one guessed id exposes another tenant's provider mix and unit economics.
     */
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    @Query("""
            SELECT a FROM DeliveryAttempt a
             WHERE a.recipientId = :recipientId
               AND a.tenantId = :tenantId
               AND a.attemptedAt >= :attemptedAtFrom
               AND a.attemptedAt <  :attemptedAtTo
             ORDER BY a.attemptedAt DESC
            """)
    List<DeliveryAttempt> findByRecipient(@Param("recipientId") UUID recipientId,
                                          @Param("tenantId") Long tenantId,
                                          @Param("attemptedAtFrom") Instant attemptedAtFrom,
                                          @Param("attemptedAtTo") Instant attemptedAtTo);

    /**
     * Finds the attempt a given idempotency token already produced.
     *
     * <p>This is the read that makes redelivery safe: before making a network call, the worker
     * checks whether an attempt row for this token exists. If it does, the send already happened
     * (or is in flight) and repeating it would charge the tenant twice and possibly deliver a
     * second OTP. Served by the unique index {@code da_token_uk}.
     *
     * <p>{@code da_token_uk} is on {@code (idempotency_token, attempted_at)} and is therefore
     * global, so the tenant predicate narrows the read but not the constraint. That asymmetry is
     * intentional and safe: the token is derived from the recipient UUID and the attempt number
     * (see {@code AbstractChannelWorker}), so a cross-tenant token collision is a UUID collision.
     * Reading another tenant's attempt row — which is what the unscoped version does on a
     * collision, or on a caller that passes a token it did not generate — is the realistic risk,
     * and it is the one the predicate removes.
     */
    @Query("""
            SELECT a FROM DeliveryAttempt a
             WHERE a.idempotencyToken = :idempotencyToken
               AND a.tenantId = :tenantId
               AND a.attemptedAt >= :attemptedAtFrom
               AND a.attemptedAt <  :attemptedAtTo
            """)
    Optional<DeliveryAttempt> findByIdempotencyToken(@Param("idempotencyToken") String idempotencyToken,
                                                     @Param("tenantId") Long tenantId,
                                                     @Param("attemptedAtFrom") Instant attemptedAtFrom,
                                                     @Param("attemptedAtTo") Instant attemptedAtTo);

    /**
     * Attempts left {@code PENDING} past the point where any provider call could still be open.
     *
     * <p><strong>Deliberately cross-tenant.</strong> This is the reconciler's input, and a send
     * whose outcome is unknown is unknown to the platform, not to a tenant — there is no request
     * and no caller to scope it by. Running it per tenant would mean the tenants nobody thought to
     * enumerate never get reconciled. Allowlisted in {@code TenantScopedQueryArchTest}.
     *
     * <p>Native with a literal state so the partial index {@code da_provider_fail_ix … WHERE state
     * <> 'SUCCEEDED'} is usable; a bound parameter would defeat it. Every row this returns is a
     * send whose outcome we do not know, which is the input to reconciliation — the one class of
     * failure where guessing wrong means either a duplicate message or a silent drop.
     */
    @Query(value = """
            SELECT *
              FROM notif.delivery_attempt a
             WHERE a.state = 'PENDING'
               AND a.request_started_at < :startedBefore
               AND a.attempted_at >= :attemptedAtFrom
               AND a.attempted_at <  :attemptedAtTo
             ORDER BY a.attempted_at
             LIMIT :maxRows
            """, nativeQuery = true)
    List<DeliveryAttempt> findAbandonedAttempts(@Param("startedBefore") Instant startedBefore,
                                                @Param("attemptedAtFrom") Instant attemptedAtFrom,
                                                @Param("attemptedAtTo") Instant attemptedAtTo,
                                                @Param("maxRows") int maxRows);

    /**
     * Raw success/failure counts for one provider over a window.
     *
     * <p><strong>Deliberately cross-tenant.</strong> The question it answers is "is this vendor
     * working", which is a property of our contract with the vendor and not of any tenant. It
     * returns {@code [state, count]} pairs and never a row, so no tenant-owned column leaves the
     * database through it. Allowlisted in {@code TenantScopedQueryArchTest}.
     *
     * <p>Exists for reconciliation and backfill only. <strong>The dashboards and the circuit
     * breaker must not call this</strong> — measured at 481 MB and 291 ms over a single day's
     * partition. Live provider health comes from the Valkey sliding window, and reporting from
     * the per-minute rollup, which answers the same question in 2 buffers.
     *
     * @return one row of {@code [state, count]} per attempt state
     */
    @Query(value = """
            SELECT a.state AS state, count(*) AS attempts
              FROM notif.delivery_attempt a
             WHERE a.provider_id = :providerId
               AND a.attempted_at >= :attemptedAtFrom
               AND a.attempted_at <  :attemptedAtTo
             GROUP BY a.state
            """, nativeQuery = true)
    List<Object[]> countByStateForProvider(@Param("providerId") Short providerId,
                                           @Param("attemptedAtFrom") Instant attemptedAtFrom,
                                           @Param("attemptedAtTo") Instant attemptedAtTo);
}
