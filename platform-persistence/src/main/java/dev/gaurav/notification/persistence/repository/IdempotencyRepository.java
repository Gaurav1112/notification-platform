package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.persistence.entity.IdempotencyRecord;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

/**
 * The idempotency claim, and the replay lookup that follows a lost claim.
 */
public interface IdempotencyRepository
        extends JpaRepository<IdempotencyRecord, IdempotencyRecord.Key> {

    /**
     * Attempts to take ownership of an idempotency key.
     *
     * <p>{@code INSERT … ON CONFLICT DO NOTHING} in one statement, because the unique index
     * <em>is</em> the serialisation point. The obvious alternative — {@code SELECT}, and insert if
     * absent — has a window between the two statements in which both concurrent requests decide
     * they are the first, and the result is the same OTP charged and sent twice. Here exactly one
     * INSERT wins; the loser gets zero rows and knows to read the winner's response instead.
     *
     * <p>{@code DO NOTHING} without a conflict target, deliberately: an inference target has to
     * name the index columns exactly, and this is a partitioned table where the constraint lives
     * on the leaf. There is only one unique constraint to collide with, so nothing is lost.
     *
     * <p>{@code createdAt} is the hourly partition key and must be supplied by the caller rather
     * than defaulted, so the row and its 24-hour expiry land in the same partition the reader
     * will look in.
     *
     * @return 1 if this caller now owns the key, 0 if somebody else already claimed it
     */
    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            INSERT INTO notif.idempotency_record
                        (tenant_id, idempotency_key, created_at, request_fingerprint,
                         state, request_id, locked_until, expires_at)
                 VALUES (:tenantId, :idempotencyKey, :createdAt, :requestFingerprint,
                         'IN_PROGRESS', :requestId, :lockedUntil, :expiresAt)
            ON CONFLICT DO NOTHING
            """, nativeQuery = true)
    int claim(@Param("tenantId") Long tenantId,
              @Param("idempotencyKey") String idempotencyKey,
              @Param("createdAt") Instant createdAt,
              @Param("requestFingerprint") byte[] requestFingerprint,
              @Param("requestId") UUID requestId,
              @Param("lockedUntil") Instant lockedUntil,
              @Param("expiresAt") Instant expiresAt);

    /**
     * Reads the record behind a key so a losing claimer can replay the original response.
     *
     * <p>Window-bounded like every other partitioned read: without it this probes every hourly
     * partition that still exists.
     */
    @Query("""
            SELECT r FROM IdempotencyRecord r
             WHERE r.tenantId = :tenantId
               AND r.idempotencyKey = :idempotencyKey
               AND r.createdAt >= :createdAtFrom
               AND r.createdAt <  :createdAtTo
            """)
    Optional<IdempotencyRecord> findInWindow(@Param("tenantId") Long tenantId,
                                             @Param("idempotencyKey") String idempotencyKey,
                                             @Param("createdAtFrom") Instant createdAtFrom,
                                             @Param("createdAtTo") Instant createdAtTo);

    /**
     * Records the response the first caller received, so every replay returns the same thing.
     *
     * <p>Guarded by {@code state = 'IN_PROGRESS'} so a late completion cannot overwrite a record
     * that a fencing sweep already failed.
     *
     * @return 1 if the completion was recorded, 0 if the record was no longer in progress
     */
    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE notif.idempotency_record
               SET state           = 'COMPLETED',
                   response_status = :responseStatus,
                   response_body   = CAST(:responseBody AS jsonb),
                   request_id      = :requestId,
                   locked_until    = NULL
             WHERE tenant_id       = :tenantId
               AND idempotency_key = :idempotencyKey
               AND created_at     >= :createdAtFrom
               AND created_at      < :createdAtTo
               AND state           = 'IN_PROGRESS'
            """, nativeQuery = true)
    int complete(@Param("tenantId") Long tenantId,
                 @Param("idempotencyKey") String idempotencyKey,
                 @Param("createdAtFrom") Instant createdAtFrom,
                 @Param("createdAtTo") Instant createdAtTo,
                 @Param("responseStatus") short responseStatus,
                 @Param("responseBody") String responseBody,
                 @Param("requestId") UUID requestId);
}
