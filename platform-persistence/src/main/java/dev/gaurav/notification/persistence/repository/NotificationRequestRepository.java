package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.persistence.entity.NotificationRequest;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Reads over the accepted-request log. Partitioned daily by {@code created_at}, so every finder
 * here takes a window — see the package javadoc.
 */
public interface NotificationRequestRepository extends JpaRepository<NotificationRequest, UUID> {

    @Query("""
            SELECT r FROM NotificationRequest r
             WHERE r.id = :id
               AND r.createdAt >= :createdAtFrom
               AND r.createdAt <  :createdAtTo
            """)
    Optional<NotificationRequest> findInWindow(@Param("id") UUID id,
                                               @Param("createdAtFrom") Instant createdAtFrom,
                                               @Param("createdAtTo") Instant createdAtTo);

    @Query("""
            SELECT r FROM NotificationRequest r
             WHERE r.tenantId = :tenantId
               AND r.idempotencyKey = :idempotencyKey
               AND r.createdAt >= :createdAtFrom
               AND r.createdAt <  :createdAtTo
            """)
    Optional<NotificationRequest> findByIdempotencyKeyInWindow(@Param("tenantId") Long tenantId,
                                                               @Param("idempotencyKey") String idempotencyKey,
                                                               @Param("createdAtFrom") Instant createdAtFrom,
                                                               @Param("createdAtTo") Instant createdAtTo);

    /**
     * Requests the expander still owes work on.
     *
     * <p>Callers pass {@code ACCEPTED} <em>and</em> {@code EXPANDING}. A row left in
     * {@code EXPANDING} means the expander died mid fan-out, and a sweep that only looked at
     * {@code ACCEPTED} would leave that campaign half-sent forever with nothing to notice it.
     */
    @Query("""
            SELECT r FROM NotificationRequest r
             WHERE r.status IN :statuses
               AND r.createdAt >= :createdAtFrom
               AND r.createdAt <  :createdAtTo
             ORDER BY r.createdAt
            """)
    List<NotificationRequest> findByStatusInWindow(
            @Param("statuses") Collection<NotificationRequest.RequestStatus> statuses,
            @Param("createdAtFrom") Instant createdAtFrom,
            @Param("createdAtTo") Instant createdAtTo,
            Limit limit);
}
