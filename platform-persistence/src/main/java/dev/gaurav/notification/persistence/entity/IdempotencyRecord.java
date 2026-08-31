package dev.gaurav.notification.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * One caller-supplied idempotency key, and the response we already returned for it.
 *
 * <p>The unique index <em>is</em> the serialisation point. Two concurrent requests carrying the
 * same key race to insert; exactly one wins and does the work, and the loser reads the winner's
 * response. That is why the claim in {@code IdempotencyRepository} is a single
 * {@code INSERT … ON CONFLICT DO NOTHING} rather than a SELECT followed by an INSERT — the
 * check-then-act version has a window in which both requests decide they are the first.
 *
 * <p>{@code requestFingerprint} is the SHA-256 of the canonical request body, and it is the
 * difference between idempotency and a data leak: the same key with a <em>different</em> body
 * must be a 409, not a silent replay of an unrelated response to whoever asked second.
 *
 * <p><strong>This is the one entity with a composite {@code @IdClass}</strong>, and the exception
 * is deliberate. Everywhere else the composite PK exists only because PostgreSQL demands the
 * partition key inside every unique constraint, and there is a surrogate {@code uuid} to use
 * instead. Here the key genuinely is {@code (tenant, key)} — there is no surrogate column, and
 * declaring {@code @Id} on {@code idempotencyKey} alone would tell Hibernate that two tenants
 * using the string {@code "abc"} are the same row, which is a cross-tenant response leak inside
 * the persistence context. Partitioning is hourly (by {@code createdAt}) purely so expiry is a
 * {@code DROP TABLE} instead of a {@code DELETE} that leaves twenty million dead tuples a day for
 * autovacuum to chase.
 */
@Entity
@Table(name = "idempotency_record", schema = "notif")
@IdClass(IdempotencyRecord.Key.class)
public class IdempotencyRecord {

    /** Progress of the request this key is guarding. */
    public enum State {
        IN_PROGRESS,
        COMPLETED,
        FAILED
    }

    /** Composite identifier: see the class javadoc for why this one is not a surrogate uuid. */
    public static class Key implements Serializable {

        private Instant createdAt;
        private Long tenantId;
        private String idempotencyKey;

        public Key() {
            // for JPA
        }

        public Key(Instant createdAt, Long tenantId, String idempotencyKey) {
            this.createdAt = createdAt;
            this.tenantId = tenantId;
            this.idempotencyKey = idempotencyKey;
        }

        public Instant getCreatedAt() {
            return createdAt;
        }

        public Long getTenantId() {
            return tenantId;
        }

        public String getIdempotencyKey() {
            return idempotencyKey;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            return other instanceof Key key
                    && Objects.equals(createdAt, key.createdAt)
                    && Objects.equals(tenantId, key.tenantId)
                    && Objects.equals(idempotencyKey, key.idempotencyKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(createdAt, tenantId, idempotencyKey);
        }
    }

    @Id
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Id
    @Column(name = "tenant_id", nullable = false, updatable = false)
    private Long tenantId;

    @Id
    @Column(name = "idempotency_key", nullable = false, updatable = false, length = 128)
    private String idempotencyKey;

    /** SHA-256 of the canonical request body. Same key + different body must be a 409. */
    @Column(name = "request_fingerprint", nullable = false)
    private byte[] requestFingerprint;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false, length = 16)
    private State state = State.IN_PROGRESS;

    @Column(name = "request_id")
    private UUID requestId;

    @Column(name = "response_status")
    private Short responseStatus;

    // TODO(phase-3): typed once the accept-path response DTO is stable. Stored as raw JSON text so
    // the replay returns byte-identical content to what the first caller received.
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "response_body")
    private String responseBody;

    /**
     * Fences a crashed in-flight request. Without it, a caller that dies mid-write leaves a key
     * stuck in {@code IN_PROGRESS} and every retry gets a 409 until expiry.
     */
    @Column(name = "locked_until")
    private Instant lockedUntil;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    protected IdempotencyRecord() {
        // for JPA
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public byte[] getRequestFingerprint() {
        return requestFingerprint;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public void setRequestId(UUID requestId) {
        this.requestId = requestId;
    }

    public Short getResponseStatus() {
        return responseStatus;
    }

    public void setResponseStatus(Short responseStatus) {
        this.responseStatus = responseStatus;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public void setResponseBody(String responseBody) {
        this.responseBody = responseBody;
    }

    public Instant getLockedUntil() {
        return lockedUntil;
    }

    public void setLockedUntil(Instant lockedUntil) {
        this.lockedUntil = lockedUntil;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }
}
