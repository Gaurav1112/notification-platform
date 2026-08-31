package dev.gaurav.notification.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.UUID;

/**
 * A billing and isolation boundary. Everything else in the schema carries a {@code tenant_id}.
 *
 * <p>Two identifiers on purpose. {@code id} is a {@code bigint} because it is denormalised onto
 * billions of rows and 8 bytes beats a 16-byte UUID by roughly 60 GB a year; {@code publicId} is
 * the UUID that appears in URLs and API responses, so an enumerable sequence is never exposed.
 *
 * <p>{@link Version} belongs here: tenant rows are edited by humans through an admin UI, at low
 * frequency, and "someone else changed this while you were editing" is the correct thing to say
 * to a human. That reasoning does <em>not</em> extend to the hot tables — see
 * {@link NotificationEntity}.
 */
@Entity
@Table(name = "tenant", schema = "notif")
public class Tenant {

    /** Lifecycle of the tenant itself; mirrors {@code tenant_status_ck}. */
    public enum TenantStatus {
        ACTIVE,
        SUSPENDED,
        CLOSED
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false)
    private Long id;

    @Column(name = "public_id", nullable = false, updatable = false)
    private UUID publicId = UUID.randomUUID();

    @Column(name = "slug", nullable = false, length = 64)
    private String slug;

    @Column(name = "display_name", nullable = false)
    private String displayName;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private TenantStatus status = TenantStatus.ACTIVE;

    @Column(name = "daily_send_quota", nullable = false)
    private long dailySendQuota = 1_000_000L;

    @Column(name = "rate_limit_rps", nullable = false)
    private int rateLimitRps = 100;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @Version
    // Boxed, not primitive: Spring Data uses a null version to recognise a brand-new
    // entity. With a primitive it falls back to the assigned id, decides the row might
    // already exist, and turns every insert into a pointless SELECT first.
    @Column(name = "row_version", nullable = false)
    private Long rowVersion;

    protected Tenant() {
        // for JPA
    }

    public Tenant(String slug, String displayName) {
        this.slug = slug;
        this.displayName = displayName;
    }

    public Long getId() {
        return id;
    }

    public UUID getPublicId() {
        return publicId;
    }

    public void setPublicId(UUID publicId) {
        this.publicId = publicId;
    }

    public String getSlug() {
        return slug;
    }

    public void setSlug(String slug) {
        this.slug = slug;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public TenantStatus getStatus() {
        return status;
    }

    public void setStatus(TenantStatus status) {
        this.status = status;
    }

    public long getDailySendQuota() {
        return dailySendQuota;
    }

    public void setDailySendQuota(long dailySendQuota) {
        this.dailySendQuota = dailySendQuota;
    }

    public int getRateLimitRps() {
        return rateLimitRps;
    }

    public void setRateLimitRps(int rateLimitRps) {
        this.rateLimitRps = rateLimitRps;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Long getRowVersion() {
        return rowVersion;
    }
}
