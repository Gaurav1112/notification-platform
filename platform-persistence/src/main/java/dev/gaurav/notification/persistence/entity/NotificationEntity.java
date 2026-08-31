package dev.gaurav.notification.persistence.entity;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.TrafficClass;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.UUID;

/**
 * One notification: a single {@link Channel} for a single logical send, fanned out from a
 * {@link NotificationRequest}.
 *
 * <p>Named {@code NotificationEntity} rather than {@code Notification} so it cannot be mistaken
 * for the domain aggregate; this class is the row, nothing more.
 *
 * <p><strong>Partitioned by {@code created_at} (daily).</strong> The real primary key is
 * {@code (created_at, id)} with a secondary {@code UNIQUE (id, created_at)} to serve lookup by
 * id; only {@code id} is mapped as {@code @Id}, per the package javadoc. The consequence is
 * load-bearing rather than cosmetic: <strong>every query must carry a {@code created_at}
 * bound</strong>. Without one PostgreSQL cannot prune, so a point lookup that costs 7 buffers
 * today costs a scan of ninety partitions after ninety days of retention — the query gets slower
 * as the system ages, which is the failure mode nobody notices in staging.
 *
 * <h2>Why {@link Version} coexists with the monotonic guard</h2>
 *
 * <p>They protect different things and neither subsumes the other.
 *
 * <ul>
 *   <li>{@code @Version} protects <em>config-style whole-entity edits</em> — an operator
 *       rescheduling a notification, an admin tool changing the expiry. Those go through JPA,
 *       read-modify-write a whole row, and a lost update should surface as a conflict.</li>
 *   <li>The <strong>monotonic guard</strong>
 *       ({@code NotificationRepository#applyStatusTransition}) protects <em>status
 *       transitions</em>, which are a completely different access pattern: twenty million rows a
 *       day, written concurrently by workers and provider webhooks, with no human anywhere to
 *       resolve a conflict. Running those through optimistic locking would produce an
 *       {@code OptimisticLockException} storm on the hottest path in the system, and the only
 *       sane response to each one would be to retry — which is exactly what the guard does in a
 *       single statement, without the round trip.</li>
 * </ul>
 *
 * <p>Because the guard is a native {@code UPDATE}, it does <strong>not</strong> bump
 * {@code row_version}. That is intentional: a status transition is not a lost update, it is the
 * expected traffic. Any code that loads this entity and then writes it must re-read after a
 * transition rather than assume its in-memory status is current.
 *
 * <p>{@code statusRank} is denormalised from {@code delivery_status} so the guard stays a single
 * index probe instead of a join; a nightly consistency query catches drift.
 */
@Entity
@Table(name = "notification", schema = "notif")
public class NotificationEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    /** Partition key. Assigned by the application; updating it is a DELETE+INSERT, so never do. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "request_id", nullable = false)
    private UUID requestId;

    @Column(name = "tenant_id", nullable = false)
    private Long tenantId;

    @Enumerated(EnumType.STRING)
    @Column(name = "channel", nullable = false, length = 16)
    private Channel channel;

    @Enumerated(EnumType.STRING)
    @Column(name = "traffic_class", nullable = false, length = 16)
    private TrafficClass trafficClass;

    /** 0–9, low is urgent. A smallint, not the domain {@code Priority} enum, because the column is. */
    @Column(name = "priority", nullable = false)
    private short priority = 5;

    @Column(name = "template_code", length = 96)
    private String templateCode;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 24)
    private DeliveryStatus status = DeliveryStatus.PENDING;

    /** Denormalised {@code delivery_status.rank}. Must always agree with {@link #status}. */
    @Column(name = "status_rank", nullable = false)
    private short statusRank = (short) DeliveryStatus.PENDING.rank();

    @Column(name = "status_at", nullable = false)
    private Instant statusAt = Instant.now();

    @Column(name = "scheduled_at")
    private Instant scheduledAt;

    @Column(name = "dispatched_at")
    private Instant dispatchedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    @Column(name = "total_recipients", nullable = false)
    private int totalRecipients;

    @Column(name = "delivered_count", nullable = false)
    private int deliveredCount;

    @Column(name = "failed_count", nullable = false)
    private int failedCount;

    @Column(name = "suppressed_count", nullable = false)
    private int suppressedCount;

    @Column(name = "trace_id", length = 64)
    private String traceId;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @Version
    // Boxed, not primitive: Spring Data uses a null version to recognise a brand-new
    // entity. With a primitive it falls back to the assigned id, decides the row might
    // already exist, and turns every insert into a pointless SELECT first.
    @Column(name = "row_version", nullable = false)
    private Long rowVersion;

    protected NotificationEntity() {
        // for JPA
    }

    public NotificationEntity(UUID id, UUID requestId, Long tenantId, Channel channel,
                              TrafficClass trafficClass, Instant expiresAt) {
        this.id = id;
        this.requestId = requestId;
        this.tenantId = tenantId;
        this.channel = channel;
        this.trafficClass = trafficClass;
        this.expiresAt = expiresAt;
    }

    public UUID getId() {
        return id;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public void setRequestId(UUID requestId) {
        this.requestId = requestId;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public TrafficClass getTrafficClass() {
        return trafficClass;
    }

    public void setTrafficClass(TrafficClass trafficClass) {
        this.trafficClass = trafficClass;
    }

    public short getPriority() {
        return priority;
    }

    public void setPriority(short priority) {
        this.priority = priority;
    }

    public String getTemplateCode() {
        return templateCode;
    }

    public void setTemplateCode(String templateCode) {
        this.templateCode = templateCode;
    }

    public DeliveryStatus getStatus() {
        return status;
    }

    /**
     * Sets status and rank together.
     *
     * <p>They are one fact stored in two columns; letting a caller set them independently is how
     * {@code status_rank} drifts out of agreement with {@code status}, and a drifted rank makes
     * the monotonic guard accept transitions it should reject.
     */
    public void setStatus(DeliveryStatus status) {
        this.status = status;
        this.statusRank = (short) status.rank();
    }

    public short getStatusRank() {
        return statusRank;
    }

    public Instant getStatusAt() {
        return statusAt;
    }

    public void setStatusAt(Instant statusAt) {
        this.statusAt = statusAt;
    }

    public Instant getScheduledAt() {
        return scheduledAt;
    }

    public void setScheduledAt(Instant scheduledAt) {
        this.scheduledAt = scheduledAt;
    }

    public Instant getDispatchedAt() {
        return dispatchedAt;
    }

    public void setDispatchedAt(Instant dispatchedAt) {
        this.dispatchedAt = dispatchedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }

    public int getTotalRecipients() {
        return totalRecipients;
    }

    public void setTotalRecipients(int totalRecipients) {
        this.totalRecipients = totalRecipients;
    }

    public int getDeliveredCount() {
        return deliveredCount;
    }

    public void setDeliveredCount(int deliveredCount) {
        this.deliveredCount = deliveredCount;
    }

    public int getFailedCount() {
        return failedCount;
    }

    public void setFailedCount(int failedCount) {
        this.failedCount = failedCount;
    }

    public int getSuppressedCount() {
        return suppressedCount;
    }

    public void setSuppressedCount(int suppressedCount) {
        this.suppressedCount = suppressedCount;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
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
