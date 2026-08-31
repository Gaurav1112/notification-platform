package dev.gaurav.notification.persistence.entity;

import dev.gaurav.notification.domain.enums.ScheduleType;
import dev.gaurav.notification.domain.enums.TrafficClass;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * What the caller asked for, before fan-out. One request becomes N {@link NotificationEntity}
 * rows — one per channel per recipient.
 *
 * <p>Keeping the request as its own row is what makes a bulk send resumable. A campaign to two
 * million recipients cannot be expanded inside the HTTP request, so the API persists this row,
 * returns 202, and an expander works through it. If the expander dies halfway, the request row
 * and its {@code recipientCount} say exactly how far it got.
 *
 * <p><strong>Partitioned by {@code created_at} (daily).</strong> The table's real primary key is
 * {@code (created_at, id)}; only {@code id} is mapped as {@code @Id} — see the package javadoc.
 * <strong>Every query against this entity must carry a {@code created_at} bound</strong>, or
 * PostgreSQL scans every partition that exists and the plan degrades with retention rather than
 * with result size.
 */
@Entity
@Table(name = "notification_request", schema = "notif")
public class NotificationRequest {

    /** Where the recipient list comes from; mirrors {@code nr_source_ck}. */
    public enum RecipientSource {
        INLINE,
        USER_IDS,
        S3_MANIFEST,
        AUDIENCE_REF
    }

    /** Fan-out progress of the request itself, not of any delivery. */
    public enum RequestStatus {
        ACCEPTED,
        EXPANDING,
        EXPANDED,
        CANCELLED,
        FAILED
    }

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    /** Partition key. Assigned by the application, never updated — see the class javadoc. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "tenant_id", nullable = false)
    private Long tenantId;

    @Column(name = "idempotency_key", length = 128)
    private String idempotencyKey;

    @Enumerated(EnumType.STRING)
    @Column(name = "traffic_class", nullable = false, length = 16)
    private TrafficClass trafficClass;

    // TODO(phase-3): should be List<Channel>. Mapped as String[] because the column is
    // varchar(16)[] and PgJDBC refuses to bind a plain String to an array column — a String
    // mapping compiles and then fails at the first INSERT. Element values are Channel.name().
    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "channels", nullable = false)
    private String[] channels;

    @Enumerated(EnumType.STRING)
    @Column(name = "schedule_type", nullable = false, length = 16)
    private ScheduleType scheduleType;

    /** Non-null whenever {@code scheduleType} is SCHEDULED; the schema refuses the other case. */
    @Column(name = "scheduled_at")
    private Instant scheduledAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "recipient_source", nullable = false, length = 16)
    private RecipientSource recipientSource;

    @Column(name = "recipient_ref")
    private String recipientRef;

    @Column(name = "recipient_count", nullable = false)
    private int recipientCount;

    @Column(name = "template_code", length = 96)
    private String templateCode;

    @Column(name = "template_locale", length = 16)
    private String templateLocale;

    // TODO(phase-3): typed once templates land. Raw JSON text keeps unknown keys intact across a
    // rolling deploy where one pod knows a field the other does not.
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false)
    private String payload = "{}";

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private RequestStatus status = RequestStatus.ACCEPTED;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    @Column(name = "created_by", nullable = false)
    private String createdBy = "system";

    @Column(name = "trace_id", length = 64)
    private String traceId;

    protected NotificationRequest() {
        // for JPA
    }

    public NotificationRequest(UUID id, Long tenantId, TrafficClass trafficClass, Instant expiresAt) {
        this.id = id;
        this.tenantId = tenantId;
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

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public TrafficClass getTrafficClass() {
        return trafficClass;
    }

    public void setTrafficClass(TrafficClass trafficClass) {
        this.trafficClass = trafficClass;
    }

    public String[] getChannels() {
        return channels == null ? null : channels.clone();
    }

    public void setChannels(String[] channels) {
        this.channels = channels == null ? null : channels.clone();
    }

    public ScheduleType getScheduleType() {
        return scheduleType;
    }

    public void setScheduleType(ScheduleType scheduleType) {
        this.scheduleType = scheduleType;
    }

    public Instant getScheduledAt() {
        return scheduledAt;
    }

    public void setScheduledAt(Instant scheduledAt) {
        this.scheduledAt = scheduledAt;
    }

    public RecipientSource getRecipientSource() {
        return recipientSource;
    }

    public void setRecipientSource(RecipientSource recipientSource) {
        this.recipientSource = recipientSource;
    }

    public String getRecipientRef() {
        return recipientRef;
    }

    public void setRecipientRef(String recipientRef) {
        this.recipientRef = recipientRef;
    }

    public int getRecipientCount() {
        return recipientCount;
    }

    public void setRecipientCount(int recipientCount) {
        this.recipientCount = recipientCount;
    }

    public String getTemplateCode() {
        return templateCode;
    }

    public void setTemplateCode(String templateCode) {
        this.templateCode = templateCode;
    }

    public String getTemplateLocale() {
        return templateLocale;
    }

    public void setTemplateLocale(String templateLocale) {
        this.templateLocale = templateLocale;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public RequestStatus getStatus() {
        return status;
    }

    public void setStatus(RequestStatus status) {
        this.status = status;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }
}
