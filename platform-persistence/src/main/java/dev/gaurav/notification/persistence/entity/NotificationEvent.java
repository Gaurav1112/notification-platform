package dev.gaurav.notification.persistence.entity;

import dev.gaurav.notification.domain.enums.DeliveryStatus;
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
 * Every state-change signal the platform received, applied or not.
 *
 * <p>{@code applied = false} means the monotonic guard rejected the event — it was stale,
 * duplicated or illegal. Those rows are the most valuable debugging artefact in the system: they
 * are the webhooks we correctly ignored. Without them, "why is this notification stuck in SENT"
 * has no answer, because the evidence that a later DELIVERED arrived and was discarded exists
 * nowhere else. Rejecting an event is normal traffic at volume, not an error.
 *
 * <p>{@code dedupHash} carries a unique index, so a provider that retries its callback five times
 * produces one row rather than five, and the second insert fails fast instead of being detected
 * by an application-side lookup that races.
 *
 * <p><strong>Partitioned by {@code occurred_at}; {@code @Id} is {@code id} alone.</strong> Note
 * that the partition column here is {@code occurred_at} — the provider's timestamp, not ours —
 * so a badly-skewed provider clock lands rows in an unexpected partition. Every query must carry
 * an {@code occurred_at} bound.
 */
@Entity
@Table(name = "notification_event", schema = "notif")
public class NotificationEvent {

    /** Which subsystem produced the event; mirrors {@code ne_source_ck}. */
    public enum EventSource {
        API,
        WORKER,
        PROVIDER,
        SCHEDULER,
        ADMIN,
        SYSTEM
    }

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    /** Partition key, and the provider's notion of when it happened. */
    @Column(name = "occurred_at", nullable = false, updatable = false)
    private Instant occurredAt;

    /** When we learned about it. The gap between this and {@code occurredAt} is webhook lag. */
    @Column(name = "recorded_at", nullable = false)
    private Instant recordedAt = Instant.now();

    @Column(name = "tenant_id", nullable = false)
    private Long tenantId;

    @Column(name = "notification_id")
    private UUID notificationId;

    @Column(name = "recipient_id")
    private UUID recipientId;

    @Column(name = "event_type", nullable = false, length = 32)
    private String eventType;

    @Enumerated(EnumType.STRING)
    @Column(name = "source", nullable = false, length = 16)
    private EventSource source;

    @Column(name = "provider_id")
    private Short providerId;

    @Enumerated(EnumType.STRING)
    @Column(name = "from_status", length = 24)
    private DeliveryStatus fromStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "to_status", length = 24)
    private DeliveryStatus toStatus;

    /** False when the monotonic guard rejected it. See the class javadoc — these rows matter. */
    @Column(name = "applied", nullable = false)
    private boolean applied = true;

    /** Uniquely identifies the signal, so a provider retrying its callback yields one row. */
    @Column(name = "dedup_hash", nullable = false)
    private byte[] dedupHash;

    // TODO(phase-8): typed per event_type once the webhook adapters land.
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "attributes", nullable = false)
    private String attributes = "{}";

    protected NotificationEvent() {
        // for JPA
    }

    public NotificationEvent(UUID id, Instant occurredAt, Long tenantId, String eventType,
                             EventSource source, byte[] dedupHash) {
        this.id = id;
        this.occurredAt = occurredAt;
        this.tenantId = tenantId;
        this.eventType = eventType;
        this.source = source;
        this.dedupHash = dedupHash == null ? null : dedupHash.clone();
    }

    public UUID getId() {
        return id;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public Instant getRecordedAt() {
        return recordedAt;
    }

    public void setRecordedAt(Instant recordedAt) {
        this.recordedAt = recordedAt;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public UUID getNotificationId() {
        return notificationId;
    }

    public void setNotificationId(UUID notificationId) {
        this.notificationId = notificationId;
    }

    public UUID getRecipientId() {
        return recipientId;
    }

    public void setRecipientId(UUID recipientId) {
        this.recipientId = recipientId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public EventSource getSource() {
        return source;
    }

    public void setSource(EventSource source) {
        this.source = source;
    }

    public Short getProviderId() {
        return providerId;
    }

    public void setProviderId(Short providerId) {
        this.providerId = providerId;
    }

    public DeliveryStatus getFromStatus() {
        return fromStatus;
    }

    public void setFromStatus(DeliveryStatus fromStatus) {
        this.fromStatus = fromStatus;
    }

    public DeliveryStatus getToStatus() {
        return toStatus;
    }

    public void setToStatus(DeliveryStatus toStatus) {
        this.toStatus = toStatus;
    }

    public boolean isApplied() {
        return applied;
    }

    public void setApplied(boolean applied) {
        this.applied = applied;
    }

    public byte[] getDedupHash() {
        return dedupHash == null ? null : dedupHash.clone();
    }

    public void setDedupHash(byte[] dedupHash) {
        this.dedupHash = dedupHash == null ? null : dedupHash.clone();
    }

    public String getAttributes() {
        return attributes;
    }

    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }
}
