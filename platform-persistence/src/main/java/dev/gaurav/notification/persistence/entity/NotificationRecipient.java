package dev.gaurav.notification.persistence.entity;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.domain.enums.SuppressionReason;
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
 * One address we are trying to reach for one {@link NotificationEntity}. This is the row the
 * workers actually operate on, and the row a webhook resolves to.
 *
 * <p>The address is stored three ways, and each one exists to make a different thing possible:
 *
 * <ul>
 *   <li>{@code addressCipher} — AES-GCM under a per-user data key. GDPR erasure destroys the key,
 *       not the rows, so a right-to-be-forgotten request does not require rewriting billions of
 *       partitioned records.</li>
 *   <li>{@code addressHash} — a tenant-keyed HMAC. Deduplication and suppression matching happen
 *       against this, so neither path needs to decrypt anything. Tenant-keyed, so the same phone
 *       number under two tenants does not correlate.</li>
 *   <li>{@code addressHint} — {@code 'g***@example.com'}. Safe to put in a log line, which is the
 *       only reason support can debug a delivery without a decrypt capability.</li>
 * </ul>
 *
 * <p><strong>Partitioned by {@code created_at} (daily); {@code @Id} is {@code id} alone.</strong>
 * See the package javadoc. Every query must carry a {@code created_at} bound —
 * {@code notificationCreatedAt} is denormalised onto this row precisely so a caller that has the
 * parent notification can supply that bound when walking to its recipients.
 *
 * <p>{@link Version} is present here because a recipient row is also mutated as a whole by the
 * retry engine (attempt counter, next attempt time, current provider), where two workers racing
 * on the same row would otherwise double-send. Kafka partition ownership makes that rare;
 * {@code @Version} makes it impossible. Status transitions still go through the monotonic guard —
 * see {@link NotificationEntity} for why the two mechanisms coexist.
 */
@Entity
@Table(name = "notification_recipient", schema = "notif")
public class NotificationRecipient {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    /** Partition key. Assigned by the application, never updated. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "notification_id", nullable = false)
    private UUID notificationId;

    /** Denormalised so a walk from notification to recipients can supply a partition bound. */
    @Column(name = "notification_created_at", nullable = false)
    private Instant notificationCreatedAt;

    @Column(name = "tenant_id", nullable = false)
    private Long tenantId;

    @Column(name = "user_ref", length = 128)
    private String userRef;

    @Enumerated(EnumType.STRING)
    @Column(name = "channel", nullable = false, length = 16)
    private Channel channel;

    /** AES-GCM ciphertext under a per-user DEK. Erasure destroys the key, not the row. */
    @Column(name = "address_cipher", nullable = false)
    private byte[] addressCipher;

    /** Tenant-keyed HMAC. Dedup and suppression match on this without decrypting. */
    @Column(name = "address_hash", nullable = false)
    private byte[] addressHash;

    /** Redacted form, safe to log. */
    @Column(name = "address_hint", length = 64)
    private String addressHint;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 24)
    private DeliveryStatus status = DeliveryStatus.PENDING;

    @Column(name = "status_rank", nullable = false)
    private short statusRank = (short) DeliveryStatus.PENDING.rank();

    @Column(name = "attempt_count", nullable = false)
    private short attemptCount;

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "current_provider_id")
    private Short currentProviderId;

    @Column(name = "provider_message_id", length = 128)
    private String providerMessageId;

    @Column(name = "dedup_key", length = 128)
    private String dedupKey;

    @Enumerated(EnumType.STRING)
    @Column(name = "suppression_reason", length = 32)
    private SuppressionReason suppressionReason;

    @Enumerated(EnumType.STRING)
    @Column(name = "failure_type", length = 32)
    private FailureType failureType;

    @Column(name = "failure_detail")
    private String failureDetail;

    @Column(name = "sent_at")
    private Instant sentAt;

    @Column(name = "delivered_at")
    private Instant deliveredAt;

    /** Guards against applying an event older than the one already recorded. */
    @Column(name = "last_status_at", nullable = false)
    private Instant lastStatusAt = Instant.now();

    @Version
    // Boxed, not primitive: Spring Data uses a null version to recognise a brand-new
    // entity. With a primitive it falls back to the assigned id, decides the row might
    // already exist, and turns every insert into a pointless SELECT first.
    @Column(name = "row_version", nullable = false)
    private Long rowVersion;

    protected NotificationRecipient() {
        // for JPA
    }

    public NotificationRecipient(UUID id, UUID notificationId, Instant notificationCreatedAt,
                                 Long tenantId, Channel channel, byte[] addressCipher, byte[] addressHash) {
        this.id = id;
        this.notificationId = notificationId;
        this.notificationCreatedAt = notificationCreatedAt;
        this.tenantId = tenantId;
        this.channel = channel;
        this.addressCipher = addressCipher == null ? null : addressCipher.clone();
        this.addressHash = addressHash == null ? null : addressHash.clone();
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

    public UUID getNotificationId() {
        return notificationId;
    }

    public void setNotificationId(UUID notificationId) {
        this.notificationId = notificationId;
    }

    public Instant getNotificationCreatedAt() {
        return notificationCreatedAt;
    }

    public void setNotificationCreatedAt(Instant notificationCreatedAt) {
        this.notificationCreatedAt = notificationCreatedAt;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public String getUserRef() {
        return userRef;
    }

    public void setUserRef(String userRef) {
        this.userRef = userRef;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public byte[] getAddressCipher() {
        return addressCipher == null ? null : addressCipher.clone();
    }

    public void setAddressCipher(byte[] addressCipher) {
        this.addressCipher = addressCipher == null ? null : addressCipher.clone();
    }

    public byte[] getAddressHash() {
        return addressHash == null ? null : addressHash.clone();
    }

    public void setAddressHash(byte[] addressHash) {
        this.addressHash = addressHash == null ? null : addressHash.clone();
    }

    public String getAddressHint() {
        return addressHint;
    }

    public void setAddressHint(String addressHint) {
        this.addressHint = addressHint;
    }

    public DeliveryStatus getStatus() {
        return status;
    }

    /** Sets status and rank together; see {@link NotificationEntity#setStatus}. */
    public void setStatus(DeliveryStatus status) {
        this.status = status;
        this.statusRank = (short) status.rank();
    }

    public short getStatusRank() {
        return statusRank;
    }

    public short getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(short attemptCount) {
        this.attemptCount = attemptCount;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public void setNextAttemptAt(Instant nextAttemptAt) {
        this.nextAttemptAt = nextAttemptAt;
    }

    public Short getCurrentProviderId() {
        return currentProviderId;
    }

    public void setCurrentProviderId(Short currentProviderId) {
        this.currentProviderId = currentProviderId;
    }

    public String getProviderMessageId() {
        return providerMessageId;
    }

    public void setProviderMessageId(String providerMessageId) {
        this.providerMessageId = providerMessageId;
    }

    public String getDedupKey() {
        return dedupKey;
    }

    public void setDedupKey(String dedupKey) {
        this.dedupKey = dedupKey;
    }

    public SuppressionReason getSuppressionReason() {
        return suppressionReason;
    }

    public void setSuppressionReason(SuppressionReason suppressionReason) {
        this.suppressionReason = suppressionReason;
    }

    public FailureType getFailureType() {
        return failureType;
    }

    public void setFailureType(FailureType failureType) {
        this.failureType = failureType;
    }

    public String getFailureDetail() {
        return failureDetail;
    }

    public void setFailureDetail(String failureDetail) {
        this.failureDetail = failureDetail;
    }

    public Instant getSentAt() {
        return sentAt;
    }

    public void setSentAt(Instant sentAt) {
        this.sentAt = sentAt;
    }

    public Instant getDeliveredAt() {
        return deliveredAt;
    }

    public void setDeliveredAt(Instant deliveredAt) {
        this.deliveredAt = deliveredAt;
    }

    public Instant getLastStatusAt() {
        return lastStatusAt;
    }

    public void setLastStatusAt(Instant lastStatusAt) {
        this.lastStatusAt = lastStatusAt;
    }

    public Long getRowVersion() {
        return rowVersion;
    }
}
