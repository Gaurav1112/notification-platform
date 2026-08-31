package dev.gaurav.notification.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * A message the pipeline could not process, kept so it can be triaged and replayed.
 *
 * <p>Deliberately <strong>not partitioned</strong>. If this table grows enough to need
 * partitioning, the answer is to fix the bug producing the rows, not to make the table cheaper to
 * truncate — partitioning it would remove the pressure that makes anyone look.
 *
 * <p>{@code (sourceTopic, messageKey, stackDigest)} is unique so the writer can use
 * {@code ON CONFLICT DO UPDATE SET occurrence_count = occurrence_count + 1}. That single choice
 * turns a four-million-row poison-pill storm — one bad deserialiser against a full topic — into
 * roughly thirty rows with high counts, which is the difference between a readable triage queue
 * and an unusable one.
 *
 * <p>{@code stackDigest} is a hash rather than the stack trace itself: the trace varies by line
 * number across deploys, so hashing the normalised frames is what makes the dedup key stable.
 */
@Entity
@Table(name = "dead_letter_message", schema = "notif")
public class DeadLetterMessage {

    /** Where the row is in the triage workflow; mirrors {@code dlm_triage_ck}. */
    public enum TriageState {
        NEW,
        TRIAGED,
        REPLAYING,
        RESOLVED,
        DISCARDED
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false)
    private Long id;

    @Column(name = "first_seen_at", nullable = false, updatable = false)
    private Instant firstSeenAt = Instant.now();

    @Column(name = "last_seen_at", nullable = false)
    private Instant lastSeenAt = Instant.now();

    @Column(name = "tenant_id")
    private Long tenantId;

    @Column(name = "notification_id")
    private UUID notificationId;

    @Column(name = "source_topic", nullable = false, length = 128)
    private String sourceTopic;

    @Column(name = "source_partition")
    private Integer sourcePartition;

    @Column(name = "source_offset")
    private Long sourceOffset;

    @Column(name = "message_key", length = 256)
    private String messageKey;

    /** The original record, verbatim, so replay does not depend on regenerating it. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "error_class", nullable = false, length = 96)
    private String errorClass;

    @Column(name = "error_message", nullable = false)
    private String errorMessage;

    /** Hash of the normalised stack, so the dedup key survives a deploy that shifts line numbers. */
    @Column(name = "stack_digest")
    private byte[] stackDigest;

    @Column(name = "occurrence_count", nullable = false)
    private int occurrenceCount = 1;

    @Enumerated(EnumType.STRING)
    @Column(name = "triage_state", nullable = false, length = 16)
    private TriageState triageState = TriageState.NEW;

    @Column(name = "replayed_at")
    private Instant replayedAt;

    @Column(name = "resolved_at")
    private Instant resolvedAt;

    protected DeadLetterMessage() {
        // for JPA
    }

    public DeadLetterMessage(String sourceTopic, String payload, String errorClass, String errorMessage) {
        this.sourceTopic = sourceTopic;
        this.payload = payload;
        this.errorClass = errorClass;
        this.errorMessage = errorMessage;
    }

    public Long getId() {
        return id;
    }

    public Instant getFirstSeenAt() {
        return firstSeenAt;
    }

    public Instant getLastSeenAt() {
        return lastSeenAt;
    }

    public void setLastSeenAt(Instant lastSeenAt) {
        this.lastSeenAt = lastSeenAt;
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

    public String getSourceTopic() {
        return sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public Integer getSourcePartition() {
        return sourcePartition;
    }

    public void setSourcePartition(Integer sourcePartition) {
        this.sourcePartition = sourcePartition;
    }

    public Long getSourceOffset() {
        return sourceOffset;
    }

    public void setSourceOffset(Long sourceOffset) {
        this.sourceOffset = sourceOffset;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getErrorClass() {
        return errorClass;
    }

    public void setErrorClass(String errorClass) {
        this.errorClass = errorClass;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public byte[] getStackDigest() {
        return stackDigest == null ? null : stackDigest.clone();
    }

    public void setStackDigest(byte[] stackDigest) {
        this.stackDigest = stackDigest == null ? null : stackDigest.clone();
    }

    public int getOccurrenceCount() {
        return occurrenceCount;
    }

    public void setOccurrenceCount(int occurrenceCount) {
        this.occurrenceCount = occurrenceCount;
    }

    public TriageState getTriageState() {
        return triageState;
    }

    public void setTriageState(TriageState triageState) {
        this.triageState = triageState;
    }

    public Instant getReplayedAt() {
        return replayedAt;
    }

    public void setReplayedAt(Instant replayedAt) {
        this.replayedAt = replayedAt;
    }

    public Instant getResolvedAt() {
        return resolvedAt;
    }

    public void setResolvedAt(Instant resolvedAt) {
        this.resolvedAt = resolvedAt;
    }
}
