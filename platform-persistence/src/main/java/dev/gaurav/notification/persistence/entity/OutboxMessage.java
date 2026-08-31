package dev.gaurav.notification.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * A Kafka record that has been decided on but not yet published — the reason nothing in this
 * system is silently lost.
 *
 * <p>The failure this prevents: writing a notification row and then publishing to Kafka are two
 * systems, and there is no transaction across them. Publish first and a database rollback leaves
 * a phantom message; write first and a broker outage loses the send entirely. Writing the row
 * <em>and</em> this outbox row in one local transaction removes the gap; a relay then publishes
 * and deletes.
 *
 * <p>Two storage details that stop mattering only if throughput stays low:
 *
 * <ul>
 *   <li>Rows are <strong>DELETEd</strong> after publish, never stamped with a
 *       {@code published_at} and left behind. An update-based design grows the table and its
 *       index without bound; {@code published_at} exists for crash forensics, not as the normal
 *       terminal state.</li>
 *   <li>The index is partial ({@code WHERE published_at IS NULL}) so published rows leave it and
 *       it stays a few pages regardless of volume, and the table is {@code fillfactor = 70} so
 *       updates can stay HOT.</li>
 * </ul>
 *
 * <p>Not partitioned: a healthy outbox is nearly empty.
 */
@Entity
@Table(name = "outbox_message", schema = "notif")
public class OutboxMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false)
    private Long id;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "aggregate_type", nullable = false, length = 32)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false, length = 48)
    private String eventType;

    @Column(name = "topic", nullable = false, length = 128)
    private String topic;

    /** Decides the Kafka partition, and therefore the ordering guarantee the consumer gets. */
    @Column(name = "partition_key", nullable = false, length = 160)
    private String partitionKey;

    // TODO(phase-5): headers become a typed map once the messaging module defines the envelope.
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "headers", nullable = false)
    private String headers = "{}";

    /** The serialised event. Raw JSON text: the relay republishes bytes, it does not interpret. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false)
    private String payload;

    /** Set only when a publish succeeded but the delete did not; normally the row is gone. */
    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "attempts", nullable = false)
    private short attempts;

    protected OutboxMessage() {
        // for JPA
    }

    public OutboxMessage(String aggregateType, String aggregateId, String eventType,
                         String topic, String partitionKey, String payload) {
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.topic = topic;
        this.partitionKey = partitionKey;
        this.payload = payload;
    }

    public Long getId() {
        return id;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(String aggregateType) {
        this.aggregateType = aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getHeaders() {
        return headers;
    }

    public void setHeaders(String headers) {
        this.headers = headers;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public void setPublishedAt(Instant publishedAt) {
        this.publishedAt = publishedAt;
    }

    public short getAttempts() {
        return attempts;
    }

    public void setAttempts(short attempts) {
        this.attempts = attempts;
    }
}
