package dev.gaurav.notification.messaging.producer;

import dev.gaurav.notification.messaging.event.DeadLetterEvent;
import dev.gaurav.notification.messaging.event.DeliveryStatusEvent;
import dev.gaurav.notification.messaging.event.NotificationDispatchEvent;
import dev.gaurav.notification.messaging.event.NotificationEvent;
import dev.gaurav.notification.messaging.event.NotificationRequestedEvent;
import dev.gaurav.notification.messaging.event.RetryScheduledEvent;
import dev.gaurav.notification.messaging.topic.Topics;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * The only class that produces to Kafka. One method per event type, each with the topic and the
 * partition key already decided.
 *
 * <p>Callers never pass a topic or a key. That is the point: a topic name typed at a call site is
 * a typo waiting to become a topic nobody consumes, and a key chosen at a call site is how the
 * {@code recipientId} eventually gets left out of one of them and that one path builds a hot
 * partition. Both decisions live here, next to the reasoning for them.
 *
 * <p><strong>Publishing is a fast path, not the commitment.</strong> The row is already committed
 * in PostgreSQL before any of these methods is called — PostgreSQL is the system of record and
 * Kafka is transport. A failed publish is picked up by the outbox sweeper, so the returned future
 * is for latency instrumentation and backpressure, not for correctness. Nothing here should be
 * blocked on with {@code .get()} inside a request thread.
 *
 * @see PartitionKeys
 */
@Component
public class NotificationEventPublisher {

    /**
     * The deduplication key, mirrored into a header so the idempotent receiver can check it
     * <em>before</em> deserialising the payload. On a redelivery storm that is the difference
     * between parsing 100 records per poll and parsing none of them.
     */
    public static final String HEADER_EVENT_ID = "notification-event-id";

    /** The wire discriminator, mirrored for the same reason — cheap routing without a parse. */
    public static final String HEADER_EVENT_TYPE = "notification-event-type";

    /** Lets an operator grep a topic dump by tenant during an incident without a JSON parser. */
    public static final String HEADER_TENANT_ID = "notification-tenant-id";

    /** W3C trace context, so a consumer can continue the trace before touching the body. */
    public static final String HEADER_TRACEPARENT = "traceparent";

    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;

    public NotificationEventPublisher(KafkaTemplate<String, NotificationEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /** Accepted request onto {@code notification.requested}, keyed {@code tenantId|idempotencyKey}. */
    public CompletableFuture<SendResult<String, NotificationEvent>> publishRequested(
            NotificationRequestedEvent event) {
        String key = PartitionKeys.forRequested(
                event.tenantId(), event.idempotencyKey(), event.requestId());
        return send(Topics.REQUESTED, key, event);
    }

    /** Future-dated request onto {@code notification.scheduled}, keyed {@code tenantId|requestId}. */
    public CompletableFuture<SendResult<String, NotificationEvent>> publishScheduled(
            NotificationRequestedEvent event) {
        String key = PartitionKeys.forScheduled(event.tenantId(), event.requestId());
        return send(Topics.SCHEDULED, key, event);
    }

    /**
     * One recipient's work onto its channel-and-lane topic.
     *
     * <p>The topic comes from the event itself, which derives it from
     * {@code TrafficClass.dispatchTopic(channel)}. An OTP therefore cannot be published to the
     * bulk lane by a caller that forgot which lane it was in.
     */
    public CompletableFuture<SendResult<String, NotificationEvent>> publishDispatch(
            NotificationDispatchEvent event) {
        String key = PartitionKeys.forDispatch(
                event.tenantId(), event.recipientId(), event.channel());
        return send(event.topic(), key, event);
    }

    /** Raw send outcome onto {@code notification.delivery}, keyed {@code notificationId}. */
    public CompletableFuture<SendResult<String, NotificationEvent>> publishDelivery(
            DeliveryStatusEvent event) {
        return send(Topics.DELIVERY, PartitionKeys.forNotification(event.notificationId()), event);
    }

    /**
     * Projected status onto the compacted {@code notification.status} topic.
     *
     * <p>Separate from {@link #publishDelivery} because the two topics have different cleanup
     * policies and different consumers. Compaction keeps the highest offset per key, not the
     * latest state, so the payload's {@code version} — not the topic — is what makes the
     * projection correct.
     */
    public CompletableFuture<SendResult<String, NotificationEvent>> publishStatusProjection(
            DeliveryStatusEvent event) {
        return send(Topics.STATUS, PartitionKeys.forNotification(event.notificationId()), event);
    }

    /**
     * Parks a transiently failed dispatch on its delay tier.
     *
     * <p>Keyed on the <em>original</em> dispatch triple, not on the retry event, so the message
     * stays on the same logical key across every tier hop. Re-keying at each hop would spray one
     * recipient's retries across five partitions and lose what ordering the tiers still preserve.
     */
    public CompletableFuture<SendResult<String, NotificationEvent>> publishRetry(
            RetryScheduledEvent event) {
        NotificationDispatchEvent dispatch = event.dispatch();
        String key = PartitionKeys.forDispatch(
                dispatch.tenantId(), dispatch.recipientId(), dispatch.channel());
        return send(event.tier().topic(), key, event);
    }

    /**
     * Parks an exhausted record on {@code notification.dlq}.
     *
     * <p>Keyed on {@code notificationId} where we have one, and on the source coordinates when the
     * payload was too broken to yield one — a null key there would fall back to sticky
     * partitioning and pile a poison storm onto whichever partition the batch happened to pick.
     */
    public CompletableFuture<SendResult<String, NotificationEvent>> publishDeadLetter(
            DeadLetterEvent event) {
        String key = event.notificationId() != null
                ? PartitionKeys.forNotification(event.notificationId())
                : event.sourceTopic() + PartitionKeys.SEPARATOR + event.sourceOffset();
        return send(Topics.DLQ, key, event);
    }

    private CompletableFuture<SendResult<String, NotificationEvent>> send(
            String topic, String key, NotificationEvent event) {
        // auto.create.topics.enable=false on the broker, so an unknown name is an outage that
        // starts at deploy time. Catch it here, where the stack trace names the caller.
        if (!Topics.isKnown(topic)) {
            throw new IllegalArgumentException("refusing to produce to unknown topic: " + topic);
        }
        var record = new ProducerRecord<String, NotificationEvent>(topic, key, event);
        record.headers().add(header(HEADER_EVENT_ID, event.eventId().toString()));
        record.headers().add(header(HEADER_EVENT_TYPE, event.eventType()));
        record.headers().add(header(HEADER_TENANT_ID, Long.toString(event.tenantId())));
        if (event.traceparent() != null) {
            record.headers().add(header(HEADER_TRACEPARENT, event.traceparent()));
        }
        return kafkaTemplate.send(record);
    }

    private static RecordHeader header(String name, String value) {
        return new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
