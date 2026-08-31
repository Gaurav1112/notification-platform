package dev.gaurav.notification.messaging.event;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;
import java.util.UUID;

/**
 * The envelope every message on every topic carries.
 *
 * <p><strong>{@link #eventId()} is the key the idempotent receiver deduplicates on.</strong>
 * Kafka is at-least-once: a rebalance mid-batch, a consumer restart before the offset commit, or a
 * DLQ replay all redeliver records that were already processed. Without a stable per-event
 * identifier there is nothing to compare, and "process it again" means a second SMS to a real
 * person's phone. The id is generated once, at the moment the event is created, and survives every
 * hop, retry tier and replay unchanged — it identifies the <em>event</em>, not the delivery
 * attempt of the event.
 *
 * <p>UUIDv7 is the intended generator: it is time-ordered, so the Redis dedup keyspace and the
 * Postgres fallback index both stay locality-friendly instead of scattering random writes.
 *
 * <p>The subtype discriminator is written <em>into the payload</em> as {@code eventType} rather
 * than into a Kafka header. Header-based type ids (spring-kafka's {@code __TypeId__}) carry a Java
 * fully-qualified class name, which couples the wire format to our package layout, breaks the
 * moment a class is moved, and is unreadable to a non-JVM consumer or to a human triaging the DLQ.
 *
 * <p>Records are immutable, which is what makes them safe to hand to a producer callback, to
 * requeue on a retry tier and to log.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "eventType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = NotificationRequestedEvent.class,
                name = NotificationEvent.TYPE_REQUESTED),
        @JsonSubTypes.Type(value = NotificationDispatchEvent.class,
                name = NotificationEvent.TYPE_DISPATCH),
        @JsonSubTypes.Type(value = DeliveryStatusEvent.class,
                name = NotificationEvent.TYPE_DELIVERY_STATUS),
        @JsonSubTypes.Type(value = RetryScheduledEvent.class,
                name = NotificationEvent.TYPE_RETRY_SCHEDULED),
        @JsonSubTypes.Type(value = DeadLetterEvent.class,
                name = NotificationEvent.TYPE_DEAD_LETTER)
})
public sealed interface NotificationEvent
        permits NotificationRequestedEvent, NotificationDispatchEvent, DeliveryStatusEvent,
                RetryScheduledEvent, DeadLetterEvent {

    /** Wire name of {@link NotificationRequestedEvent}. Changing it is a breaking schema change. */
    String TYPE_REQUESTED = "notification.requested";
    /** Wire name of {@link NotificationDispatchEvent}. */
    String TYPE_DISPATCH = "notification.dispatch";
    /** Wire name of {@link DeliveryStatusEvent}. */
    String TYPE_DELIVERY_STATUS = "notification.delivery-status";
    /** Wire name of {@link RetryScheduledEvent}. */
    String TYPE_RETRY_SCHEDULED = "notification.retry-scheduled";
    /** Wire name of {@link DeadLetterEvent}. */
    String TYPE_DEAD_LETTER = "notification.dead-letter";

    /**
     * Sentinel for a tenant we could not determine — only legitimately used by
     * {@link DeadLetterEvent}, where the original payload may be unparseable.
     */
    long UNKNOWN_TENANT = 0L;

    /**
     * The deduplication key. Stable across redelivery, retry tiers and DLQ replay.
     *
     * @see dev.gaurav.notification.messaging.consumer.IdempotentConsumer
     */
    UUID eventId();

    /**
     * When the fact happened, not when it was produced.
     *
     * <p>These differ by the outbox sweeper's lag, and by minutes on a replay. Ordering decisions
     * that use the produce time instead re-order every replayed event to "now" and let a stale
     * status overwrite a fresh one.
     */
    Instant occurredAt();

    /** Owning tenant. Part of every partition key so one tenant cannot span the whole keyspace. */
    long tenantId();

    /**
     * W3C trace context, propagated so a trace spans the accept HTTP request, the Kafka hops and
     * the provider call. Nullable: internally-generated events (the reconciler, the sweeper) have
     * no inbound trace to continue.
     */
    String traceparent();

    /** The wire discriminator. Not a record component — Jackson writes it from the type id. */
    @JsonIgnore
    String eventType();
}
