package dev.gaurav.notification.adapter.persistence;

import dev.gaurav.notification.application.command.RecipientKind;
import dev.gaurav.notification.application.command.SendNotificationCommand;
import dev.gaurav.notification.application.port.AcceptanceWriter;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.Priority;
import dev.gaurav.notification.messaging.config.KafkaProducerConfig;
import dev.gaurav.notification.messaging.event.NotificationEvent;
import dev.gaurav.notification.messaging.event.NotificationRequestedEvent;
import dev.gaurav.notification.messaging.producer.PartitionKeys;
import dev.gaurav.notification.messaging.topic.Topics;
import dev.gaurav.notification.persistence.entity.NotificationEntity;
import dev.gaurav.notification.persistence.entity.NotificationRequest;
import dev.gaurav.notification.persistence.entity.OutboxMessage;
import dev.gaurav.notification.persistence.repository.NotificationRepository;
import dev.gaurav.notification.persistence.repository.NotificationRequestRepository;
import dev.gaurav.notification.persistence.repository.OutboxRepository;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * The accept transaction: {@code notification_request} + one {@code notification} per channel + one
 * {@code outbox_message}, committed together.
 *
 * <p>The seam exists so {@code AcceptNotificationUseCase} can express "this request is now durable"
 * as a single call. Splitting it — rows through one port and the outbox entry through another —
 * would let a future caller compose a partial accept, and a partial accept is a dual write: either
 * a notification nobody will ever publish, or a published message for a row that rolled back. One
 * {@code @Transactional} method makes both impossible by construction.
 *
 * <p><strong>The outbox payload is the fully-formed {@link NotificationRequestedEvent}</strong>,
 * serialised with the same {@code JsonMapper} the Kafka producer uses, so the sweeper in
 * {@code app-scheduler} republishes bytes it can deserialise without any second encoding to keep in
 * step. That is also why this module depends on {@code platform-messaging}: the outbox table
 * already stores a topic, a partition key and an event payload, so the wire contract is a
 * persistence concern here whether or not the type is imported.
 *
 * <p>Recipients are deliberately not expanded. A 10M-recipient campaign has to answer inside the
 * 250 ms budget, so the audience reference is stored and {@code RequestFanOut} expands it off the
 * request thread — the Claim Check pattern.
 *
 * <p>TODO(phase-3): {@link NotificationRequestedEvent} has no field for inline content, only for a
 * template code and its variables. A request that carries {@code content} rather than
 * {@code template} is therefore persisted correctly — the body is on
 * {@code notification_request.payload} — but the expander's renderer has no template code to render
 * and dead-letters it. Adding {@code inlineSubject}/{@code inlineBody} to the event is a wire-format
 * change and belongs with the template store, not here.
 */
@Component
public class JpaAcceptanceWriter implements AcceptanceWriter {

    /** {@code aggregate_type} on the outbox row; matches the table the aggregate lives in. */
    private static final String AGGREGATE_TYPE = "notification_request";

    private final NotificationRequestRepository requests;
    private final NotificationRepository notifications;
    private final OutboxRepository outbox;
    private final TenantDirectory tenants;
    private final JsonMapper mapper;

    public JpaAcceptanceWriter(NotificationRequestRepository requests,
                               NotificationRepository notifications,
                               OutboxRepository outbox,
                               TenantDirectory tenants,
                               @Qualifier(KafkaProducerConfig.EVENT_JSON_MAPPER) JsonMapper mapper) {
        this.requests = Objects.requireNonNull(requests, "requests");
        this.notifications = Objects.requireNonNull(notifications, "notifications");
        this.outbox = Objects.requireNonNull(outbox, "outbox");
        this.tenants = Objects.requireNonNull(tenants, "tenants");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    @Transactional
    public AcceptedRecords persist(SendNotificationCommand command, UUID requestId,
                                   Instant acceptedAt, Instant expiresAt) {
        long tenant = tenants.requireInternalId(command.tenantId());

        requests.save(requestRow(command, tenant, requestId, acceptedAt, expiresAt));

        var persisted = new ArrayList<PersistedNotification>(command.channels().size());
        for (Channel channel : command.channels()) {
            var notification = notificationRow(command, tenant, requestId, channel, acceptedAt, expiresAt);
            notifications.save(notification);
            persisted.add(new PersistedNotification(notification.getId(), acceptedAt, channel));
        }

        outbox.save(outboxRow(command, tenant, requestId, acceptedAt, expiresAt));

        return new AcceptedRecords(requestId, acceptedAt, persisted, command.recipientCount());
    }

    private NotificationRequest requestRow(SendNotificationCommand command, long tenant, UUID requestId,
                                           Instant acceptedAt, Instant expiresAt) {
        var row = new NotificationRequest(requestId, tenant, command.trafficClass(), expiresAt);
        // Assigned, never defaulted: created_at is the partition key, and letting each insert in
        // this transaction call now() can straddle midnight and scatter one request across two
        // daily partitions.
        row.setCreatedAt(acceptedAt);
        row.setIdempotencyKey(command.idempotencyKey());
        row.setChannels(channelNames(command.channels()));
        row.setScheduleType(command.schedule().type());
        row.setScheduledAt(command.schedule().sendAt());
        row.setRecipientSource(recipientSourceOf(command.recipients().kind()));
        row.setRecipientRef(command.recipients().ref());
        row.setRecipientCount(command.recipientCount());
        if (command.template() != null) {
            row.setTemplateCode(command.template().code());
            row.setTemplateLocale(command.template().locale());
        }
        row.setPayload(payloadJson(command));
        row.setStatus(NotificationRequest.RequestStatus.ACCEPTED);
        row.setTraceId(command.traceparent());
        return row;
    }

    private NotificationEntity notificationRow(SendNotificationCommand command, long tenant, UUID requestId,
                                               Channel channel, Instant acceptedAt, Instant expiresAt) {
        var row = new NotificationEntity(UUID.randomUUID(), requestId, tenant, channel,
                command.trafficClass(), expiresAt);
        row.setCreatedAt(acceptedAt);
        row.setStatusAt(acceptedAt);
        row.setUpdatedAt(acceptedAt);
        row.setPriority(priorityValue(command.priority()));
        row.setScheduledAt(command.schedule().sendAt());
        // SCHEDULED rather than PENDING for a deferred send, because the two are ranked
        // differently and the cancel guard reads that rank: cancelling a scheduled send must
        // succeed, and cancelling a queued one must not.
        row.setStatus(command.schedule().isDeferred() ? DeliveryStatus.SCHEDULED : DeliveryStatus.PENDING);
        row.setTotalRecipients(command.recipientCount());
        if (command.template() != null) {
            row.setTemplateCode(command.template().code());
        }
        row.setTraceId(command.traceparent());
        return row;
    }

    /**
     * The outbox entry that makes the Kafka publish optional.
     *
     * <p>A deferred send goes to {@code notification.scheduled} and an immediate one to
     * {@code notification.requested}. The topic is decided here rather than by the sweeper because
     * the sweeper republishes bytes and does not interpret them — a sweeper that had to re-derive
     * the lane would be a second implementation of the routing rule.
     */
    private OutboxMessage outboxRow(SendNotificationCommand command, long tenant, UUID requestId,
                                    Instant acceptedAt, Instant expiresAt) {
        var event = requestedEvent(command, tenant, requestId, acceptedAt, expiresAt);
        boolean deferred = command.schedule().isDeferred();
        String topic = deferred ? Topics.SCHEDULED : Topics.REQUESTED;
        String key = deferred
                ? PartitionKeys.forScheduled(tenant, requestId)
                : PartitionKeys.forRequested(tenant, command.idempotencyKey(), requestId);
        return new OutboxMessage(AGGREGATE_TYPE, requestId.toString(), NotificationEvent.TYPE_REQUESTED,
                topic, key, mapper.writeValueAsString(event));
    }

    private NotificationRequestedEvent requestedEvent(SendNotificationCommand command, long tenant,
                                                      UUID requestId, Instant acceptedAt, Instant expiresAt) {
        var recipients = command.recipients();
        boolean claimCheck = recipients.isClaimCheck();
        return new NotificationRequestedEvent(
                UUID.randomUUID(),
                acceptedAt,
                tenant,
                command.traceparent(),
                requestId,
                acceptedAt,
                command.idempotencyKey(),
                command.channels(),
                command.trafficClass(),
                command.priority(),
                command.schedule().type(),
                command.schedule().sendAt(),
                expiresAt,
                command.template() == null ? null : command.template().code(),
                command.template() == null ? null : command.template().locale(),
                templateData(command),
                claimCheck ? recipients.ref() : null,
                claimCheck ? List.of() : inlineRecipients(recipients.kind(), recipients),
                command.recipientCount());
    }

    /**
     * The inline audience, whichever of the two inline forms the caller used.
     *
     * <p>{@code INLINE} addresses and {@code USER_IDS} both travel in {@code recipientIds}: the
     * expander resolves a reference to an address through the vault, and for an inline address the
     * reference <em>is</em> the address. Keeping them in one field means the expander has one code
     * path rather than two that can drift.
     */
    private static List<String> inlineRecipients(RecipientKind kind,
                                                 dev.gaurav.notification.application.command.RecipientSelector recipients) {
        return kind == RecipientKind.INLINE ? recipients.addresses() : recipients.userIds();
    }

    /**
     * Template variables, flattened to strings for the wire.
     *
     * <p>{@code LinkedHashMap} so the rendered body is byte-stable: the echo renderer sorts, but a
     * future renderer that does not would otherwise produce a different dedup hash per pod for
     * identical input.
     */
    private static Map<String, String> templateData(SendNotificationCommand command) {
        var data = new LinkedHashMap<String, String>();
        command.variables().forEach((k, v) -> data.put(k, v == null ? null : String.valueOf(v)));
        return data;
    }

    /**
     * Everything the request asked for that has no column of its own.
     *
     * <p>Raw JSON text rather than a typed column, matching the entity's own note: it keeps keys
     * the current version does not understand intact across a rolling deploy.
     */
    private String payloadJson(SendNotificationCommand command) {
        var payload = new LinkedHashMap<String, Object>();
        if (command.content() != null) {
            payload.put("subject", command.content().subject());
            payload.put("body", command.content().body());
        }
        if (!command.variables().isEmpty()) {
            payload.put("variables", command.variables());
        }
        if (!command.metadata().isEmpty()) {
            payload.put("metadata", command.metadata());
        }
        return mapper.writeValueAsString(payload);
    }

    private static String[] channelNames(java.util.Set<Channel> channels) {
        return channels.stream().map(Channel::name).toArray(String[]::new);
    }

    private static NotificationRequest.RecipientSource recipientSourceOf(RecipientKind kind) {
        // Same four names on both sides, checked by the compiler through the switch rather than by
        // a valueOf that would fail at the first INSERT if either enum gained a member.
        return switch (kind) {
            case INLINE -> NotificationRequest.RecipientSource.INLINE;
            case USER_IDS -> NotificationRequest.RecipientSource.USER_IDS;
            case S3_MANIFEST -> NotificationRequest.RecipientSource.S3_MANIFEST;
            case AUDIENCE_REF -> NotificationRequest.RecipientSource.AUDIENCE_REF;
        };
    }

    /** {@code notification.priority} is a 0-9 smallint; the domain enum is ordered urgent-first. */
    private static short priorityValue(Priority priority) {
        return (short) (priority == null ? Priority.P2_NORMAL.ordinal() : priority.ordinal());
    }
}
