package dev.gaurav.notification.adapter.persistence;

import dev.gaurav.notification.application.port.DeliveryStatusWriter;
import dev.gaurav.notification.persistence.entity.NotificationEvent;
import dev.gaurav.notification.persistence.repository.NotificationEventRepository;
import dev.gaurav.notification.persistence.repository.NotificationRecipientRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.UUID;

/**
 * Bridges {@link DeliveryStatusWriter} onto the per-recipient monotonic guard and the append-only
 * event log.
 *
 * <p>The seam exists so the status use case can say "apply this transition" and read a row count,
 * without knowing that the guard is one conditional {@code UPDATE} whose {@code WHERE} clause
 * carries the partition bounds, the rank comparison and the terminal anti-join. Everything that
 * makes an at-least-once, out-of-order status stream safe lives in that statement; this class only
 * supplies its parameters.
 *
 * <p><strong>Zero rows updated is a success.</strong> It is the expected answer for a duplicated
 * webhook or a late {@code SENT} arriving after {@code DELIVERED}, and the use case turns it into
 * an {@code applied = false} row rather than an error. This adapter does not interpret the count.
 *
 * <p>The unapplied-event insert races by construction: two replicas processing the same redelivered
 * webhook both try to append the same {@code dedup_hash}. {@code ne_dedup_uk} decides it, and the
 * loser's constraint violation is swallowed here — the row it wanted to write already exists, which
 * is the outcome it was asking for.
 */
@Component
public class JpaDeliveryStatusWriter implements DeliveryStatusWriter {

    /** {@code notification_event.event_type} for a transition the guard refused. */
    private static final String EVENT_TYPE_UNAPPLIED = "STATUS_REFUSED";

    private static final Logger log = LoggerFactory.getLogger(JpaDeliveryStatusWriter.class);

    private final NotificationRecipientRepository recipients;
    private final NotificationEventRepository events;
    private final TenantDirectory tenants;

    public JpaDeliveryStatusWriter(NotificationRecipientRepository recipients,
                                   NotificationEventRepository events,
                                   TenantDirectory tenants) {
        this.recipients = Objects.requireNonNull(recipients, "recipients");
        this.events = Objects.requireNonNull(events, "events");
        this.tenants = Objects.requireNonNull(tenants, "tenants");
    }

    /**
     * <p>The tenant is resolved and passed into the guard rather than trusted from the recipient
     * id. Webhook payloads name a recipient, and a recipient id is a UUID an attacker can guess or
     * replay; the guard's {@code tenant_id} predicate is what makes a transition addressed at
     * someone else's row affect zero rows instead of ending their delivery.
     *
     * <p>{@code requireInternalId} rather than a silent empty: reaching this method with a tenant
     * reference that resolves to nothing means the edge check was bypassed, and returning "0 rows,
     * stale" for it would record the forgery as ordinary out-of-order traffic.
     */
    @Override
    public int applyStatus(StatusTransition transition) {
        long tenant = tenants.requireInternalId(transition.tenantId());
        return recipients.applyStatusTransition(
                transition.recipientId(),
                tenant,
                PartitionWindows.dayStart(transition.recipientCreatedAt()),
                PartitionWindows.dayEnd(transition.recipientCreatedAt()),
                transition.proposed().name(),
                (short) transition.proposed().rank(),
                transition.occurredAt());
    }

    @Override
    public void recordUnappliedEvent(StatusTransition transition, String reason) {
        long tenant = tenants.requireInternalId(transition.tenantId());
        var row = new NotificationEvent(UUID.randomUUID(), transition.occurredAt(), tenant,
                EVENT_TYPE_UNAPPLIED, NotificationEvent.EventSource.PROVIDER, transition.dedupHash());
        row.setNotificationId(transition.notificationId());
        row.setRecipientId(transition.recipientId());
        row.setToStatus(transition.proposed());
        row.setApplied(false);
        // The reason is the single most useful field on this row six weeks later: it is what
        // separates "the bounce never reached us" from "it reached us and was correctly ignored".
        row.setAttributes(attributesJson(reason, transition));
        try {
            events.save(row);
        } catch (DataIntegrityViolationException e) {
            // ne_dedup_uk fired: another replica already recorded this exact refusal. Nothing to
            // do — appending a second row for one fact is the thing the index exists to prevent.
            log.debug("refused status for recipient {} was already recorded", transition.recipientId());
        }
    }

    /**
     * Hand-built rather than serialised, so this adapter needs no JSON mapper for two fields whose
     * shape is fixed.
     *
     * <p>Both values are narrowed to a conservative character set before they are interpolated.
     * {@code providerCode} originates outside this JVM, and a quote or a backslash in it would
     * either corrupt the {@code jsonb} column or, if the column accepted it, forge a member of this
     * object. Escaping by allowlist is shorter than escaping by rule and cannot be got wrong.
     */
    private static String attributesJson(String reason, StatusTransition transition) {
        return "{\"reason\":\"%s\",\"providerCode\":\"%s\"}".formatted(
                jsonSafe(reason, "UNSPECIFIED"), jsonSafe(transition.providerCode(), ""));
    }

    private static String jsonSafe(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value.replaceAll("[^A-Za-z0-9._:-]", "_");
    }
}
