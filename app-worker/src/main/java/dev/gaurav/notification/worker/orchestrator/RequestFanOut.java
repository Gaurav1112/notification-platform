package dev.gaurav.notification.worker.orchestrator;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.Priority;
import dev.gaurav.notification.domain.enums.ScheduleType;
import dev.gaurav.notification.domain.enums.SuppressionReason;
import dev.gaurav.notification.messaging.config.KafkaProducerConfig;
import dev.gaurav.notification.messaging.event.NotificationDispatchEvent;
import dev.gaurav.notification.messaging.event.NotificationRequestedEvent;
import dev.gaurav.notification.persistence.entity.NotificationEntity;
import dev.gaurav.notification.persistence.entity.NotificationRecipient;
import dev.gaurav.notification.persistence.repository.NotificationRecipientRepository;
import dev.gaurav.notification.persistence.repository.NotificationRepository;
import dev.gaurav.notification.persistence.schedule.ScheduledNotificationWriter;
import dev.gaurav.notification.worker.support.RecipientAddressVault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Turns one accepted request into the rows and the dispatch events that represent it.
 *
 * <p>Separated from {@link RequestedEventListener} for one reason: this method must run inside a
 * transaction that has <strong>committed</strong> before anything is published to Kafka, and a
 * {@code @Transactional} annotation on the listener method itself would commit only after the
 * publish, inverting that order. A Kafka record referring to a row that does not exist yet is a
 * dispatch worker that loads nothing, decides the message was cancelled, and drops it.
 *
 * <p>One {@code notification} row per (request, channel), one {@code notification_recipient} row
 * per (recipient, channel). The channel is on the parent because it selects the topic, the
 * provider set and the failure semantics; a single row spanning three channels would have to carry
 * three statuses.
 *
 * <p>Preferences are evaluated here, at dispatch, not at accept. A campaign accepted at 09:00 and
 * expanded at 14:00 has five hours in which the user can unsubscribe, and a suppression that
 * arrives during that window must win.
 *
 * <p><strong>Not everything expanded here goes to Kafka.</strong> A recipient whose message is not
 * sendable <em>yet</em> — a future {@code scheduledAt}, quiet hours, or both — gets a row in
 * {@code notif.scheduled_notification} instead of a dispatch event, in the same transaction as its
 * recipient row. That table is the scheduler's ledger; until it was written to, a deferred
 * recipient sat at {@code SCHEDULED} with nothing anywhere that would ever pick it up, and the
 * message was simply never sent. Everything after the write belongs to {@code app-scheduler}: the
 * due scan hydrates it into Redis, a shard-affine claimer leases it, and the lease is what bounds
 * the damage if the pod holding it dies.
 */
@Service
public class RequestFanOut {

    private static final Logger log = LoggerFactory.getLogger(RequestFanOut.class);

    /**
     * The window used to find the accept-time notification rows.
     *
     * <p>`notification` is partitioned by `created_at`, so a query without a time bound scans
     * every partition. Accept and fan-out are normally milliseconds apart; the lookback is
     * generous enough to cover a broker outage drained by the outbox sweeper, and the lookahead
     * covers clock skew between the API pod and this one.
     */
    private static final java.time.Duration FANOUT_LOOKBACK = java.time.Duration.ofHours(25);
    private static final java.time.Duration FANOUT_LOOKAHEAD = java.time.Duration.ofMinutes(5);

    private final NotificationRepository notifications;
    private final NotificationRecipientRepository recipients;
    private final RecipientManifestReader manifestReader;
    private final PreferenceResolver preferences;
    private final TemplateRenderer templates;
    private final RecipientAddressVault vault;
    private final ScheduledNotificationWriter scheduled;
    private final JsonMapper eventMapper;
    private final Clock clock;

    public RequestFanOut(NotificationRepository notifications,
                         NotificationRecipientRepository recipients,
                         RecipientManifestReader manifestReader,
                         PreferenceResolver preferences,
                         TemplateRenderer templates,
                         RecipientAddressVault vault,
                         ScheduledNotificationWriter scheduled,
                         @Qualifier(KafkaProducerConfig.EVENT_JSON_MAPPER) JsonMapper eventMapper,
                         Clock clock) {
        this.notifications = notifications;
        this.recipients = recipients;
        this.manifestReader = manifestReader;
        this.preferences = preferences;
        this.templates = templates;
        this.vault = vault;
        this.scheduled = scheduled;
        this.eventMapper = eventMapper;
        this.clock = clock;
    }

    /**
     * What one request became.
     *
     * @param dispatches events to publish, one per (recipient, channel) that is actually sendable
     * @param suppressed rows written as {@code SUPPRESSED}; reported, never silently dropped
     * @param deferred   rows handed to {@code notif.scheduled_notification} instead of Kafka —
     *                   quiet hours, a future {@code scheduledAt}, or both
     */
    public record FanOutResult(List<NotificationDispatchEvent> dispatches, int suppressed, int deferred) {

        public int total() {
            return dispatches.size() + suppressed + deferred;
        }
    }

    /**
     * Expands the request and commits every row it produced.
     *
     * <p>{@code REQUIRES_NEW} is deliberate even though the caller has no transaction today: it
     * makes the commit boundary a property of this method rather than of whoever calls it, so a
     * future caller that opens a transaction to do something else cannot silently extend this one
     * across the Kafka publish.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public FanOutResult expand(NotificationRequestedEvent event) {
        Instant now = clock.instant();
        if (event.expiresAt().isBefore(now)) {
            // Fanning out a request that is already dead costs one row per recipient and one
            // provider call per recipient, all of which are then discarded by the TTL check in
            // the channel worker. Stop here instead.
            log.info("dropping request {} for tenant {}: expired at {}",
                    event.requestId(), event.tenantId(), event.expiresAt());
            return new FanOutResult(List.of(), 0, 0);
        }

        List<String> userRefs = manifestReader.read(event);
        var dispatches = new ArrayList<NotificationDispatchEvent>(userRefs.size() * event.channels().size());
        // Accumulated and written once at the end. A campaign expanded into a quiet-hours window
        // is thousands of rows, and at that size one round trip per recipient is the cost, not
        // the insert.
        var parked = new ArrayList<ScheduledNotificationWriter.ScheduledInsert>();
        int suppressed = 0;
        int deferred = 0;

        // The accept transaction has already written one notification row per channel, and their
        // ids are what we returned in the 202 and what the caller is polling. Creating fresh rows
        // here — which this method used to do — orphaned every id we had handed out: the status
        // endpoint kept reporting PENDING against a row nothing would ever advance, while a second,
        // invisible row did the actual work. It also doubled the row count for every request.
        //
        // So adopt what accept created, keyed by channel. Anything not found is created, because
        // a request can legitimately reach fan-out without an accept row: a replay after the
        // notification partition was dropped, or a future scheduled path that enqueues directly.
        //
        // Scoped by tenant as well as by request id, because this is an adoption and not a read:
        // a row found here has this campaign's recipients hung off it and its status advanced. A
        // request id that reached another tenant's row would graft one tenant's audience onto the
        // other's notification.
        var existing = notifications
                .findByRequest(event.requestId(), event.tenantId(),
                        now.minus(FANOUT_LOOKBACK), now.plus(FANOUT_LOOKAHEAD))
                .stream()
                .collect(java.util.stream.Collectors.toMap(
                        NotificationEntity::getChannel,
                        java.util.function.Function.identity(),
                        (first, duplicate) -> first));

        for (Channel channel : event.channels()) {
            // Rendered once per channel, not once per recipient: the copy is identical for every
            // recipient of a campaign, and 10M identical renders is pure CPU with no output.
            var rendered = templates.render(
                    event.templateCode(), event.templateLocale(), channel, event.templateData());

            var notification = existing.get(channel);
            if (notification == null) {
                notification = newNotification(event, channel, now);
                log.warn("no accept-time notification row for request {} channel {}; creating one. "
                                + "Expected only on replay or a direct-enqueue path.",
                        event.requestId(), channel);
            }
            notification.setTotalRecipients(userRefs.size());
            // QUEUED (30) outranks PENDING (10), so this advances the row the caller is polling
            // instead of leaving it stuck behind a second row it cannot see.
            notification.setStatus(DeliveryStatus.QUEUED);
            notification.setStatusAt(now);
            notifications.save(notification);

            for (String userRef : userRefs) {
                var decision = preferences.resolve(
                        event.tenantId(), userRef, channel, event.trafficClass());
                var sealed = vault.sealFor(event.tenantId(), userRef, channel);

                if (!decision.allowed()) {
                    recipients.save(suppressedRecipient(
                            event, notification, channel, userRef, sealed.orElse(null), decision.reason(), now));
                    suppressed++;
                    continue;
                }
                if (sealed.isEmpty()) {
                    // A user with no push token is not a failure and must not be retried: there
                    // is nothing to retry against. It is a suppression with a reason, so the
                    // tenant can see it on the delivery report.
                    recipients.save(suppressedRecipient(
                            event, notification, channel, userRef, null,
                            SuppressionReason.INVALID_ADDRESS, now));
                    suppressed++;
                    continue;
                }

                var row = newRecipient(event, notification, channel, userRef, sealed.get(), now);
                Instant dueAt = dueAt(event, decision, now);
                if (dueAt != null) {
                    // The recipient row still gets written and still says SCHEDULED, because it is
                    // what the delivery report and the status endpoint read. What changed is that
                    // the work itself now exists somewhere that will act on it: before this, the
                    // row was parked and nothing anywhere was ever going to pick it up again.
                    row.setStatus(DeliveryStatus.SCHEDULED);
                    row.setNextAttemptAt(dueAt);
                    recipients.save(row);
                    parked.add(park(event, notification, row, channel, dueAt,
                            dispatchFor(event, notification, row, channel, sealed.get(), rendered)));
                    deferred++;
                    continue;
                }

                row.setStatus(DeliveryStatus.QUEUED);
                recipients.save(row);
                dispatches.add(dispatchFor(event, notification, row, channel, sealed.get(), rendered));
            }
        }

        // Same transaction as the recipient rows above, so "parked" and "scheduled" commit as one
        // fact. Splitting them would create the state this whole path exists to avoid: a recipient
        // row saying SCHEDULED with no scheduled_notification row behind it, which is invisible to
        // every queue and every metric and never sends.
        int written = scheduled.insertAll(parked);
        if (written != parked.size()) {
            // ON CONFLICT DO NOTHING absorbed a duplicate — a retried statement, or a re-expansion
            // that reused a recipient id. Harmless, and worth seeing, because a persistent
            // shortfall means fan-out is running twice.
            log.info("request {}: {} of {} deferred rows were already scheduled",
                    event.requestId(), parked.size() - written, parked.size());
        }
        return new FanOutResult(List.copyOf(dispatches), suppressed, deferred);
    }

    /**
     * When this recipient's message may first be sent, or {@code null} for "now".
     *
     * <p>The <em>later</em> of the two constraints, never the first one found. A receipt scheduled
     * for 09:00 whose owner is in quiet hours until 08:00 goes at 09:00; one scheduled for 02:00
     * with the same quiet hours goes at 08:00. Taking whichever was checked first gets one of
     * those two cases wrong, and the wrong one is the one that wakes the user up.
     *
     * <p>A {@code scheduledAt} already in the past is not a deferral. Requests sit in Kafka, and a
     * broker outage drained by the outbox sweeper routinely delivers a request whose scheduled
     * time has passed; parking it would schedule work that is already due and add a full hydrator
     * round trip to a message that should go now.
     */
    private static Instant dueAt(NotificationRequestedEvent event,
                                 PreferenceResolver.PreferenceDecision decision,
                                 Instant now) {
        Instant latest = null;
        if (event.scheduleType() == ScheduleType.SCHEDULED
                && event.scheduledAt() != null && event.scheduledAt().isAfter(now)) {
            latest = event.scheduledAt();
        }
        Instant deferUntil = decision.deferUntil();
        if (deferUntil != null && deferUntil.isAfter(now)
                && (latest == null || deferUntil.isAfter(latest))) {
            latest = deferUntil;
        }
        return latest;
    }

    /**
     * One row of {@code notif.scheduled_notification}.
     *
     * <p><strong>The whole dispatch event is serialised into the row.</strong> Not a pointer to
     * it: {@code ShardAffineClaimer} deserialises this string and publishes it unchanged, so the
     * claim loop performs no joins at 74,600 rows/s, and the event carries the same
     * {@code eventId} however many times the row is redelivered — which is what makes the
     * at-least-once duplicate the lease permits harmless rather than a second message.
     *
     * <p>The scheduled row's id <em>is</em> the recipient row's id. That makes triage trivial (a
     * DLQ record's {@code sourceKey} is a recipient you can look up) and gives the writer's
     * {@code ON CONFLICT DO NOTHING} something real to protect: a retried insert of the same
     * batch. It does not make re-expansion idempotent — a genuine re-expansion mints a new
     * recipient row and therefore a new id — and it is not meant to. That is the dedup key in
     * {@link RequestedEventListener}'s job.
     */
    private ScheduledNotificationWriter.ScheduledInsert park(NotificationRequestedEvent event,
                                                             NotificationEntity notification,
                                                             NotificationRecipient row,
                                                             Channel channel,
                                                             Instant dueAt,
                                                             NotificationDispatchEvent dispatch) {
        return new ScheduledNotificationWriter.ScheduledInsert(
                row.getId(),
                dueAt,
                // Hashed from the same (tenant, recipient, channel) triple that keys the dispatch
                // topics, so a recipient's deferred traffic spreads exactly like their live
                // traffic and no shard becomes the "everything scheduled for 09:00" shard.
                ScheduledNotificationWriter.shardFor(event.tenantId(), row.getId(), channel.name()),
                event.tenantId(),
                notification.getId(),
                notification.getCreatedAt(),
                row.getId(),
                channel.name(),
                event.trafficClass().name(),
                eventMapper.writeValueAsString(dispatch));
    }

    private NotificationEntity newNotification(NotificationRequestedEvent event, Channel channel, Instant now) {
        var notification = new NotificationEntity(
                UUID.randomUUID(), event.requestId(), event.tenantId(), channel,
                event.trafficClass(), event.expiresAt());
        notification.setCreatedAt(now);
        notification.setTemplateCode(event.templateCode());
        notification.setPriority(priorityValue(event.priority()));
        notification.setScheduledAt(event.scheduledAt());
        notification.setStatus(DeliveryStatus.QUEUED);
        notification.setStatusAt(now);
        notification.setTraceId(event.traceparent());
        return notification;
    }

    private NotificationRecipient newRecipient(NotificationRequestedEvent event,
                                               NotificationEntity notification,
                                               Channel channel,
                                               String userRef,
                                               RecipientAddressVault.SealedAddress sealed,
                                               Instant now) {
        var row = new NotificationRecipient(UUID.randomUUID(), notification.getId(),
                notification.getCreatedAt(), event.tenantId(), channel,
                sealed.cipher(), sealed.hash());
        row.setCreatedAt(now);
        row.setUserRef(userRef);
        row.setAddressHint(sealed.hint());
        row.setLastStatusAt(now);
        return row;
    }

    /**
     * A suppressed recipient still gets a row.
     *
     * <p>Not writing it would make "we chose not to send this" indistinguishable from "we never
     * heard about this recipient", and the two have opposite answers when a tenant asks why a user
     * did not receive a message. The address may be absent, in which case the placeholder bytes
     * satisfy the {@code NOT NULL} columns without inventing an address.
     */
    private NotificationRecipient suppressedRecipient(NotificationRequestedEvent event,
                                                      NotificationEntity notification,
                                                      Channel channel,
                                                      String userRef,
                                                      RecipientAddressVault.SealedAddress sealed,
                                                      SuppressionReason reason,
                                                      Instant now) {
        byte[] cipher = sealed == null ? new byte[0] : sealed.cipher();
        byte[] hash = sealed == null ? new byte[0] : sealed.hash();
        var row = new NotificationRecipient(UUID.randomUUID(), notification.getId(),
                notification.getCreatedAt(), event.tenantId(), channel, cipher, hash);
        row.setCreatedAt(now);
        row.setUserRef(userRef);
        row.setAddressHint(sealed == null ? null : sealed.hint());
        row.setStatus(DeliveryStatus.SUPPRESSED);
        row.setSuppressionReason(reason);
        row.setLastStatusAt(now);
        return row;
    }

    private NotificationDispatchEvent dispatchFor(NotificationRequestedEvent event,
                                                  NotificationEntity notification,
                                                  NotificationRecipient row,
                                                  Channel channel,
                                                  RecipientAddressVault.SealedAddress sealed,
                                                  TemplateRenderer.RenderedMessage rendered) {
        return new NotificationDispatchEvent(
                UUID.randomUUID(),
                clock.instant(),
                event.tenantId(),
                event.traceparent(),
                notification.getId(),
                notification.getCreatedAt(),
                row.getId(),
                event.requestId(),
                channel,
                event.trafficClass(),
                event.priority(),
                sealed.cipherBase64(),
                sealed.hint(),
                sealed.dekRef(),
                rendered.subject(),
                // TODO(phase-9): the body is not yet encrypted, because the vault seals addresses
                // only. It carries the same exposure as the address for the retention window.
                rendered.body(),
                null,
                event.templateCode(),
                // The base token. The per-attempt token is derived from it in the channel worker:
                // the attempt row's unique constraint needs a distinct value per attempt, while
                // the provider needs one that is stable across redeliveries of the same attempt.
                row.getId().toString(),
                null,
                1,
                event.expiresAt(),
                Map.of());
    }

    /**
     * {@code notification.priority} is a {@code smallint} with {@code CHECK (0..9)}, and the
     * domain enum is a four-value ladder. Spacing the values leaves room to insert a level without
     * a migration that rewrites every partition.
     */
    private static short priorityValue(Priority priority) {
        return switch (priority) {
            case P0_URGENT -> (short) 0;
            case P1_HIGH -> (short) 3;
            case P2_NORMAL -> (short) 5;
            case P3_LOW -> (short) 8;
        };
    }
}
