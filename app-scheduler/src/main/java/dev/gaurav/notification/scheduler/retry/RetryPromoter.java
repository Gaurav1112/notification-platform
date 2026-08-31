package dev.gaurav.notification.scheduler.retry;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import dev.gaurav.notification.domain.enums.Priority;
import dev.gaurav.notification.messaging.event.NotificationDispatchEvent;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.persistence.entity.NotificationEntity;
import dev.gaurav.notification.persistence.entity.NotificationRecipient;
import dev.gaurav.notification.persistence.repository.NotificationRecipientRepository;
import dev.gaurav.notification.persistence.repository.NotificationRepository;
import dev.gaurav.notification.scheduler.config.SchedulerProperties;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The database-side backstop for retries that the Kafka tiers dropped.
 *
 * <p><strong>Retrying is not this class's job.</strong> A transiently failed dispatch is parked on
 * one of the five delay tiers — {@code notification.retry.5s} through {@code .1h} — and the
 * <em>worker</em> consumes them. Tiers exist because sleeping in a consumer holds the partition:
 * every record behind the sleeper waits, and a sleep longer than {@code max.poll.interval.ms}
 * evicts the consumer, rebalances the group and reassigns the same record, which is a rebalance
 * storm that looks like perfect broker health.
 *
 * <p>What this class covers is the gap the tiers cannot: a topic that lost its records inside the
 * retention window, a consumer group whose offsets were reset past the parked messages, a worker
 * deployment that was down long enough for a tier's records to age out, a {@code next_attempt_at}
 * written by a worker that died before it produced the retry event. In every one of those cases the
 * row in PostgreSQL is the only remaining evidence that a retry was owed. Without this sweep the
 * recipient simply never hears from us and nothing anywhere records that.
 *
 * <p><strong>The grace period is what stops this from being a duplicate generator.</strong> Rows
 * are only touched once they are {@link SchedulerProperties.Retry#grace()} past due. Sweeping at
 * {@code next_attempt_at} exactly would race the tier consumer on every ordinary retry in the
 * platform and publish a second copy of work that was never actually missed.
 *
 * <p>The tier delays themselves do not need to be added to the grace: {@code next_attempt_at} is
 * the instant the retry is <em>supposed to fire</em>, not the instant it was parked, so a message
 * sitting on the 1-hour tier is not "late" until an hour after it was parked. What the grace has to
 * cover is only the tier consumer's own lag between that instant and the republish — and two
 * minutes is far more than that on any tier.
 *
 * <p>In a healthy system this sweep finds nothing. That is the point, and it is also why the
 * per-row lookup of the parent notification is acceptable here and would not be on the claim path.
 */
@Component
public class RetryPromoter {

    /**
     * Daily partitions of {@code notification_recipient} to scan.
     *
     * <p>Four days: the longest TTL in the platform is BULK's 72 hours, plus a day of slack for a
     * row created just before a boundary. A row older than that cannot have a live retry owed, and
     * widening the window turns a five-partition scan into a scan of the whole retention range.
     */
    private static final int RECIPIENT_PARTITION_DAYS = 4;

    private static final Logger log = LoggerFactory.getLogger(RetryPromoter.class);

    private final NotificationRecipientRepository recipients;
    private final NotificationRepository notifications;
    private final NotificationEventPublisher publisher;
    private final SchedulerProperties.Retry settings;
    private final Duration publishTimeout;
    private final Clock clock;
    private final MeterRegistry meters;

    public RetryPromoter(NotificationRecipientRepository recipients,
                         NotificationRepository notifications,
                         NotificationEventPublisher publisher,
                         SchedulerProperties properties,
                         Clock clock,
                         MeterRegistry meters) {
        this.recipients = recipients;
        this.notifications = notifications;
        this.publisher = publisher;
        this.settings = properties.retry();
        this.publishTimeout = properties.dispatch().publishTimeout();
        this.clock = clock;
        this.meters = meters;
    }

    /**
     * One sweep.
     *
     * <p>Runs on every replica. Two promoters finding the same row publish the same event with the
     * same {@code eventId} (see {@link #eventIdFor}), and the idempotent receiver drops the second
     * — so electing a leader would buy nothing and add a way for the backstop itself to be down.
     */
    @Scheduled(fixedDelayString = "${notification.scheduler.retry.sweep-interval:60s}")
    public void promoteMissedRetries() {
        var now = clock.instant();
        var dueBy = now.minus(settings.grace());
        var notBefore = now.minus(settings.lookback());
        var createdFrom = now.minus(RECIPIENT_PARTITION_DAYS, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS);
        var createdTo = now.plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS);

        var late = recipients.findDueForRetry(notBefore, dueBy, createdFrom, createdTo,
                settings.batchSize());
        if (late.isEmpty()) {
            return;
        }

        // Not DEBUG. A non-empty result means the Kafka retry path failed to do its job, which is
        // a fact worth a line in the log even when the backstop handles it cleanly.
        log.warn("{} recipient rows are more than {} past next_attempt_at; the retry tiers missed "
                + "them and the database sweep is republishing", late.size(), settings.grace());

        int promoted = 0;
        for (var recipient : late) {
            if (promote(recipient, now)) {
                promoted++;
            }
        }
        meters.counter("scheduler.retry.promoted").increment(promoted);
    }

    private boolean promote(NotificationRecipient recipient, Instant now) {
        var notificationCreated = recipient.getNotificationCreatedAt();
        // The sweep itself is cross-tenant by design, but this walk is not: the tenant comes from
        // the recipient row we already hold, so a recipient whose notification_id points at another
        // tenant's row — corruption, or a replayed event that wrote a bad reference — resolves to
        // nothing and is reported as orphaned rather than rebuilt into a dispatch for that tenant.
        var parent = notifications.findInWindow(recipient.getNotificationId(),
                recipient.getTenantId(),
                notificationCreated.truncatedTo(ChronoUnit.DAYS),
                notificationCreated.truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.DAYS));

        if (parent.isEmpty()) {
            // The recipient row outlived its parent's partition. Nothing can be rebuilt from it,
            // and re-reading it every minute forever is worse than saying so once per sweep.
            meters.counter("scheduler.retry.orphaned").increment();
            log.error("recipient {} references notification {} which is not in its partition; "
                    + "cannot rebuild a dispatch for it",
                    recipient.getId(), recipient.getNotificationId());
            return false;
        }

        var notification = parent.get();
        if (!now.isBefore(notification.getExpiresAt())) {
            // Sending now would cost a provider call to deliver something the user no longer
            // wants. A 40-minute-old one-time passcode is worse than no passcode.
            meters.counter("scheduler.retry.expired").increment();
            return false;
        }

        try {
            publisher.publishDispatch(toDispatch(recipient, notification, now))
                    .get(publishTimeout.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            // The row keeps its next_attempt_at, so the next sweep finds it again. Nothing is
            // written here, so there is no half-applied state to unwind.
            meters.counter("scheduler.retry.publish.failed").increment();
            log.warn("could not republish missed retry for recipient {}", recipient.getId(), e);
            return false;
        }
    }

    private static NotificationDispatchEvent toDispatch(NotificationRecipient recipient,
                                                        NotificationEntity notification,
                                                        Instant now) {
        int attemptNumber = recipient.getAttemptCount() + 1;
        return new NotificationDispatchEvent(
                eventIdFor(recipient.getId(), attemptNumber),
                now,
                recipient.getTenantId(),
                null,
                recipient.getNotificationId(),
                recipient.getNotificationCreatedAt(),
                recipient.getId(),
                notification.getRequestId(),
                recipient.getChannel(),
                notification.getTrafficClass(),
                // The column is a 0–9 smallint for future headroom and does not map onto the
                // four-value enum. Rebuilding it here would be a guess; the tie-break only
                // matters inside an already-shallow lane, so the default is honest.
                Priority.P2_NORMAL,
                Base64.getEncoder().encodeToString(recipient.getAddressCipher()),
                recipient.getAddressHint(),
                null,
                null,
                // No body: the worker re-renders from the template. Carrying a stale rendered body
                // across a sweep would deliver content from before the last template change.
                null,
                null,
                notification.getTemplateCode(),
                idempotencyTokenFor(recipient.getId(), attemptNumber),
                null,
                attemptNumber,
                notification.getExpiresAt(),
                Map.of());
    }

    /**
     * Deterministic from {@code (recipientId, attemptNumber)}.
     *
     * <p>Two sweeps — or two replicas sweeping at once — therefore produce the identical
     * {@code eventId} and the idempotent receiver drops the duplicate. A random id here would make
     * every sweep of an unfixed row a fresh message, and a backlog the tiers already lost would be
     * amplified rather than recovered.
     *
     * <p>It deliberately does <em>not</em> match the id a tier retry would carry: the tier keeps
     * the original event's id, and this path has no way to know it. That gap is what the grace
     * period covers.
     */
    private static UUID eventIdFor(UUID recipientId, int attemptNumber) {
        return UUID.nameUUIDFromBytes(
                ("retry-promoted:" + recipientId + ":" + attemptNumber).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Stable across every retry of the same logical attempt, which is what makes provider-level
     * idempotency work and what the {@code da_token_uk} unique index on {@code delivery_attempt}
     * relies on. Regenerating it per publish would create a second attempt row for one send and
     * defeat both layers.
     */
    private static String idempotencyTokenFor(UUID recipientId, int attemptNumber) {
        return recipientId + "#" + attemptNumber;
    }
}
