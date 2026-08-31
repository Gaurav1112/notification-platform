package dev.gaurav.notification.worker.status;

import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.persistence.entity.NotificationEvent;
import dev.gaurav.notification.persistence.repository.NotificationEventRepository;
import dev.gaurav.notification.persistence.repository.NotificationRecipientRepository;
import dev.gaurav.notification.persistence.repository.NotificationRepository;
import dev.gaurav.notification.worker.support.PartitionWindow;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.UUID;

/**
 * The monotonic guard, wired to the two rows it has to move and the log it has to write.
 *
 * <p>The guard itself is one SQL statement per row and lives in the repositories. What this class
 * adds is the three decisions around it:
 *
 * <p><strong>1. Zero rows is not an error.</strong> It means the event was stale, duplicated or
 * illegal — the expected outcome for an out-of-order webhook, which happens routinely at volume.
 * The event is recorded with {@code applied = false} and a counter is incremented. Those rows are
 * the most valuable debugging artefact in the system: they are the only place that can answer "why
 * is this stuck in SENT" with "three later signals arrived and every one was correctly discarded".
 *
 * <p><strong>2. The parent notification is advanced only through the dispatch ladder.</strong>
 * Applying a recipient's terminal outcome to the parent would mark a 10M-recipient campaign
 * {@code DELIVERED} the moment the first phone buzzed. Aggregate state belongs in the counters,
 * and the counters are deliberately not maintained with
 * {@code SET delivered_count = delivered_count + 1} — every worker in the fleet would serialise on
 * one row (§8.2). TODO(phase-10): Redis increment with a periodic flush.
 *
 * <p><strong>3. The dedup check races, and that is fine.</strong> The unique index on
 * {@code (dedup_hash, occurred_at)} is the real defence; the pre-check exists to keep the common
 * case out of the exception path. A lost race throws, the record is retried by the listener, and
 * the second pass sees the row.
 */
@Service
public class MonotonicDeliveryStatusService implements ApplyDeliveryStatusUseCase {

    private static final Logger log = LoggerFactory.getLogger(MonotonicDeliveryStatusService.class);

    private final NotificationRepository notifications;
    private final NotificationRecipientRepository recipients;
    private final NotificationEventRepository events;
    private final Clock clock;
    private final Counter appliedCounter;
    private final Counter unappliedCounter;
    private final Counter duplicateCounter;

    public MonotonicDeliveryStatusService(NotificationRepository notifications,
                                          NotificationRecipientRepository recipients,
                                          NotificationEventRepository events,
                                          Clock clock,
                                          MeterRegistry meters) {
        this.notifications = notifications;
        this.recipients = recipients;
        this.events = events;
        this.clock = clock;
        this.appliedCounter = Counter.builder("notification.status.applied")
                .description("status observations the monotonic guard accepted")
                .register(meters);
        this.unappliedCounter = Counter.builder("notification.status.unapplied")
                .description("status observations correctly rejected as stale, duplicate or illegal "
                        + "— expected traffic, not an error")
                .register(meters);
        this.duplicateCounter = Counter.builder("notification.status.duplicate")
                .description("status observations already recorded under the same dedup hash")
                .register(meters);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Result apply(Command command) {
        byte[] dedupHash = sha256(command.dedupSeed());
        var eventWindow = PartitionWindow.day(command.occurredAt());
        if (events.existsByDedupHashInWindow(dedupHash, eventWindow.from(), eventWindow.to())) {
            duplicateCounter.increment();
            return Result.alreadySeen();
        }

        var rowWindow = PartitionWindow.forDispatch(command.notificationCreatedAt());
        short rank = (short) command.status().rank();

        // The tenant travels on the command and goes into both guards as a predicate. Neither row
        // is addressable by id alone: a dispatch event or a webhook naming another tenant's
        // recipient now updates nothing, and because these updates are monotonic that is the only
        // place the check can live — once a forged terminal status lands, nothing can move the row
        // back and the genuine provider callback is refused as post-terminal.
        int recipientRows = recipients.applyStatusTransition(
                command.recipientId(), command.tenantId(), rowWindow.from(), rowWindow.to(),
                command.status().name(), rank, command.occurredAt());

        if (advancesParent(command.status())) {
            notifications.applyStatusTransition(
                    command.notificationId(), command.tenantId(), rowWindow.from(), rowWindow.to(),
                    command.status().name(), rank, command.occurredAt());
        }

        boolean applied = recipientRows > 0;
        record(command, dedupHash, applied);
        if (applied) {
            appliedCounter.increment();
        } else {
            unappliedCounter.increment();
            log.debug("status {} for recipient {} was stale or illegal and was recorded unapplied",
                    command.status(), command.recipientId());
        }
        return applied ? Result.accepted() : Result.stale();
    }

    /**
     * True for the statuses that describe <em>our</em> progress rather than one recipient's fate.
     *
     * <p>Everything up to and including {@code SENDING} is a fact about the notification as a
     * whole — it was queued, it was claimed, dispatch started. Everything above it is a fact about
     * one address.
     */
    private static boolean advancesParent(DeliveryStatus status) {
        return status.rank() <= DeliveryStatus.SENDING.rank();
    }

    private void record(Command command, byte[] dedupHash, boolean applied) {
        var row = new NotificationEvent(UUID.randomUUID(), command.occurredAt(), command.tenantId(),
                eventTypeOf(command.status()), command.source(), dedupHash);
        row.setRecordedAt(clock.instant());
        row.setNotificationId(command.notificationId());
        row.setRecipientId(command.recipientId());
        row.setProviderId(command.providerId());
        row.setToStatus(command.status());
        row.setApplied(applied);
        events.save(row);
    }

    /** {@code event_type} is {@code varchar(32)}; every status name fits well inside it. */
    private static String eventTypeOf(DeliveryStatus status) {
        return "status." + status.name().toLowerCase();
    }

    private static byte[] sha256(String seed) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(seed.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the JDK specification; absent means a broken runtime.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
