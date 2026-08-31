package dev.gaurav.notification.scheduler.due;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.messaging.event.DeadLetterEvent;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.scheduler.config.SchedulerProperties;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Returns abandoned leases to the ready set, and takes poison rows out of circulation.
 *
 * <p><strong>Why the lease, and not the lock, is the correctness boundary.</strong> A row lock
 * lives until the transaction ends, and the claim transaction ends before the Kafka publish
 * deliberately (see {@link ShardAffineClaimer}). Between the commit and the publish the row belongs
 * to a pod that may be OOM-killed, evicted, or partitioned away. Nothing releases it. The
 * 60-second lease is what bounds that: a pod that dies mid-dispatch gives the work back in at most
 * a minute. This is exactly what makes the platform at-least-once rather than at-most-once, and it
 * is why provider-level idempotency is not optional.
 *
 * <p><strong>{@code claim_count > 5} is a poison-pill detector, not a retry counter.</strong> The
 * count is incremented here, on reclaim — never on a successful claim — so it counts one specific
 * thing: the number of times taking this row ended with the pod that took it failing to finish.
 * A row that does that repeatedly is not unlucky. It is killing the process that touches it, and
 * without this branch it does so forever: claim, crash, lease expires, reclaim, claim, crash. The
 * pod restarts cleanly each time, the consumer group looks healthy, throughput for that shard is
 * zero, and no metric anywhere counts the loop. Routing to the DLQ is the only thing that both
 * stops the loop and makes it visible.
 *
 * <p><strong>Order matters: dead-letter first, abandon second.</strong> Marking the row FAILED
 * before the DLQ record is acknowledged would make it disappear from the ready set, from the
 * expired-lease sweep and from the triage queue simultaneously — the exact invisibility this class
 * exists to end.
 */
@Component
public class LeaseReaper {

    /** Not a Java exception type: no code throws this. It is the DLQ triage label for this cause. */
    private static final String POISON_LABEL = "ScheduleClaimCountExceeded";

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final ScheduledWorkStore store;
    private final NotificationEventPublisher publisher;
    private final SchedulerProperties.Claimer settings;
    private final Duration publishTimeout;
    private final Clock clock;
    private final MeterRegistry meters;

    public LeaseReaper(ScheduledWorkStore store, NotificationEventPublisher publisher,
                       SchedulerProperties properties, Clock clock, MeterRegistry meters) {
        this.store = store;
        this.publisher = publisher;
        this.settings = properties.claimer();
        this.publishTimeout = properties.dispatch().publishTimeout();
        this.clock = clock;
        this.meters = meters;
    }

    /**
     * One reap pass.
     *
     * <p>30 seconds against a 60-second lease: two passes inside every lease window, so a single
     * missed run — a long GC, a slow partition job hogging the scheduler pool — still leaves the
     * work reclaimed before a second lease would have elapsed.
     *
     * <p>Runs on every replica, unelected. Reclaiming is idempotent and the statement re-checks
     * {@code claim_expires_at < now}, so two reapers racing produce one reclaim and one no-op.
     * Electing a leader here would add a failure mode to buy nothing.
     */
    @Scheduled(fixedDelayString = "${notification.scheduler.claimer.reap-interval:30s}")
    public void reap() {
        var now = clock.instant();
        List<ScheduledWork> expired = store.findExpiredLeases(now, settings.batchSize());
        if (expired.isEmpty()) {
            return;
        }

        var reclaimable = new ArrayList<UUID>(expired.size());
        var poison = new ArrayList<ScheduledWork>();
        for (var work : expired) {
            if (work.claimCount() > settings.maxClaimsPerRow()) {
                poison.add(work);
            } else {
                reclaimable.add(work.id());
            }
        }

        if (!reclaimable.isEmpty()) {
            int reclaimed = store.reclaim(reclaimable, now);
            meters.counter("scheduler.lease.expired").increment(reclaimed);
            log.info("reclaimed {} expired leases", reclaimed);
        }

        if (!poison.isEmpty()) {
            deadLetter(poison, now);
        }
    }

    private void deadLetter(List<ScheduledWork> poison, Instant now) {
        var parked = new ArrayList<UUID>(poison.size());
        for (var work : poison) {
            try {
                publisher.publishDeadLetter(toDeadLetter(work, now))
                        .get(publishTimeout.toMillis(), TimeUnit.MILLISECONDS);
                parked.add(work.id());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Leave it CLAIMED-and-expired. The next pass tries again; the row stays visible
                // in the expired-lease sweep, which is the state we want it stuck in.
                log.error("could not dead-letter poison row {} (claim_count={}); it stays in the "
                        + "expired-lease sweep until the DLQ accepts it",
                        work.id(), work.claimCount(), e);
            }
        }

        if (!parked.isEmpty()) {
            store.abandon(parked);
            meters.counter("scheduler.claim.count.exceeded").increment(parked.size());
            log.error("{} scheduled rows exceeded {} claims and were routed to the DLQ — each one "
                    + "has killed a pod every time it was claimed",
                    parked.size(), settings.maxClaimsPerRow());
        }
    }

    /**
     * Builds the triage record.
     *
     * <p>{@code sourcePartition} carries the scheduler shard and {@code sourceOffset} is zero: this
     * record never came from a Kafka partition, and inventing plausible-looking coordinates would
     * send whoever triages it to seek an offset that does not exist. {@code sourceKey} is the
     * scheduled-work id, which is the coordinate that does.
     */
    private static DeadLetterEvent toDeadLetter(ScheduledWork work, Instant now) {
        return new DeadLetterEvent(
                UUID.randomUUID(),
                now,
                work.tenantId(),
                null,
                work.dispatchTopic(),
                work.shard(),
                0L,
                work.id().toString(),
                "notification-scheduler",
                work.notificationId(),
                work.recipientId(),
                FailureType.PERMANENT_UNKNOWN,
                POISON_LABEL,
                "claim_count %d exceeded the limit of claims that ended in an expired lease"
                        .formatted(work.claimCount()),
                null,
                work.claimCount(),
                0,
                work.payload());
    }
}
