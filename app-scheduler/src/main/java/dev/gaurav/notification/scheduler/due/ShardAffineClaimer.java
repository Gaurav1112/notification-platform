package dev.gaurav.notification.scheduler.due;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import dev.gaurav.notification.messaging.config.KafkaProducerConfig;
import dev.gaurav.notification.messaging.event.NotificationDispatchEvent;
import dev.gaurav.notification.messaging.event.NotificationEvent;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.scheduler.config.NodeIdentity;
import dev.gaurav.notification.scheduler.config.SchedulerProperties;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Takes due work off this pod's own shards, leases it, and publishes it to the dispatch lane.
 *
 * <p><strong>The measured reason for shard affinity.</strong> 16 concurrent claimers, 3M READY
 * rows, 100 rows per transaction:
 *
 * <pre>
 *   FOR UPDATE (stampede)        159 tps    15,900 rows/s   100.5 ms avg
 *   FOR UPDATE SKIP LOCKED       453 tps    45,300 rows/s    35.3 ms avg
 *   shard-affine + SKIP LOCKED   746 tps    74,600 rows/s    21.5 ms avg
 * </pre>
 *
 * <p>The naive version raises no errors at all. It serialises, because all sixteen pods walk the
 * same index in the same order, and the symptom presents as "the database is slow" rather than as
 * a design fault — which is why the instinct is to add claimers, and why adding claimers makes it
 * worse. With affinity, two pods do not reach for the same row, so {@code SKIP LOCKED} has almost
 * nothing to skip; it earns its place only during a rebalance window, and that narrow role is the
 * whole 746-versus-453 gap.
 *
 * <p><strong>The transaction never spans the network call.</strong> Claim commits, then the publish
 * happens, then a second short transaction records the outcome. Holding the claim transaction open
 * across a 10-second Kafka timeout would pin {@code xmin}, and a pinned {@code xmin} stops
 * autovacuum from reclaiming the twenty million dead tuples a day that status updates generate.
 * That is the failure that takes the cluster down at 3 a.m., and it always starts as a convenience.
 *
 * <p><strong>A failed publish is a no-op, never a partial state.</strong> If the produce fails, the
 * row simply stays CLAIMED, its lease expires, and {@link LeaseReaper} returns it to READY. There
 * is no compensating write to get wrong.
 */
@Component
public class ShardAffineClaimer {

    private static final Logger log = LoggerFactory.getLogger(ShardAffineClaimer.class);

    private final ScheduledWorkStore store;
    private final DueIndex dueIndex;
    private final ShardAssignment shards;
    private final NotificationEventPublisher publisher;
    private final JsonMapper mapper;
    private final NodeIdentity node;
    private final SchedulerProperties.Claimer settings;
    private final Duration publishTimeout;
    private final Clock clock;
    private final MeterRegistry meters;

    /**
     * {@code now − dueAt} at the moment of dispatch. The SLO is p99 &lt; 30 s, and the histogram
     * buckets for it are declared in {@code application.yml}; the burn-rate rule has no series to
     * read unless this timer is recorded under exactly this name.
     */
    private final Timer dispatchLag;

    public ShardAffineClaimer(ScheduledWorkStore store, DueIndex dueIndex, ShardAssignment shards,
                              NotificationEventPublisher publisher,
                              @Qualifier(KafkaProducerConfig.EVENT_JSON_MAPPER) JsonMapper mapper,
                              NodeIdentity node, SchedulerProperties properties, Clock clock,
                              MeterRegistry meters) {
        this.store = store;
        this.dueIndex = dueIndex;
        this.shards = shards;
        this.publisher = publisher;
        this.mapper = mapper;
        this.node = node;
        this.settings = properties.claimer();
        this.publishTimeout = properties.dispatch().publishTimeout();
        this.clock = clock;
        this.meters = meters;
        this.dispatchLag = Timer.builder("scheduler.dispatch.lag")
                .description("now - scheduled_at at the moment the dispatch event is produced")
                .publishPercentileHistogram()
                .register(meters);
    }

    /**
     * One pass over every shard this pod owns.
     *
     * <p>100 ms, because the near-horizon lives in Redis and a {@code ZRANGEBYSCORE} against a
     * sorted set costs the database nothing. Polling PostgreSQL this often with this many pods is
     * the design that produced the 159-tps number above.
     */
    @Scheduled(fixedDelayString = "${notification.scheduler.claimer.poll-interval:100ms}")
    public void claimDueWork() {
        for (int shard : shards.ownedShards()) {
            try {
                claimShard(shard);
            } catch (RuntimeException e) {
                // One bad shard must not stop the other 255. The loop is the only thing standing
                // between a transient failure on one shard and a pod that schedules nothing.
                meters.counter("scheduler.claimer.errors", "shard", Integer.toString(shard)).increment();
                log.warn("claim pass failed for shard {}", shard, e);
            }
        }
    }

    private void claimShard(int shard) {
        var now = clock.instant();
        List<UUID> candidates = dueIndex.poll(shard, now, settings.batchSize());
        if (candidates.isEmpty()) {
            return;
        }

        var leaseExpiresAt = now.plus(settings.leaseDuration());
        List<ScheduledWork> claimed = store.claim(shard, candidates, node.instanceId(),
                now, leaseExpiresAt);

        // Trim the whole polled window, not just what was claimed. See DueIndex#remove.
        dueIndex.remove(shard, candidates);
        if (claimed.isEmpty()) {
            return;
        }

        var dispatched = new ArrayList<UUID>(claimed.size());
        for (var work : claimed) {
            if (publish(work, now)) {
                dispatched.add(work.id());
            }
        }

        if (!dispatched.isEmpty()) {
            int marked = store.markDispatched(dispatched, node.instanceId());
            if (marked != dispatched.size()) {
                // We published but could not record it: the lease was lost mid-flight, so the
                // reaper will hand the row to another pod and the message goes out twice. Safe —
                // the idempotent receiver and the monotonic guard absorb it — but it is the one
                // duplicate this design knowingly creates, so it must be visible.
                meters.counter("scheduler.claimer.lease.lost")
                        .increment(dispatched.size() - (double) marked);
                log.warn("published {} rows on shard {} but only recorded {}; the difference will "
                        + "be redelivered by another claimer", dispatched.size(), shard, marked);
            }
            meters.counter("scheduler.claimer.dispatched").increment(dispatched.size());
        }
    }

    /**
     * Publishes one row's pre-built dispatch event.
     *
     * <p>The event was serialised into the row when the work was scheduled, so nothing is rebuilt
     * here. That is what keeps the claim loop free of joins at 74,600 rows/s, and it also means the
     * event carries the same {@code eventId} however many times the row is redelivered — which is
     * what makes the duplicate above harmless.
     *
     * @return true when the broker acknowledged
     */
    private boolean publish(ScheduledWork work, Instant now) {
        NotificationDispatchEvent event;
        try {
            NotificationEvent parsed = mapper.readValue(work.payload(), NotificationEvent.class);
            if (!(parsed instanceof NotificationDispatchEvent dispatch)) {
                throw new IllegalStateException(
                        "scheduled payload is a " + parsed.eventType() + ", not a dispatch");
            }
            event = dispatch;
        } catch (JacksonException | IllegalStateException e) {
            // Unparseable, and it will be unparseable on every retry. Leaving the lease to expire
            // hands it straight back to a claimer, which is the crash loop LeaseReaper's poison
            // detector exists to break — so let the lease expire and let the counter do its job.
            meters.counter("scheduler.claimer.unparseable").increment();
            log.error("scheduled row {} has an unusable payload; leaving the lease to expire so "
                    + "the poison-pill detector can see it", work.id(), e);
            return false;
        }

        if (event.isExpiredAt(now)) {
            // A 40-minute-old OTP is worse than no OTP: the user has already asked for another.
            // Marking it dispatched without publishing is deliberate — it stops the reaper from
            // resurrecting work whose TTL has already gone.
            meters.counter("scheduler.claimer.expired").increment();
            log.info("dropping scheduled work {} — its TTL elapsed at {}", work.id(), event.expiresAt());
            return true;
        }

        try {
            publisher.publishDispatch(event).get(publishTimeout.toMillis(), TimeUnit.MILLISECONDS);
            dispatchLag.record(Duration.between(work.dueAt(), now));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            meters.counter("scheduler.claimer.publish.failed").increment();
            log.warn("could not publish scheduled work {} to {}; the lease will expire and another "
                    + "claimer will retry", work.id(), work.dispatchTopic(), e);
            return false;
        }
    }
}
