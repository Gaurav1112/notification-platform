package dev.gaurav.notification.scheduler.due;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.scheduler.config.LeaderElection;
import dev.gaurav.notification.scheduler.config.Leadership;
import dev.gaurav.notification.scheduler.config.NodeIdentity;
import dev.gaurav.notification.scheduler.config.SchedulerProperties;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The single writer of the near-horizon: one elected pod range-scans PostgreSQL for work due in the
 * next five minutes and pushes it into the per-shard Redis sorted sets.
 *
 * <p><strong>Why this stage is leader-elected while claiming is not.</strong> {@code FOR UPDATE
 * SKIP LOCKED} is the standard answer to concurrent queue consumers, and it is the right answer for
 * the <em>claim</em>. It is the wrong answer for the <em>scan</em>, because it fixes correctness and
 * serialisation without fixing bloat. PlanetScale measured the recursive-CTE and {@code SKIP
 * LOCKED} patterns side by side and found "degradation curves almost identical". A documented
 * pgsql-general case hit a hard wall at <strong>128 concurrent claimers on 80 cores</strong>;
 * Thomas Munro's diagnosis was that each session must skip every dead or non-matching tuple left
 * behind at the start of the table by all the other sessions, and "it all gets a bit explosive".
 * Wasted index visits scale as <strong>{@code B·W²/2}</strong> in batch size {@code B} and worker
 * count {@code W} — so the intuitive fix, raising the batch size to do more per pass, multiplies
 * the quadratic term. It is precisely the wrong lever.
 *
 * <p>So: the scan runs on <em>one</em> pod, from one session, as a plain read that produces no dead
 * tuples at all. {@code SKIP LOCKED} is reserved for {@link ScheduledWorkStore#claim}, where
 * contention is genuinely useful because shard affinity has already made it rare. River and Oban
 * both arrived at the same split.
 *
 * <p><strong>Losing the leader is a delay, not a loss.</strong> Rows stay READY in PostgreSQL until
 * a claimer takes them, so a hydrator that dies mid-pass costs at most one scan interval of
 * scheduling. Nothing is in flight in this class that a successor cannot redo.
 *
 * <p><strong>Jitter is applied here, to the Redis score only.</strong> The database row is never
 * updated — {@code due_at} feeds the immutable partition key, and moving it would be a
 * cross-partition row move at triple the WAL cost. Spreading in the index instead is free, and it
 * is deterministic, so re-hydrating the same row produces the same instant. See
 * {@link ScheduleJitter} for the 20,833/s → 4,167/s arithmetic that motivates it.
 */
@Component
public class DueScanHydrator {

    private static final Logger log = LoggerFactory.getLogger(DueScanHydrator.class);

    private final ScheduledWorkStore store;
    private final DueIndex dueIndex;
    private final LeaderElection election;
    private final NodeIdentity node;
    private final SchedulerProperties.Hydrator settings;
    private final Clock clock;
    private final MeterRegistry meters;
    private final Timer scanTimer;

    /** 1 when this pod is the hydrator. Summed across the fleet it must always read exactly 1. */
    private volatile boolean leading;

    /**
     * Synthetic leadership for single-node development, where {@code leader-election: false}.
     *
     * <p>Token 1 is the lowest a real election can ever issue, so a cluster that later turns
     * election on fences this one out rather than the other way round. That ordering matters: the
     * safe mistake is the development node losing, not the elected leader losing.
     */
    private final Leadership unelected;

    public DueScanHydrator(ScheduledWorkStore store, DueIndex dueIndex, LeaderElection election,
                           NodeIdentity node, SchedulerProperties properties, Clock clock,
                           MeterRegistry meters) {
        this.store = store;
        this.dueIndex = dueIndex;
        this.election = election;
        this.node = node;
        this.settings = properties.hydrator();
        this.clock = clock;
        this.meters = meters;
        this.unelected = new Leadership(settings.leaseName(), node.instanceId(), 1L,
                clock.instant().plus(Duration.ofDays(3650)));
        this.scanTimer = Timer.builder("scheduler.hydrator.scan")
                .description("Time to range-scan the due horizon and push it into Redis")
                .register(meters);
        Gauge.builder("scheduler.hydrator.leader", this, h -> h.leading ? 1d : 0d)
                .description("1 on the elected hydrator. The sum across the fleet is always 1.")
                .register(meters);
    }

    /**
     * One hydration pass.
     *
     * <p>The leadership campaign is inside the tick rather than on its own renewal thread. A
     * separate renewer keeps advertising a healthy leader while the scan thread is wedged on a
     * hung query — the lease never expires, and a replica that could make progress never gets the
     * chance. Renewing only when a pass actually starts makes a stuck hydrator lose the lease,
     * which is the behaviour that heals.
     */
    @Scheduled(fixedDelayString = "${notification.scheduler.hydrator.scan-interval:5s}")
    public void hydrate() {
        var leadership = settings.leaderElection()
                ? election.campaign(settings.leaseName(), settings.leaseDuration()).orElse(null)
                : unelected;
        leading = leadership != null;
        if (leadership == null) {
            return;
        }
        scanTimer.record(() -> scanAndPush(leadership));
    }

    private void scanAndPush(Leadership leadership) {
        var now = clock.instant();
        var horizonEnd = now.plus(settings.horizon());

        List<ScheduledWork> due = store.scanReady(horizonEnd, settings.batchSize());
        if (due.isEmpty()) {
            return;
        }

        Map<Integer, List<DueIndex.ScoredWork>> byShard = new HashMap<>();
        for (var work : due) {
            byShard.computeIfAbsent(work.shard(), s -> new ArrayList<>())
                    .add(new DueIndex.ScoredWork(work.id(), scoreOf(work)));
        }

        long added = 0;
        for (var entry : byShard.entrySet()) {
            long result = dueIndex.hydrate(entry.getKey(), leadership.fencingToken(), entry.getValue());
            if (result == DueIndex.FENCED_OUT) {
                // We paused past our lease and a new leader has already written this shard. Every
                // further write in this pass would be a zombie write; stand down immediately
                // rather than finishing the loop.
                meters.counter("scheduler.hydrator.fenced").increment();
                log.error("hydrator {} was fenced out of shard {} with token {}; a newer leader "
                        + "owns the horizon. Standing down.",
                        node.instanceId(), entry.getKey(), leadership.fencingToken());
                election.resign(settings.leaseName());
                leading = false;
                return;
            }
            added += result;
        }

        meters.counter("scheduler.hydrator.scanned").increment(due.size());
        meters.counter("scheduler.hydrator.indexed").increment(added);
        log.debug("hydrated {} of {} scanned rows across {} shards up to {}",
                added, due.size(), byShard.size(), horizonEnd);
    }

    /**
     * The sorted-set score: the due instant, jittered, in epoch millis.
     *
     * <p>CRITICAL is exempt. Its dispatch objective is five seconds and the jitter window is five
     * minutes — applying it there would miss the objective by two orders of magnitude to solve a
     * thundering-herd problem that lane does not have, because nobody schedules an OTP for 09:00.
     */
    private static long scoreOf(ScheduledWork work) {
        if (work.trafficClass() == TrafficClass.CRITICAL) {
            return work.dueAt().toEpochMilli();
        }
        return ScheduleJitter.apply(work.id(), work.dueAt()).toEpochMilli();
    }
}
