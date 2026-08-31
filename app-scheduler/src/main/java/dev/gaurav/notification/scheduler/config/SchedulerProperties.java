package dev.gaurav.notification.scheduler.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Everything under {@code notification.scheduler}, typed.
 *
 * <p>Records rather than mutable beans, so a value cannot be changed after binding — a scan
 * horizon or a lease duration that shifts at runtime is a correctness bug wearing a config
 * disguise. Defaults live in the compact constructors so a missing YAML section boots with the
 * production numbers rather than with nulls.
 *
 * <p>Note what is <em>not</em> configurable: {@link dev.gaurav.notification.scheduler.due.ShardAssignment#SHARD_COUNT}
 * and the jitter window in {@link dev.gaurav.notification.scheduler.due.ScheduleJitter}. Both are
 * baked into data already written — change the shard count and every hydrated Redis key points at
 * the wrong claimer; change the jitter window and the same row moves, which breaks the determinism
 * the whole design rests on. Constants make that a code review, not a config push.
 */
@ConfigurationProperties("notification.scheduler")
public record SchedulerProperties(Hydrator hydrator, Claimer claimer, Dispatch dispatch,
                                  Outbox outbox, Retry retry, Partition partition) {

    public SchedulerProperties {
        hydrator = hydrator == null ? new Hydrator(null, null, null, null, null, null, null) : hydrator;
        claimer = claimer == null ? new Claimer(null, null, null, null) : claimer;
        dispatch = dispatch == null ? new Dispatch(null, null, null) : dispatch;
        outbox = outbox == null ? new Outbox(null, null) : outbox;
        retry = retry == null ? new Retry(null, null, null) : retry;
        partition = partition == null ? new Partition(null, null) : partition;
    }

    /**
     * The leader-elected due scan.
     *
     * @param leaderElection    off only in single-node development. On, this is the single-writer
     *                          guarantee described in {@link LeaderElection}
     * @param leaseName         the Redis lease key suffix
     * @param leaseDuration     how long the lease survives a dead leader. Also the worst-case gap
     *                          in hydration after an ungraceful pod loss
     * @param leaseRenewInterval unused by the algorithm — renewal is tied to the scan tick on
     *                          purpose (see {@link LeaderElection#campaign}) — but bound so the
     *                          existing YAML key does not silently become dead config
     * @param horizon           how far ahead of now rows are pushed into Redis. Wide enough that a
     *                          hydrator restart is invisible, narrow enough that a cancellation
     *                          still lands before dispatch
     * @param scanInterval      tick period
     * @param batchSize         rows per scan pass. Raising this to "improve efficiency" is the
     *                          wrong lever on a contended claim — it multiplies the {@code B·W²/2}
     *                          term — but the scan is single-writer, so here it is simply a
     *                          throughput knob
     */
    public record Hydrator(Boolean leaderElection, String leaseName, Duration leaseDuration,
                           Duration leaseRenewInterval, Duration horizon, Duration scanInterval,
                           Integer batchSize) {
        public Hydrator {
            leaderElection = leaderElection == null || leaderElection;
            leaseName = leaseName == null ? "notification-hydrator" : leaseName;
            leaseDuration = leaseDuration == null ? Duration.ofSeconds(30) : leaseDuration;
            leaseRenewInterval = leaseRenewInterval == null ? Duration.ofSeconds(10) : leaseRenewInterval;
            horizon = horizon == null ? Duration.ofMinutes(5) : horizon;
            scanInterval = scanInterval == null ? Duration.ofSeconds(5) : scanInterval;
            batchSize = batchSize == null ? 1000 : batchSize;
        }
    }

    /**
     * The shard-affine claimers.
     *
     * @param shards          shards this pod owns when membership cannot be discovered; 256 / 32
     *                        replicas = 8
     * @param leaseDuration   the correctness boundary. A pod OOM-killed mid-dispatch releases its
     *                        work after this long, which is what makes the system at-least-once
     * @param batchSize       rows claimed per pass per shard
     * @param maxClaimsPerRow the poison-pill threshold; see {@link dev.gaurav.notification.scheduler.due.LeaseReaper}
     */
    public record Claimer(Integer shards, Duration leaseDuration, Integer batchSize,
                          Integer maxClaimsPerRow) {
        public Claimer {
            shards = shards == null ? 8 : shards;
            leaseDuration = leaseDuration == null ? Duration.ofSeconds(60) : leaseDuration;
            batchSize = batchSize == null ? 200 : batchSize;
            maxClaimsPerRow = maxClaimsPerRow == null ? 5 : maxClaimsPerRow;
        }
    }

    /**
     * @param jitter                spread applied to a campaign's dispatch instants
     * @param avoidQuarterHourMarks FCM documents avoiding sends within two minutes of :00/:15/:30/:45
     * @param publishTimeout        how long a claim pass waits on the Kafka produce before giving
     *                              up and letting the lease expire
     */
    public record Dispatch(Duration jitter, Boolean avoidQuarterHourMarks, Duration publishTimeout) {
        public Dispatch {
            jitter = jitter == null ? Duration.ofSeconds(90) : jitter;
            avoidQuarterHourMarks = avoidQuarterHourMarks == null || avoidQuarterHourMarks;
            publishTimeout = publishTimeout == null ? Duration.ofSeconds(10) : publishTimeout;
        }
    }

    /**
     * @param batchSize rows drained per sweep. Sized against the publish round trip, not the
     *                  backlog: the sweep holds row locks for the duration of the produce
     * @param maxAge    the age at which an unpublished row is an incident rather than lag
     */
    public record Outbox(Integer batchSize, Duration maxAge) {
        public Outbox {
            batchSize = batchSize == null ? 256 : batchSize;
            maxAge = maxAge == null ? Duration.ofSeconds(120) : maxAge;
        }
    }

    /**
     * @param grace     how far past {@code next_attempt_at} a row must be before this sweep touches
     *                  it. Without a grace period the sweep races the Kafka retry tiers and
     *                  publishes a second copy of work that was never actually missed
     * @param lookback  how far back the sweep looks. Bounds the index scan; a sweep with no lower
     *                  bound gets slower as the backlog it exists to drain gets deeper
     * @param batchSize rows per sweep
     */
    public record Retry(Duration grace, Duration lookback, Integer batchSize) {
        public Retry {
            grace = grace == null ? Duration.ofMinutes(2) : grace;
            lookback = lookback == null ? Duration.ofHours(6) : lookback;
            batchSize = batchSize == null ? 500 : batchSize;
        }
    }

    /**
     * @param daysAhead how many days of partitions to premake. Production uses pg_partman with
     *                  {@code premake = 14}; this job is the local and CI equivalent
     * @param canaryAt  the UTC hour by which tomorrow's partition must exist or the canary fires
     */
    public record Partition(Integer daysAhead, Integer canaryAt) {
        public Partition {
            daysAhead = daysAhead == null ? 14 : daysAhead;
            canaryAt = canaryAt == null ? 12 : canaryAt;
        }
    }
}
