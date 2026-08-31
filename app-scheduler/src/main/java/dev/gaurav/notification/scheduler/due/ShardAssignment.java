package dev.gaurav.notification.scheduler.due;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.IntStream;

import dev.gaurav.notification.persistence.schedule.ScheduledNotificationWriter;
import dev.gaurav.notification.scheduler.config.NodeIdentity;
import dev.gaurav.notification.scheduler.config.SchedulerProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Decides which of the 256 shards this pod claims from.
 *
 * <p><strong>Why shards exist at all.</strong> Measured with 16 concurrent claimers over 3M READY
 * rows, 100 rows per transaction:
 *
 * <table border="1">
 *   <caption>Claim strategies, measured</caption>
 *   <tr><th>Strategy</th><th>tps</th><th>rows/s</th><th>avg latency</th></tr>
 *   <tr><td>{@code FOR UPDATE}</td><td>159</td><td>15,900</td><td>100.5 ms</td></tr>
 *   <tr><td>{@code FOR UPDATE SKIP LOCKED}</td><td>453</td><td>45,300</td><td>35.3 ms</td></tr>
 *   <tr><td><strong>shard-affine + SKIP LOCKED</strong></td><td><strong>746</strong></td>
 *       <td><strong>74,600</strong></td><td><strong>21.5 ms</strong></td></tr>
 * </table>
 *
 * <p>The naive version throws <em>zero errors</em>. It simply serialises, because all 16 pods walk
 * the same index in the same order and each waits for the one ahead. That is the trap: it presents
 * as "the database is slow", so the instinct is to add claimers, and adding claimers makes it
 * worse. Shard affinity means two pods do not reach for the same row in the first place, which is
 * why {@code SKIP LOCKED} then has almost nothing to skip — that is the 746-versus-453 gap.
 *
 * <p><strong>Membership, in three fallbacks.</strong> A Redis registry is authoritative when
 * available: every pod heartbeats into one hash, the live members are sorted by name, and a pod
 * owns {@code shard % memberCount == myIndex}. If Redis is unreachable, the StatefulSet ordinal
 * gives a fixed slice. If there is no ordinal either, the pod owns every shard.
 *
 * <p>Every fallback is safe, and that is deliberate: the worst case — two pods believing they own
 * the same shard during a rebalance — is exactly what {@code SKIP LOCKED} and the lease cover.
 * Over-owning costs throughput. It never costs correctness, so this class is allowed to guess.
 *
 * <p>Modulo rather than contiguous ranges, because a shard is derived from a hash of the recipient
 * and campaigns are not uniformly distributed across the shard space. A contiguous slice hands one
 * pod every shard in a hot range; modulo interleaves them.
 */
@Component
public class ShardAssignment {

    /**
     * Fixed at 256, forever. Not configuration: a shard is stamped into
     * {@code scheduled_notification.shard} at insert and into the Redis key
     * {@code due:{shard}} at hydration. Changing the count orphans every already-hydrated key and
     * makes rows nobody polls.
     *
     * <p>Aliased from the writer rather than declared again. The reader and the writer of that
     * column live in different modules, and two independent {@code 256}s is a number that can be
     * changed in one place — after which pods poll shards no row is ever stamped with, and the
     * only symptom is throughput quietly falling by whatever fraction of the space diverged.
     */
    public static final int SHARD_COUNT = ScheduledNotificationWriter.SHARD_COUNT;

    /** Field is the pod name, value is the last heartbeat in epoch millis. */
    private static final String MEMBERS_KEY = "scheduler:members";

    /**
     * Three missed 5-second heartbeats before a pod is considered gone. One would evict a pod over
     * a single GC pause and trigger a rebalance every few minutes on a busy heap.
     */
    private static final Duration MEMBER_TIMEOUT = Duration.ofSeconds(15);

    private static final Logger log = LoggerFactory.getLogger(ShardAssignment.class);

    private final StringRedisTemplate redis;
    private final NodeIdentity node;
    private final SchedulerProperties.Claimer settings;
    private final Clock clock;

    private volatile int[] owned;

    public ShardAssignment(StringRedisTemplate redis, NodeIdentity node,
                           SchedulerProperties properties, Clock clock) {
        this.redis = redis;
        this.node = node;
        this.settings = properties.claimer();
        this.clock = clock;
        this.owned = fromOrdinalOrAll();
    }

    /** The shards this pod polls. Recomputed by the heartbeat; read on every claim pass. */
    public int[] ownedShards() {
        return owned;
    }

    /**
     * Heartbeats and recomputes ownership.
     *
     * <p>Registration and recomputation are the same operation on purpose. A pod that can no longer
     * write its heartbeat also cannot trust the membership list it just read, and doing both in one
     * pass means a Redis outage moves every pod to the fallback together rather than leaving half
     * the fleet on stale membership.
     */
    @Scheduled(fixedDelay = 5_000L, initialDelay = 0L)
    public void heartbeatAndRebalance() {
        try {
            long now = clock.millis();
            var hash = redis.<String, String>opsForHash();
            hash.put(MEMBERS_KEY, node.nodeName(), Long.toString(now));

            Map<String, String> members = hash.entries(MEMBERS_KEY);
            var live = new ArrayList<String>(members.size());
            members.forEach((name, lastSeen) -> {
                if (now - parseMillis(lastSeen) <= MEMBER_TIMEOUT.toMillis()) {
                    live.add(name);
                } else if (!name.equals(node.nodeName())) {
                    // Reaping here rather than in a separate job: the hash is tiny and the pod
                    // that noticed is the pod best placed to act on it.
                    hash.delete(MEMBERS_KEY, name);
                }
            });
            live.sort(Comparator.naturalOrder());

            int index = live.indexOf(node.nodeName());
            if (index < 0 || live.isEmpty()) {
                owned = fromOrdinalOrAll();
                return;
            }
            owned = shardsFor(index, live.size());
        } catch (DataAccessException e) {
            // Fail to a static assignment rather than to no assignment. An empty shard list is a
            // pod that has silently stopped scheduling, which no alert would catch.
            log.warn("shard membership registry unreachable; falling back to ordinal assignment", e);
            owned = fromOrdinalOrAll();
        }
    }

    /** Every {@code shard} where {@code shard % memberCount == index}. */
    static int[] shardsFor(int index, int memberCount) {
        return IntStream.range(0, SHARD_COUNT)
                .filter(shard -> shard % memberCount == index)
                .toArray();
    }

    private int[] fromOrdinalOrAll() {
        var ordinal = node.ordinal();
        if (ordinal.isEmpty()) {
            // Single-node development, or a Deployment with random pod suffixes and no Redis.
            // Owning everything is correct and slow; owning nothing would be fast and broken.
            return IntStream.range(0, SHARD_COUNT).toArray();
        }
        int replicas = Math.max(1, SHARD_COUNT / Math.max(1, settings.shards()));
        return shardsFor(ordinal.getAsInt() % replicas, replicas);
    }

    private static long parseMillis(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            // A corrupt heartbeat is treated as infinitely old, so the member is reaped.
            return Long.MIN_VALUE / 2;
        }
    }
}
