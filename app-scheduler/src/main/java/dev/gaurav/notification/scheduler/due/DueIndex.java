package dev.gaurav.notification.scheduler.due;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * The Redis near-horizon: one sorted set per shard, scored by the instant the work becomes
 * dispatchable.
 *
 * <p><strong>Redis is the index, not the ledger.</strong> This is Netflix Timestone's shape and the
 * distinction is load-bearing. Everything in here is a copy of a PostgreSQL row that is still
 * {@code READY}; a Redis flush costs a delay of one hydration interval, never a lost notification.
 * That is what makes it acceptable to put the 100 ms poll loop on a cache — {@code ZRANGEBYSCORE}
 * is sub-millisecond and costs the database nothing, and the database is the thing that cannot be
 * scaled out at 3 a.m.
 *
 * <p><strong>The fencing check in {@link #hydrate} is the real reason this class exists.</strong>
 * Only the elected hydrator writes here, and "elected" is a claim that survives exactly until the
 * leader enters a stop-the-world pause long enough for its lease to expire. It then wakes up
 * mid-pass and keeps writing, in good faith, alongside the new leader. No lease can prevent that —
 * the pod cannot observe its own pause. The guard is at the resource: the shard remembers the
 * highest fencing token it has accepted, and a write carrying a lower one is refused. A zombie
 * hydrator's writes are dropped by Redis, not by the zombie's own judgement.
 *
 * <p>Keys use a Redis Cluster hash tag — {@code due:&#123;7&#125;} and {@code due:&#123;7&#125;:fence} —
 * so a shard's sorted set and its fence counter always land in the same slot and the Lua script
 * that touches both is legal in cluster mode.
 */
@Component
public class DueIndex {

    /** {@link #hydrate} returns this when the caller's fencing token was superseded. */
    public static final long FENCED_OUT = -1L;

    /**
     * Fence, then ZADD the batch.
     *
     * <p>{@code ARGV[1]} is the fencing token, {@code ARGV[2]} the set's TTL in millis, and
     * {@code ARGV[3..]} alternating score/member pairs.
     *
     * <p>The token is written back only when it is strictly higher. Writing it on every call would
     * be a no-op today and a bug the day renewal starts minting new tokens — the guard would then
     * ratchet on the leader's own renewals and it would never fence anything out.
     */
    private static final String HYDRATE_LUA = """
            local stored = tonumber(redis.call('GET', KEYS[2]) or '0')
            local token  = tonumber(ARGV[1])
            if token < stored then return -1 end
            if token > stored then redis.call('SET', KEYS[2], token) end
            local added = 0
            for i = 3, #ARGV, 2 do
              added = added + redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i + 1])
            end
            redis.call('PEXPIRE', KEYS[1], ARGV[2])
            return added
            """;

    /**
     * One hour of idle time before a shard's set is dropped.
     *
     * <p>Refreshed on every hydration, so a live shard never expires. Its job is the dead ones: a
     * cancelled or already-dispatched row that was hydrated once and never removed would otherwise
     * sit in the sorted set forever, and the claimers would keep polling ids that resolve to
     * nothing. Losing a live entry to the TTL is harmless — the next scan re-adds it, because
     * hydration reads the same READY rows every pass.
     */
    private static final Duration SHARD_TTL = Duration.ofHours(1);

    private static final Logger log = LoggerFactory.getLogger(DueIndex.class);

    private final StringRedisTemplate redis;
    private final RedisScript<Long> hydrateScript = new DefaultRedisScript<>(HYDRATE_LUA, Long.class);

    public DueIndex(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** {@code due:&#123;7&#125;} — the sorted set of work due on shard 7. */
    public static String dueKey(int shard) {
        return "due:{" + shard + "}";
    }

    /** {@code due:&#123;7&#125;:fence} — the highest fencing token shard 7 has accepted. */
    public static String fenceKey(int shard) {
        return "due:{" + shard + "}:fence";
    }

    /**
     * Pushes a shard's slice of the horizon into Redis, under a fencing token.
     *
     * <p>Idempotent: {@code ZADD} on an existing member updates its score and returns 0, so
     * re-hydrating the same rows every tick — which is exactly what the hydrator does — neither
     * duplicates work nor grows the set.
     *
     * @param scored score/member pairs; the score is the <em>jittered</em> due instant in epoch
     *               millis, so the spread costs no database write and no partition-key update
     * @return members newly added, or {@link #FENCED_OUT} when a later leader has already written
     */
    public long hydrate(int shard, long fencingToken, List<ScoredWork> scored) {
        if (scored.isEmpty()) {
            return 0L;
        }
        var args = new ArrayList<String>(2 + scored.size() * 2);
        args.add(Long.toString(fencingToken));
        args.add(Long.toString(SHARD_TTL.toMillis()));
        for (var entry : scored) {
            args.add(Long.toString(entry.scoreMillis()));
            args.add(entry.id().toString());
        }
        Long added = redis.execute(hydrateScript,
                List.of(dueKey(shard), fenceKey(shard)), args.toArray());
        return added == null ? 0L : added;
    }

    /**
     * The next {@code limit} ids on this shard that are due at or before {@code dueBy}.
     *
     * <p>Read-only: polling does not remove. Removal happens after the claim, so a claimer that
     * dies between the poll and the claim leaves the entry for its successor rather than losing it.
     *
     * <p>Returns an empty list on a Redis failure rather than throwing. Redis being down means
     * scheduled work is late; it must not also mean the claim loop dies and stops retrying.
     */
    public List<UUID> poll(int shard, Instant dueBy, int limit) {
        try {
            Set<String> members = redis.opsForZSet().rangeByScore(
                    dueKey(shard), 0d, (double) dueBy.toEpochMilli(), 0L, limit);
            if (members == null || members.isEmpty()) {
                return List.of();
            }
            return members.stream().map(UUID::fromString).toList();
        } catch (DataAccessException e) {
            log.warn("due index unreadable for shard {}; skipping this pass", shard, e);
            return List.of();
        }
    }

    /**
     * Drops entries the claimer has finished with, whether or not the claim succeeded.
     *
     * <p>Removing ids that were <em>not</em> claimed is intentional. Those rows are either no longer
     * READY — cancelled, or already dispatched by a pod that owned the shard a moment ago — or they
     * were skipped by {@code SKIP LOCKED}, in which case the next hydration pass re-adds them
     * within one scan interval. Leaving them means the poll returns the same unclaimable ids on
     * every 100 ms tick and the shard never makes progress past them.
     */
    public long remove(int shard, Collection<UUID> ids) {
        if (ids.isEmpty()) {
            return 0L;
        }
        try {
            Long removed = redis.opsForZSet().remove(dueKey(shard),
                    ids.stream().map(UUID::toString).toArray());
            return removed == null ? 0L : removed;
        } catch (DataAccessException e) {
            log.warn("could not trim due index for shard {}; the TTL will collect it", shard, e);
            return 0L;
        }
    }

    /**
     * One entry to index.
     *
     * @param scoreMillis the jittered due instant. Kept as a primitive rather than an
     *                    {@code Instant} because it is a sorted-set score, and the conversion
     *                    belongs at the one place the jitter is applied, not at every call site
     */
    public record ScoredWork(UUID id, long scoreMillis) {
    }
}
