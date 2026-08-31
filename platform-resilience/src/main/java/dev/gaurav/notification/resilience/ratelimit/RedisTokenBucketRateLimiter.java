package dev.gaurav.notification.resilience.ratelimit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * A token bucket held in Redis and mutated by a Lua script, so the quota is shared by the whole
 * fleet instead of being multiplied by it.
 *
 * <p><strong>The failure this prevents: N pods, N× the quota.</strong> Guava's
 * {@code RateLimiter} and Resilience4j's {@code RateLimiter} are both <em>per-JVM</em>. Configuring
 * "100 requests per second" on a deployment of 40 worker pods configures 4,000 requests per second
 * at the provider. Nothing in the code looks wrong, the number in the config file is the number the
 * provider published, and the platform is still 40× over the contract. The symptom arrives as a
 * wall of 429s, or — worse, because it is silent — as carrier spam filtering and a damaged sender
 * reputation that outlasts the incident by weeks. Autoscaling makes it worse in exactly the wrong
 * direction: more load spawns more pods, and more pods raise the effective quota.
 *
 * <p>A single Redis key per bucket is the smallest thing that fixes it. The entire read-refill-
 * decide-write cycle runs inside one Lua script, which Redis executes atomically, so 40 pods racing
 * on the same bucket cannot each read the same token count and each decide they may proceed. Doing
 * the same arithmetic with {@code GET} then {@code SET} from Java would be a textbook lost update
 * on every contended call, and contention is the only case that matters.
 *
 * <p>One key per bucket, not two, so the script is Redis Cluster-safe without hash tags: all state
 * lives in one hash and therefore one slot.
 *
 * <p>Time is supplied by the caller rather than read with {@code redis.call('TIME')}. Clock skew
 * across pods is bounded by NTP at a few milliseconds, which at 100 permits/s is a fraction of one
 * token; the elapsed calculation clamps negatives, so a pod whose clock runs behind loses a little
 * refill rather than corrupting the bucket.
 *
 * <p><strong>Fail-open, and only here.</strong> If Redis is unreachable the call is allowed and
 * {@link #failOpenCount()} is incremented for the {@code redis_fallback_active} gauge. Fail open on
 * quota, never fail open on correctness: overshooting a rate limit costs some 429s and is fully
 * recoverable, while rejecting a paying tenant's one-time passcodes because a cache is down is an
 * outage we inflicted on ourselves. The same reasoning does <em>not</em> extend to idempotency or
 * suppression — failing open there sends duplicate messages to people who asked not to be
 * contacted, which is neither cheap nor reversible.
 */
public class RedisTokenBucketRateLimiter implements RateLimiter {

    /**
     * Refill, then spend, then persist — in one atomic step.
     *
     * <p>{@code PEXPIRE} on every touch is what keeps this affordable: with per-recipient buckets
     * across 50M users, keys that live forever are a slow memory leak that ends in an eviction
     * storm. An idle bucket is indistinguishable from a fresh full one, so dropping it is free.
     */
    private static final String TOKEN_BUCKET_LUA = """
            local state     = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
            local rate      = tonumber(ARGV[1])
            local capacity  = tonumber(ARGV[2])
            local now       = tonumber(ARGV[3])
            local requested = tonumber(ARGV[4])
            local ttl       = tonumber(ARGV[5])

            local tokens = tonumber(state[1])
            local last   = tonumber(state[2])
            if tokens == nil then tokens = capacity end
            if last   == nil then last = now end

            local elapsed = now - last
            if elapsed < 0 then elapsed = 0 end
            tokens = math.min(capacity, tokens + (elapsed * rate / 1000.0))

            local allowed = 0
            if tokens >= requested then
              tokens = tokens - requested
              allowed = 1
            end

            redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
            redis.call('PEXPIRE', KEYS[1], ttl)
            return allowed
            """;

    private static final String KEY_PREFIX = "rl:";

    private final StringRedisTemplate redis;
    private final RedisScript<Long> script;
    private final RateLimitQuota defaultQuota;
    private final Clock clock;
    private final LongAdder failOpen = new LongAdder();

    public RedisTokenBucketRateLimiter(StringRedisTemplate redis, RateLimitQuota defaultQuota) {
        this(redis, defaultQuota, Clock.systemUTC());
    }

    public RedisTokenBucketRateLimiter(StringRedisTemplate redis, RateLimitQuota defaultQuota, Clock clock) {
        this.redis = redis;
        this.defaultQuota = defaultQuota;
        this.clock = clock;
        this.script = new DefaultRedisScript<>(TOKEN_BUCKET_LUA, Long.class);
    }

    @Override
    public boolean tryAcquire(String key, int permits) {
        return tryAcquire(key, permits, defaultQuota);
    }

    /**
     * Checks {@code key} against an explicit quota, for the per-provider limits that are read from
     * {@code provider_configuration} rather than fixed at construction.
     *
     * <p>A request for more permits than the bucket can ever hold is refused rather than allowed:
     * it can never be satisfied, so retrying it forever is the alternative and that is worse.
     */
    public boolean tryAcquire(String key, int permits, RateLimitQuota quota) {
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be at least 1, got " + permits);
        }
        if (permits > quota.burstCapacity()) {
            return false;
        }
        try {
            Long allowed = redis.execute(script,
                    List.of(KEY_PREFIX + key),
                    Double.toString(quota.permitsPerSecond()),
                    Integer.toString(quota.burstCapacity()),
                    Long.toString(clock.millis()),
                    Integer.toString(permits),
                    Long.toString(idleTtl(quota).toMillis()));
            if (allowed == null) {
                // A null result means the script did not run — treated exactly like an outage.
                return failOpen();
            }
            return allowed == 1L;
        } catch (DataAccessException e) {
            // Covers connection failure, command timeout and cluster redirection storms. Every
            // Lettuce and Jedis failure surfaces through Spring's translation as this type.
            return failOpen();
        }
    }

    /** Calls allowed through because Redis was unavailable. Export as {@code rate_limit_fail_open_total}. */
    public long failOpenCount() {
        return failOpen.sum();
    }

    private boolean failOpen() {
        failOpen.increment();
        return true;
    }

    /**
     * Twice the time it takes to refill from empty, floored at 10 s. Expiring a bucket that is
     * still draining would silently reset it to full and hand the provider a free burst.
     */
    private static Duration idleTtl(RateLimitQuota quota) {
        long refillMillis = (long) (quota.burstCapacity() / quota.permitsPerSecond() * 1000);
        return Duration.ofMillis(Math.max(10_000L, refillMillis * 2));
    }
}
