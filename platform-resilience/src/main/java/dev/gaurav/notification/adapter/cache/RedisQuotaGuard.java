package dev.gaurav.notification.adapter.cache;

import dev.gaurav.notification.application.port.QuotaGuard;
import dev.gaurav.notification.resilience.ratelimit.RateLimitQuota;
import dev.gaurav.notification.resilience.ratelimit.RedisTokenBucketRateLimiter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Objects;

/**
 * Bridges {@link QuotaGuard} — per-tenant admission control on the accept path — onto the shared
 * Valkey token bucket in {@link RedisTokenBucketRateLimiter}.
 *
 * <p>The seam exists so the accept use case asks "may this tenant spend N permits" in domain terms,
 * while the bucket key format, the refill rate and the Lua script stay in the resilience module.
 * It is also what keeps the accept path unit-testable in milliseconds against a hand-written fake
 * instead of a Redis container.
 *
 * <p><strong>The bucket must be shared, and that is the entire point of using Valkey for it.</strong>
 * A per-JVM limiter configured at 100/s across 40 API pods is a 4,000/s limit: nothing in the code
 * looks wrong, the number in the config file is the number the contract quotes, and the platform is
 * 40× over it. Autoscaling then makes it worse in the wrong direction, because more load raises the
 * effective limit.
 *
 * <p><strong>Fail-open is the contract, and this class does not implement it — it inherits it.</strong>
 * {@link RedisTokenBucketRateLimiter} returns {@code true} when Redis is unreachable, so an outage
 * of the cache admits traffic rather than rejecting every tenant at once. Translating that into a
 * {@code false} here would turn a degraded dependency into a total outage of the send API.
 *
 * <p>TODO(phase-8): the quota is a single platform-wide default rather than the per-tenant
 * {@code tenant.daily_send_quota} and {@code tenant.rate_limit_rps} the schema already carries.
 * Reading those needs a tenant lookup on the accept path, which is a cache-warming decision this
 * module cannot make on its own.
 */
@Component
public class RedisQuotaGuard implements QuotaGuard {

    /** Namespaced so a tenant bucket can never collide with a provider or recipient bucket. */
    private static final String KEY_PREFIX = "tenant:";

    private final RedisTokenBucketRateLimiter limiter;

    public RedisQuotaGuard(StringRedisTemplate redis,
                           Clock clock,
                           @Value("${notification.api.quota.permits-per-second:1000}") int permitsPerSecond,
                           @Value("${notification.api.quota.burst-capacity:5000}") int burstCapacity) {
        Objects.requireNonNull(redis, "redis");
        this.limiter = new RedisTokenBucketRateLimiter(
                redis, new RateLimitQuota(permitsPerSecond, burstCapacity), clock);
    }

    /**
     * @param permits one per recipient, so a 10M-recipient campaign is charged as 10M. Charging per
     *                request would let a single call bypass the limit entirely
     */
    @Override
    public boolean tryConsume(String tenantId, int permits) {
        // A zero-recipient request cannot exist -- the command's own validation refuses one -- but
        // the bucket rejects a non-positive permit count outright, so a defensive floor here is
        // cheaper than an IllegalArgumentException surfacing as a 500 on the accept path.
        return limiter.tryAcquire(KEY_PREFIX + tenantId, Math.max(1, permits));
    }

    /** Calls admitted because Valkey was unreachable. Exported as {@code rate_limit_fail_open_total}. */
    public long failOpenCount() {
        return limiter.failOpenCount();
    }
}
