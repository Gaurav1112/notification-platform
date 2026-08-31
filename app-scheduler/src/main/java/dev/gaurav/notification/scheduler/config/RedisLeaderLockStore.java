package dev.gaurav.notification.scheduler.config;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * {@link LeaderLockStore} on Redis: {@code SET NX PX} for the lock, {@code INCR} for the fencing
 * token, both inside one Lua script so they cannot interleave.
 *
 * <p>Every operation is a script rather than a sequence of commands, because each one is a
 * read-then-write on shared state and Redis only guarantees atomicity within a script. A bare
 * {@code PEXPIRE} for renewal is the classic version of this bug: it refreshes whichever key is
 * there, including one a different pod now owns.
 *
 * <p><strong>This is not Redlock, and it does not claim to be.</strong> A single Redis primary
 * with a fencing token is honest about its guarantee: the lock is a <em>performance</em>
 * optimisation that keeps one hydrator scanning, and the token is what provides safety when the
 * lock is wrong. Redlock's failure analysis applies to locks used <em>as</em> the correctness
 * boundary; here the correctness boundary is the token check at the resource, plus the fact that
 * PostgreSQL — not Redis — is the ledger. A split brain costs duplicate hydration, which is
 * idempotent, not duplicate delivery.
 */
@Component
public class RedisLeaderLockStore implements LeaderLockStore {

    /**
     * Acquire and mint, atomically.
     *
     * <p>Returns the new token, or 0 when the lock is held. {@code INCR} on a key that has no
     * expiry is deliberate: the counter must outlive every lock it has ever issued a token for, or
     * a restart would reissue tokens that a resource has already seen and the guard would start
     * rejecting the legitimate leader.
     */
    private static final String ACQUIRE_LUA = """
            if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
              return redis.call('INCR', KEYS[2])
            end
            return 0
            """;

    /** Compare-and-set expiry refresh. Returns 1 only if this owner still holds the key. */
    private static final String RENEW_LUA = """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
              return redis.call('PEXPIRE', KEYS[1], ARGV[2])
            end
            return 0
            """;

    /** Compare-and-set delete. Returns 1 only if this owner still holds the key. */
    private static final String RELEASE_LUA = """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
              return redis.call('DEL', KEYS[1])
            end
            return 0
            """;

    private static final Logger log = LoggerFactory.getLogger(RedisLeaderLockStore.class);

    private final StringRedisTemplate redis;
    private final RedisScript<Long> acquireScript = new DefaultRedisScript<>(ACQUIRE_LUA, Long.class);
    private final RedisScript<Long> renewScript = new DefaultRedisScript<>(RENEW_LUA, Long.class);
    private final RedisScript<Long> releaseScript = new DefaultRedisScript<>(RELEASE_LUA, Long.class);

    public RedisLeaderLockStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public long acquire(String lockKey, String fenceKey, String owner, Duration ttl) {
        Long token = execute(acquireScript, List.of(lockKey, fenceKey),
                owner, Long.toString(ttl.toMillis()));
        return token == null ? NOT_ACQUIRED : token;
    }

    @Override
    public boolean renew(String lockKey, String owner, Duration ttl) {
        Long renewed = execute(renewScript, List.of(lockKey), owner, Long.toString(ttl.toMillis()));
        return renewed != null && renewed == 1L;
    }

    @Override
    public void release(String lockKey, String owner) {
        execute(releaseScript, List.of(lockKey), owner);
    }

    /**
     * Runs a script, translating any Redis failure into "no". Fail-closed, deliberately: a
     * connection timeout during acquisition must not be read as success, because the only thing
     * standing between "Redis is slow" and "three hydrators scanning at once" is this branch.
     */
    private Long execute(RedisScript<Long> script, List<String> keys, Object... args) {
        try {
            return redis.execute(script, keys, args);
        } catch (DataAccessException e) {
            log.warn("leader lock store unavailable for keys {}; treating as not-leader", keys, e);
            return null;
        }
    }
}
