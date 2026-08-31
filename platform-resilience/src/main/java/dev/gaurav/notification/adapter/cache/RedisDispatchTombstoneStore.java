package dev.gaurav.notification.adapter.cache;

import dev.gaurav.notification.application.port.DispatchTombstoneStore;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * Bridges {@link DispatchTombstoneStore} onto a Valkey key with a TTL.
 *
 * <p>The seam exists because the cancel use case has two writes with opposite failure semantics and
 * must not be able to confuse them. The durable {@code CANCELLED} row is the cancellation; this is
 * a best-effort marker that shrinks the window between a worker claiming a message and calling the
 * provider. Folding the two behind one port would invite an implementation that treats a Valkey
 * timeout as a failed cancellation, when the cancel has already committed.
 *
 * <p><strong>The TTL is not optional.</strong> An unbounded tombstone set grows at the cancellation
 * rate forever, and nothing ever reads an entry older than the notification's own TTL — 72 hours is
 * the longest any traffic class lives, so anything beyond that is pure leak. The container runs
 * {@code maxmemory-policy noeviction} precisely so a leak here surfaces as an OOM error rather than
 * as silently evicted idempotency keys.
 *
 * <p>Exceptions propagate rather than being swallowed here: the port documents that a cache fault
 * throws and that the caller logs and continues, and {@code CancelNotificationUseCase} does exactly
 * that. Swallowing at this level would take the decision away from the only class that knows the
 * durable write already succeeded.
 */
@Component
public class RedisDispatchTombstoneStore implements DispatchTombstoneStore {

    /** Namespaced so a tombstone cannot collide with a rate-limit or dedup key. */
    private static final String KEY_PREFIX = "cancel:";

    /** The marker's only content. The key's presence is the fact; the value is never read. */
    private static final String MARKER = "1";

    private final StringRedisTemplate redis;
    private final Duration ttl;

    public RedisDispatchTombstoneStore(
            StringRedisTemplate redis,
            @Value("${notification.api.tombstone.ttl:PT72H}") Duration ttl) {
        this.redis = Objects.requireNonNull(redis, "redis");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
    }

    @Override
    public void tombstone(String tenantId, UUID notificationId) {
        redis.opsForValue().set(KEY_PREFIX + tenantId + ":" + notificationId, MARKER, ttl);
    }
}
