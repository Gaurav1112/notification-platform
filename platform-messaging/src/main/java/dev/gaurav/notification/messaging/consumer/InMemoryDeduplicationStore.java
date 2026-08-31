package dev.gaurav.notification.messaging.consumer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A bounded, single-JVM {@link DeduplicationStore} for local development and tests.
 *
 * <p><strong>Not a production store, and not a fallback for one.</strong> It deduplicates only
 * within one process, so with two worker replicas a redelivery that lands on the other pod is
 * invisible to it. It is registered only when no Redis-backed implementation is present, which
 * makes "we shipped without wiring Valkey" show up as a metric on
 * {@link IdempotentConsumer#isFullyProtected()} rather than as duplicate SMS in production.
 *
 * <p>Entries are capped at {@value #MAX_ENTRIES} with LRU eviction on top of the TTL. An unbounded
 * map here would be a slow heap leak on a consumer that runs for weeks — the exact shape of
 * failure that only appears after the deploy that "worked fine in staging".
 */
public final class InMemoryDeduplicationStore implements DeduplicationStore {

    /** ~100k events is minutes of local traffic and a few MB of heap. */
    public static final int MAX_ENTRIES = 100_000;

    private final Clock clock;
    private final Map<String, Instant> seen;

    public InMemoryDeduplicationStore() {
        this(Clock.systemUTC());
    }

    public InMemoryDeduplicationStore(Clock clock) {
        this.clock = clock;
        this.seen = new LinkedHashMap<>(1024, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Instant> eldest) {
                return size() > MAX_ENTRIES;
            }
        };
    }

    @Override
    public synchronized boolean putIfAbsent(String key, Duration ttl) {
        Instant now = clock.instant();
        Instant existingExpiry = seen.get(key);
        // An expired entry is the same as an absent one; treating it as present would suppress a
        // legitimate re-send of the same logical event after the window closed.
        if (existingExpiry != null && existingExpiry.isAfter(now)) {
            return false;
        }
        seen.put(key, now.plus(ttl));
        return true;
    }

    @Override
    public synchronized void remove(String key) {
        seen.remove(key);
    }

    /** Visible for tests asserting eviction and expiry, never for production logic. */
    public synchronized int size() {
        return seen.size();
    }
}
