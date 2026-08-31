package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendResult;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single-JVM {@link SentTokenLog}, bounded in both entries and time.
 *
 * <p>This covers the crash-and-redeliver window <em>within one worker process</em>, which is the
 * common case because Kafka redelivers to the same consumer group and usually the same pod. It
 * does <strong>not</strong> cover a pod that dies and comes back elsewhere.
 *
 * <p>TODO(platform-persistence): back this with the {@code notif.idempotency_record} table, which
 * is hour-partitioned precisely so expiry is a {@code DROP TABLE} rather than 20M dead tuples a
 * day. This class then becomes the near-cache in front of it.
 *
 * <p>Both bounds are deliberate. An unbounded map in a process that runs for months is a slow OOM
 * whose heap dump arrives at 3am; a map with no TTL answers a replay with a stale outcome from
 * last Tuesday.
 *
 * <p><strong>Give this the same {@link Clock} as the {@link IdempotentProvider} in front of it.</strong>
 * Expiry compares a caller-supplied {@code startedAt} against this clock, so two disagreeing clocks
 * make every record look expired the moment it is written — deduplication silently stops happening
 * and nothing fails until a user receives two one-time passcodes.
 */
public final class InMemorySentTokenLog implements SentTokenLog {

    private static final Duration DEFAULT_TTL = Duration.ofHours(1);
    private static final int DEFAULT_MAX_ENTRIES = 100_000;

    private record Key(String provider, String token) {}

    private record Entry(Instant startedAt, SendResult outcome) {}

    private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();
    private final AtomicLong evictions = new AtomicLong();
    private final Clock clock;
    private final Duration ttl;
    private final int maxEntries;

    public InMemorySentTokenLog() {
        this(Clock.systemUTC(), DEFAULT_TTL, DEFAULT_MAX_ENTRIES);
    }

    public InMemorySentTokenLog(Clock clock, Duration ttl, int maxEntries) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be positive");
        this.maxEntries = maxEntries;
    }

    @Override
    public boolean recordAttempt(ProviderCode provider, String token, Instant startedAt) {
        purgeIfNeeded();
        var previous = entries.putIfAbsent(key(provider, token), new Entry(startedAt, null));
        return previous == null;
    }

    @Override
    public void recordOutcome(ProviderCode provider, String token, SendResult result) {
        entries.computeIfPresent(key(provider, token),
                (k, existing) -> new Entry(existing.startedAt(), result));
    }

    @Override
    public Optional<SendResult> previousOutcome(ProviderCode provider, String token) {
        return live(provider, token).map(Entry::outcome);
    }

    @Override
    public Optional<Instant> startedAt(ProviderCode provider, String token) {
        return live(provider, token).map(Entry::startedAt);
    }

    /** How many records were dropped by the size bound — non-zero means the TTL is too generous. */
    public long evictions() {
        return evictions.get();
    }

    public int size() {
        return entries.size();
    }

    private Optional<Entry> live(ProviderCode provider, String token) {
        var entry = entries.get(key(provider, token));
        if (entry == null) return Optional.empty();
        if (isExpired(entry)) {
            entries.remove(key(provider, token), entry);
            return Optional.empty();
        }
        return Optional.of(entry);
    }

    private boolean isExpired(Entry entry) {
        return entry.startedAt().plus(ttl).isBefore(clock.instant());
    }

    private void purgeIfNeeded() {
        if (entries.size() < maxEntries) return;
        entries.entrySet().removeIf(e -> isExpired(e.getValue()));
        // Still over budget: drop the oldest. Losing the oldest record risks one duplicate send;
        // running out of heap loses the whole pod. The trade is not close.
        while (entries.size() >= maxEntries) {
            var oldest = entries.entrySet().stream()
                    .min(Comparator.comparing(e -> e.getValue().startedAt()))
                    .map(Map.Entry::getKey);
            if (oldest.isEmpty()) return;
            entries.remove(oldest.get());
            evictions.incrementAndGet();
        }
    }

    private static Key key(ProviderCode provider, String token) {
        return new Key(provider.value(), token);
    }
}
