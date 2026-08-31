package dev.gaurav.notification.messaging.consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class IdempotentConsumerTest {

    private static final String GROUP = "dispatch-sms-tx";

    private final IdempotentConsumer consumer =
            new IdempotentConsumer(new InMemoryDeduplicationStore());

    @Test
    @DisplayName("the second sighting of an eventId returns false — this is what stops a Kafka "
            + "redelivery from sending a real user a second OTP")
    void aRedeliveredEventIsNotProcessedTwice() {
        UUID eventId = UUID.randomUUID();

        assertThat(consumer.isFirstSighting(GROUP, eventId)).isTrue();
        assertThat(consumer.isFirstSighting(GROUP, eventId)).isFalse();
        assertThat(consumer.isFirstSighting(GROUP, eventId)).isFalse();
    }

    @Test
    @DisplayName("two consumer groups each get a first sighting of the same event — a global key "
            + "would let whichever group polled first silently starve the ledger writer")
    void deduplicationIsScopedPerConsumerGroup() {
        UUID eventId = UUID.randomUUID();

        assertThat(consumer.isFirstSighting("delivery-ledger-writer", eventId)).isTrue();
        assertThat(consumer.isFirstSighting("status-projector", eventId)).isTrue();
        assertThat(consumer.isFirstSighting("delivery-ledger-writer", eventId)).isFalse();
    }

    @Test
    @DisplayName("forget() lets the pending redelivery through — a key left behind by a handler "
            + "that threw turns at-least-once into at-most-once for seven days")
    void forgettingAFailedEventRestoresRedelivery() {
        UUID eventId = UUID.randomUUID();

        assertThat(consumer.isFirstSighting(GROUP, eventId)).isTrue();
        consumer.forget(GROUP, eventId);

        assertThat(consumer.isFirstSighting(GROUP, eventId)).isTrue();
    }

    @Test
    @DisplayName("when Redis is down we process anyway — failing closed would turn a cache outage "
            + "into a total delivery outage, and layers 4 and 5 still protect us")
    void aDeadDeduplicationStoreDegradesToProcessingAnyway() {
        var down = new DeduplicationStore() {
            @Override
            public boolean putIfAbsent(String key, Duration ttl) {
                return true;
            }

            @Override
            public void remove(String key) {
            }

            @Override
            public boolean isAvailable() {
                return false;
            }
        };
        var degraded = new IdempotentConsumer(down);
        UUID eventId = UUID.randomUUID();

        assertThat(degraded.isFirstSighting(GROUP, eventId)).isTrue();
        assertThat(degraded.isFirstSighting(GROUP, eventId)).isTrue();
        assertThat(degraded.isFullyProtected())
                .as("degradation must be observable, not silent")
                .isFalse();
    }

    @Test
    @DisplayName("the dedup window is at least as long as the seven-day retention on "
            + "notification.requested, so a cross-region replay cannot duplicate every send")
    void theWindowCoversTheReplayWindow() {
        assertThat(IdempotentConsumer.DEDUPLICATION_WINDOW).isEqualTo(Duration.ofDays(7));
    }

    @Test
    @DisplayName("the store is checked exactly once per call — a GET-then-SET would let two "
            + "consumers rebalancing onto the same partition both see 'absent' and both send")
    void theStoreIsConsultedWithASingleAtomicCall() {
        var calls = new AtomicInteger();
        var counting = new DeduplicationStore() {
            private final DeduplicationStore delegate = new InMemoryDeduplicationStore();

            @Override
            public boolean putIfAbsent(String key, Duration ttl) {
                calls.incrementAndGet();
                return delegate.putIfAbsent(key, ttl);
            }

            @Override
            public void remove(String key) {
                delegate.remove(key);
            }
        };

        new IdempotentConsumer(counting).isFirstSighting(GROUP, UUID.randomUUID());

        assertThat(calls).hasValue(1);
    }

    @Test
    @DisplayName("an entry past its TTL is treated as absent, so a legitimate re-send after the "
            + "window is not suppressed forever")
    void anExpiredEntryIsIndistinguishableFromAnAbsentOne() {
        var clock = new MutableClock(Instant.parse("2026-08-31T00:00:00Z"));
        var store = new InMemoryDeduplicationStore(clock);
        var scoped = new IdempotentConsumer(store);
        UUID eventId = UUID.randomUUID();

        assertThat(scoped.isFirstSighting(GROUP, eventId)).isTrue();
        clock.advance(Duration.ofDays(6));
        assertThat(scoped.isFirstSighting(GROUP, eventId)).isFalse();
        clock.advance(Duration.ofDays(2));
        assertThat(scoped.isFirstSighting(GROUP, eventId)).isTrue();
    }

    @Test
    @DisplayName("the in-memory store is bounded — an unbounded map is a heap leak that only "
            + "appears on a consumer that has been up for weeks")
    void theInMemoryStoreEvictsRatherThanGrowingForever() {
        var store = new InMemoryDeduplicationStore();

        for (int i = 0; i < InMemoryDeduplicationStore.MAX_ENTRIES + 5_000; i++) {
            store.putIfAbsent("notif:seen:g:" + i, Duration.ofDays(7));
        }

        assertThat(store.size()).isEqualTo(InMemoryDeduplicationStore.MAX_ENTRIES);
    }

    private static final class MutableClock extends Clock {
        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        private void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }
}
