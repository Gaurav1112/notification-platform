package dev.gaurav.notification.scheduler.due;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two properties that matter: the offset never moves for a given row, and the offsets spread.
 * Both are load-bearing — the first is what makes hydration idempotent across restarts, the second
 * is the entire reason the class exists.
 */
class ScheduleJitterTest {

    private static final Instant NINE_AM = Instant.parse("2026-09-01T09:00:00Z");

    @Test
    @DisplayName("the same row jitters to the same instant on every hydration pass, forever")
    void offsetIsStableAcrossRepeatedHydration() {
        var id = UUID.fromString("019212b0-0000-7000-8000-0000000000ff");

        var first = ScheduleJitter.apply(id, NINE_AM);
        for (int pass = 0; pass < 1_000; pass++) {
            assertThat(ScheduleJitter.apply(id, NINE_AM))
                    .as("a hydrator restart re-scans the same READY rows; if the offset moved, the "
                            + "same row would be indexed at two instants and could dispatch twice")
                    .isEqualTo(first);
        }
    }

    @Test
    @DisplayName("a UUID whose hashCode is negative must not be pulled EARLIER than its send time")
    void negativeHashCodesStillJitterForward() {
        var negative = IntStream.range(0, 10_000)
                .mapToObj(i -> UUID.randomUUID())
                .filter(id -> id.hashCode() < 0)
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "no UUID with a negative hashCode in 10,000 draws — the case this test "
                                + "guards is not being exercised"));

        assertThat(ScheduleJitter.offsetSeconds(negative))
                .as("Math.floorMod, not %%: -7 %% 300 is -7 in Java, and an embargoed send that "
                        + "goes seven seconds early is a correctness bug, not smoothing")
                .isBetween(0, ScheduleJitter.WINDOW_SECONDS - 1);
        assertThat(ScheduleJitter.apply(negative, NINE_AM)).isAfterOrEqualTo(NINE_AM);
    }

    @Test
    @DisplayName("a 09:00 campaign does not land 20,833 sends in the same second")
    void spreadsAcrossTheWholeWindow() {
        int volume = 100_000;
        Map<Long, Integer> perSecond = new HashMap<>();
        for (int i = 0; i < volume; i++) {
            var at = ScheduleJitter.apply(UUID.randomUUID(), NINE_AM);
            perSecond.merge(at.getEpochSecond(), 1, Integer::sum);
        }

        assertThat(perSecond)
                .as("every one of the 300 one-second buckets must be used; an unused bucket means "
                        + "the hash is not covering the window")
                .hasSize(ScheduleJitter.WINDOW_SECONDS);

        int mean = volume / ScheduleJitter.WINDOW_SECONDS;
        assertThat(perSecond.values())
                .as("no second may carry anywhere near the unjittered cliff. Unjittered, all "
                        + "%d land in one second; the whole point is that the peak stays near the "
                        + "mean of %d", volume, mean)
                .allSatisfy(count -> assertThat(count).isLessThan(2 * mean));
    }

    @Test
    @DisplayName("jitter is additive to the caller's instant, not a replacement for it")
    void jitterIsRelativeToTheScheduledTime() {
        var id = UUID.randomUUID();
        var midnight = Instant.parse("2026-09-02T00:00:00Z");

        assertThat(ScheduleJitter.apply(id, midnight))
                .isEqualTo(midnight.plusSeconds(ScheduleJitter.offsetSeconds(id)));
        assertThat(ScheduleJitter.apply(id, NINE_AM))
                .isEqualTo(NINE_AM.plusSeconds(ScheduleJitter.offsetSeconds(id)));
    }
}
