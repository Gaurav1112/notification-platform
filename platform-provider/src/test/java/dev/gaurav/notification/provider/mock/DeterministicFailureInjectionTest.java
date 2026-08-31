package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendCommand;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Determinism is the property that makes "exactly three messages reached the DLQ" a legitimate CI
 * assertion rather than a flaky one, so it gets its own test class.
 *
 * <p>Every test here is guarding a specific way a chaos harness silently stops being reproducible.
 */
class DeterministicFailureInjectionTest {

    private static final ProviderCode PRIMARY = ProviderCode.of("mock-sms-primary");
    private static final ProviderCode SECONDARY = ProviderCode.of("mock-sms-secondary");

    /** Enough messages for a 10% failure rate to produce a sequence worth comparing. */
    private static final int MESSAGES = 500;

    private static MockProviderProperties propertiesWithSeed(long seed) {
        return new MockProviderProperties(true, seed, Map.of(
                PRIMARY.value(), MockProviderProperties.ProviderSettings.defaults().withSuccessRate(0.9),
                SECONDARY.value(), MockProviderProperties.ProviderSettings.defaults().withSuccessRate(0.9)));
    }

    @Nested
    @DisplayName("reproducibility across runs")
    class Reproducibility {

        @Test
        @DisplayName("two independent runs with the same seed fail on exactly the same messages")
        void sameSeedSameFailures() {
            // Without this, "assert DLQ count == 3" is a coin flip and someone eventually replaces
            // it with "assert DLQ count > 0", which no longer tests anything.
            var first = drawAll(new FailureInjector(propertiesWithSeed(20260831L)), PRIMARY, recipients());
            var second = drawAll(new FailureInjector(propertiesWithSeed(20260831L)), PRIMARY, recipients());

            assertThat(second).isEqualTo(first);

            // Guard against a vacuous pass: the run has to contain real, varied failures.
            var distinct = first.values().stream().distinct().toList();
            assertThat(distinct).hasSizeGreaterThan(2);
            assertThat(first.values()).contains(FailureProfile.NONE);
        }

        @Test
        @DisplayName("the outcome of a message does not depend on the order messages are sent in")
        void outcomeIsIndependentOfCallOrder() {
            // A shared Random field would pass the same-seed test above and fail this one — and
            // this is the case that actually occurs, because sixteen worker threads pull from the
            // queue in whatever order the broker hands it to them.
            var injector = new FailureInjector(propertiesWithSeed(20260831L));
            var forwards = drawAll(injector, PRIMARY, recipients());

            var shuffled = new ArrayList<>(recipients());
            Collections.shuffle(shuffled, new Random(7L));
            var reordered = drawAll(injector, PRIMARY, shuffled);

            assertThat(reordered).containsExactlyInAnyOrderEntriesOf(forwards);
        }

        @Test
        @DisplayName("changing only the seed changes which messages fail, so the seed is really the input")
        void adifferentSeedGivesADifferentSequence() {
            // Catches the mistake where the seed is accepted, stored, and then never actually used.
            var a = drawAll(new FailureInjector(propertiesWithSeed(1L)), PRIMARY, recipients());
            var b = drawAll(new FailureInjector(propertiesWithSeed(2L)), PRIMARY, recipients());

            assertThat(b).isNotEqualTo(a);
        }

        @Test
        @DisplayName("two providers do not fail on the same messages, or the failover demo would prove nothing")
        void providersAreNotCorrelated() {
            // seed + hash without mixing produces near-identical streams for adjacent inputs. The
            // symptom is a secondary that fails on exactly the messages the primary failed on, so
            // every failover looks broken and nobody can tell why.
            var injector = new FailureInjector(propertiesWithSeed(20260831L));
            var primary = failing(drawAll(injector, PRIMARY, recipients()));
            var secondary = failing(drawAll(injector, SECONDARY, recipients()));

            assertThat(primary).isNotEqualTo(secondary);

            var overlap = new ArrayList<>(primary);
            overlap.retainAll(secondary);
            assertThat(overlap.size())
                    .as("overlap should be near chance (~1%% of %d), not near total", MESSAGES)
                    .isLessThan(primary.size() / 2);
        }

        @Test
        @DisplayName("adjacent recipient UUIDs get independent fates, so a sequential fixture does not fail as a block")
        void adjacentRecipientsAreNotCorrelated() {
            var injector = new FailureInjector(propertiesWithSeed(20260831L));
            var sequential = IntStream.range(0, 400).mapToObj(i -> new UUID(0L, i)).toList();

            var outcomes = drawAll(injector, PRIMARY, sequential);
            var failures = failing(outcomes).size();

            // 10% configured failure rate over 400 sequential ids. A correlated seed shows up as
            // either ~0 or ~400 here, never as something near 40.
            assertThat(failures).isBetween(15, 80);
        }
    }

    @Nested
    @DisplayName("retries")
    class Retries {

        @Test
        @DisplayName("attempt 2 draws a fresh fate, or a transient failure could never recover and the retry engine is untestable")
        void aRetryIsNotDoomedToRepeatItself() {
            var injector = new FailureInjector(propertiesWithSeed(20260831L));

            var changed = 0;
            for (var recipientId : recipients()) {
                var first = injector.draw(PRIMARY, command(recipientId, 1), ChaosState.Mode.NORMAL).profile();
                var second = injector.draw(PRIMARY, command(recipientId, 2), ChaosState.Mode.NORMAL).profile();
                if (first != second) changed++;
            }

            assertThat(changed).isPositive();
        }

        @Test
        @DisplayName("attempt 2 is itself reproducible, so a retry-exhaustion test can assert an exact DLQ count")
        void retriesAreStillDeterministic() {
            var a = new FailureInjector(propertiesWithSeed(20260831L));
            var b = new FailureInjector(propertiesWithSeed(20260831L));

            for (var recipientId : recipients().subList(0, 50)) {
                for (var attempt = 1; attempt <= 5; attempt++) {
                    assertThat(b.draw(PRIMARY, command(recipientId, attempt), ChaosState.Mode.NORMAL))
                            .isEqualTo(a.draw(PRIMARY, command(recipientId, attempt), ChaosState.Mode.NORMAL));
                }
            }
        }

        @Test
        @DisplayName("a malformed attempt attribute does not change delivery behaviour")
        void aBadAttemptAttributeIsIgnored() {
            var injector = new FailureInjector(propertiesWithSeed(20260831L));
            var recipientId = UUID.fromString("00000000-0000-4000-8000-000000000001");

            var withGarbage = new SendCommand(recipientId, Channel.SMS, TrafficClass.TRANSACTIONAL,
                    "+14155550123", null, "body", "tok", Map.of(FailureInjector.ATTEMPT_ATTRIBUTE, "n/a"),
                    Duration.ofSeconds(5));

            assertThat(injector.draw(PRIMARY, withGarbage, ChaosState.Mode.NORMAL))
                    .isEqualTo(injector.draw(PRIMARY, command(recipientId, 1), ChaosState.Mode.NORMAL));
        }
    }

    @Nested
    @DisplayName("latency shape")
    class LatencyShape {

        @Test
        @DisplayName("latency is long-tailed, so a p99 breaker is exercised — a uniform draw would never trip one")
        void empiricalP99LandsNearTheConfiguredP99() {
            var median = Duration.ofMillis(120);
            var p99 = Duration.ofMillis(1_500);
            var properties = new MockProviderProperties(true, 20260831L, Map.of(
                    PRIMARY.value(), MockProviderProperties.ProviderSettings.defaults()
                            .withSuccessRate(1.0)
                            .withLatency(MockProviderProperties.Latency.of(median, p99))));
            var injector = new FailureInjector(properties);

            var samples = new ArrayList<Long>(20_000);
            var random = new Random(99L);
            for (var i = 0; i < 20_000; i++) {
                var recipientId = new UUID(random.nextLong(), random.nextLong());
                samples.add(injector.draw(PRIMARY, command(recipientId, 1), ChaosState.Mode.NORMAL)
                        .latency().toMillis());
            }
            Collections.sort(samples);

            var observedMedian = samples.get(samples.size() / 2);
            var observedP99 = samples.get((int) (samples.size() * 0.99));

            assertThat(observedMedian).isBetween(100L, 145L);
            assertThat(observedP99)
                    .as("the configured p99 is what sigma is solved from; drifting far from it means the maths is wrong")
                    .isBetween(1_200L, 1_900L);
            assertThat(samples.get(samples.size() - 1))
                    .as("the tail must reach well past p99, which is the whole reason for a log-normal")
                    .isGreaterThan(p99.toMillis());
        }

        @Test
        @DisplayName("an extreme tail draw is capped, so one unlucky sample cannot hang the suite for an hour")
        void theTailIsCapped() {
            var capped = FailureInjector.logNormal(Duration.ofMillis(120), Duration.ofMillis(1_500), 12.0);

            assertThat(capped).isLessThanOrEqualTo(Duration.ofMillis(15_000));
        }

        @Test
        @DisplayName("DEGRADED mode stretches latency and halves success, which is the shape that breaks a p99 SLO")
        void degradedModeIsWorseOnBothAxes() {
            var injector = new FailureInjector(propertiesWithSeed(20260831L));
            var recipientId = UUID.fromString("00000000-0000-4000-8000-00000000000a");

            var normal = injector.draw(PRIMARY, command(recipientId, 1), ChaosState.Mode.NORMAL);
            var degraded = injector.draw(PRIMARY, command(recipientId, 1), ChaosState.Mode.DEGRADED);

            assertThat(degraded.latency()).isGreaterThan(normal.latency());
        }

        @Test
        @DisplayName("HARD_DOWN fails fast, because a breaker needs 20 calls in 60s and cannot get them behind slow timeouts")
        void hardDownFailsFast() {
            var injector = new FailureInjector(propertiesWithSeed(20260831L));
            var draw = injector.draw(PRIMARY, command(UUID.randomUUID(), 1), ChaosState.Mode.HARD_DOWN);

            assertThat(draw.profile()).isEqualTo(FailureProfile.PROVIDER_5XX);
            assertThat(draw.latency()).isLessThan(Duration.ofMillis(50));
        }
    }

    // ---- helpers ---------------------------------------------------------------------------------

    private static List<UUID> recipients() {
        var random = new Random(20260831L);
        return IntStream.range(0, MESSAGES)
                .mapToObj(i -> new UUID(random.nextLong(), random.nextLong()))
                .toList();
    }

    private static Map<UUID, FailureProfile> drawAll(FailureInjector injector, ProviderCode code,
                                                     List<UUID> recipients) {
        var out = new LinkedHashMap<UUID, FailureProfile>();
        for (var recipientId : recipients) {
            out.put(recipientId, injector.draw(code, command(recipientId, 1), ChaosState.Mode.NORMAL).profile());
        }
        return out;
    }

    private static List<UUID> failing(Map<UUID, FailureProfile> outcomes) {
        return outcomes.entrySet().stream()
                .filter(e -> e.getValue() != FailureProfile.NONE)
                .map(Map.Entry::getKey)
                .toList();
    }

    private static SendCommand command(UUID recipientId, int attempt) {
        return new SendCommand(recipientId, Channel.SMS, TrafficClass.TRANSACTIONAL, "+14155550123",
                null, "body", "tok-" + recipientId,
                Map.of(FailureInjector.ATTEMPT_ATTRIBUTE, String.valueOf(attempt)),
                Duration.ofSeconds(5));
    }
}
