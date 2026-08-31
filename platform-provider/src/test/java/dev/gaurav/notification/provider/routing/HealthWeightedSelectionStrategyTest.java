package dev.gaurav.notification.provider.routing;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.provider.StubProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.DoubleSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Routing is the policy most likely to be argued about and hardest to reproduce in production, so
 * every claim in the design's §8.5 gets an assertion here with literal numbers and a pinned draw.
 */
class HealthWeightedSelectionStrategyTest {

    /** Always take the leader. */
    private static final DoubleSupplier NEVER_EXPLORE = () -> 0.99;

    /** Always take the runner-up when it is entitled to any share at all. */
    private static final DoubleSupplier ALWAYS_EXPLORE = () -> 0.0;

    private static ProviderCandidate candidate(String code, int priority, long costMicros,
                                               double successRate, double p95Ms) {
        return new ProviderCandidate(StubProvider.alwaysAccepts(code, Channel.SMS),
                priority, 1, costMicros, 100, successRate, p95Ms);
    }

    private static final ProviderCandidate HEALTHY_PRIMARY = candidate("primary", 1, 7_900L, 0.99, 200);
    private static final ProviderCandidate HEALTHY_SECONDARY = candidate("secondary", 2, 9_500L, 0.98, 400);

    @Nested
    @DisplayName("scoring")
    class Scoring {

        @Test
        @DisplayName("a provider that stopped delivering loses the top spot even though it is cheaper, faster and the stated primary")
        void deliveringBeatsCheapAndFast() {
            // The failure mode this prevents: a scoring function tuned on cost and latency happily
            // routes every OTP to the provider dropping 90% of them, because it is winning on the
            // two axes that are easy to measure.
            var failingPrimary = candidate("primary", 1, 7_900L, 0.10, 200);
            var strategy = new HealthWeightedSelectionStrategy(
                    HealthWeightedSelectionStrategy.Weights.defaults(), NEVER_EXPLORE);

            var chosen = strategy.select(Channel.SMS, "tenant-1", List.of(failingPrimary, HEALTHY_SECONDARY));

            assertThat(chosen).isPresent();
            assertThat(chosen.orElseThrow().code().value()).isEqualTo("secondary");
        }

        @Test
        @DisplayName("with both providers healthy the operator's priority-1 primary wins")
        void healthyPrimaryWins() {
            var strategy = new HealthWeightedSelectionStrategy(
                    HealthWeightedSelectionStrategy.Weights.defaults(), NEVER_EXPLORE);

            var chosen = strategy.select(Channel.SMS, "tenant-1", List.of(HEALTHY_PRIMARY, HEALTHY_SECONDARY));

            assertThat(chosen.orElseThrow().code().value()).isEqualTo("primary");
        }

        @Test
        @DisplayName("latency and cost are normalised against the field, so no per-channel magic constant is needed")
        void normalisationIsRelative() {
            // Absolute thresholds would need one number for email and another for SMS, and both
            // would be wrong the first time a vendor changed its infrastructure.
            var strategy = new HealthWeightedSelectionStrategy();
            var fast = candidate("fast", 1, 1_000L, 0.99, 100);
            var slow = candidate("slow", 1, 1_000L, 0.99, 1_000);

            var scores = strategy.scores(List.of(fast, slow));

            assertThat(scores.get("fast")).isGreaterThan(scores.get("slow"));
            // 10x slower costs exactly 0.9 of the latency weight, and no more: latency must never
            // outvote a collapsed success rate.
            assertThat(scores.get("fast") - scores.get("slow")).isCloseTo(0.18, within(0.001));
        }

        @Test
        @DisplayName("a freshly registered provider is not condemned by having no history")
        void coldStartDoesNotDeadlock() {
            // successRate seeded at 1.0, p95 at 0. Starting at zero would mean it never gets
            // traffic, so it never earns a score, so it never gets traffic.
            var fresh = ProviderCandidate.freshlyRegistered(
                    StubProvider.alwaysAccepts("fresh", Channel.SMS), 1, 7_900L);
            var strategy = new HealthWeightedSelectionStrategy();

            var scores = strategy.scores(List.of(fresh, HEALTHY_SECONDARY));

            assertThat(scores.get("fresh")).isGreaterThan(scores.get("secondary"));
        }
    }

    @Nested
    @DisplayName("keeping the backup warm")
    class Exploration {

        @Test
        @DisplayName("the runner-up still gets traffic, so the failover path is warm and its health number is real")
        void theRunnerUpIsNotStarved() {
            // A cold backup pays DNS, TLS, an empty connection pool and a stale success rate on the
            // first request after the primary dies — at the worst possible moment.
            var strategy = new HealthWeightedSelectionStrategy(
                    HealthWeightedSelectionStrategy.Weights.defaults(), ALWAYS_EXPLORE);

            var chosen = strategy.select(Channel.SMS, "tenant-1", List.of(HEALTHY_PRIMARY, HEALTHY_SECONDARY));

            assertThat(chosen.orElseThrow().code().value()).isEqualTo("secondary");
        }

        @Test
        @DisplayName("the runner-up's share is capped at 25%, or two close scores quietly double the bill")
        void explorationIsCapped() {
            // Two identical providers would otherwise split traffic 50/50 and "primary" would stop
            // meaning anything.
            var twinA = candidate("twin-a", 1, 7_900L, 0.99, 200);
            var twinB = candidate("twin-b", 1, 7_900L, 0.99, 200);
            var justOverTheCap = new HealthWeightedSelectionStrategy(
                    HealthWeightedSelectionStrategy.Weights.defaults(), () -> 0.30);

            var chosen = justOverTheCap.select(Channel.SMS, "tenant-1", List.of(twinA, twinB));

            assertThat(HealthWeightedSelectionStrategy.MAX_EXPLORATION_SHARE).isEqualTo(0.25);
            assertThat(chosen.orElseThrow().code().value())
                    .as("a draw of 0.30 is above the 0.25 cap, so it must fall to the leader")
                    .isEqualTo("twin-a");
        }

        @Test
        @DisplayName("a provider scoring at or below zero is avoided, not warmed — that is not exploration, it is loss")
        void aDeadRunnerUpGetsNoTraffic() {
            var dead = candidate("dead", 3, 900_000L, 0.0, 20_000);
            var strategy = new HealthWeightedSelectionStrategy(
                    HealthWeightedSelectionStrategy.Weights.defaults(), ALWAYS_EXPLORE);

            var chosen = strategy.select(Channel.SMS, "tenant-1", List.of(HEALTHY_PRIMARY, dead));

            assertThat(strategy.scores(List.of(HEALTHY_PRIMARY, dead)).get("dead")).isNegative();
            assertThat(chosen.orElseThrow().code().value()).isEqualTo("primary");
        }

        @Test
        @DisplayName("equal scores break the tie deterministically, so a failover test does not pass only on some machines")
        void tiesAreBrokenDeterministically() {
            var a = candidate("alpha", 1, 7_900L, 0.99, 200);
            var b = candidate("beta", 1, 7_900L, 0.99, 200);
            var strategy = new HealthWeightedSelectionStrategy(
                    HealthWeightedSelectionStrategy.Weights.defaults(), NEVER_EXPLORE);

            assertThat(strategy.select(Channel.SMS, null, List.of(a, b)).orElseThrow().code().value())
                    .isEqualTo(strategy.select(Channel.SMS, null, List.of(b, a)).orElseThrow().code().value());
        }
    }

    @Nested
    @DisplayName("eligibility")
    class Eligibility {

        @Test
        @DisplayName("an unhealthy provider is never selected, even when it would score highest")
        void unhealthyIsExcluded() {
            // The caller's circuit filter runs earlier; a provider can go HARD_DOWN in between.
            var down = new ProviderCandidate(
                    StubProvider.alwaysAccepts("down", Channel.SMS).unhealthy(),
                    1, 1, 100L, 100, 1.0, 10);
            var strategy = new HealthWeightedSelectionStrategy(
                    HealthWeightedSelectionStrategy.Weights.defaults(), NEVER_EXPLORE);

            assertThat(strategy.scores(List.of(down, HEALTHY_SECONDARY)).get("down"))
                    .isGreaterThan(strategy.scores(List.of(down, HEALTHY_SECONDARY)).get("secondary"));
            assertThat(strategy.select(Channel.SMS, "tenant-1", List.of(down, HEALTHY_SECONDARY)).orElseThrow()
                    .code().value()).isEqualTo("secondary");
        }

        @Test
        @DisplayName("every provider down returns empty rather than throwing — it is an operational state, not a bug")
        void allDownIsEmptyNotAnException() {
            var down = new ProviderCandidate(
                    StubProvider.alwaysAccepts("down", Channel.SMS).unhealthy(),
                    1, 1, 100L, 100, 1.0, 10);
            var strategy = new HealthWeightedSelectionStrategy();

            assertThat(strategy.select(Channel.SMS, "tenant-1", List.of(down))).isEmpty();
            assertThat(strategy.select(Channel.SMS, "tenant-1", List.of())).isEmpty();
            assertThat(strategy.select(Channel.SMS, "tenant-1", null)).isEmpty();
        }

        @Test
        @DisplayName("with a single candidate there is nothing to explore and it is returned as-is")
        void aSingleCandidateIsReturned() {
            var strategy = new HealthWeightedSelectionStrategy(
                    HealthWeightedSelectionStrategy.Weights.defaults(), ALWAYS_EXPLORE);

            assertThat(strategy.select(Channel.SMS, "tenant-1", List.of(HEALTHY_PRIMARY)).orElseThrow())
                    .isEqualTo(HEALTHY_PRIMARY);
        }
    }
}
