package dev.gaurav.notification.provider.routing;

import dev.gaurav.notification.domain.enums.Channel;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * The default router: score every eligible provider, then draw from the top two.
 *
 * <h2>The score (§8.5)</h2>
 *
 * <pre>
 *   score = w1 * successRate
 *         + w2 * (1 / normLatency)
 *         + w3 * (1 / normCost)
 *         + w4 * priorityBoost
 *         - w5 * recentFailurePenalty
 * </pre>
 *
 * <p>Latency and cost are normalised <em>against the candidate set</em>, so the best candidate on
 * each axis scores 1.0 and the others degrade proportionally. Absolute normalisation would need a
 * tuned constant per channel — a 300 ms email and a 300 ms SMS are not comparable — and that
 * constant would be wrong the first time a vendor changed its infrastructure. Relative
 * normalisation has no such constant and stays correct as the field moves.
 *
 * <p>Both reciprocals mean "cheaper and faster is better" without letting either dominate: a
 * provider ten times slower still only loses {@code w2 * 0.9}, so latency can never outvote a
 * success rate of zero. That ordering is deliberate — <strong>delivering matters more than
 * delivering quickly</strong>, and a scoring function that forgets it will route an OTP to a fast
 * provider that drops half of them.
 *
 * <h2>Why draw from the top two instead of always taking the winner</h2>
 *
 * <p>A router that sends 100% to the leader leaves the backup cold, and "cold" is not a metaphor:
 * its connection pool is empty, its DNS entry has aged out, its TLS sessions are gone, its JIT
 * profile is stale, and — worst — its {@code successRate5m} is a stale number from whenever it last
 * saw traffic. The first request after a failover then pays all of that at once, at the exact
 * moment the primary has just died and the retry queue is filling. Sending the runner-up a minority
 * share keeps the path warm and, just as importantly, keeps its health statistic <em>true</em>, so
 * the failover decision is made on evidence rather than on a five-hour-old average.
 *
 * <p>The runner-up's share is capped at {@link #MAX_EXPLORATION_SHARE}. An uncapped proportional
 * draw sends nearly half the traffic to the backup whenever the scores are close, which doubles the
 * bill for no reliability gain and quietly makes "primary" meaningless.
 */
@Component
public class HealthWeightedSelectionStrategy implements ProviderSelectionStrategy {

    /** The backup stays warm on a minority of traffic; it must never become a second primary. */
    public static final double MAX_EXPLORATION_SHARE = 0.25;

    /** A provider scoring at or below this is not warmed, it is avoided. */
    private static final double EXPLORATION_SCORE_FLOOR = 0.0;

    /**
     * Relative importance of each term.
     *
     * <p>{@code successRate} outweighs latency and cost combined, and the failure penalty is heavy
     * enough to move a provider off the top spot on its own. Cost is the smallest term on purpose:
     * a router that optimises the invoice ahead of delivery will find the cheapest way to not
     * deliver a password reset.
     */
    public record Weights(double successRate, double latency, double cost,
                          double priority, double recentFailure) {

        public Weights {
            if (successRate < 0 || latency < 0 || cost < 0 || priority < 0 || recentFailure < 0) {
                throw new IllegalArgumentException("weights must not be negative");
            }
        }

        public static Weights defaults() {
            return new Weights(0.45, 0.20, 0.10, 0.15, 0.30);
        }
    }

    private final Weights weights;
    private final DoubleSupplier randomSource;

    public HealthWeightedSelectionStrategy() {
        this(Weights.defaults(), () -> ThreadLocalRandom.current().nextDouble());
    }

    /**
     * @param randomSource injected so a test can pin the exploration draw; a strategy that calls
     *                     {@code Math.random()} internally can only be tested statistically, and a
     *                     statistical test of a routing decision is a flaky test
     */
    public HealthWeightedSelectionStrategy(Weights weights, DoubleSupplier randomSource) {
        this.weights = Objects.requireNonNull(weights, "weights");
        this.randomSource = Objects.requireNonNull(randomSource, "randomSource");
    }

    @Override
    public Optional<ProviderCandidate> select(Channel channel, String tenantId,
                                              List<ProviderCandidate> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return Optional.empty();
        }

        // A provider that reports itself unhealthy is excluded regardless of score. The caller has
        // already applied the circuit filter, but a HARD_DOWN mock or a failed readiness probe can
        // arrive between that filter and this call.
        var eligible = candidates.stream().filter(c -> c.provider().isHealthy()).toList();
        if (eligible.isEmpty()) {
            return Optional.empty();
        }
        if (eligible.size() == 1) {
            return Optional.of(eligible.get(0));
        }

        var scores = scores(eligible);
        var ranked = eligible.stream()
                .sorted(Comparator
                        .comparingDouble((ProviderCandidate c) -> scores.get(c.code().value()))
                        .reversed()
                        // Deterministic tie-break, so an equal-score pair does not reorder between
                        // JVM runs and make a failover test reproduce only on some machines.
                        .thenComparingInt(ProviderCandidate::priority)
                        .thenComparing(c -> c.code().value()))
                .toList();

        var leader = ranked.get(0);
        var runnerUp = ranked.get(1);

        var leaderScore = Math.max(scores.get(leader.code().value()), 0.0);
        var runnerUpScore = scores.get(runnerUp.code().value());
        if (runnerUpScore <= EXPLORATION_SCORE_FLOOR) {
            return Optional.of(leader);
        }

        var total = leaderScore + runnerUpScore;
        var explorationShare = total <= 0 ? 0.0 : Math.min(runnerUpScore / total, MAX_EXPLORATION_SHARE);
        return Optional.of(randomSource.getAsDouble() < explorationShare ? runnerUp : leader);
    }

    /**
     * The score of every candidate, keyed by provider code.
     *
     * <p>Public because an operator staring at an unexpected routing decision needs to see the
     * numbers, and because it lets a test assert on the ordering without going through the
     * probabilistic draw.
     */
    public Map<String, Double> scores(List<ProviderCandidate> candidates) {
        var bestLatency = candidates.stream()
                .mapToDouble(ProviderCandidate::p95LatencyMs)
                .filter(v -> v > 0)
                .min()
                .orElse(1.0);
        var bestCost = candidates.stream()
                .mapToLong(ProviderCandidate::costMicros)
                .filter(v -> v > 0)
                .min()
                .orElse(1L);

        var out = new LinkedHashMap<String, Double>();
        for (var candidate : candidates) {
            out.put(candidate.code().value(), score(candidate, bestLatency, bestCost));
        }
        return Map.copyOf(out);
    }

    private double score(ProviderCandidate candidate, double bestLatency, long bestCost) {
        // A candidate with no measurement yet (p95 = 0) is treated as best-in-class rather than
        // divide-by-zero or worst-in-class; see ProviderCandidate.freshlyRegistered.
        var normLatency = candidate.p95LatencyMs() <= 0 ? 1.0 : candidate.p95LatencyMs() / bestLatency;
        var normCost = candidate.costMicros() <= 0 ? 1.0 : (double) candidate.costMicros() / bestCost;

        // 1/priority, so priority 1 contributes the full weight and priority 4 a quarter of it.
        // Priority nudges, it does not decide — an operator's stated preference must not keep
        // traffic on a provider that has stopped delivering.
        var priorityBoost = 1.0 / candidate.priority();
        var recentFailurePenalty = 1.0 - candidate.successRate5m();

        return weights.successRate() * candidate.successRate5m()
                + weights.latency() * (1.0 / normLatency)
                + weights.cost() * (1.0 / normCost)
                + weights.priority() * priorityBoost
                - weights.recentFailure() * recentFailurePenalty;
    }
}
