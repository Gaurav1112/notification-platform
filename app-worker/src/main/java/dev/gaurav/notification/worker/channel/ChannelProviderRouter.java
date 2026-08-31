package dev.gaurav.notification.worker.channel;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.CircuitState;
import dev.gaurav.notification.provider.registry.ProviderRegistry;
import dev.gaurav.notification.provider.routing.ProviderCandidate;
import dev.gaurav.notification.provider.routing.ProviderSelectionStrategy;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.resilience.circuitbreaker.ProviderCircuitBreakers;
import dev.gaurav.notification.worker.config.DecoratedProviders;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Picks the provider for one send: filter for eligibility, then score.
 *
 * <p>The split matters. Filtering is the part that touches live state — circuit breakers, health
 * probes, the set of providers this message has already failed on — and scoring is a pure function
 * of numbers. Keeping them apart is what lets
 * {@link dev.gaurav.notification.provider.routing.HealthWeightedSelectionStrategy} stay unit
 * testable, and it is why the strategy's contract says the caller filters first.
 *
 * <p>An {@code OPEN} circuit removes a provider from the list rather than causing the send to
 * fail. That is the difference between a breaker that protects a vendor and a breaker that takes
 * an outage: with two providers on a channel, an open primary should mean traffic on the secondary,
 * not an error.
 *
 * <p>Empty is a normal return, not an exception. Every provider for a channel being down is an
 * operational state — park the message on a retry tier and page — and push genuinely has one route
 * to a device, so "nothing eligible" is a Tuesday, not a bug.
 */
@Component
public class ChannelProviderRouter {

    /**
     * @param provider              the <em>decorated</em> adapter — never the raw one. Calling the
     *                              adapter directly bypasses the timeout, and an unbounded provider
     *                              call is what turns a vendor incident into a rebalance storm
     * @param alternativesAvailable whether a failover target exists right now. Feeds the retry
     *                              decision: choosing to fail over with nowhere to go is a drop
     */
    public record Selection(NotificationProvider provider, ProviderCode code,
                            boolean alternativesAvailable) {
    }

    private final ProviderRegistry registry;
    private final ProviderSelectionStrategy strategy;
    private final ProviderCircuitBreakers circuitBreakers;
    private final DecoratedProviders decorated;

    public ChannelProviderRouter(ProviderRegistry registry,
                                 ProviderSelectionStrategy strategy,
                                 ProviderCircuitBreakers circuitBreakers,
                                 DecoratedProviders decorated) {
        this.registry = registry;
        this.strategy = strategy;
        this.circuitBreakers = circuitBreakers;
        this.decorated = decorated;
    }

    /**
     * @param preferred providers pinned by the dispatch event, honoured only if still eligible. A
     *                  pin that survives an outage is not a pin, it is a single point of failure
     * @param excluded  providers this message has already failed on. Without it a failover picks
     *                  the same provider again and the "failover" is an immediate retry
     */
    public Optional<Selection> select(Channel channel, long tenantId,
                                      String preferred, Set<ProviderCode> excluded) {
        var eligible = eligible(channel, excluded);
        if (eligible.isEmpty()) {
            return Optional.empty();
        }
        if (preferred != null && !preferred.isBlank()) {
            var pinned = ProviderCode.of(preferred);
            var match = eligible.stream().filter(c -> c.code().equals(pinned)).findFirst();
            if (match.isPresent()) {
                return Optional.of(selection(match.get(), eligible));
            }
        }
        return strategy.select(channel, Long.toString(tenantId), eligible)
                .map(chosen -> selection(chosen, eligible));
    }

    private Selection selection(ProviderCandidate chosen, List<ProviderCandidate> eligible) {
        return new Selection(chosen.provider(), chosen.code(), eligible.size() > 1);
    }

    private List<ProviderCandidate> eligible(Channel channel, Set<ProviderCode> excluded) {
        var candidates = new ArrayList<ProviderCandidate>();
        var registered = registry.forChannel(channel);
        for (int i = 0; i < registered.size(); i++) {
            var provider = registered.get(i);
            if (excluded.contains(provider.code())) {
                continue;
            }
            var state = circuitBreakers.stateOf(provider.code().value(), channel);
            if (state == CircuitState.OPEN || state == CircuitState.FORCED_OPEN) {
                continue;
            }
            if (!provider.isHealthy()) {
                continue;
            }
            // Registration order is the operator's stated preference; the score breaks ties. Cost
            // is zero until provider_configuration is read at startup (TODO(phase-9)), which makes
            // the cost term neutral rather than wrong — a made-up price would bias every choice.
            candidates.add(new ProviderCandidate(
                    decorated.wrap(provider), i + 1, 1, 0L, 0,
                    successRate(provider.code(), channel), 0.0));
        }
        return candidates;
    }

    /**
     * Live success rate from the circuit breaker's own sliding window.
     *
     * <p>Reusing the breaker's window rather than querying {@code delivery_attempt} is not a
     * shortcut: that query was measured at 481 MB and 291 ms over a single day's partition, and it
     * would run on the send path. The breaker already keeps exactly this number in memory.
     *
     * <p>Resilience4j returns {@code -1} until the minimum call count is reached. That is mapped to
     * {@code 1.0}, not to {@code 0.0}: a provider with no measurements yet must not score last
     * forever and thereby never receive the traffic that would give it a score.
     */
    private double successRate(ProviderCode code, Channel channel) {
        float failureRate = circuitBreakers.forProvider(code.value(), channel)
                .getMetrics().getFailureRate();
        return failureRate < 0 ? 1.0 : Math.max(0.0, 1.0 - (failureRate / 100.0));
    }
}
