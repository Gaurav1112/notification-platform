package dev.gaurav.notification.resilience.circuitbreaker;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publishes {@code provider_circuit_state} so an operator can see a breaker open.
 *
 * <p>This did not exist while {@code CircuitBreakerProvider} was a pass-through, and its absence
 * was invisible for the same reason the pass-through was: a Prometheus rule referencing a metric
 * nothing emits does not error, it simply evaluates to no series. The
 * {@code AllProvidersOpenForChannel} alert has therefore been permanently silent. That is worse
 * than having no alert — a dashboard with an empty panel reads as "healthy" rather than
 * "uninstrumented".
 *
 * <h2>One gauge per breaker, not one per state</h2>
 *
 * <p>The obvious modelling — a series per {@code (provider, channel, state)} with a 0/1 value — is
 * a trap here. {@code AllProvidersOpenForChannel} compares
 * {@code count(provider_circuit_state{state="OPEN"} == 1)} against
 * {@code count(provider_circuit_state)}; with a series per state the denominator counts
 * providers × states and the equality can never hold, so the alert would still never fire.
 *
 * <p>So each breaker gets exactly one series carrying its state as an ordinal:
 * {@code 0 CLOSED · 1 HALF_OPEN · 2 OPEN · 3 FORCED_OPEN}. The denominator is then the provider
 * count, which is what the rule assumes.
 *
 * <p>Gauges are registered on the registry's {@code onEntryAdded} event rather than eagerly,
 * because breakers are created lazily on first call to a provider. Registering up front would
 * require knowing every (provider, channel) pair at startup, which is exactly the coupling the
 * lazy registry exists to avoid.
 */
public final class ProviderCircuitBreakerMetrics
        implements RegistryEventConsumer<CircuitBreaker> {

    /** {@code provider_circuit_state} once Micrometer converts dots to underscores. */
    static final String METRIC = "provider.circuit.state";

    private final MeterRegistry meters;

    /** Strong references: Micrometer holds gauge state weakly and would otherwise collect them. */
    private final Map<String, CircuitBreaker> tracked = new ConcurrentHashMap<>();

    public ProviderCircuitBreakerMetrics(MeterRegistry meters) {
        this.meters = meters;
    }

    /**
     * Registers the gauges for breakers that already exist.
     *
     * <p>Needed because the registry may have been populated before this consumer was attached —
     * event consumers only see entries added after registration.
     */
    public void bindExisting(CircuitBreakerRegistry registry) {
        registry.getAllCircuitBreakers().forEach(this::bind);
    }

    @Override
    public void onEntryAddedEvent(EntryAddedEvent<CircuitBreaker> event) {
        bind(event.getAddedEntry());
    }

    @Override
    public void onEntryRemovedEvent(EntryRemovedEvent<CircuitBreaker> event) {
        tracked.remove(event.getRemovedEntry().getName());
    }

    @Override
    public void onEntryReplacedEvent(EntryReplacedEvent<CircuitBreaker> event) {
        tracked.remove(event.getOldEntry().getName());
        bind(event.getNewEntry());
    }

    private void bind(CircuitBreaker breaker) {
        if (tracked.putIfAbsent(breaker.getName(), breaker) != null) {
            return;
        }
        // The name is "provider:channel" — see ProviderCircuitBreakers.name(...).
        String[] parts = breaker.getName().split(":", 2);
        String provider = parts[0];
        String channel = parts.length > 1 ? parts[1] : "unknown";

        Gauge.builder(METRIC, breaker, b -> ordinalOf(b.getState()))
                .description("Circuit breaker state per provider and channel: "
                        + "0 CLOSED, 1 HALF_OPEN, 2 OPEN, 3 FORCED_OPEN")
                .tag("provider", provider)
                .tag("channel", channel)
                .register(meters);
    }

    /**
     * Deliberately not {@code State.ordinal()} — Resilience4j's enum order is an implementation
     * detail and includes transitional states. These values are a published contract that the
     * Prometheus rules and the Grafana dashboard depend on.
     */
    static double ordinalOf(CircuitBreaker.State state) {
        return switch (state) {
            case CLOSED, METRICS_ONLY -> 0d;
            case HALF_OPEN -> 1d;
            case OPEN -> 2d;
            case FORCED_OPEN, DISABLED -> 3d;
        };
    }
}
