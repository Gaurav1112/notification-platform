package dev.gaurav.notification.resilience.circuitbreaker;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.CircuitState;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Resolves the circuit breaker for a {@code (provider, channel)} pair.
 *
 * <p><strong>The granularity is the point.</strong> One breaker per provider would be wrong:
 * providers are multi-product, and an SES sending-quota problem on email says nothing about the
 * same vendor's push endpoint. Trip them together and one degraded product takes out an unrelated,
 * healthy one. One breaker per channel would be worse still — it would open on the aggregate of
 * three providers and remove the failover target along with the failing provider, which is the
 * exact opposite of what a breaker is for.
 *
 * <p>Breakers are created lazily by name and cached by the registry, so a provider added at runtime
 * gets one on first call without a restart.
 */
public class ProviderCircuitBreakers {

    private final CircuitBreakerRegistry registry;
    private final CircuitBreakerConfig config;

    public ProviderCircuitBreakers(CircuitBreakerRegistry registry, CircuitBreakerConfig config) {
        this.registry = registry;
        this.config = config;
    }

    /**
     * The breaker guarding calls to {@code providerCode} on {@code channel}.
     *
     * <p>The config is passed explicitly rather than relying on the registry default so that this
     * still applies the jittered, 4xx-aware configuration when the registry was contributed by
     * Resilience4j's autoconfiguration instead of by
     * {@link ProviderCircuitBreakerConfiguration}.
     */
    public CircuitBreaker forProvider(String providerCode, Channel channel) {
        return registry.circuitBreaker(name(providerCode, channel), config);
    }

    /** {@code twilio:sms} — also the {@code provider_circuit_state} metric label. */
    public static String name(String providerCode, Channel channel) {
        return providerCode + ":" + channel.name().toLowerCase();
    }

    /**
     * Current state as the domain enum, for the {@code /providers/health} endpoint and the provider
     * selection filter.
     *
     * <p>Resilience4j's {@code DISABLED} and {@code METRICS_ONLY} both let every call through, so
     * they map to {@link CircuitState#CLOSED} — the domain models "can I send", not the library's
     * internal bookkeeping.
     */
    public CircuitState stateOf(String providerCode, Channel channel) {
        return switch (forProvider(providerCode, channel).getState()) {
            case OPEN -> CircuitState.OPEN;
            case HALF_OPEN -> CircuitState.HALF_OPEN;
            case FORCED_OPEN -> CircuitState.FORCED_OPEN;
            case CLOSED, DISABLED, METRICS_ONLY -> CircuitState.CLOSED;
        };
    }
}
