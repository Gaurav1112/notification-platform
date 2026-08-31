package dev.gaurav.notification.resilience.circuitbreaker;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Random;

/**
 * Builds the Resilience4j configuration used by every {@code (provider, channel)} circuit breaker.
 *
 * <p><strong>The failure this prevents: the synchronised half-open stampede.</strong>
 * {@code permittedNumberOfCallsInHalfOpenState} is enforced <em>inside a single JVM</em>. Nothing
 * in Resilience4j coordinates across pods. A provider outage fails calls on all 40 worker pods at
 * roughly the same second, so all 40 breakers open at roughly the same second — and 30 seconds
 * later all 40 transition to HALF_OPEN at roughly the same second and each admits its 3 probes.
 * The recovering provider is hit by 120 synchronised probes in one instant instead of the 3 the
 * configuration appears to promise. Enough of them fail, all 40 breakers re-open together, and the
 * fleet settles into a 30-second oscillation that keeps the provider down. The configured number is
 * off by a factor of the fleet size, and the fleet size is exactly what grows during an incident.
 *
 * <p>The fix here is deliberately the cheap one: each JVM adds a uniform random offset of up to the
 * base wait to its own {@code waitDurationInOpenState}, drawn <em>once at startup</em>. Forty pods
 * then probe spread across a 30–60 s band rather than all at t+30 s, and the provider sees a
 * trickle. It is drawn once rather than per transition so a pod's behaviour stays predictable and
 * reproducible from its logs.
 *
 * <p>Two alternatives were considered. Publishing aggregate breaker state to Redis at 1 Hz gives
 * true fleet-wide coordination and is the design's longer-term answer for state <em>sharing</em>,
 * but it does not by itself desynchronise the probe instant and it puts a dependency on the failure
 * path. A distributed lock around the half-open probe is correct and far too expensive for a
 * 3,900 calls/s hot path.
 *
 * <p>The registry bean is created only if the application has not already supplied one, so this
 * composes with Resilience4j's own autoconfiguration rather than colliding with it.
 */
@Configuration
public class ProviderCircuitBreakerConfiguration {

    /**
     * Per-JVM jitter source. {@link SecureRandom} rather than a seeded {@link Random} specifically
     * so that identical pods rolled from one image do not draw identical offsets — a seeded PRNG
     * would reproduce the very synchronisation this exists to break.
     */
    private final Random jitterSource;

    public ProviderCircuitBreakerConfiguration() {
        this(new SecureRandom());
    }

    ProviderCircuitBreakerConfiguration(Random jitterSource) {
        this.jitterSource = jitterSource;
    }

    @Bean
    @ConditionalOnMissingBean
    public CircuitBreakerSettings providerCircuitBreakerSettings() {
        return CircuitBreakerSettings.defaults();
    }

    /**
     * The shared template for every provider breaker.
     *
     * <p>The window is TIME_BASED: a count-based window on a low-volume channel evaluates calls
     * that are hours old and reports them as current health.
     */
    @Bean
    public CircuitBreakerConfig providerCircuitBreakerConfig(CircuitBreakerSettings settings) {
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(settings.failureRateThreshold())
                .minimumNumberOfCalls(settings.minimumNumberOfCalls())
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
                .slidingWindowSize(settings.slidingWindowSeconds())
                .waitDurationInOpenState(jitteredWaitDuration(settings.waitDurationInOpenState()))
                .permittedNumberOfCallsInHalfOpenState(settings.permittedCallsInHalfOpen())
                // Without this the breaker only leaves OPEN when a call arrives. On a channel that
                // goes quiet during an outage — because upstream shed the load — nothing ever
                // probes, and the provider stays fenced off long after it recovered.
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                // The 4xx rule. See FailureClassifier: a malformed payload is our bug, and counting
                // it takes a healthy provider offline.
                .recordException(FailureClassifier::shouldRecordAsCircuitFailure)
                .build();
    }

    @Bean
    @ConditionalOnMissingBean
    public CircuitBreakerRegistry circuitBreakerRegistry(CircuitBreakerConfig providerCircuitBreakerConfig) {
        return CircuitBreakerRegistry.of(providerCircuitBreakerConfig);
    }

    @Bean
    public ProviderCircuitBreakers providerCircuitBreakers(CircuitBreakerRegistry registry,
                                                           CircuitBreakerConfig providerCircuitBreakerConfig) {
        return new ProviderCircuitBreakers(registry, providerCircuitBreakerConfig);
    }

    /**
     * {@code base + random(0, base)} — 30 s becomes a draw from [30 s, 60 s).
     *
     * <p>Package-private so a test can pin the {@link Random} and assert the band, which is the
     * only way to prove the desynchronisation actually happens.
     */
    Duration jitteredWaitDuration(Duration base) {
        long baseMillis = base.toMillis();
        return Duration.ofMillis(baseMillis + jitterSource.nextLong(baseMillis));
    }
}
