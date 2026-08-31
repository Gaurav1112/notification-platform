package dev.gaurav.notification.resilience.circuitbreaker;

import java.time.Duration;

/**
 * The numbers behind every provider circuit breaker, kept in one value object so they can be
 * asserted on in a test and overridden per provider without editing the builder.
 *
 * <p>Each default is a trade-off, not a convention:
 *
 * <ul>
 *   <li><b>50% failure rate</b> — a provider failing half its calls is unusable, but a lower
 *       threshold trips on ordinary noise. SMS routes fail a few percent of calls permanently
 *       (dead numbers) even when perfectly healthy.
 *   <li><b>20 minimum calls</b> — without a floor, the first two calls after a deploy failing gives
 *       a 100% failure rate on a sample size of two and opens the circuit on a healthy provider.
 *       This is the single most common circuit breaker misconfiguration.
 *   <li><b>60 s time-based window</b> — count-based windows lie on a low-traffic channel: the last
 *       100 calls can span an hour, so the breaker opens on failures that were fixed 50 minutes
 *       ago. Time-based means "recently", which is what the word health means.
 *   <li><b>30 s in open state</b> — long enough for a provider restart or a failover to complete,
 *       short enough that a transient blip does not cost meaningful delivery latency. Jittered per
 *       JVM; see {@link ProviderCircuitBreakerConfiguration}.
 *   <li><b>3 half-open probes</b> — enough signal to distinguish recovery from a fluke, few enough
 *       that a still-broken provider is not re-flooded. Note this is per JVM, which is why the
 *       wait duration must be jittered.
 * </ul>
 *
 * @param failureRateThreshold     percentage of failed calls that opens the circuit
 * @param minimumNumberOfCalls     calls required in the window before the rate is evaluated at all
 * @param slidingWindowSeconds     width of the time-based window
 * @param waitDurationInOpenState  base time spent OPEN before probing; jitter is added per instance
 * @param permittedCallsInHalfOpen probes allowed through per JVM while HALF_OPEN
 */
public record CircuitBreakerSettings(float failureRateThreshold,
                                     int minimumNumberOfCalls,
                                     int slidingWindowSeconds,
                                     Duration waitDurationInOpenState,
                                     int permittedCallsInHalfOpen) {

    public CircuitBreakerSettings {
        if (failureRateThreshold <= 0 || failureRateThreshold > 100) {
            throw new IllegalArgumentException("failureRateThreshold must be in (0,100]");
        }
        if (minimumNumberOfCalls < 1) {
            throw new IllegalArgumentException("minimumNumberOfCalls must be at least 1");
        }
        if (slidingWindowSeconds < 1) {
            throw new IllegalArgumentException("slidingWindowSeconds must be at least 1");
        }
        if (waitDurationInOpenState.isNegative() || waitDurationInOpenState.isZero()) {
            throw new IllegalArgumentException("waitDurationInOpenState must be positive");
        }
        if (permittedCallsInHalfOpen < 1) {
            throw new IllegalArgumentException("permittedCallsInHalfOpen must be at least 1");
        }
    }

    /** 50% over at least 20 calls in 60 s, 30 s open, 3 half-open probes. */
    public static CircuitBreakerSettings defaults() {
        return new CircuitBreakerSettings(50f, 20, 60, Duration.ofSeconds(30), 3);
    }
}
