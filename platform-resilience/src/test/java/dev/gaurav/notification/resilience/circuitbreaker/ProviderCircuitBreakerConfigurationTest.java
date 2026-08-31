package dev.gaurav.notification.resilience.circuitbreaker;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Proves the two properties a fleet of 40 pods depends on: desynchronised probes, and the 4xx rule. */
class ProviderCircuitBreakerConfigurationTest {

    private final CircuitBreakerSettings settings = CircuitBreakerSettings.defaults();

    @Test
    @DisplayName("40 pods that opened together must not all probe at t+30s")
    void waitDurationIsJitteredPerInstance() {
        // Each instance stands in for one pod drawing its offset at startup.
        var distinctSeconds = new HashSet<Long>();
        for (int pod = 0; pod < 40; pod++) {
            var configuration = new ProviderCircuitBreakerConfiguration(new Random(pod));
            Duration wait = configuration.jitteredWaitDuration(settings.waitDurationInOpenState());

            assertThat(wait)
                    .as("jitter must extend the wait, never shorten it below the configured floor")
                    .isBetween(Duration.ofSeconds(30), Duration.ofSeconds(60));
            distinctSeconds.add(wait.toSeconds());
        }

        assertThat(distinctSeconds)
                .as("permittedNumberOfCallsInHalfOpenState is per JVM, so synchronised pods send "
                        + "40x3 probes in one instant; spreading the wait is what prevents it")
                .hasSizeGreaterThan(10);
    }

    @Test
    @DisplayName("a malformed payload does not count towards the failure rate that opens the circuit")
    void clientErrorsAreNotRecorded() {
        CircuitBreakerConfig config = config();

        assertThat(config.getRecordExceptionPredicate().test(new IllegalArgumentException("bad E.164")))
                .as("an unrecognised exception is our bug, and opening on it takes a healthy "
                        + "provider offline for every tenant")
                .isFalse();
    }

    @Test
    @DisplayName("a provider timeout does count, because that is what the breaker exists to detect")
    void providerFailuresAreRecorded() {
        assertThat(config().getRecordExceptionPredicate().test(new SocketTimeoutException("read timeout")))
                .isTrue();
    }

    @Test
    @DisplayName("the window is time-based, so a quiet channel is not judged on hour-old calls")
    void windowIsTimeBased() {
        CircuitBreakerConfig config = config();

        assertThat(config.getSlidingWindowType()).isEqualTo(CircuitBreakerConfig.SlidingWindowType.TIME_BASED);
        assertThat(config.getSlidingWindowSize()).isEqualTo(60);
        assertThat(config.getMinimumNumberOfCalls())
                .as("without a floor, two failed calls after a deploy are a 100% failure rate")
                .isEqualTo(20);
        assertThat(config.getFailureRateThreshold()).isEqualTo(50f);
        assertThat(config.getPermittedNumberOfCallsInHalfOpenState()).isEqualTo(3);
    }

    @Test
    @DisplayName("a channel that goes silent during an outage still probes, instead of staying open forever")
    void openStateTransitionsAutomatically() {
        assertThat(config().isAutomaticTransitionFromOpenToHalfOpenEnabled()).isTrue();
    }

    private CircuitBreakerConfig config() {
        return new ProviderCircuitBreakerConfiguration(new Random(1L))
                .providerCircuitBreakerConfig(settings);
    }
}
