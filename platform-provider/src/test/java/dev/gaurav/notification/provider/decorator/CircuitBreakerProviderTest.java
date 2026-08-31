package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCapabilities;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;
import dev.gaurav.notification.resilience.circuitbreaker.ProviderCircuitBreakers;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * These tests exist because this decorator shipped as {@code return delegate.send(command);}.
 *
 * <p>Nothing failed. The build was green, 371 tests passed, and the provider layer looked complete
 * — because every breaker stayed permanently {@code CLOSED}, which reads exactly like "no provider
 * has failed yet". The failure only becomes visible if you assert on the breaker's <em>state</em>
 * after driving traffic through it, which nothing did.
 *
 * <p>So the assertions here are deliberately about observable breaker state and short-circuit
 * behaviour, not about the decorator being present in the chain. A pass-through passes any test
 * that only checks the result of a healthy call.
 */
class CircuitBreakerProviderTest {

    private static final SendCommand COMMAND = new SendCommand(
            UUID.randomUUID(), Channel.SMS, dev.gaurav.notification.domain.enums.TrafficClass.CRITICAL,
            "+15551234567", null, "body", "tok-1", Map.of(), Duration.ofSeconds(5));

    /** Trips after 4 failures in a window of 4, so the tests stay short and deterministic. */
    private static ProviderCircuitBreakers breakers() {
        var config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(4)
                .minimumNumberOfCalls(4)
                .failureRateThreshold(50f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .build();
        return new ProviderCircuitBreakers(CircuitBreakerRegistry.of(config), config);
    }

    /** A provider whose every call returns whatever the supplier says. */
    private record ScriptedProvider(Supplier<SendResult> script) implements NotificationProvider {
        public Channel channel() { return Channel.SMS; }
        public ProviderCode code() { return ProviderCode.of("scripted"); }
        public ProviderCapabilities capabilities() { return ProviderCapabilities.singleSend(true); }
        public SendResult send(SendCommand c) { return script.get(); }
        public boolean isHealthy() { return true; }
    }

    private static SendResult serverError() {
        return SendResult.Rejected.of(FailureType.PROVIDER_5XX, "500", "boom", Duration.ofMillis(5));
    }

    private static SendResult invalidRecipient() {
        return SendResult.Rejected.of(FailureType.INVALID_RECIPIENT, "21614", "bad number",
                Duration.ofMillis(5));
    }

    private static SendResult accepted() {
        return new SendResult.Accepted("msg-1", Duration.ofMillis(5), 100L);
    }

    private static CircuitBreaker breakerOf(ProviderCircuitBreakers registry) {
        return registry.forProvider("scripted", Channel.SMS);
    }

    @Test
    @DisplayName("repeated 5xx opens the circuit — the state a pass-through could never reach")
    void serverErrorsOpenTheCircuit() {
        var registry = breakers();
        var provider = new CircuitBreakerProvider(new ScriptedProvider(
                CircuitBreakerProviderTest::serverError), registry);

        for (int i = 0; i < 4; i++) {
            provider.send(COMMAND);
        }

        assertThat(breakerOf(registry).getState())
                .as("four consecutive 5xx in a window of four must trip the breaker")
                .isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    @DisplayName("an open circuit short-circuits to Rejected, so the router fails over instead of throwing")
    void openCircuitReturnsRejectedNotAnException() {
        var registry = breakers();
        var provider = new CircuitBreakerProvider(new ScriptedProvider(
                CircuitBreakerProviderTest::serverError), registry);
        for (int i = 0; i < 4; i++) {
            provider.send(COMMAND);
        }

        var result = provider.send(COMMAND);

        assertThat(result).isInstanceOf(SendResult.Rejected.class);
        var rejected = (SendResult.Rejected) result;
        assertThat(rejected.code()).isEqualTo("CIRCUIT_OPEN");
        assertThat(rejected.type().isRetryable())
                .as("a short-circuit must be retryable — the message is fine, this provider is not")
                .isTrue();
    }

    @Test
    @DisplayName("an open circuit stops calling the provider at all")
    void openCircuitDoesNotTouchTheDelegate() {
        var registry = breakers();
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var provider = new CircuitBreakerProvider(new ScriptedProvider(() -> {
            calls.incrementAndGet();
            return serverError();
        }), registry);

        for (int i = 0; i < 4; i++) {
            provider.send(COMMAND);
        }
        int callsWhenOpened = calls.get();
        provider.send(COMMAND);
        provider.send(COMMAND);

        assertThat(calls.get())
                .as("the whole point is to stop hammering a service that is already failing")
                .isEqualTo(callsWhenOpened);
    }

    @Test
    @DisplayName("a 4xx does NOT trip the breaker — a bad phone number is our problem, not the provider's")
    void permanentFailuresDoNotTripTheBreaker() {
        var registry = breakers();
        var provider = new CircuitBreakerProvider(new ScriptedProvider(
                CircuitBreakerProviderTest::invalidRecipient), registry);

        for (int i = 0; i < 8; i++) {
            provider.send(COMMAND);
        }

        assertThat(breakerOf(registry).getState())
                .as("a batch of invalid numbers must not take a healthy provider offline for everyone")
                .isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    @DisplayName("a timeout counts against the provider even though the message outcome is unknown")
    void indeterminateCountsAsAProviderFailure() {
        var registry = breakers();
        var provider = new CircuitBreakerProvider(new ScriptedProvider(() ->
                new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT, "timed out",
                        Duration.ofSeconds(5))), registry);

        for (int i = 0; i < 4; i++) {
            provider.send(COMMAND);
        }

        assertThat(breakerOf(registry).getState())
                .as("ambiguous for the message, unambiguous for the provider: it stopped answering")
                .isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    @DisplayName("successes keep the circuit closed and pass the result through untouched")
    void successesPassThrough() {
        var registry = breakers();
        var provider = new CircuitBreakerProvider(new ScriptedProvider(
                CircuitBreakerProviderTest::accepted), registry);

        SendResult result = null;
        for (int i = 0; i < 8; i++) {
            result = provider.send(COMMAND);
        }

        assertThat(breakerOf(registry).getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(result).isInstanceOf(SendResult.Accepted.class);
        assertThat(((SendResult.Accepted) result).providerMessageId()).isEqualTo("msg-1");
    }

    @Test
    @DisplayName("the breaker is per (provider, channel), so one bad provider cannot mute another")
    void breakersAreIsolatedPerProvider() {
        var registry = breakers();
        var failing = new CircuitBreakerProvider(new ScriptedProvider(
                CircuitBreakerProviderTest::serverError), registry);
        for (int i = 0; i < 4; i++) {
            failing.send(COMMAND);
        }

        assertThat(registry.forProvider("scripted", Channel.SMS).getState())
                .isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(registry.forProvider("other-provider", Channel.SMS).getState())
                .as("a second provider on the same channel must be unaffected — that is what failover needs")
                .isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(registry.forProvider("scripted", Channel.EMAIL).getState())
                .as("the same provider on a different channel is a different failure domain")
                .isEqualTo(CircuitBreaker.State.CLOSED);
    }
}
