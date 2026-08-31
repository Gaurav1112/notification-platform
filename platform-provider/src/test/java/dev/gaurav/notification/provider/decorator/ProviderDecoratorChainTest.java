package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.provider.StubProvider;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.resilience.circuitbreaker.ProviderCircuitBreakers;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Decorator order is a design decision with observable consequences, so it is asserted rather than
 * left to whoever wires the beans.
 */
class ProviderDecoratorChainTest {

    private static ProviderCircuitBreakers breakers() {
        var config = CircuitBreakerConfig.ofDefaults();
        return new ProviderCircuitBreakers(CircuitBreakerRegistry.of(config), config);
    }

    @Test
    @DisplayName("the chain is assembled in the canonical order regardless of the order the builder was called in")
    void orderIsFixedNotCallerChosen() {
        // Wired by hand, this gets subtly wrong once and nobody notices for six months — typically
        // metrics ending up inside the breaker, which makes an open circuit look like perfect health.
        var registry = new SimpleMeterRegistry();
        var adapter = StubProvider.alwaysAccepts("mock-sms-primary", Channel.SMS);

        var chain = ProviderDecoratorChain.around(adapter)
                .idempotent(new InMemorySentTokenLog())
                .rateLimited()
                .traced()
                .timeoutWithDedicatedPool(2, 4, Duration.ofSeconds(1))
                .circuitBroken(breakers())
                .metered(registry)
                .build();

        assertThat(layerNames(chain)).containsExactly(
                "TracedProvider",
                "MeteredProvider",
                "CircuitBreakerProvider",
                "RateLimitedProvider",
                "TimeoutProvider",
                "IdempotentProvider",
                "StubProvider");
    }

    @Test
    @DisplayName("idempotency sits inside the timeout, so a timed-out call still left a record that it happened")
    void idempotencyIsInsideTheTimeout() {
        // The other way round, the one case the token log exists for — a call cut off mid-flight —
        // would leave nothing behind at all.
        var chain = ProviderDecoratorChain.around(StubProvider.alwaysAccepts("p", Channel.SMS))
                .full(new SimpleMeterRegistry(), breakers(), new InMemorySentTokenLog(), Duration.ofSeconds(1))
                .build();

        var layers = layerNames(chain);
        assertThat(layers.indexOf("TimeoutProvider")).isLessThan(layers.indexOf("IdempotentProvider"));
    }

    @Test
    @DisplayName("metrics sit outside the breaker, or an open circuit reports as zero traffic and perfect health")
    void metricsAreOutsideTheBreaker() {
        var chain = ProviderDecoratorChain.around(StubProvider.alwaysAccepts("p", Channel.SMS))
                .full(new SimpleMeterRegistry(), breakers(), new InMemorySentTokenLog(), Duration.ofSeconds(1))
                .build();

        var layers = layerNames(chain);
        assertThat(layers.indexOf("MeteredProvider")).isLessThan(layers.indexOf("CircuitBreakerProvider"));
    }

    @Test
    @DisplayName("omitting a stage shortens the chain without reordering the rest")
    void optionalStagesAreOptional() {
        var chain = ProviderDecoratorChain.around(StubProvider.alwaysAccepts("p", Channel.SMS))
                .metered(new SimpleMeterRegistry())
                .idempotent(new InMemorySentTokenLog())
                .build();

        assertThat(layerNames(chain)).containsExactly("MeteredProvider", "IdempotentProvider", "StubProvider");
    }

    @Test
    @DisplayName("identity survives the whole stack, so the registry and the chaos endpoint still resolve it")
    void identitySurvivesWrapping() {
        var adapter = StubProvider.alwaysAccepts("mock-sms-primary", Channel.SMS);
        var chain = ProviderDecoratorChain.around(adapter)
                .full(new SimpleMeterRegistry(), breakers(), new InMemorySentTokenLog(), Duration.ofSeconds(1))
                .build();

        assertThat(chain.code()).isEqualTo(adapter.code());
        assertThat(chain.channel()).isEqualTo(adapter.channel());
        assertThat(chain.capabilities()).isEqualTo(adapter.capabilities());
    }

    private static List<String> layerNames(NotificationProvider chain) {
        var names = new ArrayList<String>();
        var current = chain;
        while (current instanceof AbstractProviderDecorator decorator) {
            names.add(current.getClass().getSimpleName());
            current = decorator.delegate();
        }
        names.add(current.getClass().getSimpleName());
        return names;
    }
}
