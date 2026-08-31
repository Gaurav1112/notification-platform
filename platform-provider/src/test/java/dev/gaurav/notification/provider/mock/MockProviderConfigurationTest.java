package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.provider.registry.ProviderRegistry;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wiring tests. These exist because the two things that break here — property binding and bean
 * collection — both fail <em>silently</em>: a mistyped prefix leaves every provider on its default
 * chaos settings, and a missed bean leaves a channel unroutable. Neither shows up in a unit test.
 */
class MockProviderConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(MockProviderConfiguration.class, RegistryConfiguration.class);

    /** Stands in for the app module's component scan. */
    @Configuration(proxyBeanMethods = false)
    static class RegistryConfiguration {
        @Bean
        ProviderRegistry providerRegistry(List<NotificationProvider> providers) {
            return new ProviderRegistry(providers);
        }
    }

    @Test
    @DisplayName("Spring collects every adapter into the registry, so adding a vendor is one @Bean and nothing else")
    void adaptersAreCollectedIntoTheRegistry() {
        runner.run(context -> {
            var registry = context.getBean(ProviderRegistry.class);

            assertThat(registry.forChannel(Channel.SMS))
                    .extracting(p -> p.code().value())
                    .containsExactlyInAnyOrder("mock-sms-primary", "mock-sms-secondary");
            assertThat(registry.forChannel(Channel.EMAIL)).hasSize(2);
            assertThat(registry.forChannel(Channel.PUSH)).hasSize(1);
            assertThat(registry.byCode(ProviderCode.of("mock-sms-secondary"))).isPresent();
        });
    }

    @Test
    @DisplayName("each channel that has an alternative has two providers, or the failover half of the design is untestable")
    void smsAndEmailHaveSomewhereToFailOverTo() {
        runner.run(context -> {
            var registry = context.getBean(ProviderRegistry.class);

            assertThat(registry.forChannel(Channel.SMS).size()).isGreaterThan(1);
            assertThat(registry.forChannel(Channel.EMAIL).size()).isGreaterThan(1);
            // Push is deliberately alone: there is no competitor to FCM for an Android device.
            assertThat(registry.forChannel(Channel.PUSH)).hasSize(1);
        });
    }

    @Test
    @DisplayName("per-provider chaos properties actually bind — a mistyped prefix would leave every provider on defaults")
    void propertiesBind() {
        runner.withPropertyValues(
                "notification.providers.mock.seed=777",
                "notification.providers.mock.providers.mock-sms-primary.success-rate=0.5",
                "notification.providers.mock.providers.mock-sms-primary.latency.median=250ms",
                "notification.providers.mock.providers.mock-sms-primary.latency.p99=4s",
                "notification.providers.mock.providers.mock-sms-primary.seed=999",
                "notification.providers.mock.providers.mock-sms-primary.failures[0].profile=SILENT_SUCCESS",
                "notification.providers.mock.providers.mock-sms-primary.failures[0].weight=100"
        ).run(context -> {
            var properties = context.getBean(MockProviderProperties.class);
            var primary = properties.forProvider(ProviderCode.of("mock-sms-primary"));

            assertThat(properties.seed()).isEqualTo(777L);
            assertThat(primary.successRate()).isEqualTo(0.5);
            assertThat(primary.latency().median()).isEqualTo(Duration.ofMillis(250));
            assertThat(primary.latency().p99()).isEqualTo(Duration.ofSeconds(4));
            assertThat(properties.seedFor(ProviderCode.of("mock-sms-primary")))
                    .as("a per-provider seed override lets one failover test be brutal without moving anyone else's DLQ count")
                    .isEqualTo(999L);
            assertThat(primary.failures())
                    .singleElement()
                    .extracting(MockProviderProperties.WeightedProfile::profile)
                    .isEqualTo(FailureProfile.SILENT_SUCCESS);
        });
    }

    @Test
    @DisplayName("an unconfigured provider falls back to defaults rather than a null success rate")
    void unconfiguredProvidersGetDefaults() {
        runner.run(context -> {
            var properties = context.getBean(MockProviderProperties.class);
            var untouched = properties.forProvider(ProviderCode.of("mock-email-secondary"));

            assertThat(untouched.successRate()).isEqualTo(0.98);
            assertThat(untouched.failures()).isNotEmpty();
            assertThat(properties.seedFor(ProviderCode.of("mock-email-secondary")))
                    .isEqualTo(properties.seed());
        });
    }

    @Test
    @DisplayName("disabling the mocks removes them entirely, so a real-provider deployment does not ship a chaos monkey")
    void mocksCanBeDisabled() {
        runner.withPropertyValues("notification.providers.mock.enabled=false")
                .run(context -> assertThat(context.getBeansOfType(NotificationProvider.class)).isEmpty());
    }
}
