package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.provider.spi.ProviderCode;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Registers the mock adapters as beans so {@link dev.gaurav.notification.provider.registry.ProviderRegistry}
 * picks them up through Spring's {@code List<NotificationProvider>} collection.
 *
 * <p><strong>Two providers per channel where failover matters.</strong> A single provider makes the
 * whole selection-and-failover half of the design untestable — you cannot fail over to nothing. The
 * secondary is deliberately worse on the axes the router scores: slower and about 20% dearer, so a
 * healthy primary always wins on score and only a real outage moves traffic. That asymmetry is what
 * the exploration cap in
 * {@link dev.gaurav.notification.provider.routing.HealthWeightedSelectionStrategy} is defending.
 *
 * <p>Enabled by default, because the mocks <em>are</em> the providers for this platform: there is
 * no vendor account behind it. Set {@code notification.providers.mock.enabled=false} to run against
 * real adapters once they exist.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(MockProviderProperties.class)
@ConditionalOnProperty(prefix = "notification.providers.mock", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class MockProviderConfiguration {

    public static final ProviderCode SMS_PRIMARY = ProviderCode.of("mock-sms-primary");
    public static final ProviderCode SMS_SECONDARY = ProviderCode.of("mock-sms-secondary");
    public static final ProviderCode EMAIL_PRIMARY = ProviderCode.of("mock-email-primary");
    public static final ProviderCode EMAIL_SECONDARY = ProviderCode.of("mock-email-secondary");
    public static final ProviderCode PUSH_PRIMARY = ProviderCode.of("mock-push-primary");

    /**
     * Shared across every adapter on purpose: one chaos command names one provider, but the
     * operator needs a single place to read back what is currently broken.
     */
    @Bean
    @ConditionalOnMissingBean
    public ChaosState chaosState() {
        return new ChaosState();
    }

    @Bean
    @ConditionalOnMissingBean
    public FailureInjector failureInjector(MockProviderProperties properties) {
        return new FailureInjector(properties);
    }

    /**
     * Real time, because these beans exist to make latency and timeouts observable. A test that
     * wants speed constructs the adapters directly with {@link Sleeper#NONE}.
     */
    @Bean
    @ConditionalOnMissingBean
    public Sleeper providerSleeper() {
        return Sleeper.REAL;
    }

    @Bean
    public MockSmsProvider mockSmsPrimary(ChaosState chaos, FailureInjector injector, Sleeper sleeper) {
        return new MockSmsProvider(SMS_PRIMARY, chaos, injector, sleeper, 7_900L);
    }

    @Bean
    public MockSmsProvider mockSmsSecondary(ChaosState chaos, FailureInjector injector, Sleeper sleeper) {
        return new MockSmsProvider(SMS_SECONDARY, chaos, injector, sleeper, 9_500L);
    }

    @Bean
    public MockEmailProvider mockEmailPrimary(ChaosState chaos, FailureInjector injector, Sleeper sleeper) {
        return new MockEmailProvider(EMAIL_PRIMARY, chaos, injector, sleeper, 100L);
    }

    @Bean
    public MockEmailProvider mockEmailSecondary(ChaosState chaos, FailureInjector injector, Sleeper sleeper) {
        return new MockEmailProvider(EMAIL_SECONDARY, chaos, injector, sleeper, 400L);
    }

    /**
     * One push provider, and no secondary — deliberately. FCM and APNs are the only routes to their
     * respective devices; there is no competitor to fail over to. Pretending otherwise would model
     * a resilience option that does not exist, and the honest answer for a push outage is retry
     * with backoff until the TTL expires.
     */
    @Bean
    public MockPushProvider mockPushPrimary(ChaosState chaos, FailureInjector injector, Sleeper sleeper) {
        return new MockPushProvider(PUSH_PRIMARY, chaos, injector, sleeper);
    }

    /** The deadline a demo stack gives a provider before declaring the outcome unknown. */
    public static Duration defaultSendCeiling() {
        return Duration.ofSeconds(5);
    }
}
