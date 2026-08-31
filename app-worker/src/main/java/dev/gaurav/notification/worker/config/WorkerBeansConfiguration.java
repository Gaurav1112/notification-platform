package dev.gaurav.notification.worker.config;

import dev.gaurav.notification.provider.decorator.InMemorySentTokenLog;
import dev.gaurav.notification.provider.decorator.SentTokenLog;
import dev.gaurav.notification.resilience.retry.DefaultRetryPolicy;
import dev.gaurav.notification.resilience.retry.RetryBudget;
import dev.gaurav.notification.resilience.retry.RetryPolicy;
import dev.gaurav.notification.worker.orchestrator.EchoTemplateRenderer;
import dev.gaurav.notification.worker.orchestrator.InlineRecipientManifestReader;
import dev.gaurav.notification.worker.orchestrator.PermissivePreferenceResolver;
import dev.gaurav.notification.worker.orchestrator.PreferenceResolver;
import dev.gaurav.notification.worker.orchestrator.RecipientManifestReader;
import dev.gaurav.notification.worker.orchestrator.TemplateRenderer;
import dev.gaurav.notification.worker.status.LoggingSuppressionWriter;
import dev.gaurav.notification.worker.status.SuppressionWriter;
import dev.gaurav.notification.worker.support.DevelopmentAddressVault;
import dev.gaurav.notification.worker.support.RecipientAddressVault;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.security.SecureRandom;
import java.time.Clock;

/**
 * The worker's own wiring, and the placeholders that stand in for modules that do not exist yet.
 *
 * <p>Every placeholder is registered here behind {@code @ConditionalOnMissingBean} rather than
 * annotated {@code @Component} on the class. A conditional evaluated during component scanning
 * depends on scan order, so the real implementation would win or lose depending on its package
 * name — which is a startup-order bug that only reproduces on some machines. Registered from a
 * configuration class, the conditionals are evaluated after all user beans are known, and the
 * rule is simply "the real one wins".
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(WorkerProperties.class)
@EnableScheduling
public class WorkerBeansConfiguration {

    /**
     * Injected everywhere rather than calling {@code Instant.now()}.
     *
     * <p>Nearly every rule in this module is a comparison against a deadline — TTL elapsed, retry
     * due, attempt abandoned. A static clock call makes all of them untestable except by sleeping,
     * and a test suite that sleeps is a test suite that gets deleted.
     */
    @Bean
    @ConditionalOnMissingBean
    public Clock workerClock() {
        return Clock.systemUTC();
    }

    /**
     * Five attempts, full-jitter backoff, 72-minute deadline — the tier ladder expressed as a
     * policy, so a message can never be scheduled into a tier that outlives its own budget.
     *
     * <p>A per-instance {@link SecureRandom}, never {@code Math.random()}: the jitter is what stops
     * 100,000 messages that failed at the same instant from retrying at the same instant and
     * re-killing the provider the moment it recovers. A shared, contended source would be both a
     * bottleneck and, under contention, less uniform than it looks.
     */
    @Bean
    @ConditionalOnMissingBean
    public RetryPolicy retryPolicy() {
        return DefaultRetryPolicy.platformDefault(new SecureRandom());
    }

    /** The per-JVM amplification guard. See {@link WorkerProperties#retryBudgetRatio()}. */
    @Bean
    @ConditionalOnMissingBean
    public RetryBudget retryBudget(WorkerProperties properties) {
        return new RetryBudget(properties.retryBudgetRatio(), properties.retryBudgetBurst());
    }

    /**
     * The near-cache the idempotent decorator writes its token to before every call.
     *
     * <p>TODO(phase-9): back it with Valkey. In-memory means a pod restart forgets tokens written
     * seconds earlier, and the redelivery that follows is then indistinguishable from a first
     * sighting — which is exactly why layer 4, the committed {@code PENDING} attempt row, is the
     * defence that actually holds and this is only an optimisation.
     */
    @Bean
    @ConditionalOnMissingBean
    public SentTokenLog sentTokenLog() {
        return new InMemorySentTokenLog();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public ChannelExecutors channelExecutors(WorkerProperties properties, MeterRegistry meters) {
        return new ChannelExecutors(properties, meters);
    }

    @Bean
    @ConditionalOnMissingBean
    public DecoratedProviders decoratedProviders(ChannelExecutors executors, MeterRegistry meters,
                                                 SentTokenLog tokenLog, WorkerProperties properties) {
        return new DecoratedProviders(executors, meters, tokenLog, properties);
    }

    // ---- placeholders for modules that have not landed yet -------------------------------------

    @Bean
    @ConditionalOnMissingBean(PreferenceResolver.class)
    public PreferenceResolver permissivePreferenceResolver() {
        return new PermissivePreferenceResolver();
    }

    @Bean
    @ConditionalOnMissingBean(TemplateRenderer.class)
    public TemplateRenderer echoTemplateRenderer() {
        return new EchoTemplateRenderer();
    }

    @Bean
    @ConditionalOnMissingBean(RecipientManifestReader.class)
    public RecipientManifestReader inlineRecipientManifestReader() {
        return new InlineRecipientManifestReader();
    }

    @Bean
    @ConditionalOnMissingBean(RecipientAddressVault.class)
    public RecipientAddressVault developmentAddressVault() {
        return new DevelopmentAddressVault();
    }

    @Bean
    @ConditionalOnMissingBean(SuppressionWriter.class)
    public SuppressionWriter loggingSuppressionWriter(MeterRegistry meters) {
        return new LoggingSuppressionWriter(meters);
    }
}
