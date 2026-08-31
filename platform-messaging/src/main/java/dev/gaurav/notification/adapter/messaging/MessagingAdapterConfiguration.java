package dev.gaurav.notification.adapter.messaging;

import dev.gaurav.notification.application.port.ResponseSerializer;
import dev.gaurav.notification.messaging.config.KafkaProducerConfig;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

/**
 * Registers the messaging module's fallback adapters.
 *
 * <p>Registered here rather than annotated {@code @Component} on the classes themselves, for the
 * reason {@code WorkerBeansConfiguration} already documents: a {@code @ConditionalOnMissingBean}
 * evaluated during component scanning depends on scan order, so the real implementation would win
 * or lose depending on its package name — a startup-order bug that only reproduces on some
 * machines. Evaluated from a configuration class, the conditional runs after all user beans are
 * known and the rule is simply "the real one wins".
 */
@Configuration(proxyBeanMethods = false)
public class MessagingAdapterConfiguration {

    /**
     * Only used where nothing more specific exists — see {@link JacksonResponseSerializer}, which
     * explains why {@code app-api} must override it.
     */
    @Bean
    @ConditionalOnMissingBean(ResponseSerializer.class)
    public ResponseSerializer jacksonResponseSerializer(
            @Qualifier(KafkaProducerConfig.EVENT_JSON_MAPPER) JsonMapper mapper) {
        return new JacksonResponseSerializer(mapper);
    }
}
