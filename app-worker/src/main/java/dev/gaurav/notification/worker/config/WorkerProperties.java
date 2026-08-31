package dev.gaurav.notification.worker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * The knobs {@code application.yml} already documents, bound to a type.
 *
 * <p>Bound rather than read with {@code @Value} so that a typo in the YAML is a startup failure
 * with a property path in the message, instead of a default silently taking effect. A retry budget
 * that quietly reverts to its default because someone wrote {@code retryBudgetRatio} is a guard
 * that is switched off and still shows up in the config file as switched on.
 *
 * @param retryBudgetRatio        retries permitted per successful call. Four independent designs
 *                                converge on ~10%: Google SRE 10%, gRPC {@code retryThrottling}
 *                                ~10%, Envoy 20%, AWS ~22%
 * @param retryBudgetBurst        retries that can be banked, bounding the burst at the start of an
 *                                incident. A long quiet period must not fund a stampede later
 * @param providerDeadline        hard ceiling on one provider call. Strictly below the per-record
 *                                budget: this is the single highest-leverage number in the file,
 *                                because an unbounded provider call is what turns a vendor incident
 *                                into a rebalance storm
 * @param channelPoolSize         threads per channel bulkhead
 * @param channelQueueCapacity    bounded queue per channel bulkhead. Bounded on purpose — see
 *                                {@link ChannelExecutors}
 * @param suppressScaleOutWhenCircuitOpen lag is a symptom of the provider being down, not of
 *                                insufficient consumer capacity; scaling on it points more
 *                                consumers at a recovering provider
 */
@ConfigurationProperties(prefix = "notification.worker")
public record WorkerProperties(double retryBudgetRatio,
                               int retryBudgetBurst,
                               Duration providerDeadline,
                               int channelPoolSize,
                               int channelQueueCapacity,
                               boolean suppressScaleOutWhenCircuitOpen) {

    public WorkerProperties {
        if (retryBudgetRatio <= 0) {
            retryBudgetRatio = 0.10;
        }
        if (retryBudgetBurst <= 0) {
            retryBudgetBurst = 100;
        }
        if (providerDeadline == null || providerDeadline.isZero() || providerDeadline.isNegative()) {
            // Matches resilience4j.timelimiter.configs.provider.timeout-duration in application.yml.
            // Duplicated deliberately: the decorator chain must bound the call even when
            // Resilience4j's autoconfiguration is not active, and a chain with no ceiling looks
            // identical to one with a ceiling right up until a provider stops answering.
            providerDeadline = Duration.ofSeconds(8);
        }
        if (channelPoolSize <= 0) {
            channelPoolSize = 16;
        }
        if (channelQueueCapacity <= 0) {
            channelQueueCapacity = 64;
        }
    }
}
