package dev.gaurav.notification.worker.config;

import dev.gaurav.notification.provider.decorator.ProviderDecoratorChain;
import dev.gaurav.notification.provider.decorator.SentTokenLog;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCode;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wraps every raw adapter in the canonical decorator stack, once, and hands out the wrapped one.
 *
 * <p>The registry collects the <em>bare</em> vendor adapters — that is what makes adding a provider
 * one class and nothing else. Nothing calls a bare adapter: it has no timeout, no breaker, no rate
 * limit, no metrics and no idempotency token. A single call site that reaches past this class is a
 * provider call with no deadline, and an unbounded provider call is the thing that turns a vendor
 * incident into a rebalance storm.
 *
 * <p>Wrapping is cached per {@link ProviderCode} and must be. Building a fresh chain per send would
 * create a new circuit breaker per send, so the breaker would never accumulate the twenty calls it
 * needs to open — a breaker that is present, configured, exercised in tests, and can never trip.
 *
 * <p>The timeout stage runs on the channel bulkhead from {@link ChannelExecutors} rather than on a
 * pool of its own, so a saturated channel is visible as one queue rather than as several.
 */
public class DecoratedProviders {

    private final ChannelExecutors executors;
    private final MeterRegistry meters;
    private final SentTokenLog tokenLog;
    private final WorkerProperties properties;
    private final Map<ProviderCode, NotificationProvider> cache = new ConcurrentHashMap<>();

    public DecoratedProviders(ChannelExecutors executors,
                              MeterRegistry meters,
                              SentTokenLog tokenLog,
                              WorkerProperties properties) {
        this.executors = executors;
        this.meters = meters;
        this.tokenLog = tokenLog;
        this.properties = properties;
    }

    /** The production chain around {@code adapter}: traced, metered, broken, limited, bounded, idempotent. */
    public NotificationProvider wrap(NotificationProvider adapter) {
        return cache.computeIfAbsent(adapter.code(), code -> ProviderDecoratorChain.around(adapter)
                .traced()
                .metered(meters)
                .circuitBroken()
                .rateLimited()
                .timeout(executors.forChannel(adapter.channel()), properties.providerDeadline())
                .idempotent(tokenLog)
                .build());
    }
}
