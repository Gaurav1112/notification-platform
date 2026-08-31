package dev.gaurav.notification.worker.config;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.CircuitState;
import dev.gaurav.notification.provider.registry.ProviderRegistry;
import dev.gaurav.notification.resilience.circuitbreaker.ProviderCircuitBreakers;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Stops consuming a channel's dispatch lanes while every provider on that channel is circuit-open.
 *
 * <p>This is the counterweight to lag-based autoscaling, which is a feedback loop pointed the wrong
 * way. When a provider fails, consumer lag rises. An autoscaler reads lag as "not enough consumer
 * capacity" and adds pods. The new pods point more concurrency at a provider that is already
 * failing, which deepens the outage and raises the lag further, which adds more pods. The signal is
 * real and the response is exactly backwards: lag here is a symptom of the <em>provider</em> being
 * down, not of insufficient capacity.
 *
 * <p>So the containers are paused. Three consequences, all wanted:
 *
 * <ul>
 *   <li>A paused container <strong>keeps polling</strong>. That is the whole reason to pause rather
 *       than to stop or to sleep: {@code poll()} is still called, so {@code max.poll.interval.ms}
 *       never elapses, the consumer is not evicted and the group does not rebalance. Sleeping the
 *       listener instead would evict every worker at once, at precisely the moment the fleet must
 *       stay stable.</li>
 *   <li>Messages stay on the topic with their offsets uncommitted, so nothing is lost and nothing
 *       is retried against a dead provider.</li>
 *   <li>Lag stops being a scaling signal and starts being an outage signal, which is what it is.</li>
 * </ul>
 *
 * <p>Pausing is per channel, never global: an SES quota problem says nothing about push.
 *
 * <p>The gate re-evaluates on a timer rather than on a circuit-breaker event because the resume
 * condition is a state, not an edge. A half-open breaker probing successfully must bring the lane
 * back, and there is no single event that means "and it stayed that way".
 */
@Component
public class ProviderHealthGate {

    private static final Logger log = LoggerFactory.getLogger(ProviderHealthGate.class);

    private final KafkaListenerEndpointRegistry endpoints;
    private final ProviderRegistry providers;
    private final ProviderCircuitBreakers circuitBreakers;
    private final boolean enabled;
    private final Map<Channel, Boolean> paused = new EnumMap<>(Channel.class);

    public ProviderHealthGate(KafkaListenerEndpointRegistry endpoints,
                              ProviderRegistry providers,
                              ProviderCircuitBreakers circuitBreakers,
                              WorkerProperties properties,
                              MeterRegistry meters) {
        this.endpoints = endpoints;
        this.providers = providers;
        this.circuitBreakers = circuitBreakers;
        this.enabled = properties.suppressScaleOutWhenCircuitOpen();
        for (Channel channel : Channel.values()) {
            paused.put(channel, false);
            meters.gauge("notification.dispatch.gated",
                    List.of(io.micrometer.core.instrument.Tag.of("channel", channel.name())),
                    channel, c -> paused.getOrDefault(c, false) ? 1.0 : 0.0);
        }
    }

    /**
     * Every two seconds. Fast enough that a recovered provider resumes within one half-open probe
     * window, cheap enough to be free: the check reads in-memory breaker state and nothing else.
     */
    @Scheduled(fixedDelayString = "${notification.worker.health-gate-interval-ms:2000}")
    public void reconcile() {
        if (!enabled) {
            return;
        }
        for (Channel channel : Channel.values()) {
            boolean shouldPause = allCircuitsOpen(channel);
            if (shouldPause != paused.get(channel)) {
                apply(channel, shouldPause);
                paused.put(channel, shouldPause);
            }
        }
    }

    /**
     * True only when there is at least one provider and every one of them is open.
     *
     * <p>A channel with no registered provider is deliberately <em>not</em> gated. Pausing it would
     * hide the misconfiguration behind a metric that reads "provider outage", and the real problem
     * — no provider bean at all — would never surface.
     */
    private boolean allCircuitsOpen(Channel channel) {
        var registered = providers.forChannel(channel);
        if (registered.isEmpty()) {
            return false;
        }
        return registered.stream().allMatch(provider -> {
            var state = circuitBreakers.stateOf(provider.code().value(), channel);
            return state == CircuitState.OPEN || state == CircuitState.FORCED_OPEN;
        });
    }

    private void apply(Channel channel, boolean pause) {
        for (String id : listenerIds(channel)) {
            var container = endpoints.getListenerContainer(id);
            if (container == null) {
                continue;
            }
            if (pause) {
                container.pause();
            } else {
                container.resume();
            }
        }
        log.warn("{} dispatch lanes for channel {}: every provider circuit is {}",
                pause ? "pausing" : "resuming", channel, pause ? "OPEN" : "no longer OPEN");
    }

    /** Mirrors the {@code id} attributes on the channel workers' {@code @KafkaListener}s. */
    public static List<String> listenerIds(Channel channel) {
        String prefix = "dispatch-" + channel.name().toLowerCase();
        return List.of(prefix + "-tx", prefix + "-bulk");
    }
}
