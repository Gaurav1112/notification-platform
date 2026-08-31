package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.Objects;

/**
 * Times and counts every provider call.
 *
 * <p><strong>The failure this prevents:</strong> a provider that degrades rather than breaks.
 * A vendor that starts rejecting 4% of messages with {@code 21610} does not trip a breaker, does
 * not raise an exception, and does not show up in an error-rate panel — it just quietly stops
 * delivering to a slice of users until someone opens a support ticket. Success rate and p95
 * latency <em>per provider, per channel</em> are the only signals that surface it, and they are
 * also the inputs the router scores on, so this decorator is load-bearing rather than decorative.
 *
 * <p><strong>Why it sits outside the breaker:</strong> a circuit-breaker short-circuit is a real
 * outcome the caller experienced. Measuring inside the breaker would make an open circuit look
 * like zero traffic and perfect health.
 *
 * <p><strong>Tag cardinality is bounded on purpose.</strong> Every tag is an enum or a
 * configuration-fixed identifier: {@code provider} (single digits), {@code channel} (3),
 * {@code traffic_class} (3), {@code outcome} (4), {@code failure} (14 including {@code none}).
 * The vendor's error <em>message</em> is deliberately not a tag — free text from a provider is how
 * a metrics backend acquires a million-series label.
 */
public final class MeteredProvider extends AbstractProviderDecorator {

    public static final String SEND_TIMER = "notification.provider.send";
    public static final String COST_COUNTER = "notification.provider.cost.micros";
    public static final String RETRY_AFTER_COUNTER = "notification.provider.retry_after.honoured";

    private final MeterRegistry registry;

    public MeteredProvider(NotificationProvider delegate, MeterRegistry registry) {
        super(delegate);
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    @Override
    public SendResult send(SendCommand command) {
        var startedAt = System.nanoTime();
        SendResult result;
        try {
            result = delegate.send(command);
        } catch (RuntimeException e) {
            // An adapter bug still has to be visible. Recording it under outcome=error and
            // rethrowing keeps the "adapters never throw" contract observable instead of theoretical.
            record(command, "error", "none", Duration.ofNanos(System.nanoTime() - startedAt));
            throw e;
        }

        var elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
        // Java 17: pattern matching in a switch is still preview, so this is an instanceof chain.
        // The sealed interface still makes the three cases exhaustive by inspection.
        if (result instanceof SendResult.Accepted accepted) {
            record(command, "accepted", "none", elapsed);
            Counter.builder(COST_COUNTER)
                    .tags(baseTags(command))
                    .description("provider spend in micros, so a failover to a pricier vendor shows up before the invoice does")
                    .register(registry)
                    .increment(accepted.costMicros());
        } else if (result instanceof SendResult.Rejected rejected) {
            record(command, "rejected", rejected.type().name(), elapsed);
            if (rejected.retryAfter().isPresent()) {
                Counter.builder(RETRY_AFTER_COUNTER).tags(baseTags(command)).register(registry).increment();
            }
        } else if (result instanceof SendResult.Indeterminate indeterminate) {
            // Tracked separately from rejected: this is the population the reconciler has to
            // resolve, and its size is the honest measure of how much duplicate risk we carry.
            record(command, "indeterminate", indeterminate.type().name(), elapsed);
        }
        return result;
    }

    private void record(SendCommand command, String outcome, String failure, Duration elapsed) {
        Timer.builder(SEND_TIMER)
                .tags(baseTags(command).and("outcome", outcome).and("failure", failure))
                .description("provider call latency by outcome; the router scores on p95 from this series")
                .publishPercentileHistogram()
                .register(registry)
                .record(elapsed);
    }

    private Tags baseTags(SendCommand command) {
        return Tags.of(
                "provider", code().value(),
                "channel", command.channel().name(),
                "traffic_class", command.trafficClass() == null ? "UNKNOWN" : command.trafficClass().name());
    }
}
