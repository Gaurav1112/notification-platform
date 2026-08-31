package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;

/**
 * Outermost decorator: opens the span that everything below it happens inside.
 *
 * <p><strong>The failure this prevents:</strong> a latency investigation that blames the vendor
 * for time the message spent parked on our own rate limiter, or waiting for a slot in the timeout
 * pool. If the span starts below those, the trace reports a fast provider and a mysteriously slow
 * system, and the fix gets applied to the wrong component.
 *
 * <p>It is also where {@code recipient_id} and {@code provider} become span attributes — the two
 * fields that turn "delivery was slow yesterday" into a single trace.
 *
 * <p>TODO(platform-resilience / platform-observability): wire the Micrometer {@code Observation}
 * API here — span name {@code provider.send}, low-cardinality keys {@code provider},
 * {@code channel}, {@code traffic_class}, {@code outcome}, and the vendor message id recorded as a
 * high-cardinality key so a support ticket quoting a Twilio SID resolves to a trace. Until that
 * module lands this is a transparent pass-through, which keeps the chain order stable so no other
 * decorator has to move when it arrives.
 */
public final class TracedProvider extends AbstractProviderDecorator {

    public TracedProvider(NotificationProvider delegate) {
        super(delegate);
    }

    @Override
    public SendResult send(SendCommand command) {
        return delegate.send(command);
    }
}
