package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;

/**
 * Stops calling a provider that has stopped working.
 *
 * <p><strong>The failure this prevents:</strong> retry amplification into a vendor that is already
 * down. Forty worker pods each discovering the outage independently, each retrying five times,
 * turns a provider blip into a self-inflicted DDoS — and every one of those calls holds a thread
 * for the full timeout, so the consumer lag that follows is ours, not the vendor's.
 *
 * <p>Two design constraints that the eventual implementation must honour, both from §8.5 of the
 * design:
 * <ul>
 *   <li><strong>A 4xx never trips the breaker.</strong> An invalid phone number, a rejected
 *       template or an unsubscribed recipient is our data being wrong. Counting those as provider
 *       ill-health takes a perfectly healthy vendor offline for everyone.</li>
 *   <li><strong>Breaker state is shared through Redis</strong>, so pod 37 does not have to
 *       rediscover an outage that pod 1 already paid for; and half-open admits a small fixed
 *       number of probes globally, or all forty pods probe at once and re-open the circuit on the
 *       provider's first recovering second.</li>
 * </ul>
 *
 * <p>TODO(platform-resilience): back this with the Resilience4j {@code CircuitBreaker} configured
 * at &gt;50% failures over &ge;20 calls in a 60s window, half-open after 30s with 3 probes, and a
 * {@code recordResultPredicate} that ignores every {@link dev.gaurav.notification.domain.enums.FailureType}
 * whose {@code isPermanent()} is true. A short-circuit must surface as
 * {@code Rejected(PROVIDER_5XX, "CIRCUIT_OPEN", …)} so the router fails over rather than treating
 * it as a dead message. Pass-through until then, so the chain order does not change later.
 */
public final class CircuitBreakerProvider extends AbstractProviderDecorator {

    public CircuitBreakerProvider(NotificationProvider delegate) {
        super(delegate);
    }

    @Override
    public SendResult send(SendCommand command) {
        return delegate.send(command);
    }
}
