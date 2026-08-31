package dev.gaurav.notification.provider.spi;

import dev.gaurav.notification.domain.enums.Channel;

/**
 * The provider service provider interface.
 *
 * <p>Implementations contain only vendor-specific logic. Tracing, metrics, circuit breaking, rate
 * limiting, timeouts and idempotency are supplied by the decorator chain that wraps them, so a
 * mock adapter gets byte-identical resilience behaviour to a real one — which is what makes the
 * failure tests meaningful.
 *
 * <p>Implementations must <strong>never throw</strong> for a business failure. Return
 * {@link SendResult.Rejected} or {@link SendResult.Indeterminate} instead; an exception means the
 * adapter itself is broken.
 */
public interface NotificationProvider {

    Channel channel();

    ProviderCode code();

    ProviderCapabilities capabilities();

    SendResult send(SendCommand command);

    /** Lightweight liveness signal for the health endpoint. Must not call the vendor on every invocation. */
    boolean isHealthy();
}
