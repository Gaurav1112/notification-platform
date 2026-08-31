package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;

/**
 * Keeps our own send rate under the contracted vendor limit.
 *
 * <p><strong>The failure this prevents:</strong> earning the 429s ourselves. A campaign fan-out
 * across forty pods will exceed a 100 msg/s contract instantly, and the resulting rate-limit
 * rejections are indistinguishable at the worker from a provider outage — so the breaker opens,
 * traffic fails over to a secondary that costs more, and the incident is entirely self-inflicted.
 * Shaping before the call is cheaper than classifying afterwards.
 *
 * <p>The budget has to be <em>global</em>, not per-pod: dividing 100/s by the current replica count
 * is wrong the moment the HPA scales, and wrong in both directions during a rolling deploy when
 * old and new pods overlap.
 *
 * <p>TODO(platform-resilience): back this with the Redis sliding-window limiter. Two behaviours
 * matter. First, a rejection here must be
 * {@code Rejected(RATE_LIMITED, …, retryAfter=<remaining window>)} — our own back-pressure, which
 * {@code FailureType.RATE_LIMITED} already routes to immediate failover without burning an attempt.
 * Second, any wait for a token must be bounded by {@link SendCommand#deadline()}: parking a
 * CRITICAL OTP behind a bulk campaign's token queue delivers it after it has expired, which is
 * worse than not sending it. Pass-through until then, so the chain order does not change later.
 */
public final class RateLimitedProvider extends AbstractProviderDecorator {

    public RateLimitedProvider(NotificationProvider delegate) {
        super(delegate);
    }

    @Override
    public SendResult send(SendCommand command) {
        return delegate.send(command);
    }
}
