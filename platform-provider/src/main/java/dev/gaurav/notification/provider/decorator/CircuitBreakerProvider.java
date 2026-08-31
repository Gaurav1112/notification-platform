package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;
import dev.gaurav.notification.resilience.circuitbreaker.FailureClassifier;
import dev.gaurav.notification.resilience.circuitbreaker.ProviderCircuitBreakers;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Stops calling a provider that is clearly failing, and lets the router move on.
 *
 * <p>This class was a pass-through for one release — {@code return delegate.send(command);} — and
 * the consequences are worth recording, because nothing failed loudly. Every breaker stayed
 * permanently {@code CLOSED}, so the router's "filter out open circuits" step could never remove
 * anything; {@code getFailureRate()} returned {@code -1} for an empty window, which the router
 * mapped to a success rate of {@code 1.0}, so the dominant term of the provider score was a
 * constant; and the health gate that pauses a lane when every provider is down could never fire.
 * A provider returning 5xx on 100% of calls would have kept winning selection. The only signal
 * that could remove a provider was a manual chaos injection — which is exactly why the demo
 * worked while the mechanism did not.
 *
 * <h2>Three decisions</h2>
 *
 * <p><b>A 4xx does not trip the breaker.</b> {@link FailureClassifier#shouldRecordAsCircuitFailure}
 * returns false for permanent failures — an invalid recipient, an unsubscribe, a payload we built
 * wrong. Those are <em>our</em> bug or the recipient's state, not the provider's health. Recording
 * them would let a batch of bad phone numbers take a perfectly healthy provider offline for
 * everyone.
 *
 * <p><b>A short-circuit is reported as {@link SendResult.Rejected}, never as an exception.</b> The
 * SPI contract is that implementations do not throw for business outcomes, and the router needs a
 * value it can act on. {@code CIRCUIT_OPEN} is surfaced as a retryable {@link FailureType} so the
 * message fails over to the next candidate rather than being treated as dead.
 *
 * <p><b>{@link SendResult.Indeterminate} counts as a failure.</b> A timeout may mean the provider
 * delivered and we never heard back, so it is ambiguous for the <em>message</em> — but it is
 * unambiguous evidence about the <em>provider</em>. A provider that stops answering is unhealthy
 * whether or not the work landed.
 *
 * @see ProviderCircuitBreakers for the per-(provider, channel) registry and the jittered
 *      open-state duration that prevents forty pods probing a recovering provider in lockstep
 */
public final class CircuitBreakerProvider extends AbstractProviderDecorator {

    private final ProviderCircuitBreakers breakers;

    public CircuitBreakerProvider(NotificationProvider delegate, ProviderCircuitBreakers breakers) {
        super(delegate);
        this.breakers = Objects.requireNonNull(breakers, "breakers");
    }

    @Override
    public SendResult send(SendCommand command) {
        CircuitBreaker breaker = breakers.forProvider(code().value(), channel());

        try {
            breaker.acquirePermission();
        } catch (CallNotPermittedException open) {
            // Not an error condition: the breaker is doing its job. Retryable so the router
            // fails over to the next candidate instead of dead-lettering the message.
            return new SendResult.Rejected(
                    FailureType.PROVIDER_5XX,
                    "CIRCUIT_OPEN",
                    "circuit is " + breaker.getState() + " for " + code() + "/" + channel(),
                    Duration.ZERO,
                    Optional.empty());
        }

        long startNanos = breaker.getCurrentTimestamp();
        SendResult result;
        try {
            result = delegate.send(command);
        } catch (RuntimeException adapterBug) {
            // An adapter that throws has violated the SPI. Record it — a provider whose adapter
            // is blowing up is not usable — then let it propagate so the bug is visible.
            breaker.onError(breaker.getCurrentTimestamp() - startNanos,
                    breaker.getTimestampUnit(), adapterBug);
            throw adapterBug;
        }

        long elapsed = breaker.getCurrentTimestamp() - startNanos;
        record(breaker, elapsed, result);
        return result;
    }

    /**
     * TODO(java-21): a pattern-matching {@code switch} over the sealed {@link SendResult} would let
     * the compiler prove this handles every case. On Java 17 that is a preview feature, so the
     * exhaustiveness is enforced by {@code SendResultHandlerTest} instead — a weaker guarantee, and
     * one of the concrete reasons to move the baseline to 21.
     */
    private void record(CircuitBreaker breaker, long elapsed, SendResult result) {
        if (result instanceof SendResult.Accepted) {
            breaker.onSuccess(elapsed, breaker.getTimestampUnit());
            return;
        }
        if (result instanceof SendResult.Indeterminate indeterminate) {
            // Ambiguous for the message, unambiguous for the provider: it stopped answering.
            breaker.onError(elapsed, breaker.getTimestampUnit(),
                    new ProviderUnhealthy(indeterminate.type(), indeterminate.message()));
            return;
        }
        if (result instanceof SendResult.Rejected rejected) {
            if (FailureClassifier.shouldRecordAsCircuitFailure(rejected.type())) {
                breaker.onError(elapsed, breaker.getTimestampUnit(),
                        new ProviderUnhealthy(rejected.type(), rejected.message()));
            } else {
                // A permanent, message-specific failure. The provider answered correctly and
                // promptly; it simply refused this payload. Counting it would be wrong.
                breaker.onSuccess(elapsed, breaker.getTimestampUnit());
            }
            return;
        }
        throw new IllegalStateException(
                "unhandled SendResult subtype " + result.getClass()
                        + " — a case was added to the sealed interface and not handled here");
    }

    /**
     * Carries the {@link FailureType} into Resilience4j, which records failures as exceptions.
     * Stackless: these are created on the hot path at provider-failure rate and no one reads the
     * trace — the type and message are the whole payload.
     */
    static final class ProviderUnhealthy extends RuntimeException {
        private final transient FailureType type;

        ProviderUnhealthy(FailureType type, String message) {
            super(type + ": " + message, null, false, false);
            this.type = type;
        }

        FailureType type() {
            return type;
        }
    }
}
