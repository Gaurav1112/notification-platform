package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;

import java.time.Clock;
import java.util.Objects;

/**
 * The innermost decorator: writes the idempotency token down before the vendor call, and answers a
 * proven replay without making a second one.
 *
 * <p><strong>The failure this prevents:</strong> duplicate sends on Kafka redelivery. At-least-once
 * consumption means the same dispatch record <em>will</em> be handed to a worker twice — after a
 * rebalance, after a pod eviction, after a DLQ replay. For a provider that accepts no client
 * idempotency key (Twilio has no client-reference field; FCM's {@code apns-id} is correlation
 * only) the vendor cannot help, so the deduplication has to happen on our side of the wire.
 *
 * <p><strong>Why it is innermost.</strong> The record must exist before the request is made, and
 * the {@link TimeoutProvider} above it can cut the call off at any moment. If idempotency sat
 * outside the timeout, the exact case it exists for — a call that timed out and may have
 * delivered — would leave no trace at all.
 *
 * <p>Only an {@code Accepted} outcome short-circuits. A previous {@code Rejected} is genuinely
 * retryable in some classifications, and a previous {@code Indeterminate} <em>must</em> be allowed
 * through to the caller's own policy: silently treating "we do not know" as "already sent" would
 * drop real messages.
 */
public final class IdempotentProvider extends AbstractProviderDecorator {

    private final SentTokenLog tokenLog;
    private final Clock clock;

    public IdempotentProvider(NotificationProvider delegate, SentTokenLog tokenLog, Clock clock) {
        super(delegate);
        this.tokenLog = Objects.requireNonNull(tokenLog, "tokenLog");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public IdempotentProvider(NotificationProvider delegate, SentTokenLog tokenLog) {
        this(delegate, tokenLog, Clock.systemUTC());
    }

    @Override
    public SendResult send(SendCommand command) {
        var token = command.idempotencyToken();

        var previous = tokenLog.previousOutcome(code(), token);
        if (previous.isPresent() && previous.get().isSuccess()) {
            return previous.get();
        }

        // Deliberately before the call, and deliberately not conditional on the return value:
        // a crash on the next line must still leave evidence that a send was started.
        tokenLog.recordAttempt(code(), token, clock.instant());

        var result = delegate.send(command);
        tokenLog.recordOutcome(code(), token, result);
        return result;
    }
}
