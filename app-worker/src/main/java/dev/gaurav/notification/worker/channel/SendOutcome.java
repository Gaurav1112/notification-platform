package dev.gaurav.notification.worker.channel;

import dev.gaurav.notification.domain.enums.AttemptState;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.spi.SendResult;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * One provider call, flattened into the fields the attempt row, the status event and the retry
 * decision all need.
 *
 * <p>It exists so that the sealed {@link SendResult} is destructured exactly once per send, at the
 * point where all three cases are in view, instead of being re-tested with {@code instanceof} in
 * the attempt writer, then again in the status publisher, then again in the retry router. Each of
 * those re-tests is a place where {@code Indeterminate} gets folded into "failure" by someone who
 * was only thinking about two cases.
 *
 * @param attemptState what goes in {@code delivery_attempt.state}: {@code UNKNOWN} is a real value
 *                     here, not a placeholder, and it is what the reconciler looks for
 * @param status       the proposed delivery status; the monotonic guard decides whether it sticks
 * @param retryAfter   a provider-supplied {@code Retry-After}, honoured as a floor on the backoff
 */
public record SendOutcome(AttemptState attemptState,
                          DeliveryStatus status,
                          FailureType failureType,
                          String failureCode,
                          String failureDetail,
                          String providerMessageId,
                          Duration latency,
                          Long costMicros,
                          Optional<Duration> retryAfter,
                          boolean indeterminate) {

    public SendOutcome {
        Objects.requireNonNull(attemptState, "attemptState");
        Objects.requireNonNull(status, "status");
        retryAfter = retryAfter == null ? Optional.empty() : retryAfter;
    }

    /** True when the provider took it. Anything else goes through the retry router. */
    public boolean succeeded() {
        return attemptState == AttemptState.SUCCEEDED;
    }

    /**
     * The single destructuring of {@link SendResult}, via the handler that forces all three cases.
     *
     * <p>{@code SENT} rather than {@code DELIVERED} for an accepted send: an SMTP 250 or a Twilio
     * queued response means "we have it", not "the user has it". Treating acceptance as delivery
     * is what makes a bounce look like a status regression later.
     *
     * <p>{@code UNKNOWN} for an indeterminate result, and note that {@code UNKNOWN} is deliberately
     * <em>not</em> terminal: the webhook or the reconciler is expected to resolve it, and a
     * terminal status here would block that resolution from ever being applied.
     */
    public static SendOutcome from(SendResult result) {
        return SendResultHandler.dispatch(result, new SendResultHandler<SendOutcome>() {

            @Override
            public SendOutcome onAccepted(SendResult.Accepted accepted) {
                return new SendOutcome(AttemptState.SUCCEEDED, DeliveryStatus.SENT,
                        null, null, null, accepted.providerMessageId(),
                        accepted.latency(), accepted.costMicros(), Optional.empty(), false);
            }

            @Override
            public SendOutcome onRejected(SendResult.Rejected rejected) {
                return new SendOutcome(AttemptState.FAILED, DeliveryStatus.SEND_FAILED,
                        rejected.type(), rejected.code(), rejected.message(), null,
                        rejected.latency(), null, rejected.retryAfter(), false);
            }

            @Override
            public SendOutcome onIndeterminate(SendResult.Indeterminate indeterminate) {
                return new SendOutcome(AttemptState.UNKNOWN, DeliveryStatus.UNKNOWN,
                        indeterminate.type(), null, indeterminate.message(), null,
                        indeterminate.latency(), null, Optional.empty(), true);
            }
        });
    }
}
