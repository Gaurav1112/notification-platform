package dev.gaurav.notification.application.result;

import dev.gaurav.notification.domain.enums.Channel;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * What {@code POST /v1/notifications} answers: either a fresh acceptance, or the verbatim replay of
 * one we already answered for this idempotency key.
 *
 * <p>The two cases are one type because the endpoint has one contract, and one status code:
 * {@code 202}. Not {@code 200} — a {@code 200} implies the notification was delivered, and it has
 * not been. It is durably queued, which is a different promise and the only one we can keep at this
 * point in the sequence.
 *
 * <p><strong>A replay carries the original bytes, not a rebuilt model.</strong> That is the point of
 * {@link Replay}: the caller writes {@code replay.body()} straight to the socket. Re-serialising
 * from a reloaded row would produce a response that drifts from the original after any deploy that
 * changed the response shape — and "identical response for an identical key" is exactly what the
 * {@code Idempotency-Key} header sells.
 *
 * @param requestId     the {@code notification_request} id; {@code null} on a replay
 * @param acceptedAt    when the accept transaction committed; {@code null} on a replay
 * @param notifications one entry per requested channel
 */
public record AcceptResult(
        UUID requestId,
        Instant acceptedAt,
        int recipientCount,
        List<AcceptedNotification> notifications,
        Replay replay) {

    /** The status word echoed in the body. Not a {@code DeliveryStatus}: nothing has been sent. */
    public static final String STATUS_ACCEPTED = "ACCEPTED";

    public AcceptResult {
        notifications = notifications == null ? List.of() : List.copyOf(notifications);
        // Either it is a replay, or it is a real acceptance with a real request id. A record with
        // neither would render as a 202 pointing at nothing.
        if (replay == null) {
            Objects.requireNonNull(requestId, "requestId");
            Objects.requireNonNull(acceptedAt, "acceptedAt");
            if (notifications.isEmpty()) {
                throw new IllegalArgumentException("an acceptance with no notification delivers nothing");
            }
        }
    }

    /** A freshly committed acceptance. */
    public static AcceptResult accepted(
            UUID requestId, Instant acceptedAt, int recipientCount, List<AcceptedNotification> notifications) {
        return new AcceptResult(requestId, acceptedAt, recipientCount, notifications, null);
    }

    /** The stored response for a key we have already answered. */
    public static AcceptResult replayed(int status, String body) {
        return new AcceptResult(null, null, 0, List.of(), new Replay(status, body));
    }

    /** True when the caller must write {@link Replay#body()} rather than serialise this record. */
    /**
     * Named {@code hasReplay}, not {@code isReplay}, and that is load-bearing.
     *
     * <p>Jackson maps a no-arg {@code isXxx()} to a boolean property named {@code xxx}. With this
     * method called {@code isReplay()} it collided with the {@code replay} record component: the
     * serialiser wrote {@code "replay": false} instead of the {@link Replay} object, and reading
     * the stored idempotency response back then failed with
     * {@code MismatchedInputException: cannot construct Replay from boolean value (false)}.
     *
     * <p>The effect was that every idempotent <em>replay</em> — the entire point of the
     * {@code Idempotency-Key} header — returned a 500. Unit tests did not catch it because they
     * assert on the object, never on a serialise/deserialise round trip; only an end-to-end POST
     * of the same key twice surfaced it. {@code AcceptResultSerializationTest} is the regression
     * oracle.
     */
    public boolean hasReplay() {
        return replay != null;
    }

    public Optional<Replay> replayed() {
        return Optional.ofNullable(replay);
    }

    /** The bytes of the original response, and the status we returned with them. */
    public record Replay(int status, String body) {

        public Replay {
            Objects.requireNonNull(body, "body");
        }
    }

    /**
     * One accepted channel.
     *
     * @param status always {@code ACCEPTED} at this point. Present in the payload so clients parse
     *               one shape here and on the status endpoint, instead of special-casing the 202
     */
    public record AcceptedNotification(UUID id, Channel channel, String status) {

        public AcceptedNotification {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(channel, "channel");
            status = status == null ? STATUS_ACCEPTED : status;
        }

        public static AcceptedNotification of(UUID id, Channel channel) {
            return new AcceptedNotification(id, channel, STATUS_ACCEPTED);
        }
    }
}
