package dev.gaurav.notification.api.dto;

import dev.gaurav.notification.domain.enums.Channel;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * The body of a {@code 202 Accepted}.
 *
 * <p><strong>202, never 200.</strong> A 200 means "done", and nothing has been delivered — the
 * request has been durably committed and queued. The difference matters because callers build
 * retry logic on status codes: a 200 that later turns out to be a failed send teaches them to poll
 * defensively for everything, and a 202 tells them exactly where to look.
 *
 * <p>{@code status} is the literal string {@code "ACCEPTED"} rather than a
 * {@link dev.gaurav.notification.domain.enums.DeliveryStatus}. The delivery state machine has no
 * ACCEPTED-at-ingress member — its {@code ACCEPTED(70)} means "the provider took it", which has
 * emphatically not happened yet. Reusing that enum here would put a rank-70 word in front of a
 * caller for a rank-10 fact.
 *
 * @param notificationRequestId the envelope id; the per-channel notifications hang off it
 * @param recipientCount        echoed as declared for a manifest, reconciled after fan-out
 */
public record AcceptResponse(
        UUID notificationRequestId,
        String status,
        Instant acceptedAt,
        long recipientCount,
        List<AcceptedNotification> notifications,
        Links links
) {

    /** The value of {@link #status} on every successful accept. */
    public static final String ACCEPTED = "ACCEPTED";

    /** One per requested channel, each with the id the caller will poll. */
    public record AcceptedNotification(UUID id, Channel channel, String status) {
        public static AcceptedNotification accepted(UUID id, Channel channel) {
            return new AcceptedNotification(id, channel, ACCEPTED);
        }
    }

    /**
     * Where to look next.
     *
     * <p>Cheap hypermedia, and it earns its bytes: without it every client hard-codes the status
     * path, and the day the path changes they all break at once.
     */
    public record Links(String status) {
    }
}
