package dev.gaurav.notification.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.TrafficClass;

import java.time.Instant;
import java.util.UUID;

/**
 * Aggregate status of one notification — one channel, many recipients.
 *
 * <p>{@code status} is a string rather than the {@code DeliveryStatus} enum because the aggregate
 * has states the per-recipient machine does not: {@code PARTIALLY_COMPLETED} is meaningful for a
 * fan-out where one address bounced and one delivered, and meaningless for a single recipient.
 * Forcing the aggregate into the per-recipient enum would require inventing a rank for it and
 * feeding it to the monotonic guard, which would then start rejecting legitimate per-recipient
 * transitions.
 *
 * <p>{@code dispatchedAt} is null until a worker claims it, and stays null forever for anything
 * that was suppressed or expired before dispatch. That null is information.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record NotificationStatusResponse(
        UUID id,
        Channel channel,
        TrafficClass trafficClass,
        String status,
        Instant createdAt,
        Instant dispatchedAt,
        Counts counts,
        Links links
) {

    /**
     * Per-recipient tallies.
     *
     * <p>{@code suppressed} is broken out from {@code failed} deliberately. A suppression is the
     * system working — quiet hours, an opt-out, a frequency cap — and folding it into a failure
     * rate produces a dashboard where respecting user preference looks like an incident, which is
     * how a team ends up quietly disabling the preference layer.
     */
    public record Counts(long total, long delivered, long failed, long suppressed, long pending) {
    }

    /** Relative paths to the two sub-collections, so the caller does not build them by hand. */
    public record Links(String recipients, String attempts) {
    }
}
