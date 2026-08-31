package dev.gaurav.notification.api.dto;

import dev.gaurav.notification.domain.enums.DeliveryStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * Result of a successful cancel.
 *
 * <p>A 200 here means the row moved to {@code CANCELLED} before anything claimed it — the state
 * machine's rank ordering ({@code CANCELLED} 28 &lt; {@code QUEUED} 30) is what makes that
 * atomic rather than a check-then-act. If a worker had already claimed it, the caller got a
 * {@code 409 already-dispatched} instead, because a best-effort 200 would be read as a guarantee.
 */
public record CancelResponse(UUID id, DeliveryStatus status, Instant cancelledAt) {

    public static CancelResponse cancelled(UUID id, Instant at) {
        return new CancelResponse(id, DeliveryStatus.CANCELLED, at);
    }
}
