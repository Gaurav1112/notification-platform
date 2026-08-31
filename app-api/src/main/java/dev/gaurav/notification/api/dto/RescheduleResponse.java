package dev.gaurav.notification.api.dto;

import dev.gaurav.notification.domain.enums.DeliveryStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * Confirmation of a reschedule, echoing the resolved instant.
 *
 * <p>{@code sendAt} is echoed as a UTC {@link Instant} rather than as the caller's original string.
 * That is the point of the round trip: a caller who sent {@code 2026-09-02T14:00:00+05:30} sees
 * {@code 2026-09-02T08:30:00Z} and can confirm the server read their offset the way they meant it,
 * before the send rather than after.
 */
public record RescheduleResponse(UUID id, Instant sendAt, DeliveryStatus status) {
}
