package dev.gaurav.notification.api.port;

import dev.gaurav.notification.api.dto.AcceptResponse;
import dev.gaurav.notification.api.dto.CancelResponse;
import dev.gaurav.notification.api.dto.RescheduleRequest;
import dev.gaurav.notification.api.dto.RescheduleResponse;
import dev.gaurav.notification.api.dto.SendNotificationRequest;

import java.util.UUID;

/**
 * Everything that changes state. Three methods, all of which must complete inside the 250 ms p99
 * accept budget.
 *
 * <p><strong>{@link #accept} owns the idempotency decision, not the controller.</strong> Claiming
 * the key, comparing the request fingerprint and writing the notification rows have to happen in
 * one transaction — a controller that claimed the key first and then called a service would leave
 * a claimed-but-unfulfilled key behind on any failure between the two, and every retry of that
 * request would then get {@code 409 request-in-progress} until the 24-hour TTL expired.
 *
 * <p>Implementations signal outcomes by throwing
 * {@link dev.gaurav.notification.api.error.ApiException} subtypes rather than returning a result
 * union. That is a deliberate exception to the "return, don't throw" preference used inside the
 * domain: these are all transport-shaped conditions with a one-to-one mapping onto an HTTP status,
 * they are rare relative to the success path, and a union type would force every controller to
 * re-implement the same exhaustive switch that the {@code @RestControllerAdvice} already owns.
 */
public interface NotificationCommandPort {

    /**
     * Durably queue one notification per requested channel.
     *
     * @param idempotencyKey the validated {@code Idempotency-Key} header, scoped to the tenant
     * @return the {@code 202} body — either freshly created, or the stored response replayed
     * verbatim when the same key arrives with the same fingerprint
     * @throws dev.gaurav.notification.api.error.IdempotencyKeyReusedException same key, different body
     * @throws dev.gaurav.notification.api.error.RequestInProgressException    a concurrent request holds the key
     * @throws dev.gaurav.notification.api.error.ScheduleInvalidException      unzoned, past or beyond-horizon sendAt
     */
    AcceptResponse accept(ApiCaller caller, String idempotencyKey, SendNotificationRequest request);

    /**
     * Move a not-yet-claimed notification to {@code CANCELLED}.
     *
     * @throws dev.gaurav.notification.api.error.NotificationNotFoundException unknown id, or another tenant's
     * @throws dev.gaurav.notification.api.error.AlreadyDispatchedException    already claimed or dispatched
     */
    CancelResponse cancel(ApiCaller caller, UUID notificationId);

    /**
     * Move a scheduled send to a new instant.
     *
     * <p>Implemented as {@code DELETE} + {@code INSERT} on the schedule row rather than
     * {@code UPDATE}, because {@code send_at} is the partition key and PostgreSQL turns a
     * partition-key update into exactly that, silently, at roughly triple the WAL cost.
     *
     * @throws dev.gaurav.notification.api.error.NotificationNotFoundException unknown id, or another tenant's
     * @throws dev.gaurav.notification.api.error.AlreadyDispatchedException    already claimed or dispatched
     * @throws dev.gaurav.notification.api.error.ScheduleInvalidException      unzoned, past or beyond-horizon sendAt
     */
    RescheduleResponse reschedule(
            ApiCaller caller, UUID notificationId, RescheduleRequest request);
}
