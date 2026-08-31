package dev.gaurav.notification.api.port;

import dev.gaurav.notification.api.dto.AttemptListResponse;
import dev.gaurav.notification.api.dto.NotificationStatusResponse;
import dev.gaurav.notification.api.dto.RecipientPageResponse;

import java.util.UUID;

/**
 * The read side. Separate from {@link NotificationCommandPort} because the two have different
 * consistency requirements and, eventually, different datasources: reads can be served from a
 * replica with a few hundred milliseconds of lag, and the accept path cannot.
 *
 * <p>Every method takes the {@link ApiCaller} because tenant scoping is a predicate in the query,
 * not a check on the result. Filtering after the fact would mean the row was loaded — and a loaded
 * row is one refactor away from being returned.
 */
public interface NotificationQueryPort {

    /**
     * @throws dev.gaurav.notification.api.error.NotificationNotFoundException unknown id, or another tenant's
     */
    NotificationStatusResponse status(ApiCaller caller, UUID notificationId);

    /**
     * One keyset page of recipients.
     *
     * @param cursor opaque; {@code null} for the first page. Opaque on purpose — a caller who can
     *               read it will eventually hand-craft one, and then the cursor encoding becomes a
     *               public contract that cannot be changed when the index changes
     * @param limit  already clamped to {@code 1..200} by the controller
     * @throws dev.gaurav.notification.api.error.NotificationNotFoundException unknown id, or another tenant's
     */
    RecipientPageResponse recipients(ApiCaller caller, UUID notificationId, String cursor, int limit);

    /**
     * The full attempt ledger. Not paged: the retry policy caps attempts in single digits, so a
     * cursor here would be ceremony for a list that cannot grow.
     *
     * @throws dev.gaurav.notification.api.error.NotificationNotFoundException unknown id, or another tenant's
     */
    AttemptListResponse attempts(ApiCaller caller, UUID notificationId);
}
