package dev.gaurav.notification.application.usecase;

import dev.gaurav.notification.application.exception.NotificationNotFoundException;
import dev.gaurav.notification.application.port.NotificationQuery;
import dev.gaurav.notification.application.result.DeliveryAttemptView;
import dev.gaurav.notification.application.result.NotificationStatusView;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * {@code GET /v1/notifications/{id}} and {@code .../attempts} — the read side.
 *
 * <p>Two things here are policy rather than plumbing.
 *
 * <p><strong>A cross-tenant read is a {@code 404}, not a {@code 403}.</strong> A {@code 403}
 * confirms the id exists, which turns the endpoint into an existence oracle: anyone with a valid
 * token for any tenant can enumerate UUIDs and learn which ones are real. The tenant reference goes
 * into the query, so there is no path where the row is loaded and then rejected.
 *
 * <p><strong>Attempt history is capped.</strong> A notification can legitimately accumulate 64
 * attempts per recipient across a failover, and an unbounded read of that during an incident — when
 * everyone is polling — is how a status endpoint takes down the database it is reporting on.
 */
@Service
public class GetNotificationStatusUseCase {

    /** Enough to show a full failover story; small enough that the page always renders. */
    public static final int MAX_ATTEMPTS_RETURNED = 200;

    private final NotificationQuery notificationQuery;

    public GetNotificationStatusUseCase(NotificationQuery notificationQuery) {
        this.notificationQuery = Objects.requireNonNull(notificationQuery, "notificationQuery");
    }

    /** @throws NotificationNotFoundException when absent <em>or</em> owned by another tenant */
    public NotificationStatusView status(String tenantId, UUID notificationId) {
        return notificationQuery.findStatus(tenantId, notificationId)
                .orElseThrow(() -> new NotificationNotFoundException(notificationId));
    }

    /**
     * Attempt history, newest first.
     *
     * <p>Existence is re-checked through {@link #status} first so an unknown id returns
     * {@code 404} rather than an empty list — an empty list here reads as "we never tried", which
     * is a materially different answer to support than "no such notification".
     *
     * @param limit clamped to {@link #MAX_ATTEMPTS_RETURNED}
     */
    public List<DeliveryAttemptView> attempts(String tenantId, UUID notificationId, int limit) {
        status(tenantId, notificationId);
        int capped = Math.max(1, Math.min(limit, MAX_ATTEMPTS_RETURNED));
        return notificationQuery.findAttempts(tenantId, notificationId, capped);
    }
}
