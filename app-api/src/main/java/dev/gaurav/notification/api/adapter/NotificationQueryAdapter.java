package dev.gaurav.notification.api.adapter;

import dev.gaurav.notification.adapter.persistence.RecipientPageReader;
import dev.gaurav.notification.api.dto.AttemptListResponse;
import dev.gaurav.notification.api.dto.NotificationStatusResponse;
import dev.gaurav.notification.api.dto.RecipientPageResponse;
import dev.gaurav.notification.api.error.ApiException;
import dev.gaurav.notification.api.error.FieldViolation;
import dev.gaurav.notification.api.port.ApiCaller;
import dev.gaurav.notification.api.port.NotificationQueryPort;
import dev.gaurav.notification.application.exception.ApplicationException;
import dev.gaurav.notification.application.result.DeliveryAttemptView;
import dev.gaurav.notification.application.result.NotificationStatusView;
import dev.gaurav.notification.application.usecase.GetNotificationStatusUseCase;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Bridges the HTTP read surface onto {@code GetNotificationStatusUseCase}, and onto the recipient
 * page reader for the one endpoint the use case does not cover.
 *
 * <p>The seam exists so the read models stay separate from the wire shape. The use case returns
 * projections; this class turns them into the DTOs {@code docs/API.md} documents. Returning the
 * projections directly would make a field rename in the read model a breaking API change, and
 * returning entities would let a lazy association load during serialisation.
 *
 * <p>Every method passes the caller's tenant reference down as a query predicate. A cross-tenant
 * read therefore finds nothing and renders as {@code 404}, never {@code 403} — a {@code 403}
 * confirms the id exists and turns the endpoint into an existence oracle.
 */
@Component
public class NotificationQueryAdapter implements NotificationQueryPort {

    private final GetNotificationStatusUseCase status;
    private final RecipientPageReader recipients;
    private final Clock clock;

    public NotificationQueryAdapter(GetNotificationStatusUseCase status,
                                    RecipientPageReader recipients,
                                    Clock clock) {
        this.status = Objects.requireNonNull(status, "status");
        this.recipients = Objects.requireNonNull(recipients, "recipients");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public NotificationStatusResponse status(ApiCaller caller, UUID notificationId) {
        return toResponse(loadStatus(caller, notificationId));
    }

    @Override
    public AttemptListResponse attempts(ApiCaller caller, UUID notificationId) {
        try {
            var attempts = status.attempts(tenantRef(caller), notificationId,
                    GetNotificationStatusUseCase.MAX_ATTEMPTS_RETURNED);
            return new AttemptListResponse(attempts.stream()
                    .map(NotificationQueryAdapter::toAttemptView)
                    .toList());
        } catch (ApplicationException e) {
            throw ApplicationProblems.translate(e, clock.instant());
        }
    }

    /**
     * One keyset page of recipients.
     *
     * <p>The status read runs first for two reasons, and only one of them is the 404: it also
     * yields the notification's {@code created_at}, which is the partition key the page query
     * prunes on. Without it the page is a scan of every partition in the retention window.
     */
    @Override
    public RecipientPageResponse recipients(ApiCaller caller, UUID notificationId, String cursor, int limit) {
        var view = loadStatus(caller, notificationId);
        var rows = recipients.page(tenantRef(caller), notificationId, view.createdAt(),
                decodeCursor(cursor), limit);
        var items = rows.stream()
                .map(row -> new RecipientPageResponse.RecipientView(
                        row.recipientId(), row.addressHint(), row.status(),
                        row.deliveredAt(), row.attemptCount(), row.suppressionReason()))
                .toList();
        return new RecipientPageResponse(items, nextCursor(rows, limit));
    }

    private NotificationStatusView loadStatus(ApiCaller caller, UUID notificationId) {
        try {
            return status.status(tenantRef(caller), notificationId);
        } catch (ApplicationException e) {
            throw ApplicationProblems.translate(e, clock.instant());
        }
    }

    /**
     * The tenant reference the application layer speaks in: the caller's public tenant id, never a
     * surrogate key. See {@code TenantDirectory} for why the two are kept apart.
     */
    private static String tenantRef(ApiCaller caller) {
        return caller.tenantId().toString();
    }

    private static NotificationStatusResponse toResponse(NotificationStatusView view) {
        var counts = view.counts();
        return new NotificationStatusResponse(
                view.id(),
                view.channel(),
                view.trafficClass(),
                // The aggregate word, not the per-recipient enum: PARTIALLY_COMPLETED is real for a
                // fan-out and has no rank the monotonic guard could accept.
                view.aggregateStatus(),
                view.createdAt(),
                view.dispatchedAt(),
                new NotificationStatusResponse.Counts(counts.total(), counts.delivered(),
                        counts.failed(), counts.suppressed(), counts.pending()),
                new NotificationStatusResponse.Links(
                        "/v1/notifications/" + view.id() + "/recipients",
                        "/v1/notifications/" + view.id() + "/attempts"));
    }

    private static AttemptListResponse.AttemptView toAttemptView(DeliveryAttemptView attempt) {
        return new AttemptListResponse.AttemptView(
                attempt.attemptNumber(),
                attempt.providerCode(),
                attempt.state(),
                attempt.failureType(),
                attempt.providerMessageId(),
                attempt.latencyMs() == null ? null : Long.valueOf(attempt.latencyMs()),
                attempt.costMicros(),
                attempt.startedAt(),
                attempt.retryScheduledAt());
    }

    /**
     * Opaque on purpose.
     *
     * <p>A cursor a caller can read is a cursor a caller will eventually hand-craft, and then its
     * encoding becomes a public contract that cannot change when the index changes. Base64 is not
     * security — it is a signal that the value is ours.
     *
     * <p>A malformed cursor is a {@code 400}, not a silent first page: silently restarting a
     * reconciliation loop from the beginning is how a client double-counts ten million recipients.
     */
    private static UUID decodeCursor(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            throw ApiException.validationFailed("The cursor is not one this API issued.",
                    List.of(FieldViolation.of("cursor", "MALFORMED_CURSOR")));
        }
    }

    /**
     * {@code null} when this was the last page.
     *
     * <p>A short page means there is no more; a full page means there might be. Issuing a cursor
     * after a full page that happened to be the last one costs the client one empty request and is
     * the only answer available without an extra count query.
     */
    private static String nextCursor(List<RecipientPageReader.RecipientRow> rows, int limit) {
        if (rows.size() < limit) {
            return null;
        }
        var last = rows.get(rows.size() - 1).recipientId().toString();
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(last.getBytes(StandardCharsets.UTF_8));
    }
}
