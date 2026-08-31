package dev.gaurav.notification.api.rest;

import dev.gaurav.notification.api.dto.AcceptResponse;
import dev.gaurav.notification.api.dto.AttemptListResponse;
import dev.gaurav.notification.api.dto.CancelResponse;
import dev.gaurav.notification.api.dto.NotificationStatusResponse;
import dev.gaurav.notification.api.dto.RecipientPageResponse;
import dev.gaurav.notification.api.dto.RescheduleRequest;
import dev.gaurav.notification.api.dto.RescheduleResponse;
import dev.gaurav.notification.api.dto.SendNotificationRequest;
import dev.gaurav.notification.api.error.ApiException;
import dev.gaurav.notification.api.error.FieldViolation;
import dev.gaurav.notification.api.filter.Idempotent;
import dev.gaurav.notification.api.filter.IdempotencyKeyInterceptor;
import dev.gaurav.notification.api.port.ApiCaller;
import dev.gaurav.notification.api.port.NotificationCommandPort;
import dev.gaurav.notification.api.port.NotificationQueryPort;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * The notification lifecycle over HTTP: accept, observe, cancel, reschedule.
 *
 * <p>The controller is deliberately thin. It validates transport-shaped things — the idempotency
 * header, the page size, the id format — and delegates every decision that has a business
 * consequence to a port. <strong>The reason is testability of the accept transaction:</strong>
 * claiming an idempotency key, writing the notification rows and writing the outbox row must happen
 * in one database transaction, and anything the controller does between those steps is outside it.
 * A controller that "just" writes an audit row before calling the service has already broken the
 * atomicity that makes a client retry safe.
 *
 * <p>Every handler takes an explicit {@link ApiCaller}. See
 * {@link dev.gaurav.notification.api.filter.ApiCallerArgumentResolver} for why the tenant is a
 * parameter rather than a thread-local.
 */
@RestController
@RequestMapping("/v1/notifications")
@Tag(name = "Notifications", description = "Send, observe, cancel and reschedule notifications")
public class NotificationController {

    /** Matches {@code docs/API.md}: cursor pagination, capped. */
    static final int DEFAULT_PAGE_SIZE = 50;
    static final int MAX_PAGE_SIZE = 200;

    private final NotificationCommandPort commands;
    private final NotificationQueryPort queries;
    private final Clock clock;

    public NotificationController(NotificationCommandPort commands, NotificationQueryPort queries, Clock clock) {
        this.commands = Objects.requireNonNull(commands, "commands");
        this.queries = Objects.requireNonNull(queries, "queries");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Durably queue one notification per requested channel.
     *
     * <p><strong>202, not 200, and not 201.</strong> 200 would claim delivery happened. 201 would
     * claim a single resource was created, and this creates one per channel plus a request
     * envelope. 202 is the only code that says what is true: the work is committed and will happen.
     *
     * <p>The {@code Location} header points at the aggregate status resource, so a caller who
     * wants to poll does not have to assemble the URL from the body.
     */
    @PostMapping
    @Idempotent
    @Operation(summary = "Send a notification",
            description = "Requires an Idempotency-Key header. Returns 202; delivery has not happened yet.")
    public ResponseEntity<AcceptResponse> send(ApiCaller caller,
                                               @Valid @RequestBody SendNotificationRequest request,
                                               HttpServletRequest servletRequest) {
        var idempotencyKey = (String) servletRequest.getAttribute(IdempotencyKeyInterceptor.ATTRIBUTE);
        // Resolved at the edge rather than inside the accept transaction. An unzoned or past
        // sendAt is a 400 the caller must fix; discovering it after the idempotency key has been
        // claimed would burn the key and make the caller's corrected retry a 409.
        request.scheduleOrImmediate().resolveSendAt(clock.instant());
        var accepted = commands.accept(caller, idempotencyKey, request);
        return ResponseEntity.accepted()
                .location(URI.create("/v1/notifications/" + accepted.notificationRequestId()))
                .body(accepted);
    }

    /** Aggregate status for one notification: one channel, counts across its recipients. */
    @GetMapping("/{id}")
    @Operation(summary = "Aggregate status")
    public NotificationStatusResponse status(ApiCaller caller, @PathVariable UUID id) {
        return queries.status(caller, id);
    }

    /**
     * Per-recipient status, one keyset page at a time.
     *
     * <p>{@code limit} is validated rather than silently clamped. A caller asking for 5 000 has a
     * bug or an assumption worth correcting, and quietly returning 200 of them teaches them the
     * request worked — so their reconciliation loop then misses 96% of the recipients and nobody
     * finds out until a campaign is audited.
     */
    @GetMapping("/{id}/recipients")
    @Operation(summary = "Per-recipient status (cursor paged, max 200 per page)")
    public RecipientPageResponse recipients(ApiCaller caller,
                                            @PathVariable UUID id,
                                            @RequestParam(required = false) String cursor,
                                            @RequestParam(defaultValue = "" + DEFAULT_PAGE_SIZE) int limit) {
        return queries.recipients(caller, id, cursor, requireValidLimit(limit));
    }

    /**
     * Every provider call made for this notification.
     *
     * <p>Not paged: the retry policy caps attempts in single digits, so a cursor here would be
     * ceremony around a list that cannot grow.
     */
    @GetMapping("/{id}/attempts")
    @Operation(summary = "Delivery attempts, including UNKNOWN outcomes")
    public AttemptListResponse attempts(ApiCaller caller, @PathVariable UUID id) {
        return queries.attempts(caller, id);
    }

    /**
     * Cancel a notification that has not been claimed yet.
     *
     * <p><strong>No {@code Idempotency-Key} is required here, unlike the send.</strong> Cancel is
     * naturally idempotent: {@code CANCELLED} is a terminal state, so a repeated cancel of an
     * already-cancelled notification is a no-op, and a repeated cancel of a dispatched one is the
     * same 409 either time. Demanding a key would add a failure mode — a client retrying a cancel
     * with a fresh key — in exchange for nothing.
     */
    @PostMapping("/{id}/cancel")
    @Operation(summary = "Cancel if not yet dispatched",
            description = "409 already-dispatched once a worker has claimed it; a Valkey tombstone is best-effort.")
    public CancelResponse cancel(ApiCaller caller, @PathVariable UUID id) {
        return commands.cancel(caller, id);
    }

    /**
     * Move a scheduled send to a new instant.
     *
     * <p>{@code PATCH} rather than {@code PUT}: the schedule is one attribute of the notification,
     * and a {@code PUT} would imply the caller is replacing the whole resource — including fields
     * they never sent, which is how a reschedule quietly clears a template variable.
     */
    @PatchMapping("/{id}/schedule")
    @Operation(summary = "Reschedule", description = "sendAt must carry a UTC offset. Implemented as DELETE + INSERT.")
    public RescheduleResponse reschedule(ApiCaller caller,
                                         @PathVariable UUID id,
                                         @Valid @RequestBody RescheduleRequest request) {
        request.resolveSendAt(clock.instant());
        return commands.reschedule(caller, id, request);
    }

    private static int requireValidLimit(int limit) {
        if (limit < 1 || limit > MAX_PAGE_SIZE) {
            throw ApiException.validationFailed(
                    "limit must be between 1 and %d.".formatted(MAX_PAGE_SIZE),
                    List.of(FieldViolation.of("limit", "OUT_OF_RANGE")));
        }
        return limit;
    }
}
