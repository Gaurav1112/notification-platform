package dev.gaurav.notification.api.adapter;

import dev.gaurav.notification.adapter.persistence.TenantDirectory;
import dev.gaurav.notification.api.dto.AcceptResponse;
import dev.gaurav.notification.api.dto.CancelResponse;
import dev.gaurav.notification.api.dto.RescheduleRequest;
import dev.gaurav.notification.api.dto.RescheduleResponse;
import dev.gaurav.notification.api.dto.SendNotificationRequest;
import dev.gaurav.notification.api.error.ApiException;
import dev.gaurav.notification.api.error.ProblemType;
import dev.gaurav.notification.api.filter.RequestIdFilter;
import dev.gaurav.notification.api.port.ApiCaller;
import dev.gaurav.notification.api.port.NotificationCommandPort;
import dev.gaurav.notification.application.command.RecipientSelector;
import dev.gaurav.notification.application.command.Schedule;
import dev.gaurav.notification.application.command.SendNotificationCommand;
import dev.gaurav.notification.application.command.TemplateRef;
import dev.gaurav.notification.application.exception.ApplicationException;
import dev.gaurav.notification.application.result.AcceptResult;
import dev.gaurav.notification.application.usecase.AcceptNotificationUseCase;
import dev.gaurav.notification.application.usecase.CancelNotificationUseCase;
import dev.gaurav.notification.domain.enums.ScheduleType;
import dev.gaurav.notification.messaging.config.KafkaProducerConfig;

import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Bridges the HTTP write surface onto {@code AcceptNotificationUseCase} and
 * {@code CancelNotificationUseCase}.
 *
 * <p>The seam exists so the REST contract and the accept logic can change independently: the DTOs
 * carry the wire shape and its validation annotations, the commands carry the meaning, and this
 * class is the only place that knows both. It is also where {@code ApplicationException} becomes
 * {@code ApiException}, so a change to a use case's failure vocabulary cannot silently change the
 * public error contract — see {@link ApplicationProblems}.
 *
 * <p>Nothing here decides anything. The idempotency claim, the quota charge, the transaction and
 * the outbox write all belong to the use case, precisely because a controller-side step between the
 * claim and the write would sit outside the transaction that makes a client retry safe.
 */
@Component
public class NotificationCommandAdapter implements NotificationCommandPort {

    private final AcceptNotificationUseCase accept;
    private final CancelNotificationUseCase cancel;
    private final TenantDirectory tenants;
    private final JsonMapper mapper;
    private final Clock clock;

    public NotificationCommandAdapter(AcceptNotificationUseCase accept,
                                      CancelNotificationUseCase cancel,
                                      TenantDirectory tenants,
                                      @Qualifier(KafkaProducerConfig.EVENT_JSON_MAPPER) JsonMapper mapper,
                                      Clock clock) {
        this.accept = Objects.requireNonNull(accept, "accept");
        this.cancel = Objects.requireNonNull(cancel, "cancel");
        this.tenants = Objects.requireNonNull(tenants, "tenants");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public AcceptResponse accept(ApiCaller caller, String idempotencyKey, SendNotificationRequest request) {
        var tenantRef = requireKnownTenant(caller);
        AcceptResult result;
        try {
            result = accept.accept(toCommand(tenantRef, idempotencyKey, request));
        } catch (ApplicationException e) {
            throw ApplicationProblems.translate(e, clock.instant());
        }
        return toResponse(result);
    }

    @Override
    public CancelResponse cancel(ApiCaller caller, UUID notificationId) {
        var tenantRef = requireKnownTenant(caller);
        var cancelledAt = clock.instant();
        try {
            cancel.cancel(tenantRef, notificationId);
        } catch (ApplicationException e) {
            throw ApplicationProblems.translate(e, cancelledAt);
        }
        return CancelResponse.cancelled(notificationId, cancelledAt);
    }

    /**
     * Not implemented, and it says so rather than pretending.
     *
     * <p>There is no reschedule use case and, more fundamentally, no {@code notification_schedule}
     * table: {@code V1__baseline.sql} has no row to move. The endpoint's own contract describes the
     * move as a {@code DELETE} plus an {@code INSERT} on that table, so an implementation here would
     * have to invent the storage first.
     *
     * <p>Answering a {@code 400} would be worse than a {@code 500}: it would tell the caller their
     * request was wrong when it was perfectly valid, and they would spend the afternoon on it. The
     * detail names the real reason.
     *
     * <p>TODO(phase-8): add {@code notification_schedule}, a {@code RescheduleNotificationUseCase}
     * and the {@code ScheduleWriter} port behind it.
     */
    @Override
    public RescheduleResponse reschedule(ApiCaller caller, UUID notificationId, RescheduleRequest request) {
        throw new ApiException(ProblemType.INTERNAL_ERROR,
                "Rescheduling is not implemented: the schedule table this endpoint moves a row in "
                        + "does not exist yet. Cancel and re-send with a new sendAt instead.");
    }

    /**
     * Fails closed on a token whose tenant is not registered.
     *
     * <p>The alternative — letting the write reach the accept transaction — writes a
     * {@code notification} row whose {@code tenant_id} joins to nothing, and the hot tables carry
     * no foreign key that would catch it. {@code 401} rather than {@code 404} because the problem
     * is the credential, not the resource.
     */
    private String requireKnownTenant(ApiCaller caller) {
        var tenantRef = caller.tenantId().toString();
        if (tenants.internalIdOf(tenantRef).isEmpty()) {
            throw ApiException.unauthenticated(
                    "Token names tenant '%s', which is not registered.".formatted(tenantRef));
        }
        return tenantRef;
    }

    private SendNotificationCommand toCommand(String tenantRef, String idempotencyKey,
                                              SendNotificationRequest request) {
        try {
            return new SendNotificationCommand(
                    tenantRef,
                    idempotencyKey,
                    fingerprintOf(request),
                    request.trafficClass(),
                    // LinkedHashSet: the 202 lists one entry per channel and the caller reads them
                    // positionally often enough that reordering them looks like a bug.
                    new LinkedHashSet<>(request.channels()),
                    null,
                    templateOf(request),
                    contentOf(request),
                    recipientsOf(request),
                    request.variables(),
                    scheduleOf(request),
                    request.ttlSeconds(),
                    request.metadata(),
                    MDC.get(RequestIdFilter.MDC_TRACE_ID));
        } catch (ApplicationException e) {
            // The command's compact constructors validate too, and they catch combinations bean
            // validation cannot express. Their failures are the caller's, not ours.
            throw ApplicationProblems.translate(e, clock.instant());
        }
    }

    /**
     * SHA-256 of the request, so the same key with a different body is a {@code 409} rather than a
     * silent replay of an unrelated response.
     *
     * <p>TODO(phase-3): this hashes a re-serialisation of the parsed DTO, not the raw bytes that
     * arrived. The two differ in exactly one way that matters — a deploy that reorders the record
     * components changes the fingerprint of an unchanged request, and every in-flight client retry
     * becomes a {@code 409}. Hashing the arriving bytes needs a caching request wrapper in the
     * filter chain, which is a change to the servlet plumbing rather than to this adapter.
     */
    private byte[] fingerprintOf(SendNotificationRequest request) {
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest(mapper.writeValueAsString(request).getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the JCA spec on every conformant JRE; if it is absent the
            // deployment is broken in a way no fallback should paper over.
            throw new IllegalStateException("SHA-256 is unavailable on this JVM", e);
        }
    }

    private static TemplateRef templateOf(SendNotificationRequest request) {
        var template = request.template();
        return template == null ? null : new TemplateRef(template.code(), template.locale());
    }

    private static dev.gaurav.notification.application.command.InlineContent contentOf(
            SendNotificationRequest request) {
        var content = request.content();
        return content == null
                ? null
                : new dev.gaurav.notification.application.command.InlineContent(
                        content.subject(), content.body());
    }

    /**
     * The four audience shapes, mapped one to one.
     *
     * <p>{@code INLINE} addresses lose the per-recipient locale and timezone the DTO can carry.
     * TODO(phase-8): {@code application.command.RecipientSelector} models an inline recipient as a
     * bare address string, so those two fields have nowhere to go; the preference layer that would
     * use them does not exist yet either.
     */
    private static RecipientSelector recipientsOf(SendNotificationRequest request) {
        var selector = request.recipients();
        return switch (selector.kind()) {
            case INLINE -> RecipientSelector.inline(selector.inline().stream()
                    .map(dev.gaurav.notification.api.dto.RecipientSelector.InlineRecipient::address)
                    .toList());
            case USER_IDS -> RecipientSelector.userIds(List.copyOf(selector.userIds()));
            case S3_MANIFEST -> RecipientSelector.manifest(selector.uri(), declaredCount(selector.count()));
            case AUDIENCE_REF -> RecipientSelector.audience(selector.audienceRef(), declaredCount(selector.count()));
        };
    }

    private static int declaredCount(Long count) {
        // The command refuses a claim check with no count, which is the correct rule: a 202 that
        // reports progress against an unknown denominator reports nothing.
        return count == null ? 0 : Math.toIntExact(count);
    }

    /**
     * The schedule, already resolved against the offset, past and horizon rules by the controller.
     *
     * <p>{@code RECURRING} is rejected here rather than in the DTO because the DTO models it as a
     * legal wire value and the gap is downstream: {@code Schedule} demands a {@code sendAt} for a
     * recurring schedule while {@code ScheduleRequest} forbids one and carries a cron expression
     * instead, and nothing in the platform evaluates a cron. TODO(phase-8): the recurring expander.
     */
    private Schedule scheduleOf(SendNotificationRequest request) {
        var schedule = request.scheduleOrImmediate();
        var type = schedule.type() == null ? ScheduleType.IMMEDIATE : schedule.type();
        return switch (type) {
            case IMMEDIATE -> Schedule.immediate();
            case SCHEDULED -> Schedule.at(schedule.resolveSendAt(clock.instant()).orElseThrow());
            case RECURRING -> throw new ApiException(ProblemType.SCHEDULE_INVALID,
                    "Recurring schedules are accepted by the schema but no component evaluates the "
                            + "cron expression yet. Send one SCHEDULED notification per occurrence.");
        };
    }

    /**
     * The {@code 202} body.
     *
     * <p>A replay is the bytes {@link AcceptResponseSerializer} stored, read straight back into the
     * response record: same ids, same field names, same shape as the first caller received. That is
     * the property the {@code Idempotency-Key} header sells, and reading the stored body rather
     * than rebuilding one from a reloaded row is what keeps it true across a deploy.
     *
     * <p>A fresh acceptance goes through {@link AcceptResponses}, the same mapping the serialiser
     * uses, so the two can never disagree.
     */
    private AcceptResponse toResponse(AcceptResult result) {
        return result.replayed()
                .map(replay -> mapper.readValue(replay.body(), AcceptResponse.class))
                .orElseGet(() -> AcceptResponses.from(result));
    }
}
