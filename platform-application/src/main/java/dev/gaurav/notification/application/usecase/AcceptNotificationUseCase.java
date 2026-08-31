package dev.gaurav.notification.application.usecase;

import dev.gaurav.notification.application.command.SendNotificationCommand;
import dev.gaurav.notification.application.exception.IdempotencyConflictException;
import dev.gaurav.notification.application.exception.QuotaExceededException;
import dev.gaurav.notification.application.exception.RequestInProgressException;
import dev.gaurav.notification.application.port.AcceptanceWriter;
import dev.gaurav.notification.application.port.EventPublisher;
import dev.gaurav.notification.application.port.IdempotencyStore;
import dev.gaurav.notification.application.port.QuotaGuard;
import dev.gaurav.notification.application.port.ResponseSerializer;
import dev.gaurav.notification.application.result.AcceptResult;
import dev.gaurav.notification.application.result.AcceptResult.AcceptedNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.Objects;
import java.util.UUID;

/**
 * {@code POST /v1/notifications} — the core use case, and the only place in the system where a
 * request becomes a durable commitment.
 *
 * <p>The sequence is fixed, and each step is where it is for a reason:
 *
 * <ol>
 *   <li><strong>Idempotency claim.</strong> First, before anything is charged or written. A
 *       {@code REPLAY} returns the stored response verbatim and creates nothing; a
 *       {@code CONFLICT} — same key, different body — is a {@code 409}, never a silent replay of
 *       an unrelated response.</li>
 *   <li><strong>Quota.</strong> Charged per recipient, so one call cannot bypass the limit by
 *       carrying ten million of them. <strong>Fail-open:</strong> if the limiter itself is
 *       unavailable we admit the request, because a Valkey outage that reads as "every tenant is
 *       over quota" turns a degraded cache into a total outage of the send API.</li>
 *   <li><strong>The transaction.</strong> {@code notification_request} + one {@code notification}
 *       per channel + one {@code outbox_message}, committed together. <strong>That commit is the
 *       single atomic accept decision.</strong> Before it, nothing happened and the client's retry
 *       is free; after it, the message will be delivered even if this pod dies in the next
 *       instruction. There is no in-between state to reconcile, which is the entire reason the API
 *       can be stateless and lose zero in-flight work on a rolling deploy.</li>
 *   <li><strong>Mark the key COMPLETED</strong> with the exact response bytes, so a retry replays
 *       them rather than rebuilding a response that may have drifted across a deploy.</li>
 *   <li><strong>Return 202.</strong> Not {@code 200}: nothing has been delivered, it has been
 *       durably queued.</li>
 * </ol>
 *
 * <p><strong>Deliberately excluded: preference resolution, template rendering, dedup and provider
 * selection.</strong> Every one of them is a dependency that can be slow or down, and none of them
 * should be able to make {@code POST /notifications} fail. They are also all decisions that are
 * only correct at <em>dispatch</em> time — quiet hours evaluated at accept time freeze an answer
 * for a send that happens next week, and a template pinned now renders with the typo the tenant
 * fixed yesterday. They run in the orchestrator, on the async side of the outbox.
 *
 * <p><strong>The Kafka publish happens after commit and is best-effort.</strong> It is a latency
 * optimisation, not the hand-off: the row is already durable, so a broker outage costs the ~2 s the
 * outbox sweeper takes to notice, and nothing else. Letting a publish failure propagate would turn
 * a committed accept into a {@code 500}, and the client's retry would then replay a response we
 * never actually sent them.
 *
 * <p>One thing this class deliberately does <em>not</em> do: release the idempotency claim when a
 * later step throws. The claim is left {@code IN_PROGRESS} and expires with its lock, so a retry
 * gets {@code request-in-progress} and then a clean run. Actively releasing it would mean writing
 * to the idempotency store on the failure path, which is the path most likely to be failing.
 */
@Service
public class AcceptNotificationUseCase {

    private static final Logger log = LoggerFactory.getLogger(AcceptNotificationUseCase.class);

    /** {@code 202}, stored alongside the body so a replay reproduces the original status too. */
    private static final int ACCEPTED_STATUS = 202;

    private final IdempotencyStore idempotencyStore;
    private final QuotaGuard quotaGuard;
    private final AcceptanceWriter acceptanceWriter;
    private final ResponseSerializer responseSerializer;
    private final EventPublisher eventPublisher;
    private final Clock clock;

    public AcceptNotificationUseCase(
            IdempotencyStore idempotencyStore,
            QuotaGuard quotaGuard,
            AcceptanceWriter acceptanceWriter,
            ResponseSerializer responseSerializer,
            EventPublisher eventPublisher,
            Clock clock) {
        this.idempotencyStore = Objects.requireNonNull(idempotencyStore, "idempotencyStore");
        this.quotaGuard = Objects.requireNonNull(quotaGuard, "quotaGuard");
        this.acceptanceWriter = Objects.requireNonNull(acceptanceWriter, "acceptanceWriter");
        this.responseSerializer = Objects.requireNonNull(responseSerializer, "responseSerializer");
        this.eventPublisher = Objects.requireNonNull(eventPublisher, "eventPublisher");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * @throws IdempotencyConflictException same key, different body — {@code 409}
     * @throws RequestInProgressException   a concurrent request holds the key — {@code 409}
     * @throws QuotaExceededException       the tenant is over budget — {@code 429}
     */
    public AcceptResult accept(SendNotificationCommand command) {
        var tenantId = command.tenantId();
        var key = command.idempotencyKey();

        // 1. Claim, before anything is charged or written.
        var claim = idempotencyStore.claim(tenantId, key, command.requestFingerprint());
        switch (claim.outcome()) {
            case REPLAY -> {
                var stored = claim.stored().orElseThrow();
                log.debug("idempotent replay tenant={} key={}", tenantId, key);
                return AcceptResult.replayed(stored.status(), stored.body());
            }
            case CONFLICT -> throw new IdempotencyConflictException(key);
            case IN_PROGRESS -> throw new RequestInProgressException(key);
            case CLAIMED -> { /* ours; carry on */ }
        }

        // 2. Quota, charged per recipient. Fail-open on an unavailable limiter.
        enforceQuota(tenantId, command.recipientCount());

        // 3. The single atomic accept decision.
        var acceptedAt = clock.instant();
        var requestId = UUID.randomUUID();
        var records = acceptanceWriter.persist(
                command, requestId, acceptedAt, command.expiresAt(acceptedAt));

        var result = AcceptResult.accepted(
                records.requestId(),
                records.requestCreatedAt(),
                records.recipientCount(),
                records.notifications().stream()
                        .map(n -> AcceptedNotification.of(n.id(), n.channel()))
                        .toList());

        // 4. Store the exact bytes we are about to return, so a retry replays them.
        idempotencyStore.complete(tenantId, key, ACCEPTED_STATUS, responseSerializer.serialize(result));

        // 5 (+ fast path). Post-commit, best-effort. The outbox sweeper is the guarantee.
        publishBestEffort(command, records);

        return result;
    }

    private void enforceQuota(String tenantId, int permits) {
        boolean admitted;
        try {
            admitted = quotaGuard.tryConsume(tenantId, permits);
        } catch (RuntimeException e) {
            // Fail-open: an unreachable limiter must not read as "everyone is over quota".
            log.warn("quota check unavailable, admitting tenant={} permits={}", tenantId, permits, e);
            return;
        }
        if (!admitted) {
            throw new QuotaExceededException(tenantId, permits);
        }
    }

    private void publishBestEffort(
            SendNotificationCommand command, AcceptanceWriter.AcceptedRecords records) {
        var notice = new EventPublisher.RequestedNotice(
                command.tenantId(),
                records.requestId(),
                records.requestCreatedAt(),
                command.idempotencyKey(),
                command.channels(),
                command.trafficClass(),
                command.schedule().type(),
                command.schedule().sendAt(),
                command.expiresAt(records.requestCreatedAt()),
                records.recipientCount(),
                command.traceparent());
        try {
            eventPublisher.publishRequested(notice);
        } catch (RuntimeException e) {
            // The row and its outbox entry are already committed. Swallowing here is what keeps a
            // broker outage from turning a successful accept into a 500 the client will retry.
            log.warn("fast-path publish failed for request={}, outbox sweeper will recover",
                    records.requestId(), e);
        }
    }
}
