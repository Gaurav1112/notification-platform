package dev.gaurav.notification.worker.channel;

import dev.gaurav.notification.domain.enums.AttemptState;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.persistence.entity.DeliveryAttempt;
import dev.gaurav.notification.persistence.repository.DeliveryAttemptRepository;
import jakarta.persistence.EntityManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Writes the {@code delivery_attempt} row before the provider call and completes it after.
 *
 * <p><strong>The two writes are two transactions, and that is the entire point of this class.</strong>
 * The {@code PENDING} row must be <em>committed</em> before the network call:
 *
 * <pre>
 *   t0  INSERT delivery_attempt(state=PENDING, idempotency_token=T)   ← COMMITTED
 *   t1  provider.send(payload, T)
 *   t2  pod OOM-killed
 *   t3  redelivery → attempt found PENDING, response_at NULL, age &gt; timeout
 *       → UNKNOWN, do NOT blind-retry → reconcile
 * </pre>
 *
 * <p>Write the row afterwards instead and a crash between the send and the persist is
 * indistinguishable from never having sent at all. The message is then either retried — a second
 * one-time passcode to a real person — or dropped. Committing first converts an <em>invisible</em>
 * failure into a <em>recoverable</em> one: there is a row, it is visibly stuck, and the reconciler
 * can find it. Nothing else in the platform can make that distinction after the fact.
 *
 * <p>A second consequence of the same rule: the transaction must never span the provider call. A
 * 30-second vendor timeout inside an open transaction holds row locks and pins {@code xmin}, so
 * autovacuum cannot reclaim the 20M dead tuples a day that status updates generate. That is the
 * failure that takes the cluster down at 3 a.m., days after the code that caused it shipped.
 */
@Service
public class DeliveryAttemptRecorder {

    private final DeliveryAttemptRepository attempts;
    private final EntityManager entityManager;
    private final Clock clock;

    public DeliveryAttemptRecorder(DeliveryAttemptRepository attempts,
                                   EntityManager entityManager,
                                   Clock clock) {
        this.attempts = attempts;
        this.entityManager = entityManager;
        this.clock = clock;
    }

    /** Identity of a committed attempt row: both halves of the partitioned primary key. */
    public record AttemptRef(long id, Instant attemptedAt, Instant startedAt) {
    }

    /**
     * Inserts the attempt and commits it.
     *
     * <p>{@code REQUIRES_NEW} rather than {@code REQUIRED}: if a caller ever wraps the dispatch in
     * a transaction, this row must still land on its own, or the guarantee above quietly stops
     * holding and nothing fails to tell anyone.
     *
     * @param idempotencyToken must be unique per <em>attempt</em> ({@code da_token_uk}) and stable
     *                         across redeliveries <em>of</em> that attempt — see
     *                         {@link AbstractChannelWorker} for how the two are reconciled
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public AttemptRef registerPending(UUID recipientId, long tenantId, int attemptNumber,
                                      short providerId, String idempotencyToken) {
        Instant now = clock.instant();
        var attempt = new DeliveryAttempt(
                recipientId, tenantId, (short) attemptNumber, providerId, idempotencyToken);
        attempt.setAttemptedAt(now);
        attempt.setRequestStartedAt(now);
        attempt.setState(AttemptState.PENDING);
        var saved = attempts.save(attempt);
        return new AttemptRef(saved.getId(), saved.getAttemptedAt(), now);
    }

    /**
     * Completes the row with the provider's answer.
     *
     * <p>A native {@code UPDATE} bound on both {@code attempted_at} and {@code id}, not a JPA
     * merge. The entity's {@code @Id} is {@code id} alone — it has to be, the table is partitioned
     * and Hibernate cannot express a composite key that includes the partition column here — so a
     * merge emits {@code WHERE id = ?} and the planner has no partition to prune to. One update
     * would touch all 90 daily partitions of the widest table in the platform.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void complete(AttemptRef ref, AttemptState state, Duration latency,
                         String providerMessageId, FailureType failureType,
                         String errorCode, String errorDetail, Long costMicros) {
        entityManager.createNativeQuery("""
                        UPDATE notif.delivery_attempt
                           SET state               = :state,
                               response_at         = :responseAt,
                               latency_ms          = :latencyMs,
                               provider_message_id = :providerMessageId,
                               failure_type        = :failureType,
                               error_code          = :errorCode,
                               error_detail        = :errorDetail,
                               cost_micros         = :costMicros
                         WHERE id           = :id
                           AND attempted_at = :attemptedAt
                        """)
                .setParameter("state", state.name())
                .setParameter("responseAt", clock.instant())
                .setParameter("latencyMs", latency == null ? null : (int) latency.toMillis())
                .setParameter("providerMessageId", providerMessageId)
                .setParameter("failureType", failureType == null ? null : failureType.name())
                .setParameter("errorCode", errorCode)
                .setParameter("errorDetail", errorDetail)
                .setParameter("costMicros", costMicros)
                .setParameter("id", ref.id())
                .setParameter("attemptedAt", ref.attemptedAt())
                .executeUpdate();
    }
}
