package dev.gaurav.notification.worker.channel;

import dev.gaurav.notification.domain.enums.AttemptState;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.domain.enums.SuppressionReason;
import dev.gaurav.notification.messaging.event.DeadLetterEvent;
import dev.gaurav.notification.messaging.event.DeliveryStatusEvent;
import dev.gaurav.notification.messaging.event.NotificationDispatchEvent;
import dev.gaurav.notification.messaging.event.RetryScheduledEvent;
import dev.gaurav.notification.messaging.topic.RetryTier;
import dev.gaurav.notification.persistence.entity.NotificationEvent.EventSource;
import dev.gaurav.notification.persistence.entity.NotificationRecipient;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.worker.retry.RetryContext;
import dev.gaurav.notification.worker.retry.RetryDecision;
import dev.gaurav.notification.worker.retry.RetryTiers;
import dev.gaurav.notification.worker.status.ApplyDeliveryStatusUseCase;
import dev.gaurav.notification.worker.support.Dispatches;
import dev.gaurav.notification.worker.support.PartitionWindow;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * The eight-step dispatch sequence, fixed once so three channels cannot each get it slightly wrong.
 *
 * <p>Template Method here is not ceremony. The sequence below is the correctness of the platform,
 * and every step is ordered relative to the others for a reason that only shows up under failure:
 *
 * <ol>
 *   <li><strong>Idempotent-receiver check.</strong> Kafka is at-least-once; a rebalance mid-batch
 *       redelivers a record whose effect was "send an SMS to a real phone".</li>
 *   <li><strong>Load the recipient row.</strong> The event is a pointer to committed state, never
 *       the state itself.</li>
 *   <li><strong>Re-check eligibility.</strong> Cancelled or expired since the event was published.
 *       A 40-minute-old one-time passcode is worse than no passcode: the user has already asked
 *       for another one.</li>
 *   <li><strong>Select a provider</strong> from those whose circuit is not open. An open circuit
 *       removes a candidate; it does not fail the send.</li>
 *   <li><strong>Write {@code delivery_attempt} as {@code PENDING} and commit it</strong> — before
 *       the network call. <em>This is the step that converts an invisible failure into a
 *       recoverable one.</em> A pod killed at t2 in {@code t0 commit → t1 send → t2 OOM} leaves a
 *       visible {@code PENDING} row with a request start time, so redelivery can see "we may have
 *       sent this" and reconcile. Write the row afterwards and the crash is indistinguishable from
 *       never having sent, which forces a guess: retry and duplicate, or drop and lose.</li>
 *   <li><strong>Call the provider through the decorator chain</strong>, never the raw adapter. The
 *       chain is what bounds the call; an unbounded provider call is what turns a vendor incident
 *       into a rebalance storm.</li>
 *   <li><strong>Record the outcome and publish a status event.</strong> Attempt row first, then
 *       the event: the row is the ledger, the event is the notification about it.</li>
 *   <li><strong>On failure, classify and route</strong> — one retry tier, one failover, one
 *       permanent failure or one dead letter, decided in
 *       {@link dev.gaurav.notification.worker.retry.RetryRouter} and nowhere else.</li>
 * </ol>
 *
 * <p><strong>The offset is acknowledged only after all eight.</strong> Auto-commit would ack a
 * record whose attempt row never landed, which is silent loss; acknowledging late means the worst
 * case is redelivery, and step 1 absorbs that.
 */
public abstract class AbstractChannelWorker {

    private static final Logger log = LoggerFactory.getLogger(AbstractChannelWorker.class);

    protected final ChannelWorkerSupport support;
    private final Channel channel;
    private final Counter accepted;
    private final Counter rejected;
    private final Counter indeterminate;
    private final Counter skipped;

    protected AbstractChannelWorker(Channel channel, ChannelWorkerSupport support) {
        this.channel = channel;
        this.support = support;
        this.accepted = counter("accepted");
        this.rejected = counter("rejected");
        this.indeterminate = counter("indeterminate");
        this.skipped = counter("skipped");
    }

    /** The dedup scope. Distinct per channel so one channel's replay cannot mask another's. */
    protected abstract String consumerGroup();

    /**
     * Runs the eight steps and acknowledges exactly once.
     *
     * <p>{@code final} on purpose: a subclass that overrode this to "just add a quick check" would
     * be reordering the sequence above, and the reordering that matters most — moving the attempt
     * row after the send — looks like a harmless simplification.
     */
    protected final void dispatch(NotificationDispatchEvent event,
                                  ConsumerRecordMetadata metadata, Acknowledgment ack) {
        // Step 1. Scoped by attempt, not just by event id. A retry carries the SAME eventId by
        // design — it is the same logical send, and downstream consumers must still recognise it —
        // so a scope of eventId alone would make the idempotent receiver swallow every retry as a
        // duplicate and the retry engine would silently do nothing.
        String scope = consumerGroup() + "#attempt-" + event.attemptNumber();
        if (!support.idempotentConsumer().isFirstSighting(scope, event.eventId())) {
            skipped.increment();
            ack.acknowledge();
            return;
        }
        try {
            handle(event, metadata);
            ack.acknowledge();
        } catch (RuntimeException e) {
            support.idempotentConsumer().forget(scope, event.eventId());
            throw e;
        }
    }

    private void handle(NotificationDispatchEvent event, ConsumerRecordMetadata metadata) {
        Instant now = support.clock().instant();

        // Step 2.
        var window = PartitionWindow.forDispatch(event.notificationCreatedAt());
        var recipient = support.recipients()
                .findInWindow(event.recipientId(), window.from(), window.to())
                .orElseThrow(() -> new IllegalStateException(
                        "no notification_recipient row for " + event.recipientId()
                                + " in " + window.from() + ".." + window.to()));

        // Step 3.
        if (!isStillEligible(recipient, event, now)) {
            return;
        }

        // Step 4.
        var selection = support.router()
                .select(channel, event.tenantId(), event.preferredProvider(), Set.of());
        if (selection.isEmpty()) {
            // Every provider for this channel is open or unhealthy. Not a message failure and not
            // an exception: park it on a tier and let the health gate stop the lane. No attempt row
            // is written, because there is no provider to attribute one to.
            log.warn("no eligible provider for channel {} tenant {}", channel, event.tenantId());
            route(event, metadata, null, null,
                    context(event, FailureType.PROVIDER_5XX, false, Optional.empty(), false, now));
            return;
        }
        var selected = selection.get();
        var providerCode = selected.code();

        // Step 5. Committed before the call. See the class javadoc.
        String token = attemptToken(event);
        var attemptRef = support.attempts().registerPending(
                event.recipientId(), event.tenantId(), event.attemptNumber(),
                support.providerIds().idOf(providerCode), token);
        applyAndPublish(event, DeliveryStatus.SENDING, null, providerCode, null, now);

        // Step 6.
        var result = selected.provider().send(commandFor(event, recipient, token, now));

        // Step 7.
        var outcome = SendOutcome.from(result);
        support.attempts().complete(attemptRef, outcome.attemptState(), outcome.latency(),
                outcome.providerMessageId(), outcome.failureType(), outcome.failureCode(),
                outcome.failureDetail(), outcome.costMicros());
        applyAndPublish(event, outcome.status(), outcome, providerCode,
                outcome.providerMessageId(), support.clock().instant());

        if (outcome.succeeded()) {
            accepted.increment();
            // Deposits a fraction of a retry token. Without this the budget only drains and the
            // platform silently stops retrying after the first incident.
            support.retryRouter().recordSuccess();
            return;
        }
        if (outcome.indeterminate()) {
            indeterminate.increment();
        } else {
            rejected.increment();
        }

        // Step 8.
        route(event, metadata, providerCode, outcome,
                context(event, outcome.failureType(), outcome.indeterminate(),
                        outcome.retryAfter(), selected.alternativesAvailable(),
                        support.clock().instant()));
    }

    /**
     * Cancelled, suppressed or expired since the event was published.
     *
     * <p>Terminal is checked first and by rank, not by an {@code if} per status: {@code CANCELLED}
     * ranks below {@code QUEUED} precisely so "you cannot cancel something already queued" is
     * enforced by ordering, and a row that reached any terminal state must not be sent.
     */
    private boolean isStillEligible(NotificationRecipient recipient,
                                    NotificationDispatchEvent event, Instant now) {
        if (recipient.getStatus().isTerminal()) {
            skipped.increment();
            log.debug("recipient {} is already {}; not sending", event.recipientId(), recipient.getStatus());
            return false;
        }
        if (event.isExpiredAt(now)) {
            skipped.increment();
            applyAndPublish(event, DeliveryStatus.EXPIRED, null, null, null, now);
            return false;
        }
        return true;
    }

    private SendCommand commandFor(NotificationDispatchEvent event, NotificationRecipient recipient,
                                   String token, Instant now) {
        if (event.bodyRef() != null) {
            // TODO(phase-9): fetch the claim-checked body. Refusing is deliberate — sending an
            // empty body would look like a delivered message to every metric in the platform.
            throw new UnsupportedOperationException(
                    "claim-checked body not supported yet: " + event.bodyRef());
        }
        // Opened here and nowhere earlier: the plaintext address exists for the duration of one
        // send. The hint on the event is what goes in log lines.
        String address = support.vault().open(event.addressCipher(), event.dekRef());
        return new SendCommand(recipient.getId(), channel, event.trafficClass(), address,
                event.subject(), event.bodyCipher(), token, event.attributes(),
                deadlineFor(event, now));
    }

    /**
     * The per-call deadline: the configured ceiling, or the remaining TTL when that is shorter.
     *
     * <p>Spending eight seconds on a message that expires in two delivers nothing and occupies a
     * bulkhead thread that a live message needed.
     */
    private Duration deadlineFor(NotificationDispatchEvent event, Instant now) {
        Duration remaining = Duration.between(now, event.expiresAt());
        Duration ceiling = support.properties().providerDeadline();
        return remaining.compareTo(ceiling) < 0 ? remaining : ceiling;
    }

    /**
     * The token for <em>this attempt</em>.
     *
     * <p>Two constraints pull in opposite directions and this line is where they are reconciled.
     * {@code da_token_uk} is unique, so attempt 2 cannot reuse attempt 1's token; and the token
     * sent to the provider must be stable across redeliveries so a replayed record does not
     * produce a second send. Deriving it deterministically from the base token and the attempt
     * number satisfies both: distinct per attempt, identical on every redelivery of that attempt.
     */
    private static String attemptToken(NotificationDispatchEvent event) {
        return event.idempotencyToken() + "#" + event.attemptNumber();
    }

    private RetryContext context(NotificationDispatchEvent event, FailureType type,
                                 boolean indeterminateOutcome, Optional<Duration> retryAfter,
                                 boolean alternatives, Instant now) {
        return new RetryContext(channel, type == null ? FailureType.PERMANENT_UNKNOWN : type,
                indeterminateOutcome, event.attemptNumber(), retryAfter, alternatives,
                now, event.expiresAt());
    }

    /**
     * Turns the router's decision into the one action it names.
     *
     * <p>The {@code instanceof} ladder is the Java 17 stand-in for a pattern switch over the sealed
     * {@link RetryDecision} — see {@link SendResultHandler} for why the module cannot use one yet.
     * It lives here, once, rather than in each subclass. TODO(java-21): pattern switch.
     */
    private void route(NotificationDispatchEvent event, ConsumerRecordMetadata metadata,
                       ProviderCode used, SendOutcome outcome, RetryContext ctx) {
        var decision = support.retryRouter().decide(ctx);
        Instant now = support.clock().instant();

        if (decision instanceof RetryDecision.RetrySameProvider retry) {
            support.publisher().publishRetry(new RetryScheduledEvent(
                    UUID.randomUUID(), now, event.tenantId(), event.traceparent(),
                    RetryTiers.forTopicOf(retry.tier()), now.plus(retry.delay()),
                    event.attemptNumber(), retry.failureType(), event));
            return;
        }
        if (decision instanceof RetryDecision.FailoverNow failover) {
            failover(event, used, failover, now);
            return;
        }
        if (decision instanceof RetryDecision.PermanentFailure permanent) {
            applyAndPublish(event, DeliveryStatus.FAILED, outcome, used, null, now);
            if (permanent.suppressAddress()) {
                support.suppressions().suppress(event.tenantId(), event.recipientId(), channel,
                        suppressionReasonFor(permanent.failureType()));
            }
            return;
        }
        if (decision instanceof RetryDecision.AwaitReconciliation) {
            // Nothing further, and that is the decision. The attempt row is UNKNOWN and the status
            // is UNKNOWN — deliberately not terminal — so the delivery webhook or the reconciler
            // can still resolve it. Retrying here is how a user gets three passcodes.
            log.info("recipient {} left UNKNOWN for reconciliation after {}",
                    event.recipientId(), ctx.failureType());
            return;
        }
        if (decision instanceof RetryDecision.Expired) {
            applyAndPublish(event, DeliveryStatus.EXPIRED, null, used, null, now);
            return;
        }
        if (decision instanceof RetryDecision.DeadLetter dead) {
            deadLetter(event, metadata, dead, now);
            applyAndPublish(event, DeliveryStatus.FAILED, outcome, used, null, now);
            return;
        }
        throw new IllegalStateException("unhandled RetryDecision: " + decision.getClass().getName());
    }

    private void failover(NotificationDispatchEvent event, ProviderCode used,
                          RetryDecision.FailoverNow failover, Instant now) {
        var alternative = support.router().select(
                channel, event.tenantId(), null, used == null ? Set.of() : Set.of(used));
        if (alternative.isEmpty()) {
            // The candidate that existed when the decision was taken has gone since. Treat it as
            // the ladder would: park it rather than dropping it.
            log.warn("failover requested for {} but no alternative provider remains", event.recipientId());
            support.publisher().publishRetry(new RetryScheduledEvent(
                    UUID.randomUUID(), now, event.tenantId(), event.traceparent(),
                    RetryTier.first(),
                    now.plus(RetryTier.first().delay()),
                    event.attemptNumber(), FailureType.PROVIDER_5XX, event));
            return;
        }
        if (failover.pageOnCall()) {
            // AUTH_FAILURE and QUOTA_EXCEEDED. The failover keeps traffic flowing, but running on
            // the secondary indefinitely is an incident, not a resolution.
            log.error("PAGE: {} on provider {} for channel {}; failing over to {}",
                    failover.failureType(), used, channel, alternative.get().code().value());
        }
        support.publisher().publishDispatch(
                Dispatches.nextAttempt(event, alternative.get().code().value()));
    }

    private void deadLetter(NotificationDispatchEvent event, ConsumerRecordMetadata metadata,
                            RetryDecision.DeadLetter dead, Instant now) {
        support.publisher().publishDeadLetter(new DeadLetterEvent(
                UUID.randomUUID(), now, event.tenantId(), event.traceparent(),
                metadata.topic(), metadata.partition(), metadata.offset(),
                event.tenantId() + "|" + event.recipientId() + "|" + channel,
                consumerGroup(), event.notificationId(), event.recipientId(),
                dead.failureType(), null, dead.detail(), null,
                event.attemptNumber(), 0,
                // The address hint, never the address. A DLQ dump is read by more people than any
                // other artefact in the platform.
                "recipient=" + event.recipientId() + " address=" + event.addressHint()));
    }

    /**
     * Applies the status through the monotonic guard and publishes the raw outcome.
     *
     * <p>Both, in that order, and both every time. The guard is what keeps the row correct under
     * reordering; the event is what lets the ledger, the analytics tap and the tenant's webhook see
     * it. Publishing without applying leaves the row behind; applying without publishing leaves
     * every downstream consumer blind.
     */
    private void applyAndPublish(NotificationDispatchEvent event, DeliveryStatus status,
                                 SendOutcome outcome, ProviderCode providerCode,
                                 String providerMessageId, Instant occurredAt) {
        support.applyStatus().apply(new ApplyDeliveryStatusUseCase.Command(
                event.tenantId(), event.notificationId(), event.notificationCreatedAt(),
                event.recipientId(), status, occurredAt, EventSource.WORKER,
                providerCode == null ? null : support.providerIds().idOf(providerCode),
                event.recipientId() + "|" + status.name() + "|" + event.attemptNumber()));

        support.publisher().publishDelivery(new DeliveryStatusEvent(
                UUID.randomUUID(), occurredAt, event.tenantId(), event.traceparent(),
                event.notificationId(), event.notificationCreatedAt(), event.recipientId(),
                channel, status,
                // The rank is already a monotonically increasing per-recipient counter, which is
                // exactly what the projector needs. Inventing a second counter would give two
                // orderings that can disagree.
                status.rank(),
                event.attemptNumber(),
                outcome == null ? AttemptState.PENDING : outcome.attemptState(),
                providerCode == null ? null : providerCode.value(),
                providerMessageId,
                failureTypeFor(status, outcome),
                outcome == null ? null : outcome.failureCode(),
                outcome == null ? null : outcome.failureDetail(),
                null,
                outcome == null ? null : outcome.costMicros(),
                outcome == null || outcome.latency() == null ? null : outcome.latency().toMillis()));
    }

    /**
     * {@code SEND_FAILED} and {@code FAILED} may not be published without a classification — the
     * event record refuses them — because a failure with no type forces the retry decision into a
     * catch-all branch, and that branch is how an invalid phone number gets retried five times.
     */
    private static FailureType failureTypeFor(DeliveryStatus status, SendOutcome outcome) {
        if (outcome != null && outcome.failureType() != null) {
            return outcome.failureType();
        }
        return status == DeliveryStatus.SEND_FAILED || status == DeliveryStatus.FAILED
                ? FailureType.PERMANENT_UNKNOWN
                : null;
    }

    private static SuppressionReason suppressionReasonFor(FailureType type) {
        return switch (type) {
            case UNSUBSCRIBED -> SuppressionReason.USER_OPTED_OUT;
            case INVALID_RECIPIENT, DEVICE_UNREGISTERED ->
                    SuppressionReason.INVALID_ADDRESS;
            default -> SuppressionReason.HARD_BOUNCE;
        };
    }

    private Counter counter(String outcome) {
        return Counter.builder("notification.dispatch.outcome")
                .description("channel worker dispatch outcomes")
                .tag("channel", channel.name())
                .tag("outcome", outcome)
                .register(support.meters());
    }
}
