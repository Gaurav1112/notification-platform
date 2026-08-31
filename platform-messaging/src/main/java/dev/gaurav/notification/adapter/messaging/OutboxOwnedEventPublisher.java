package dev.gaurav.notification.adapter.messaging;

import dev.gaurav.notification.application.port.EventPublisher;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Satisfies {@link EventPublisher} — the application's "state is already committed, go work on it"
 * notice — by deferring to the transactional outbox rather than producing on the request thread.
 *
 * <p>The seam exists because the application layer must not name a topic or a partition key. Both
 * decisions live in {@link NotificationEventPublisher}, next to the reasoning for them: a topic
 * typed at a call site becomes a topic nobody consumes, and a key chosen at a call site is how one
 * path drops {@code recipientId} and builds a hot partition.
 *
 * <p>What this class documents is a mismatch between the port and the wire format, and it is a gap
 * in the <em>port</em>, not something an implementation can paper over. Every method says
 * explicitly which field it cannot supply, because the alternative — inventing one — would produce
 * an event that looks valid and is not.
 *
 * <h2>Why {@link #publishRequested} does not produce</h2>
 *
 * <p>{@code RequestedNotice} carries a recipient <em>count</em> and nothing else about the audience,
 * while {@code NotificationRequestedEvent} refuses to be constructed without either the inline list
 * or the claim-check reference — an event with neither fans out to nobody and reports success. So a
 * complete event cannot be built here.
 *
 * <p>It does not need to be. The accept transaction already wrote an outbox row holding the
 * fully-formed event, and {@code OutboxSweeper} publishes it on its next 100 ms pass. Producing
 * here as well would put the same request on {@code notification.requested} twice under two
 * different {@code eventId}s, and the idempotent receiver deduplicates on {@code eventId} — so the
 * expander would fan the campaign out twice. A duplicate campaign is a far worse outcome than
 * ~100 ms of extra latency on a path whose durability never depended on this call, which is exactly
 * what the port's own javadoc means by "a hint, not a hand-off".
 *
 * <p>TODO(phase-5): to restore the fast path, {@code RequestedNotice} must carry the audience and
 * the event id chosen by the accept transaction, so the fast-path record and the outbox record are
 * the same event and the receiver collapses them. That is a change to the port.
 */
@Component
public class OutboxOwnedEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxOwnedEventPublisher.class);

    /** Records that the outbox owns this publish. Deliberately not a produce — see the class javadoc. */
    @Override
    public void publishRequested(RequestedNotice notice) {
        log.debug("request {} is published by the outbox sweeper, not by the accept thread",
                notice.requestId());
    }

    /**
     * Unreachable today, and it fails loudly rather than quietly if that changes.
     *
     * <p>{@code DispatchNotice} carries no sealed address, no rendered body and no idempotency
     * token, all of which {@code NotificationDispatchEvent} requires and none of which the
     * application layer holds. The worker's orchestrator builds and publishes that event itself,
     * from the vault and the renderer. Returning silently here would let a future caller believe a
     * message was dispatched when nothing left the JVM — the silent success this platform is
     * written to avoid.
     *
     * <p>TODO(phase-5): remove this method from the port. Dispatch belongs to the orchestrator.
     */
    @Override
    public void publishDispatch(DispatchNotice notice) {
        throw new UnsupportedOperationException(
                "dispatch events are published by the worker's orchestrator, which owns the sealed "
                        + "address and the idempotency token this notice does not carry");
    }

    /**
     * Also unreachable today, and also loud.
     *
     * <p>Two fields block it. {@code StatusNotice.tenantId} is the tenant's <em>public</em>
     * reference and {@code DeliveryStatusEvent.tenantId} is the internal {@code bigint} — this
     * module has no tenant directory and must not grow one, because that would make the messaging
     * layer depend on persistence. And {@code version} is the counter the compacted
     * {@code notification.status} topic needs in order not to regress on a late event; there is no
     * source for it here, and defaulting it to zero would make every observation for a recipient
     * tie, which is exactly the regression the field exists to prevent.
     *
     * <p>The worker's {@code MonotonicDeliveryStatusService} publishes status today and holds both.
     *
     * <p>TODO(phase-5): give {@code StatusNotice} the internal tenant id and the per-recipient
     * version, or remove the method from the port.
     */
    @Override
    public void publishStatus(StatusNotice notice) {
        throw new UnsupportedOperationException(
                "status events are published by the worker, which holds the internal tenant id and "
                        + "the per-recipient version this notice does not carry");
    }
}
