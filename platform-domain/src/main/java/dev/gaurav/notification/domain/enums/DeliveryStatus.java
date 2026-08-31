package dev.gaurav.notification.domain.enums;

import java.util.Optional;

/**
 * The lifecycle of one notification to one recipient.
 *
 * <p>Every status carries a {@code rank}, and <strong>status only ever moves forwards</strong>.
 * That single rule is what makes the whole pipeline safe: provider webhooks routinely arrive out
 * of order (a {@code DELIVERED} callback can beat our own {@code SENT} write), Kafka is
 * at-least-once so events can be replayed, and DLQ replay reorders by construction. A naive
 * {@code setStatus()} would let a late {@code SENT} overwrite a terminal {@code DELIVERED},
 * corrupting metrics and — if retries are driven off status — re-sending to a user who already
 * received the message.
 *
 * <p>The rank values are chosen deliberately, and two of them encode business rules that would
 * otherwise live in an {@code if} statement somebody forgets to write:
 *
 * <ul>
 *   <li>{@code CANCELLED} (28) sits <em>below</em> {@code QUEUED} (30), so "you cannot cancel
 *       something already queued" is enforced by the ordering itself. Both race orderings are
 *       correct: if CANCELLED commits first it is terminal, so the racing QUEUED is rejected by
 *       the terminal check instead.</li>
 *   <li>{@code DELIVERED} (80) is deliberately <strong>not</strong> terminal, because a hard
 *       bounce legitimately follows an SMTP 250. State machines that treat "delivered" as final
 *       cannot represent that, and silently drop bounce events.</li>
 * </ul>
 *
 * <p>Engagement events (opened, clicked) are deliberately absent: they are not monotone along the
 * delivery pipeline, and forcing them into this ordering breaks it.
 */
public enum DeliveryStatus {

    /** Recipient row created, not yet evaluated. */
    PENDING(10, false, false),

    /** Waiting for its scheduled time. */
    SCHEDULED(20, false, false),

    /** Blocked by preferences, quiet hours, frequency cap or the suppression list. */
    SUPPRESSED(25, true, true),

    /** TTL elapsed before we managed to send. Ranked below QUEUED: it can only precede sending. */
    EXPIRED(27, true, true),

    /** Cancelled by the caller before dispatch. Ranked below QUEUED — see the class javadoc. */
    CANCELLED(28, true, true),

    /** Published to a dispatch topic. */
    QUEUED(30, false, false),

    /** Leased by a worker. */
    CLAIMED(40, false, false),

    /** In flight to the provider. */
    SENDING(50, false, false),

    /** Transient failure; a retry is scheduled. Not terminal. */
    SEND_FAILED(55, false, true),

    /** Retries exhausted, or a permanent error. */
    FAILED(58, true, true),

    /** The provider returned 2xx and gave us a message id. */
    SENT(60, false, false),

    /** The provider confirmed it accepted the message for delivery. */
    ACCEPTED(70, false, false),

    /**
     * The provider confirmed delivery. Not terminal — a hard bounce can still follow.
     */
    DELIVERED(80, false, false),

    /** Hard bounce after acceptance. Adds the address to the suppression list. */
    BOUNCED(85, true, true),

    /** Recipient marked it as spam. The most expensive outcome: it damages sender reputation. */
    COMPLAINED(88, true, true),

    /**
     * We do not know whether it was delivered.
     *
     * <p>Ranked highest so nothing accidentally overwrites it, and non-terminal so reconciliation
     * can still resolve it to {@link #DELIVERED} or {@link #FAILED}. Reached when the provider
     * timed out after possibly delivering, or when a worker died between the network call and the
     * result write.
     */
    UNKNOWN(90, false, false);

    private final int rank;
    private final boolean terminal;
    private final boolean failure;

    DeliveryStatus(int rank, boolean terminal, boolean failure) {
        this.rank = rank;
        this.terminal = terminal;
        this.failure = failure;
    }

    /** Monotonic ordering. Higher wins; equal or lower is rejected. */
    public int rank() {
        return rank;
    }

    /** No transition may leave a terminal state. */
    public boolean isTerminal() {
        return terminal;
    }

    /** Counts against the delivery success rate SLI. */
    public boolean isFailure() {
        return failure;
    }

    /** Whether the platform still owes this notification an outcome. */
    public boolean isInFlight() {
        return !terminal && this != UNKNOWN;
    }

    /**
     * Applies the monotonic rule.
     *
     * <p>Returns empty when the proposed transition is stale, duplicated or illegal — which is
     * <strong>not an error</strong>. It is the expected outcome for an out-of-order webhook, and
     * the caller records it as an unapplied event rather than throwing. Those unapplied events
     * turn out to be the most valuable debugging artefact in the system: they are the callbacks
     * you correctly ignored, and without them "why is this stuck in SENT" is unanswerable.
     *
     * @param proposed the status the incoming event is asking for
     * @return the proposed status if the transition is legal, otherwise empty
     */
    public Optional<DeliveryStatus> transitionTo(DeliveryStatus proposed) {
        if (this.terminal) {
            return Optional.empty();
        }
        if (proposed.rank <= this.rank) {
            return Optional.empty();
        }
        return Optional.of(proposed);
    }

    /** Convenience for call sites that only need a yes/no. */
    public boolean canTransitionTo(DeliveryStatus proposed) {
        return transitionTo(proposed).isPresent();
    }
}
