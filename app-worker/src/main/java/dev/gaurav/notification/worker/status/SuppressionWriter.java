package dev.gaurav.notification.worker.status;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.SuppressionReason;

import java.util.UUID;

/**
 * Adds an address to the suppression list so the platform stops sending to it.
 *
 * <p>This is the loop that closes the reputation problem. A hard bounce, a spam complaint or an
 * SMS {@code STOP} arrives <strong>asynchronously</strong>, minutes after the send, on a completely
 * different path from the send itself. If it only updates the row for that one message, the next
 * campaign mails the same dead address again, and the one after that — and sender reputation
 * degrades for every tenant on the shared IP pool, not just the one who bought the bad list.
 *
 * <p>The write must therefore be against the <em>address</em>, keyed by the tenant-scoped HMAC so
 * no decrypt capability is needed to match it, and it must outlive the notification it came from.
 * Per §6.8 it also survives user erasure: a suppression entry that is deleted with the user is a
 * suppression entry that stops suppressing.
 */
public interface SuppressionWriter {

    /**
     * @param recipientId the row the signal arrived on; the address is resolved from it, because
     *                    the callback carries a provider message id and not an address
     * @param reason      the suppression reason, which is reported to the tenant. A suppression
     *                    with no reason is indistinguishable from a bug that stopped sending
     */
    void suppress(long tenantId, UUID recipientId, Channel channel, SuppressionReason reason);
}
