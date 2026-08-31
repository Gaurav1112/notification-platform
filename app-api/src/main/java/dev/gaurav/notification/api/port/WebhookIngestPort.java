package dev.gaurav.notification.api.port;

import dev.gaurav.notification.provider.spi.ProviderCode;

import java.time.Instant;
import java.util.UUID;

/**
 * Inbound provider callbacks, split into the part that must happen before the response and the part
 * that must not.
 *
 * <p><strong>The split is the design.</strong> A webhook handler that parses the payload, resolves
 * the notification, applies the state transition and then answers 200 is a handler that answers
 * slowly, and every provider treats slow as failed and retries. Worse, if it throws while
 * interpreting an unfamiliar payload shape the provider never sent a second time, the evidence is
 * gone. So {@link #persistRaw} writes the bytes exactly as received and commits, and
 * {@link #processLater} interprets them off the request thread. A payload we could not understand
 * is still on disk, replayable after the parser is fixed.
 */
public interface WebhookIngestPort {

    /**
     * Commit the raw payload. Must be fast, must not parse, must not throw on unknown content.
     *
     * <p>Deduplication is a UNIQUE insert on a hash of the body rather than a read-then-write:
     * two replicas receiving the same redelivery concurrently both see "not present" on a
     * {@code SELECT}, and only a constraint decides the race correctly.
     *
     * @param signature the verified signature header value, stored for the audit trail
     * @param receivedAt server clock at receipt, distinct from the provider's {@code X-Timestamp}
     * @return the stored record's id and whether this body had been seen before
     */
    RawReceipt persistRaw(ProviderCode provider, byte[] rawBody, String signature,
                          Instant providerTimestamp, Instant receivedAt);

    /**
     * Interpret a stored payload off the request thread.
     *
     * <p>Implementations must return immediately — hand to an executor, or publish an event. A
     * blocking implementation would reintroduce exactly the latency this split exists to remove.
     * Failures here are the implementation's to log and retry; the provider has already been told
     * 200 and must never be told anything else.
     */
    void processLater(UUID rawWebhookId);

    /**
     * @param duplicate true when the body hash already existed, in which case the caller still
     *                  answers 200 — see {@link dev.gaurav.notification.api.dto.WebhookAck}
     */
    record RawReceipt(UUID id, boolean duplicate) {
    }
}
