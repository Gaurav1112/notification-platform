package dev.gaurav.notification.api.adapter;

import dev.gaurav.notification.api.port.WebhookIngestPort;
import dev.gaurav.notification.provider.spi.ProviderCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * The smallest honest implementation of {@link WebhookIngestPort}: it deduplicates and it
 * acknowledges, and it is explicit that it does not persist.
 *
 * <p><strong>TODO(phase-6): {@code V1__baseline.sql} has no {@code raw_webhook} table.</strong>
 * The port's first contract — commit the bytes before interpreting them, so a payload shape the
 * parser cannot handle is still on disk and replayable after a one-line fix — cannot be honoured
 * without one. Writing the bytes somewhere else (the event log, a blob column on
 * {@code notification_event}) would put a provider-shaped payload in a table with a different
 * meaning and a different retention, which is worse than not writing it: it would look like the
 * gap was closed. The table, its hash-unique index and its retention are a schema decision.
 *
 * <p>What this class does do is the part that must be right today. It answers fast and it answers
 * {@code 200}, including for a redelivery — every provider treats slow as failed and non-2xx as
 * retry, and a {@code 409} on a duplicate converts normal at-least-once traffic into a retry loop
 * that ends with the provider disabling the endpoint.
 *
 * <p><strong>The dedup is per-JVM and therefore incomplete.</strong> Two API replicas receiving the
 * same redelivery both report it as new. That is stated rather than hidden because the real defence
 * is a UNIQUE insert on the body hash — a read-then-write across replicas cannot decide the race —
 * and the unique index is part of the same missing table.
 *
 * <p>{@link #processLater} does nothing but warn. There is no raw record to interpret, and no
 * status transition should be invented from a payload that was never stored.
 */
@Component
public class InMemoryWebhookIngestAdapter implements WebhookIngestPort {

    /**
     * Bounded so a webhook storm cannot become a heap leak.
     *
     * <p>A few thousand entries covers a provider's redelivery window at local volumes; the eldest
     * entry falling out of a bounded map is exactly the same failure the per-JVM scope already has,
     * so the bound costs nothing that is not already lost.
     */
    private static final int MAX_TRACKED_HASHES = 4_096;

    private static final Logger log = LoggerFactory.getLogger(InMemoryWebhookIngestAdapter.class);

    /** Insertion-ordered with an eldest-entry eviction, wrapped for concurrent access. */
    private final Set<String> seen = Collections.newSetFromMap(Collections.synchronizedMap(
            new LinkedHashMap<>(MAX_TRACKED_HASHES, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > MAX_TRACKED_HASHES;
                }
            }));

    public InMemoryWebhookIngestAdapter() {
        log.warn("inbound webhook payloads are NOT persisted: no raw_webhook table exists. "
                + "Deduplication is per-JVM and a payload the parser cannot handle is lost. "
                + "See InMemoryWebhookIngestAdapter, TODO(phase-6).");
    }

    @Override
    public RawReceipt persistRaw(ProviderCode provider, byte[] rawBody, String signature,
                                 Instant providerTimestamp, Instant receivedAt) {
        var hash = sha256Hex(rawBody);
        boolean firstSighting = seen.add(provider.value() + ":" + hash);
        // Size and hash, never the body: a provider payload carries recipient addresses.
        log.info("webhook from {} received at {} ({} bytes, hash {}, duplicate={})",
                provider.value(), receivedAt, rawBody.length, hash.substring(0, 12), !firstSighting);
        return new RawReceipt(UUID.randomUUID(), !firstSighting);
    }

    @Override
    public void processLater(UUID rawWebhookId) {
        log.warn("webhook {} was acknowledged but will not be processed: there is no stored payload "
                + "to interpret. TODO(phase-6).", rawWebhookId);
    }

    private static String sha256Hex(byte[] body) {
        try {
            var digest = MessageDigest.getInstance("SHA-256").digest(body);
            var hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable on this JVM", e);
        }
    }
}
