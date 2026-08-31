package dev.gaurav.notification.worker.support;

import dev.gaurav.notification.domain.enums.Channel;

import java.util.Objects;
import java.util.Optional;

/**
 * The seam between "we know which user this is" and "we know the bytes to put on the wire".
 *
 * <p>The platform never stores or transports a plaintext destination. A phone number in a Kafka
 * record is readable by anyone with topic read access for the whole 3-day retention window, by
 * every mirroring tool, and by every DLQ dump — broker-level encryption at rest protects none of
 * that. So the address is sealed once, at fan-out, and only ciphertext travels.
 *
 * <p>Three forms come out of {@link #sealFor}, and each exists to make a different operation
 * possible without decrypting:
 *
 * <ul>
 *   <li>{@code cipher} — AES-GCM under a per-user DEK. GDPR erasure destroys the key, not
 *       billions of partitioned rows.</li>
 *   <li>{@code hash} — tenant-keyed HMAC. Suppression and dedup match on this, so neither path
 *       needs a decrypt capability, and the same number under two tenants does not correlate.</li>
 *   <li>{@code hint} — {@code g***@example.com}. The only form that may appear in a log line.</li>
 * </ul>
 *
 * <p>A port rather than a concrete service because the real implementation belongs to
 * {@code platform-security}, which does not exist yet. Keeping the contract here means the worker
 * is written against the shape it will actually have, rather than against a plaintext string that
 * would have to be threaded out of every call site later.
 */
public interface RecipientAddressVault {

    /**
     * The sealed destination for one recipient on one channel.
     *
     * @param cipher       AES-GCM ciphertext, as stored in {@code notification_recipient}
     * @param cipherBase64 the same bytes Base64-encoded, as carried on the dispatch event
     * @param hash         tenant-keyed HMAC of the plaintext address
     * @param hint         redacted form, safe to log
     * @param dekRef       reference to the data encryption key, resolved again at send time
     */
    record SealedAddress(byte[] cipher, String cipherBase64, byte[] hash, String hint, String dekRef) {

        public SealedAddress {
            Objects.requireNonNull(cipher, "cipher");
            Objects.requireNonNull(cipherBase64, "cipherBase64");
            Objects.requireNonNull(hash, "hash");
            Objects.requireNonNull(dekRef, "dekRef");
            cipher = cipher.clone();
            hash = hash.clone();
        }

        @Override
        public byte[] cipher() {
            return cipher.clone();
        }

        @Override
        public byte[] hash() {
            return hash.clone();
        }
    }

    /**
     * Resolves and seals the user's address for a channel.
     *
     * @return empty when the user has no address on this channel — a normal outcome (a user with
     *         no push token is not an error), recorded as a suppression rather than a failure
     */
    Optional<SealedAddress> sealFor(long tenantId, String userRef, Channel channel);

    /**
     * Opens the ciphertext immediately before the provider call.
     *
     * <p>Deliberately not cached and deliberately not called earlier: the plaintext should exist
     * for the duration of one {@code send} and nowhere else. Returning a {@code String} rather
     * than a {@code char[]} is a conscious concession — the provider SDKs all take strings, and
     * pretending otherwise would be theatre.
     */
    String open(String addressCipherBase64, String dekRef);
}
