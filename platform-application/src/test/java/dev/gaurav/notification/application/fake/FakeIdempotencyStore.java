package dev.gaurav.notification.application.fake;

import dev.gaurav.notification.application.port.IdempotencyStore;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory {@link IdempotencyStore} with the same four outcomes as the real
 * {@code INSERT ... ON CONFLICT} against {@code notif.idempotency_record}.
 *
 * <p>Stateful on purpose: the whole point of the accept path's first step is what the second call
 * with the same key sees.
 */
public final class FakeIdempotencyStore implements IdempotencyStore {

    private record Entry(byte[] fingerprint, StoredResponse response) {}

    private final Map<String, Entry> entries = new HashMap<>();

    @Override
    public ClaimResult claim(String tenantId, String key, byte[] fingerprint) {
        var k = compositeKey(tenantId, key);
        var existing = entries.get(k);
        if (existing == null) {
            entries.put(k, new Entry(fingerprint.clone(), null));
            return ClaimResult.claimed();
        }
        // Fingerprint first: a different body under a reused key is a conflict regardless of
        // whether the first request has finished.
        if (!Arrays.equals(existing.fingerprint(), fingerprint)) {
            return ClaimResult.conflict();
        }
        return existing.response() == null
                ? ClaimResult.inProgress()
                : ClaimResult.replay(existing.response());
    }

    @Override
    public void complete(String tenantId, String key, int status, String body) {
        var k = compositeKey(tenantId, key);
        var existing = entries.get(k);
        if (existing == null) {
            throw new IllegalStateException("complete() called for a key that was never claimed: " + key);
        }
        entries.put(k, new Entry(existing.fingerprint(), new StoredResponse(status, body)));
    }

    @Override
    public Optional<StoredResponse> find(String tenantId, String key) {
        var existing = entries.get(compositeKey(tenantId, key));
        return existing == null ? Optional.empty() : Optional.ofNullable(existing.response());
    }

    private static String compositeKey(String tenantId, String key) {
        return tenantId + '|' + key;
    }
}
