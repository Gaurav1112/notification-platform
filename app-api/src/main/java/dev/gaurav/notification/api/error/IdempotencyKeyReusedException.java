package dev.gaurav.notification.api.error;

import java.time.Instant;
import java.util.List;

/**
 * The same {@code Idempotency-Key} arrived with a different request fingerprint.
 *
 * <p><strong>The failure this prevents:</strong> the silent wrong replay. The tempting
 * implementation of idempotency is "key seen before → return the stored response", and it is
 * actively dangerous. A client that reuses a key by accident — a hard-coded constant, a key derived
 * from an order id that later gets a second, different notification — receives a 200 describing a
 * send that has nothing to do with what they just asked for, and the second notification is never
 * sent. Nobody sees an error; a customer simply never gets their message.
 *
 * <p>So the stored record keeps a SHA-256 of the canonicalised body, and a mismatch is a 409. The
 * caller is told the key is already spoken for and can pick a new one. Replay only happens when the
 * fingerprint matches, which is the case the mechanism actually exists for: a retried request.
 */
public class IdempotencyKeyReusedException extends ApiException {

    public IdempotencyKeyReusedException(String key, Instant originallyUsedAt) {
        super(ProblemType.IDEMPOTENCY_KEY_REUSED,
                "Key '%s' was used at %s with a different body.".formatted(key, originallyUsedAt),
                List.of(FieldViolation.of("Idempotency-Key", "FINGERPRINT_MISMATCH")));
    }
}
