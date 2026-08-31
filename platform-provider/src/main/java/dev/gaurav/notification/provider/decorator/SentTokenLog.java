package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendResult;

import java.time.Instant;
import java.util.Optional;

/**
 * Records that a token was handed to a provider, <em>before</em> the call goes out.
 *
 * <p><strong>The failure this prevents:</strong> a worker killed between the HTTP request and the
 * attempt row write. Kafka redelivers the message, the worker sends again, and the user gets two
 * OTPs. Most providers we would plausibly integrate — Twilio and FCM among them — accept no
 * client-supplied idempotency key, so the vendor cannot deduplicate for us. Recording the token
 * first turns "we have no idea whether this was sent" into "a send was started at T, resolve it".
 *
 * <p>The order is the whole point: record, then send. Recording after the call is a no-op for the
 * exact crash window it is meant to cover.
 */
public interface SentTokenLog {

    /**
     * Marks a send as started.
     *
     * @return {@code false} if this token was already started against this provider, meaning the
     *         caller is a redelivery and must not send again
     */
    boolean recordAttempt(ProviderCode provider, String token, Instant startedAt);

    /** Attaches the outcome once known, so a replay can be answered without calling the vendor. */
    void recordOutcome(ProviderCode provider, String token, SendResult result);

    /** The resolved outcome of an earlier send, if we got one. Empty while a send is unresolved. */
    Optional<SendResult> previousOutcome(ProviderCode provider, String token);

    /** When the earlier send started, which is what the reconciler needs to bound its status query. */
    Optional<Instant> startedAt(ProviderCode provider, String token);
}
