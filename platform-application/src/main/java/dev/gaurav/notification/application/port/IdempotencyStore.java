package dev.gaurav.notification.application.port;

import java.util.Objects;
import java.util.Optional;

/**
 * The durable claim on an {@code Idempotency-Key}, and the store of the response we sent for it.
 *
 * <p>The claim is what makes a client retry safe. A network timeout on {@code POST /notifications}
 * tells the client nothing about whether the request was accepted, so every well-behaved client
 * retries — and without a claim, each retry creates another notification. One flaky mobile network
 * then becomes three OTP messages, or three charges' worth of SMS spend.
 *
 * <p><strong>The fingerprint is the part most implementations skip.</strong> Keying only on the
 * key means a client that reuses a key with a different body silently receives the response to an
 * unrelated request. That failure is invisible in metrics and near-impossible to debug from the
 * caller's side, so a mismatch must be a {@link ClaimOutcome#CONFLICT}, never a replay.
 *
 * <p>Implementations must make {@link #claim} atomic — the real one is an
 * {@code INSERT ... ON CONFLICT} against {@code notif.idempotency_record}, not a
 * {@code SELECT}-then-{@code INSERT}, because two pods racing on the same key would both see the
 * miss.
 */
public interface IdempotencyStore {

    /**
     * Atomically claims {@code key} for {@code tenantId}, or reports why it could not.
     *
     * @param tenantId    the caller's tenant reference; keys are scoped per tenant so two tenants
     *                    picking {@code "order-1"} never collide
     * @param key         the caller-supplied {@code Idempotency-Key} header value
     * @param fingerprint SHA-256 of the canonical request body, computed at the edge over the bytes
     *                    that actually arrived — re-serialising the parsed model first would make
     *                    the fingerprint change on a deploy that reorders a field
     */
    ClaimResult claim(String tenantId, String key, byte[] fingerprint);

    /**
     * Records the response we sent, so the next retry can replay it verbatim.
     *
     * <p>Called <em>after</em> the accept transaction commits. Storing it before would let a
     * crash between the two leave a COMPLETED record pointing at work that never happened.
     */
    void complete(String tenantId, String key, int status, String body);

    /** The stored response for a key, or empty if it never completed or has expired past 24 h. */
    Optional<StoredResponse> find(String tenantId, String key);

    /** Why a claim attempt did or did not succeed. */
    enum ClaimOutcome {

        /** The key is ours; proceed with the accept transaction. */
        CLAIMED,

        /** Same key, same fingerprint, already completed — replay the stored response. */
        REPLAY,

        /** Same key, <em>different</em> fingerprint. {@code 409 idempotency-key-reused}. */
        CONFLICT,

        /**
         * Same key, same fingerprint, still in flight on another thread or pod.
         * {@code 409 request-in-progress} — the client should retry after the lock expires rather
         * than get a half-built response.
         */
        IN_PROGRESS
    }

    /**
     * The result of a claim attempt.
     *
     * @param outcome what happened
     * @param stored  present only for {@link ClaimOutcome#REPLAY}
     */
    record ClaimResult(ClaimOutcome outcome, Optional<StoredResponse> stored) {

        public ClaimResult {
            Objects.requireNonNull(outcome, "outcome");
            stored = stored == null ? Optional.empty() : stored;
            // A REPLAY with nothing to replay would make the caller build a fresh response with a
            // fresh id, which is exactly the duplicate the claim exists to prevent.
            if (outcome == ClaimOutcome.REPLAY && stored.isEmpty()) {
                throw new IllegalArgumentException("REPLAY requires a stored response");
            }
        }

        public static ClaimResult claimed() {
            return new ClaimResult(ClaimOutcome.CLAIMED, Optional.empty());
        }

        public static ClaimResult replay(StoredResponse stored) {
            return new ClaimResult(ClaimOutcome.REPLAY, Optional.of(stored));
        }

        public static ClaimResult conflict() {
            return new ClaimResult(ClaimOutcome.CONFLICT, Optional.empty());
        }

        public static ClaimResult inProgress() {
            return new ClaimResult(ClaimOutcome.IN_PROGRESS, Optional.empty());
        }
    }

    /**
     * The exact bytes we returned the first time.
     *
     * <p>Stored as an opaque string rather than a re-serialisable model on purpose: a replay must
     * be byte-identical to the original, including the original ids, even across a deploy that
     * changed the response shape.
     */
    record StoredResponse(int status, String body) {

        public StoredResponse {
            Objects.requireNonNull(body, "body");
        }
    }
}
