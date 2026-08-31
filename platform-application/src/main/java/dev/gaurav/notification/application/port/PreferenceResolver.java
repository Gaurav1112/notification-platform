package dev.gaurav.notification.application.port;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.SuppressionReason;

import java.time.Instant;
import java.util.Optional;

/**
 * Answers one question: may we send to this user, on this channel, in this category, right now?
 *
 * <p><strong>Deliberately not on the accept path.</strong> A 10M-recipient campaign would need 10M
 * preference lookups inside the request thread; a per-recipient decision at accept time also
 * freezes an answer that is only valid at dispatch time, which is the whole point of quiet hours.
 * The orchestrator calls this instead, immediately before fan-out.
 *
 * <p>Returning an empty {@link Optional} means "send it". Returning a reason means "do not", and
 * the reason is recorded on the recipient row — a suppressed notification with no reason is
 * indistinguishable from a lost one when a customer asks why they never got their receipt.
 *
 * <p>Evaluation order is fixed by
 * {@code OptOut → GlobalUnsubscribe → Suppression → QuietHours → FrequencyCap → Dedup → Consent},
 * and {@code CRITICAL} traffic bypasses quiet hours — an OTP at 2 a.m. was requested by the user
 * who is awake and waiting for it.
 */
public interface PreferenceResolver {

    /**
     * @param userRef  the tenant's own user identifier, never our surrogate key
     * @param category the notification category, or {@code "*"} for the channel-wide default
     * @param now      passed in rather than read inside, because quiet hours are evaluated against
     *                 the user's IANA zone and the result must be reproducible in a test
     */
    Optional<SuppressionReason> evaluate(
            String tenantId, String userRef, Channel channel, String category, Instant now);
}
