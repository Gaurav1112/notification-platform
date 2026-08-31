package dev.gaurav.notification.worker.orchestrator;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.SuppressionReason;
import dev.gaurav.notification.domain.enums.TrafficClass;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * "Is this user willing to hear from us on this channel, right now?"
 *
 * <p>Evaluated at <strong>dispatch</strong>, never only at accept. A bulk campaign accepted at
 * 09:00 and sent at 14:00 has five hours in which the user can unsubscribe, and honouring a
 * preference that was current when the request was queued is legally and commercially the same as
 * ignoring it. Opt-out always wins over freshness.
 *
 * <p>The real implementation is the {@code PreferenceFilterChain} of §8.7 — opt-out, global
 * unsubscribe, suppression list, quiet hours, frequency cap, dedup, consent — which lands with
 * {@code platform-application} in phase 9. This port is what the fan-out is written against so
 * that arrival is a bean registration and not a rewrite of the orchestrator.
 */
public interface PreferenceResolver {

    /**
     * What the preference chain decided for one (user, channel) pair.
     *
     * <p>Three outcomes, not two. {@code deferUntil} exists because quiet hours are not a
     * suppression for {@code TRANSACTIONAL} traffic — a receipt at 03:00 should arrive at 08:00,
     * not never. Collapsing defer into suppress silently deletes messages the user does want;
     * collapsing it into allow wakes them up.
     *
     * @param allowed    true when the message may be sent now
     * @param reason     why not, when {@code allowed} is false; never null in that case
     * @param deferUntil when set, the message is allowed but not before this instant
     */
    record PreferenceDecision(boolean allowed, SuppressionReason reason, Instant deferUntil) {

        public PreferenceDecision {
            if (!allowed) {
                Objects.requireNonNull(reason, "a suppressed message must carry a reason");
            }
        }

        /** Send it now. */
        public static PreferenceDecision allow() {
            return new PreferenceDecision(true, null, null);
        }

        /** Do not send it, ever, for this reason. A terminal, reported outcome — not a drop. */
        public static PreferenceDecision suppress(SuppressionReason reason) {
            return new PreferenceDecision(false, reason, null);
        }

        /** Send it, but not before {@code until} — quiet hours on non-critical traffic. */
        public static PreferenceDecision defer(Instant until) {
            return new PreferenceDecision(true, null, Objects.requireNonNull(until, "until"));
        }

        public Optional<Instant> deferral() {
            return Optional.ofNullable(deferUntil);
        }
    }

    /**
     * @param trafficClass carried because policy depends on it: {@code CRITICAL} bypasses quiet
     *                     hours entirely — an OTP at 02:00 was requested by the user two seconds
     *                     earlier — while {@code BULK} is subject to every filter in the chain
     */
    PreferenceDecision resolve(long tenantId, String userRef, Channel channel, TrafficClass trafficClass);
}
