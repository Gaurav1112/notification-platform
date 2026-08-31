package dev.gaurav.notification.application.command;

import dev.gaurav.notification.application.exception.ValidationException;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.Priority;
import dev.gaurav.notification.domain.enums.TrafficClass;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Everything {@code POST /v1/notifications} asked for, already legal.
 *
 * <p>The compact constructor is the only validation gate in the accept path. Once a command exists,
 * {@code AcceptNotificationUseCase} can persist it without a single defensive check — which is what
 * keeps the use case readable as a sequence of decisions rather than a wall of guard clauses.
 *
 * <p>Three invariants earn their place here specifically:
 * <ul>
 *   <li><strong>At least one channel.</strong> A request with none is accepted, persisted, fanned
 *       out to nobody and reported as successful. Silent success is the hardest bug class to find,
 *       because nothing anywhere is red.</li>
 *   <li><strong>Exactly one of template or content.</strong> Two sources of body text mean the
 *       renderer picks one, and the two eventually disagree.</li>
 *   <li><strong>{@code sendAt} required, future, and bounded</strong> — enforced by
 *       {@link Schedule}, because a past time fires on the scanner's next pass rather than being
 *       rejected.</li>
 * </ul>
 *
 * @param tenantId           the caller's tenant reference from the JWT, never from the body — a
 *                           body-supplied tenant is a cross-tenant send waiting to happen
 * @param idempotencyKey     the {@code Idempotency-Key} header; required on this endpoint
 * @param requestFingerprint SHA-256 over the raw request bytes as they arrived. Computed at the
 *                           edge, not re-derived from this model: a deploy that reorders a field
 *                           would otherwise change the fingerprint of an unchanged request and turn
 *                           every in-flight client retry into a {@code 409}
 * @param ttlSeconds         {@code null} means "use the traffic class default". An explicit value
 *                           always wins, including when it is shorter
 * @param metadata           free-form tenant annotations — {@code campaignId}, {@code correlationId}.
 *                           Never interpreted here; carried so an operator can join our logs to
 *                           theirs during an incident
 * @param traceparent        W3C trace context, so the async half of the send stays on the caller's
 *                           trace instead of starting a new one at the Kafka boundary
 */
public record SendNotificationCommand(
        String tenantId,
        String idempotencyKey,
        byte[] requestFingerprint,
        TrafficClass trafficClass,
        Set<Channel> channels,
        Priority priority,
        TemplateRef template,
        InlineContent content,
        RecipientSelector recipients,
        Map<String, Object> variables,
        Schedule schedule,
        Integer ttlSeconds,
        Map<String, String> metadata,
        String traceparent) {

    /** Guards against a caller "disabling" TTL with a value the partition scheme cannot hold. */
    public static final Duration MAX_TTL = Duration.ofDays(30);

    public SendNotificationCommand {
        requireText("tenantId", tenantId);
        requireText("idempotencyKey", idempotencyKey);
        if (requestFingerprint == null || requestFingerprint.length == 0) {
            throw ValidationException.invalidField("requestFingerprint", "REQUIRED",
                    "without a fingerprint the same key with a different body would replay silently");
        }
        if (trafficClass == null) {
            throw ValidationException.invalidField("trafficClass", "REQUIRED", "trafficClass is required");
        }
        if (recipients == null) {
            throw ValidationException.invalidField("recipients", "REQUIRED", "recipients is required");
        }

        // Defensive copies: a command handed to the use case must not change under it while the
        // accept transaction is open.
        requestFingerprint = requestFingerprint.clone();
        channels = channels == null || channels.isEmpty()
                ? Set.of()
                : Set.copyOf(new LinkedHashSet<>(channels));
        variables = variables == null ? Map.of() : Map.copyOf(variables);
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        priority = priority == null ? Priority.P2_NORMAL : priority;
        schedule = schedule == null ? Schedule.immediate() : schedule;

        if (channels.isEmpty()) {
            throw ValidationException.invalidField("channels", "REQUIRED",
                    "a request with no channel is accepted, delivered to nobody, and reported as a success");
        }
        if ((template == null) == (content == null)) {
            throw ValidationException.invalidField("template", "AMBIGUOUS",
                    "exactly one of template or content must be present");
        }
        if (ttlSeconds != null && (ttlSeconds <= 0 || ttlSeconds > MAX_TTL.toSeconds())) {
            throw ValidationException.invalidField("ttlSeconds", "OUT_OF_RANGE",
                    "ttlSeconds must be between 1 and " + MAX_TTL.toSeconds());
        }
    }

    /** How many recipients this request will be charged for and will report in the {@code 202}. */
    public int recipientCount() {
        return recipients.count();
    }

    /**
     * The moment after which this notification must never be sent.
     *
     * <p>Measured from {@code sendAt} for a deferred send, not from acceptance: a request scheduled
     * for next week with a 24 h TTL would otherwise expire six days before it was due.
     */
    public Instant expiresAt(Instant acceptedAt) {
        var base = schedule.sendAt() == null ? acceptedAt : schedule.sendAt();
        var ttl = ttlSeconds == null ? trafficClass.defaultTtl() : Duration.ofSeconds(ttlSeconds);
        return base.plus(ttl);
    }

    /**
     * A copy of the fingerprint.
     *
     * <p>Overridden because the array is mutable and the default accessor hands the caller a
     * reference into the command. Note that record equality still compares the array by identity —
     * compare fingerprints with {@link java.util.Arrays#equals}, never with
     * {@code command.equals(other)}.
     */
    @Override
    public byte[] requestFingerprint() {
        return requestFingerprint.clone();
    }

    private static void requireText(String field, String value) {
        if (value == null || value.isBlank()) {
            throw ValidationException.invalidField(field, "REQUIRED", field + " is required");
        }
    }
}
