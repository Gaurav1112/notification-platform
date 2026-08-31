package dev.gaurav.notification.api.dto;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.TrafficClass;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * The body of {@code POST /v1/notifications}.
 *
 * <p>One request fans out to <em>one notification per channel</em>, each with its own id, status
 * and lifecycle. That is why {@code channels} is a list and the response carries an array: an
 * e-mail that bounces and a push that lands are two different outcomes, and collapsing them into
 * one status would force the caller to guess which half happened.
 *
 * <p><strong>{@code trafficClass} is the most consequential field in the API</strong> and it is
 * required rather than defaulted. It selects the physical Kafka topic, the TTL, the dispatch
 * latency objective and the load-shedding order. A default of {@code TRANSACTIONAL} would be
 * convenient and would mean that the day a marketing job forgets the field, ten million bulk
 * messages queue ahead of the OTP topic.
 *
 * @param variables values interpolated into the template, validated against the version's
 *                  {@code variablesSchema}. {@code Object} rather than {@code String} because a
 *                  template legitimately renders numbers, booleans and lists
 * @param metadata  caller-owned correlation data, echoed on events and never interpolated into
 *                  content — it is not validated against a schema, so treating it as content would
 *                  be an injection path
 */
public record SendNotificationRequest(

        @NotNull(message = "trafficClass is required; it selects the topic, TTL and shed order")
        TrafficClass trafficClass,

        @NotEmpty
        @Size(max = 3, message = "there are only three channels")
        List<@NotNull Channel> channels,

        @Valid
        TemplateRef template,

        @Valid
        InlineContent content,

        @NotNull
        @Valid
        RecipientSelector recipients,

        @Size(max = 200, message = "a template with more than 200 variables is a document, not a notification")
        Map<@Size(max = 64) String, Object> variables,

        @Valid
        ScheduleRequest schedule,

        /*
         * Bounded on both ends. A 30-second TTL is almost always a mistake that produces EXPIRED
         * before a worker can pick the message up; 72 hours is the BULK ceiling, beyond which a
         * message is stale enough that delivering it is worse than dropping it.
         */
        @Min(value = 30, message = "a TTL under 30s expires before dispatch can plausibly happen")
        @Max(value = 259_200, message = "72 hours is the maximum TTL")
        Integer ttlSeconds,

        @Size(max = 32)
        Map<@Size(max = 64) String, @Size(max = 256) String> metadata
) {

    /** Never null, so callers do not have to write the same null check twice. */
    public ScheduleRequest scheduleOrImmediate() {
        return schedule == null ? ScheduleRequest.immediate() : schedule;
    }

    /**
     * Exactly one content source.
     *
     * <p>Both set is ambiguous and neither set is unsendable, and both are worth a 400 rather than
     * a precedence rule nobody reads. A precedence rule here would mean a caller who adds
     * {@code content} for a quick test, forgets to remove it, and keeps shipping the template
     * silently — or the reverse.
     */
    @AssertTrue(message = "exactly one of template or content must be supplied")
    public boolean isExactlyOneContentSource() {
        return (template == null) != (content == null);
    }

    /**
     * Rejects {@code ["EMAIL", "EMAIL"]}.
     *
     * <p>A duplicate would otherwise produce two notifications, two provider calls and two invoices
     * for what the caller obviously meant once. Silently de-duplicating is the other option, but
     * then the {@code notifications} array in the 202 has fewer entries than the caller sent
     * channels, which reads like a bug.
     */
    @AssertTrue(message = "channels must not contain duplicates")
    public boolean isChannelsDistinct() {
        return channels == null || new LinkedHashSet<>(channels).size() == channels.size();
    }
}
