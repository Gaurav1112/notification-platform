package dev.gaurav.notification.application.command;

import dev.gaurav.notification.application.exception.ValidationException;

/**
 * Pre-rendered subject and body, for callers that own their own templating.
 *
 * <p>Mutually exclusive with {@link TemplateRef}: two sources of content mean the renderer has to
 * pick one, and the two will eventually disagree in a way nobody notices until a customer quotes
 * the wrong message back at support.
 *
 * @param subject required for {@code EMAIL}, meaningless for {@code SMS} — the channel worker, not
 *                this record, is where that rule can be checked, because a single command can carry
 *                both channels
 */
public record InlineContent(String subject, String body) {

    public InlineContent {
        if (body == null || body.isBlank()) {
            throw ValidationException.invalidField("content.body", "REQUIRED",
                    "an empty body would be delivered as an empty notification, and cannot be recalled");
        }
    }
}
