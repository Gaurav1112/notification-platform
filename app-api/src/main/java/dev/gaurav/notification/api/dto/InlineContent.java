package dev.gaurav.notification.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Raw subject and body for a templateless send.
 *
 * <p><strong>Why the size caps are here and not "whatever fits":</strong> an unbounded body on the
 * accept path is a heap amplifier. Ten thousand recipients times a one-megabyte body is ten
 * gigabytes of copies before anything reaches Kafka, so the ceiling has to be enforced at the edge
 * where it costs one comparison. Anything genuinely large belongs in S3 with a claim check.
 *
 * <p>Templateless sends are supported but not encouraged: {@code content} bypasses the template
 * engine's auto-escaping, so the caller owns the injection risk. Templates exist so that a variable
 * cannot become markup.
 *
 * @param subject used by EMAIL and PUSH; ignored for SMS, which has no subject line
 * @param body    the message
 */
public record InlineContent(

        @Size(max = 512)
        String subject,

        @NotBlank
        @Size(max = 64_000, message = "body over 64 KB must be sent as a template or an S3 claim check")
        String body
) {
}
