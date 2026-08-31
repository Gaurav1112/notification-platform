package dev.gaurav.notification.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Which template, in which locale. Resolved to a specific immutable template <em>version</em> at
 * accept time, and that version id is stored on the notification.
 *
 * <p><strong>Why the locale is a request field and not a user attribute:</strong> both exist. The
 * caller's explicit locale wins because the calling service often knows something the profile does
 * not — the language the user just switched their app into, or the locale of the order rather than
 * the account. Falling back to the profile when this is absent is the preference layer's job.
 *
 * @param code   stable template identifier, e.g. {@code order-shipped}
 * @param locale BCP-47 tag, e.g. {@code en-US}; optional, falls back to the recipient's preference
 */
public record TemplateRef(

        @NotBlank
        @Size(max = 128)
        @Pattern(regexp = "^[a-z0-9][a-z0-9._-]*$",
                message = "template code must be lowercase alphanumeric with . _ or -")
        String code,

        @Size(max = 35)
        @Pattern(regexp = "^[A-Za-z]{2,8}(-[A-Za-z0-9]{2,8})*$", message = "locale must be a BCP-47 tag")
        String locale
) {
}
