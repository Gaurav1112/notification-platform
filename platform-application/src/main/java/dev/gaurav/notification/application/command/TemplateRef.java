package dev.gaurav.notification.application.command;

import dev.gaurav.notification.application.exception.ValidationException;

/**
 * A pointer to a template version, resolved at render time rather than at accept time.
 *
 * <p>No version is carried on purpose. Pinning a version at accept time means a scheduled send
 * queued three weeks ago renders with a template the tenant has since corrected — including any
 * typo or wrong support number they fixed in between.
 *
 * @param code   the tenant-scoped template code, e.g. {@code order-shipped}
 * @param locale BCP-47 tag; the renderer falls back {@code fr-CA → fr → en} rather than failing
 */
public record TemplateRef(String code, String locale) {

    public TemplateRef {
        if (code == null || code.isBlank()) {
            throw ValidationException.invalidField("template.code", "REQUIRED",
                    "a template reference without a code cannot be rendered");
        }
        // A missing locale is legal and means "tenant default"; a blank one is a client bug that
        // would otherwise silently become the string "" and miss every fallback rule.
        locale = (locale == null || locale.isBlank()) ? null : locale;
    }
}
