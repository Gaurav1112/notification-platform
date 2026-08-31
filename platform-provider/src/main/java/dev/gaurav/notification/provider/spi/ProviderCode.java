package dev.gaurav.notification.provider.spi;

import java.util.Objects;

/** Stable identifier for a provider adapter, e.g. {@code mock-sms-primary}. */
public record ProviderCode(String value) {
    public ProviderCode {
        Objects.requireNonNull(value, "value");
        if (value.isBlank()) throw new IllegalArgumentException("provider code must not be blank");
    }
    public static ProviderCode of(String value) { return new ProviderCode(value); }
    @Override public String toString() { return value; }
}
