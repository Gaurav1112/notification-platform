package dev.gaurav.notification.api.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Per-provider inbound webhook verification settings.
 *
 * <p><strong>A provider with no entry here cannot post webhooks at all</strong>, and that is the
 * intended default. The alternative — an unknown {@code providerCode} falling through to a shared
 * secret, or to no verification — turns the endpoint into an unauthenticated write path into the
 * delivery state machine, where a forged {@code DELIVERED} would mark a failed OTP as sent and
 * suppress the retry.
 *
 * <p>{@code secret} holds a reference such as {@code arn:aws:secretsmanager:…} in any real
 * deployment; the schema's {@code credentials_ref} CHECK constraint enforces the same shape on the
 * database side. A literal secret belongs only in the local profile.
 *
 * @param signatureHeader   header carrying {@code sha256=<hex>}
 * @param timestampHeader   header carrying epoch seconds
 * @param timestampTolerance replay window, applied in both directions
 * @param providers         keyed by {@code ProviderCode}
 */
@ConfigurationProperties(prefix = "notification.webhooks")
public record WebhookProperties(
        String signatureHeader,
        String timestampHeader,
        Duration timestampTolerance,
        Map<String, Provider> providers
) {

    public WebhookProperties {
        signatureHeader = blankToDefault(signatureHeader, "X-Signature");
        timestampHeader = blankToDefault(timestampHeader, "X-Timestamp");
        // Five minutes each way. Wide enough to survive ordinary clock skew between our host and a
        // provider's, narrow enough that a captured payload is worthless by the time it is replayed.
        timestampTolerance = timestampTolerance == null ? Duration.ofMinutes(5) : timestampTolerance;
        providers = providers == null ? Map.of() : Map.copyOf(providers);
    }

    public Optional<Provider> forCode(String providerCode) {
        return Optional.ofNullable(providers.get(providerCode));
    }

    /**
     * @param allowedIps source addresses permitted to post for this provider. <strong>Empty means
     *                   unrestricted</strong>, which is correct for local development and wrong for
     *                   production — the allowlist is verification gate three and the only one that
     *                   an attacker holding a leaked secret cannot satisfy from their own network
     */
    public record Provider(String secret, Set<String> allowedIps) {
        public Provider {
            allowedIps = allowedIps == null ? Set.of() : Set.copyOf(allowedIps);
        }

        public boolean permits(String sourceIp) {
            return allowedIps.isEmpty() || allowedIps.contains(sourceIp);
        }
    }

    private static String blankToDefault(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
