package dev.gaurav.notification.api.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Set;
import java.util.UUID;

/**
 * Switches that decide how much of the authentication stack is real.
 *
 * <p><strong>{@code permitAll} exists so the demo runs, and it is the single most dangerous flag in
 * this module.</strong> It is therefore off by default, requires an explicit {@code true} in
 * configuration, logs a warning on every startup that enables it, and is asserted against by
 * {@link SecurityConfig}. The alternative — shipping with it on and expecting production config to
 * turn it off — inverts the failure: a forgotten override becomes an open API rather than a service
 * that refuses to start.
 *
 * @param permitAll     when true, every endpoint is unauthenticated and {@link #localTenantId} is
 *                      attributed to every request. Local and CI only
 * @param localTenantId the tenant attributed to unauthenticated callers under {@code permitAll}.
 *                      A fixed UUID rather than a random one so seeded demo data keeps working
 *                      across restarts
 * @param localScopes   scopes granted to that caller
 * @param tenantClaim   the JWT claim carrying the tenant id once a real resource server is wired in
 */
@ConfigurationProperties(prefix = "notification.api.security")
public record ApiSecurityProperties(
        boolean permitAll,
        UUID localTenantId,
        Set<String> localScopes,
        String tenantClaim
) {

    private static final UUID DEFAULT_LOCAL_TENANT = UUID.fromString("00000000-0000-7000-8000-000000000001");

    public ApiSecurityProperties {
        localTenantId = localTenantId == null ? DEFAULT_LOCAL_TENANT : localTenantId;
        localScopes = localScopes == null || localScopes.isEmpty()
                ? Set.of("notifications:send", "notifications:read", "providers:read", "admin:chaos")
                : Set.copyOf(localScopes);
        tenantClaim = tenantClaim == null || tenantClaim.isBlank() ? "tenant_id" : tenantClaim;
    }
}
