package dev.gaurav.notification.api.port;

import java.util.Set;
import java.util.UUID;

/**
 * Who is making this call, resolved once at the edge and passed down explicitly.
 *
 * <p><strong>The failure this prevents:</strong> an implicit tenant. A {@code ThreadLocal} holding
 * the current tenant reads beautifully until something runs off the request thread — an
 * {@code @Async} webhook processor, a {@code CompletableFuture}, a Kafka listener, a parallel
 * stream — at which point it holds either nothing or, far worse, whatever the previous request on
 * that pooled thread left behind. The second case is a cross-tenant read that no test will catch
 * and no log will show.
 *
 * <p>{@code tenantId} is also the value the repository layer scopes every query on, which is what
 * makes a cross-tenant lookup return no row and therefore a 404 rather than a 403. See
 * {@link dev.gaurav.notification.api.error.NotificationNotFoundException}.
 *
 * @param tenantId the scoping key for every query and every quota
 * @param subject  the OAuth2 client or user id, for the audit trail
 * @param scopes   granted scopes, e.g. {@code notifications:send}
 */
public record ApiCaller(UUID tenantId, String subject, Set<String> scopes) {

    public ApiCaller {
        scopes = Set.copyOf(scopes);
    }

    public boolean hasScope(String scope) {
        return scopes.contains(scope);
    }
}
