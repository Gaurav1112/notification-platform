package dev.gaurav.notification.adapter.persistence;

import dev.gaurav.notification.persistence.repository.TenantRepository;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Translates the tenant reference the application layer carries into the {@code bigint} surrogate
 * key every row in the {@code notif} schema is scoped by.
 *
 * <p><strong>The seam exists because the two identifiers are deliberately different.</strong>
 * {@code tenant.id} is a {@code bigint} because it is denormalised onto billions of rows;
 * {@code tenant.public_id} is a UUID because an enumerable sequence in a URL or a token lets a
 * caller count their competitors. Letting the surrogate key travel outward would collapse the two
 * and make that guarantee unenforceable, so the application layer only ever sees the public form
 * and this class is the one place the mapping happens.
 *
 * <p>Accepts either the public UUID or the slug, because both are legitimate external names for a
 * tenant and a caller that has one should not have to know which the persistence layer prefers.
 *
 * <p>The cache is an unbounded {@link ConcurrentHashMap} and that is safe here specifically because
 * the mapping is immutable — {@code public_id} and {@code slug} are both {@code updatable = false}
 * or unique-and-stable, and the row is never deleted while notifications reference it. The key
 * space is the tenant count, which is thousands, not the request rate. A miss is <em>not</em>
 * cached: a tenant created after this pod started must become visible without a restart.
 */
@Component
public class TenantDirectory {

    private final TenantRepository tenants;
    private final Map<String, Long> resolved = new ConcurrentHashMap<>();

    public TenantDirectory(TenantRepository tenants) {
        this.tenants = Objects.requireNonNull(tenants, "tenants");
    }

    /**
     * @param tenantRef the tenant's public UUID, or its slug
     * @return empty when no such tenant exists. Callers on a read path must render that as the
     *         same "not found" they would render for an unknown id — an unknown tenant and an
     *         unknown notification must be indistinguishable from outside
     */
    public Optional<Long> internalIdOf(String tenantRef) {
        if (tenantRef == null || tenantRef.isBlank()) {
            return Optional.empty();
        }
        var cached = resolved.get(tenantRef);
        if (cached != null) {
            return Optional.of(cached);
        }
        var found = lookup(tenantRef);
        found.ifPresent(id -> resolved.put(tenantRef, id));
        return found;
    }

    /**
     * The write-path variant.
     *
     * <p>Throws rather than returning empty because a write for a tenant that does not exist is a
     * misconfigured caller, not an empty result: the alternative is a {@code notification} row with
     * a {@code tenant_id} that joins to nothing, and the hot tables carry no foreign key that would
     * catch it. The API validates the tenant before it reaches an accept transaction, so reaching
     * this throw means the edge check was bypassed.
     */
    public long requireInternalId(String tenantRef) {
        return internalIdOf(tenantRef).orElseThrow(() -> new IllegalStateException(
                "no tenant is registered under reference '" + tenantRef + "'"));
    }

    private Optional<Long> lookup(String tenantRef) {
        var byPublicId = asUuid(tenantRef).flatMap(tenants::findByPublicId);
        var tenant = byPublicId.isPresent() ? byPublicId : tenants.findBySlug(tenantRef);
        return tenant.map(t -> t.getId());
    }

    private static Optional<UUID> asUuid(String candidate) {
        try {
            return Optional.of(UUID.fromString(candidate));
        } catch (IllegalArgumentException e) {
            // A slug, not a UUID. Not exceptional: both forms are supported.
            return Optional.empty();
        }
    }
}
