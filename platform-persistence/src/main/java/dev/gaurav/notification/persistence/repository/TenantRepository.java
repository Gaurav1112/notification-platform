package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.persistence.entity.Tenant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Tenant lookups. Not partitioned, so ordinary derived finders are safe here.
 *
 * <p>Lookup is by {@code publicId} or {@code slug} rather than by the {@code bigint} id, because
 * those are the two identifiers that reach the edge of the system. The internal id exists to be
 * small on the billions of rows that carry it, and letting it into a URL would make tenant ids
 * enumerable.
 */
public interface TenantRepository extends JpaRepository<Tenant, Long> {

    Optional<Tenant> findByPublicId(UUID publicId);

    Optional<Tenant> findBySlug(String slug);

    List<Tenant> findByStatus(Tenant.TenantStatus status);
}
