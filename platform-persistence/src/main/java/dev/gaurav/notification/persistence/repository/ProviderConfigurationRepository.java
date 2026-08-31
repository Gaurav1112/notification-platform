package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.persistence.entity.ProviderConfiguration;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Routing configuration lookups.
 *
 * <p>The important method is {@link #findRoutingCandidates}: a tenant override and a platform
 * default can both exist for the same provider, and the tenant's must win. Ordering by
 * {@code tenantId} descending with nulls last does that in the query, so no call site can get the
 * precedence backwards and quietly send a tenant's traffic through the shared account.
 */
public interface ProviderConfigurationRepository extends JpaRepository<ProviderConfiguration, Long> {

    List<ProviderConfiguration> findByProviderIdAndActiveTrue(Short providerId);

    Optional<ProviderConfiguration> findByProviderIdAndTenantIdAndActiveTrue(Short providerId, Long tenantId);

    /**
     * Every active configuration usable by this tenant on this channel, best first.
     *
     * <p>"Best" is: tenant-specific before platform-default, then by explicit routing priority,
     * then by weight descending. Failover walks this list in order.
     */
    @Query("""
            SELECT c FROM ProviderConfiguration c, Provider p
             WHERE p.id = c.providerId
               AND p.channel = :channel
               AND p.active = true
               AND c.active = true
               AND (c.tenantId = :tenantId OR c.tenantId IS NULL)
             ORDER BY CASE WHEN c.tenantId IS NULL THEN 1 ELSE 0 END,
                      c.routingPriority,
                      c.weight DESC
            """)
    List<ProviderConfiguration> findRoutingCandidates(@Param("channel") Channel channel,
                                                      @Param("tenantId") Long tenantId);
}
