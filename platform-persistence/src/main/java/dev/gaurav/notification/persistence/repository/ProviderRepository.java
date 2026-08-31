package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.persistence.entity.Provider;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Provider registry lookups.
 *
 * <p>Routing asks for the active providers on a channel, so that is the finder that exists.
 * Inactive providers are filtered in the query rather than by the caller — a deactivated vendor
 * that still receives traffic because one call site forgot the check is how a decommissioned
 * contract keeps being billed.
 */
public interface ProviderRepository extends JpaRepository<Provider, Short> {

    Optional<Provider> findByCode(String code);

    List<Provider> findByChannelAndActiveTrue(Channel channel);

    List<Provider> findByActiveTrue();
}
