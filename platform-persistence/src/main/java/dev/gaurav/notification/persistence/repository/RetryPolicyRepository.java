package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.persistence.entity.RetryPolicyEntity;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Retry policy lookups, keyed by the human-readable code ({@code default}, {@code critical},
 * {@code bulk}) rather than the generated id, because that is what configuration and tests refer
 * to and the id is an implementation detail of the insert order.
 */
public interface RetryPolicyRepository extends JpaRepository<RetryPolicyEntity, Short> {

    Optional<RetryPolicyEntity> findByCode(String code);
}
