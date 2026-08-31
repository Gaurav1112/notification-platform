package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.persistence.entity.DeliveryStatusRef;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * The status ladder, as stored.
 *
 * <p>Mostly read once at startup: the application needs it to prove that the Java
 * {@code DeliveryStatus} enum and the table the monotonic guard joins against still agree. If
 * they drift, the guard silently starts accepting or rejecting transitions the code does not
 * expect, and nothing else in the system would notice.
 */
public interface DeliveryStatusRefRepository extends JpaRepository<DeliveryStatusRef, String> {

    List<DeliveryStatusRef> findByTerminalTrue();

    List<DeliveryStatusRef> findByBillableTrue();
}
