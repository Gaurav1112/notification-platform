package dev.gaurav.notification.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.Immutable;

/**
 * The {@code notif.delivery_status} lookup table, mirrored into Java so the application can
 * validate a status code without hard-coding the ladder twice.
 *
 * <p>This is the one status representation that is a <em>table</em> rather than a {@code varchar}
 * plus {@code CHECK}, because {@code rank} and {@code is_terminal} are data the SQL reads: the
 * monotonic guard in {@code NotificationRepository} anti-joins against {@code is_terminal}. A
 * {@code CHECK} constraint cannot be joined, and a native PostgreSQL enum's implicit ordering
 * cannot express "terminal". The payoff is that adding a status is an {@code INSERT} — no DDL, no
 * lock, no deploy coordination.
 *
 * <p>Mapped {@link Immutable} deliberately. Rows here are seeded by migration and read by the
 * guard; an accidental dirty-check flush that rewrote {@code rank} would silently change which
 * transitions the database accepts, across every partition, with no schema change to review.
 * {@code dev.gaurav.notification.domain.enums.DeliveryStatus} remains the source of truth for
 * behaviour — this entity exists to prove the two agree.
 */
@Entity
@Immutable
@Table(name = "delivery_status", schema = "notif")
public class DeliveryStatusRef {

    @Id
    @Column(name = "code", nullable = false, length = 24)
    private String code;

    @Column(name = "rank", nullable = false)
    private short rank;

    @Column(name = "is_terminal", nullable = false)
    private boolean terminal;

    @Column(name = "is_failure", nullable = false)
    private boolean failure;

    @Column(name = "is_billable", nullable = false)
    private boolean billable;

    @Column(name = "description", nullable = false)
    private String description;

    protected DeliveryStatusRef() {
        // for JPA
    }

    public String getCode() {
        return code;
    }

    public short getRank() {
        return rank;
    }

    public boolean isTerminal() {
        return terminal;
    }

    public boolean isFailure() {
        return failure;
    }

    public boolean isBillable() {
        return billable;
    }

    public String getDescription() {
        return description;
    }
}
