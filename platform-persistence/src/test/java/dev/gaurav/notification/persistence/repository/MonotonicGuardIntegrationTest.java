package dev.gaurav.notification.persistence.repository;

import static org.assertj.core.api.Assertions.assertThat;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.persistence.AbstractPostgresTest;
import dev.gaurav.notification.persistence.entity.NotificationEntity;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Proves the monotonic guard against a real PostgreSQL.
 *
 * <p>Each test here is a webhook sequence that actually happens in production. The property being
 * proved is not "the UPDATE works" — it is that the three ways an event can be wrong (stale,
 * duplicated, post-terminal) all resolve to zero rows affected without an exception, because that
 * is what lets every consumer in the platform be at-least-once.
 */
class MonotonicGuardIntegrationTest extends AbstractPostgresTest {

    /** The tenant every row in this class belongs to. Cross-tenant refusal is proved separately. */
    private static final long TENANT = 1L;

    @Autowired
    private NotificationRepository notifications;

    private UUID notificationId;
    private Instant createdAt;
    private Instant windowFrom;
    private Instant windowTo;

    @BeforeEach
    void seedNotificationInSent() {
        notificationId = UUID.randomUUID();
        createdAt = Instant.now();
        windowFrom = createdAt.truncatedTo(ChronoUnit.DAYS);
        windowTo = windowFrom.plus(Duration.ofDays(1));

        var notification = new NotificationEntity(
                notificationId, UUID.randomUUID(), TENANT, Channel.SMS,
                TrafficClass.TRANSACTIONAL, createdAt.plus(Duration.ofHours(24)));
        notification.setCreatedAt(createdAt);
        notification.setStatus(DeliveryStatus.SENT);
        notifications.save(notification);
    }

    @Test
    @DisplayName("a late SENT does not overwrite DELIVERED")
    void lateSentAfterDeliveredIsDropped() {
        assertThat(apply(DeliveryStatus.DELIVERED, Instant.now())).isEqualTo(1);

        // The provider's 'we sent it' callback, delayed behind its own 'we delivered it'. Nothing
        // stops this: they travel different paths and there is no ordering guarantee between them.
        var rowsAffected = apply(DeliveryStatus.SENT, Instant.now());

        assertThat(rowsAffected)
                .as("a stale event must be dropped, not applied and not thrown")
                .isZero();
        assertThat(currentStatus()).isEqualTo(DeliveryStatus.DELIVERED);
    }

    @Test
    @DisplayName("a redelivered DELIVERED webhook affects no rows instead of double-counting")
    void duplicateDeliveredIsDropped() {
        assertThat(apply(DeliveryStatus.DELIVERED, Instant.now())).isEqualTo(1);

        var rowsAffected = apply(DeliveryStatus.DELIVERED, Instant.now());

        assertThat(rowsAffected)
                .as("the rank comparison is strict, so an exact duplicate is rejected too")
                .isZero();
    }

    @Test
    @DisplayName("BOUNCED still applies after DELIVERED, because delivery is not the end")
    void bounceAfterDeliveryApplies() {
        apply(DeliveryStatus.DELIVERED, Instant.now());

        // An SMTP 250 means 'accepted', not 'landed in the inbox'. A state machine that treated
        // DELIVERED as terminal would drop this, the address would never reach the suppression
        // list, and we would keep mailing a dead mailbox until our sender reputation collapsed.
        var rowsAffected = apply(DeliveryStatus.BOUNCED, Instant.now());

        assertThat(rowsAffected).isEqualTo(1);
        assertThat(currentStatus()).isEqualTo(DeliveryStatus.BOUNCED);
    }

    @Test
    @DisplayName("nothing escapes a terminal status, not even a higher-ranked one")
    void nothingAppliesAfterTerminal() {
        apply(DeliveryStatus.DELIVERED, Instant.now());
        apply(DeliveryStatus.BOUNCED, Instant.now());

        // UNKNOWN outranks BOUNCED (90 > 85), so the rank test alone would let it through. The
        // terminal anti-join is what stops it.
        var rowsAffected = apply(DeliveryStatus.UNKNOWN, Instant.now());

        assertThat(rowsAffected).isZero();
        assertThat(currentStatus()).isEqualTo(DeliveryStatus.BOUNCED);
    }

    @Test
    @DisplayName("a cancellation loses to a queue that already happened")
    void cancelAfterQueuedIsDropped() {
        // CANCELLED (28) is ranked deliberately below QUEUED (30): 'you cannot cancel something
        // already dispatched' is enforced by the ordering, not by an if-statement somebody has to
        // remember to write. The seeded row is already at SENT (60), well past both.
        var rowsAffected = apply(DeliveryStatus.CANCELLED, Instant.now());

        assertThat(rowsAffected).isZero();
        assertThat(currentStatus()).isEqualTo(DeliveryStatus.SENT);
    }

    @Test
    @DisplayName("an event carrying the wrong day window silently matches nothing")
    void transitionOutsideThePartitionWindowMatchesNothing() {
        var yesterdayFrom = windowFrom.minus(Duration.ofDays(1));

        var rowsAffected = notifications.applyStatusTransition(
                notificationId, TENANT, yesterdayFrom, windowFrom,
                DeliveryStatus.DELIVERED.name(), (short) DeliveryStatus.DELIVERED.rank(), Instant.now());

        // Worth pinning down: the window is a correctness parameter, not a hint. Derive it from
        // the notification's own created_at, never from 'now', or a transition arriving just after
        // midnight updates nothing and reports success.
        assertThat(rowsAffected).isZero();
        assertThat(currentStatus()).isEqualTo(DeliveryStatus.SENT);
    }

    @Test
    @DisplayName("status_at moves forward only, so a reordered event cannot rewind the clock")
    void statusAtNeverMovesBackwards() {
        var late = Instant.now();
        var early = late.minus(Duration.ofMinutes(10));

        apply(DeliveryStatus.ACCEPTED, late);
        apply(DeliveryStatus.DELIVERED, early);

        var statusAt = jdbcTemplate.queryForObject(
                "SELECT status_at FROM notif.notification WHERE id = ? AND created_at >= ? AND created_at < ?",
                java.sql.Timestamp.class,
                notificationId, java.sql.Timestamp.from(windowFrom), java.sql.Timestamp.from(windowTo));

        assertThat(statusAt).isNotNull();
        assertThat(statusAt.toInstant())
                .as("greatest() keeps the later timestamp even when the later event arrives first")
                .isAfterOrEqualTo(late.truncatedTo(ChronoUnit.MILLIS));
    }

    private int apply(DeliveryStatus proposed, Instant occurredAt) {
        return notifications.applyStatusTransition(
                notificationId, TENANT, windowFrom, windowTo,
                proposed.name(), (short) proposed.rank(), occurredAt);
    }

    private DeliveryStatus currentStatus() {
        return notifications.findInWindow(notificationId, TENANT, windowFrom, windowTo)
                .map(NotificationEntity::getStatus)
                .orElseThrow();
    }
}
