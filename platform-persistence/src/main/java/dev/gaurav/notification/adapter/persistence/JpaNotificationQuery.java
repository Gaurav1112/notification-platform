package dev.gaurav.notification.adapter.persistence;

import dev.gaurav.notification.application.port.NotificationQuery;
import dev.gaurav.notification.application.result.DeliveryAttemptView;
import dev.gaurav.notification.application.result.NotificationStatusView;
import dev.gaurav.notification.domain.enums.AttemptState;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.persistence.entity.NotificationEntity;
import dev.gaurav.notification.persistence.repository.NotificationRepository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Bridges the read side of {@link NotificationQuery} onto PostgreSQL.
 *
 * <p>The seam exists so the status use case returns projections rather than entities. Handing an
 * entity back would let a lazy association load outside its transaction — a
 * {@code LazyInitializationException} raised during JSON serialisation, which surfaces as a 500 on
 * a read endpoint — and would make a column rename a breaking API change.
 *
 * <p>Tenant scoping is a predicate in both queries, never a check on the result. Absence and
 * cross-tenant access therefore produce the same empty answer, which is what lets the endpoint
 * render {@code 404} for both and stops it being used as an existence oracle.
 *
 * <p>{@link #findStatus} reads the denormalised counters on the {@code notification} row rather than
 * aggregating {@code notification_recipient}. For a 10M-recipient campaign that aggregation is a
 * partition-wide scan per status poll, and clients poll.
 */
@Component
public class JpaNotificationQuery implements NotificationQuery {

    /**
     * Attempt history for one notification, newest first.
     *
     * <p>Native and joined rather than two round trips, because {@code delivery_attempt} is keyed
     * by {@code recipient_id} and has no notification column: loading the recipients first and then
     * their attempts is N+1 against a table with tens of millions of rows a day.
     *
     * <p>Both partitioned tables carry their own window. They are different columns —
     * {@code created_at} on the recipient, {@code attempted_at} on the attempt — and an attempt made
     * just after midnight for a recipient created just before it lives in the next partition, so
     * reusing one window for both would silently drop it.
     *
     * <p>{@code LEFT JOIN} on provider: a provider row can legitimately be missing for an attempt
     * recorded against a decommissioned vendor, and an inner join would make that attempt vanish
     * from the ledger — the one place a support engineer is looking for it.
     */
    private static final String ATTEMPTS_SQL = """
            SELECT a.attempt_no,
                   p.code                AS provider_code,
                   a.state,
                   a.failure_type,
                   a.error_code,
                   a.provider_message_id,
                   a.latency_ms,
                   a.cost_micros,
                   a.request_started_at,
                   a.response_at,
                   r.next_attempt_at
              FROM notif.delivery_attempt a
              JOIN notif.notification_recipient r
                ON r.id = a.recipient_id
               AND r.created_at >= ?
               AND r.created_at <  ?
              LEFT JOIN notif.provider p
                ON p.id = a.provider_id
             WHERE r.notification_id = ?
               AND r.tenant_id       = ?
               AND a.attempted_at   >= ?
               AND a.attempted_at   <  ?
             ORDER BY a.attempted_at DESC
             LIMIT ?
            """;

    private final NotificationRepository notifications;
    private final JdbcTemplate jdbc;
    private final TenantDirectory tenants;
    private final Clock clock;

    public JpaNotificationQuery(NotificationRepository notifications, JdbcTemplate jdbc,
                                TenantDirectory tenants, Clock clock) {
        this.notifications = Objects.requireNonNull(notifications, "notifications");
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
        this.tenants = Objects.requireNonNull(tenants, "tenants");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public Optional<NotificationStatusView> findStatus(String tenantId, UUID notificationId) {
        var now = clock.instant();
        return tenants.internalIdOf(tenantId)
                .flatMap(tenant -> notifications.findInWindow(notificationId, tenant,
                        PartitionWindows.lookbackFrom(now), PartitionWindows.lookbackTo(now)))
                .map(JpaNotificationQuery::toStatusView);
    }

    @Override
    public List<DeliveryAttemptView> findAttempts(String tenantId, UUID notificationId, int limit) {
        var tenant = tenants.internalIdOf(tenantId);
        if (tenant.isEmpty()) {
            return List.of();
        }
        var now = clock.instant();
        var from = utc(PartitionWindows.lookbackFrom(now));
        var to = utc(PartitionWindows.lookbackTo(now));
        return jdbc.query(ATTEMPTS_SQL, ATTEMPT_MAPPER,
                from, to, notificationId, tenant.get(), from, to, limit);
    }

    private static NotificationStatusView toStatusView(NotificationEntity row) {
        int settled = row.getDeliveredCount() + row.getFailedCount() + row.getSuppressedCount();
        // Derived rather than stored so it cannot drift from the three counters it complements,
        // and floored at zero so a counter that overshot never renders as a negative pending count.
        int pending = Math.max(0, row.getTotalRecipients() - settled);
        return new NotificationStatusView(
                row.getId(),
                row.getRequestId(),
                row.getChannel(),
                row.getTrafficClass(),
                row.getStatus(),
                row.getCreatedAt(),
                row.getScheduledAt(),
                row.getDispatchedAt(),
                row.getCompletedAt(),
                new NotificationStatusView.Counts(row.getTotalRecipients(), row.getDeliveredCount(),
                        row.getFailedCount(), row.getSuppressedCount(), pending));
    }

    private static final RowMapper<DeliveryAttemptView> ATTEMPT_MAPPER = JpaNotificationQuery::mapAttempt;

    private static DeliveryAttemptView mapAttempt(ResultSet rs, int rowNum) throws SQLException {
        return new DeliveryAttemptView(
                rs.getShort("attempt_no"),
                rs.getString("provider_code"),
                AttemptState.valueOf(rs.getString("state")),
                enumOrNull(rs.getString("failure_type")),
                rs.getString("error_code"),
                rs.getString("provider_message_id"),
                (Integer) rs.getObject("latency_ms"),
                (Long) rs.getObject("cost_micros"),
                instantOf(rs, "request_started_at"),
                instantOf(rs, "response_at"),
                instantOf(rs, "next_attempt_at"));
    }

    private static FailureType enumOrNull(String value) {
        return value == null ? null : FailureType.valueOf(value);
    }

    /**
     * {@code OffsetDateTime}, not {@code Instant}: PgJDBC's {@code getObject(String, Class)} has no
     * mapping for {@code java.time.Instant} and throws rather than converting.
     */
    private static Instant instantOf(ResultSet rs, String column) throws SQLException {
        var value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    /** Bound as an {@code OffsetDateTime} so PgJDBC sends a {@code timestamptz}. */
    private static OffsetDateTime utc(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
