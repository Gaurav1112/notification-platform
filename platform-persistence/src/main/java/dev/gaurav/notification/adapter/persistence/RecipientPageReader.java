package dev.gaurav.notification.adapter.persistence;

import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.SuppressionReason;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * One keyset page of {@code notification_recipient}, for the API's per-recipient status endpoint.
 *
 * <p>This is not an application port. {@code NotificationQuery} has no {@code recipients} method,
 * and adding one is a design step rather than wiring — see {@code docs/STATUS.md}. What this class
 * does provide is the seam that keeps the SQL out of {@code app-api}: the API adapter states which
 * page it wants and this module decides how the partitioned table is read.
 *
 * <p><strong>Cursor, never offset.</strong> {@code notification_recipient} is RANGE-partitioned by
 * day, so {@code OFFSET 200000} makes PostgreSQL walk and discard two hundred thousand rows across
 * every partition the planner cannot prune — the query gets slower the further a caller paginates,
 * which is backwards from what they need when reconciling a ten-million-recipient campaign.
 *
 * <p>Tenant is a predicate, not a post-fetch check, so a cross-tenant read returns no rows and
 * therefore a 404 rather than a 403.
 *
 * <p>TODO(phase-3): the keyset is {@code ORDER BY id} and there is no index on
 * {@code (notification_id, id)}, so within the pruned partitions this sorts. That is affordable at
 * a page size capped at 200 and is not affordable at campaign scale; the index belongs in the next
 * migration, alongside a cursor that encodes {@code (created_at, id)} so the sort disappears.
 */
@Component
public class RecipientPageReader {

    /**
     * The cursor predicate is written as {@code (?::uuid IS NULL OR r.id > ?::uuid)} so the first
     * page and every later page share one prepared statement. Two statements would be marginally
     * faster and would be two places for the tenant predicate to be forgotten.
     */
    private static final String PAGE_SQL = """
            SELECT r.id,
                   r.address_hint,
                   r.status,
                   r.delivered_at,
                   r.attempt_count,
                   r.suppression_reason
              FROM notif.notification_recipient r
             WHERE r.notification_id = ?
               AND r.tenant_id       = ?
               AND r.created_at     >= ?
               AND r.created_at      < ?
               AND (?::uuid IS NULL OR r.id > ?::uuid)
             ORDER BY r.id
             LIMIT ?
            """;

    /**
     * How far past the notification's own day recipient rows can appear.
     *
     * <p>Fan-out happens asynchronously and a BULK notification lives for 72 hours, so a recipient
     * row can legitimately land three days after the notification was accepted. Four days is that
     * plus a day of slack; anything wider is partitions scanned for rows that cannot exist.
     */
    private static final Duration FAN_OUT_HORIZON = Duration.ofDays(4);

    private final JdbcTemplate jdbc;
    private final TenantDirectory tenants;

    public RecipientPageReader(JdbcTemplate jdbc, TenantDirectory tenants) {
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
        this.tenants = Objects.requireNonNull(tenants, "tenants");
    }

    /**
     * @param notificationCreatedAt the notification's partition key, which anchors the window this
     *                              read prunes to. Without it every page scans every partition
     * @param after                 the last id of the previous page, or {@code null} for the first
     * @param limit                 already clamped by the controller to {@code 1..200}
     */
    public List<RecipientRow> page(String tenantRef, UUID notificationId,
                                   Instant notificationCreatedAt, UUID after, int limit) {
        var tenant = tenants.internalIdOf(tenantRef);
        if (tenant.isEmpty()) {
            return List.of();
        }
        var from = utc(PartitionWindows.dayStart(notificationCreatedAt));
        var to = utc(PartitionWindows.dayEnd(notificationCreatedAt.plus(FAN_OUT_HORIZON)));
        var cursor = after == null ? null : after.toString();
        return jdbc.query(PAGE_SQL, RecipientPageReader::mapRow,
                notificationId, tenant.get(), from, to, cursor, cursor, limit);
    }

    /**
     * One recipient's outcome, in persistence terms.
     *
     * <p>Carries {@code addressHint} and never the address. The status endpoint is reachable by any
     * token with {@code notifications:read}, and one that returned plaintext e-mail addresses and
     * phone numbers would be a bulk PII export with pagination built in.
     */
    public record RecipientRow(
            UUID recipientId,
            String addressHint,
            DeliveryStatus status,
            Instant deliveredAt,
            Integer attemptCount,
            SuppressionReason suppressionReason) {
    }

    private static RecipientRow mapRow(ResultSet rs, int rowNum) throws SQLException {
        var suppression = rs.getString("suppression_reason");
        return new RecipientRow(
                rs.getObject("id", UUID.class),
                rs.getString("address_hint"),
                DeliveryStatus.valueOf(rs.getString("status")),
                instantOf(rs, "delivered_at"),
                rs.getInt("attempt_count"),
                suppression == null ? null : SuppressionReason.valueOf(suppression));
    }

    /** PgJDBC has no {@code getObject(…, Instant.class)} mapping; it throws rather than converting. */
    private static Instant instantOf(ResultSet rs, String column) throws SQLException {
        var value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static OffsetDateTime utc(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
