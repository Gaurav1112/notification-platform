package dev.gaurav.notification.persistence.config;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * Creates the range partitions the hot tables need, by calling the two SQL functions that
 * {@code V1__baseline.sql} installs.
 *
 * <p><strong>This is not how partitions are managed in production.</strong> Production uses
 * {@code pg_partman} driven by {@code pg_cron}, with {@code premake = 14} and a pre-drop export to
 * S3. A Spring {@code @Scheduled} job was considered and rejected for that role, for two reasons
 * that are hard to argue with: it cannot run while the application is being deployed, and its
 * failure mode is silent — every insert quietly lands in the DEFAULT partition, and a row in the
 * DEFAULT partition <em>blocks</em> creation of the real one while holding an ACCESS EXCLUSIVE
 * lock during the scan. By the time anyone notices, the fix is an outage.
 *
 * <p>What this class is for: local development and tests against the stock postgres image, and a
 * deterministic way for CI to provision the partitions a test needs before it writes. It exists
 * so that "run the tests" does not require {@code pg_partman} to be installed.
 *
 * <p>The functions it calls are idempotent ({@code IF to_regclass(...) IS NULL}), so calling
 * {@link #ensureWindow} repeatedly is free.
 */
public class PartitionMaintenanceService {

    /** Partitioned daily by their respective time column. */
    private static final List<String> DAILY_TABLES = List.of(
            "notification_request",
            "notification",
            "notification_recipient",
            "delivery_attempt",
            "notification_event");

    /**
     * Partitioned hourly, and only this one. Idempotency records expire after 24 hours, and
     * {@code DROP TABLE} on an hourly partition is O(1) with no dead tuples where
     * {@code DELETE … WHERE expires_at < now()} would leave twenty million a day for autovacuum.
     */
    private static final List<String> HOURLY_TABLES = List.of("idempotency_record");

    /**
     * Both SQL functions are {@code RETURNS void}, which over JDBC is still a one-row result set —
     * {@code update()} rejects that with "a result was returned when none was expected". This
     * discards it.
     */
    private static final ResultSetExtractor<Void> DISCARD_RESULT = rs -> null;

    private static final Logger log = LoggerFactory.getLogger(PartitionMaintenanceService.class);

    private final JdbcTemplate jdbcTemplate;

    public PartitionMaintenanceService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Creates the daily partition covering {@code day} for one table, if it does not exist.
     *
     * @param table the unqualified table name within the {@code notif} schema
     * @param day   the UTC day the partition covers
     */
    public void ensureDailyPartition(String table, LocalDate day) {
        jdbcTemplate.query("SELECT notif.ensure_daily_partition(?, ?)", DISCARD_RESULT, table, day);
    }

    /**
     * Creates the hourly partition containing {@code hour} for one table, if it does not exist.
     * The function truncates to the hour itself, so any instant within the hour works.
     */
    public void ensureHourlyPartition(String table, Instant hour) {
        // Bound as an OffsetDateTime so PgJDBC sends a timestamptz; a java.sql.Timestamp would
        // arrive as an unzoned timestamp and be reinterpreted in the server's timezone, which
        // silently puts the row in the wrong hourly partition on any server not running in UTC.
        jdbcTemplate.query("SELECT notif.ensure_hourly_partition(?, ?)", DISCARD_RESULT,
                table, OffsetDateTime.ofInstant(hour, ZoneOffset.UTC));
    }

    /**
     * Premakes every partition the platform needs from yesterday through {@code daysAhead}.
     *
     * <p>Yesterday is included on purpose. Backdated writes are normal here — a webhook carries
     * the <em>provider's</em> {@code occurred_at}, and a vendor whose clock is behind, or who
     * retries a callback across midnight, produces a row that belongs to a day that has already
     * ended. Provisioning only forwards leaves those rows landing in DEFAULT.
     *
     * <p>Hourly partitions are premade for 48 hours regardless of {@code daysAhead}, matching the
     * idempotency retention window; there is no value in an idempotency partition for next week.
     *
     * @param daysAhead how many future days of daily partitions to create
     */
    public void ensureWindow(int daysAhead) {
        var today = LocalDate.now(ZoneOffset.UTC);
        for (var day = today.minusDays(1); !day.isAfter(today.plusDays(daysAhead)); day = day.plusDays(1)) {
            for (var table : DAILY_TABLES) {
                ensureDailyPartition(table, day);
            }
        }

        var startHour = Instant.now().minus(Duration.ofHours(1));
        for (var i = 0; i <= 49; i++) {
            var hour = startHour.plus(Duration.ofHours(i));
            for (var table : HOURLY_TABLES) {
                ensureHourlyPartition(table, hour);
            }
        }

        log.debug("ensured partitions for {} daily tables through +{}d and {} hourly tables through +48h",
                DAILY_TABLES.size(), daysAhead, HOURLY_TABLES.size());
    }

    /**
     * Counts rows sitting in a DEFAULT partition.
     *
     * <p>Alarm on any non-zero result. A DEFAULT partition is a safety net — without one, a
     * missing partition is a hard outage — but it is never a destination: while a row lives
     * there, {@code CREATE TABLE … PARTITION OF} for the covering range must scan the default
     * partition under an ACCESS EXCLUSIVE lock, and fails if it finds a conflicting row. One
     * stray row therefore turns routine partition creation into an incident.
     *
     * @param table the unqualified parent table name
     */
    public long countRowsInDefaultPartition(String table) {
        var sql = "SELECT count(*) FROM notif.%s_default".formatted(table);
        var count = jdbcTemplate.queryForObject(sql, Long.class);
        return count == null ? 0L : count;
    }
}
