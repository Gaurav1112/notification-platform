package dev.gaurav.notification.scheduler.due;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.TrafficClass;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * {@link ScheduledWorkStore} on PostgreSQL, in hand-written SQL.
 *
 * <p>JDBC rather than JPA throughout. Every statement here depends on something JPQL cannot say —
 * {@code FOR UPDATE SKIP LOCKED} inside a CTE, {@code UPDATE … FROM … RETURNING}, and a partition
 * key that has to appear in the predicate of a bulk update or the planner touches every partition.
 * Hibernate would also dirty-check an entity graph on the claim path, which is 200 rows of change
 * tracking on the hottest loop in the service.
 *
 * <p><strong>{@code due_bucket} is in every scan predicate.</strong> It is the immutable partition
 * key — derived from {@code due_at} at insert and never updated — and without it the scan is an
 * Append across every partition. Immutable for a second reason: a partition-key {@code UPDATE} is a
 * silent DELETE+INSERT at roughly triple the WAL cost, can raise error {@code 40001} under
 * concurrency, and {@code SKIP LOCKED} cannot silently ignore a tuple that moved out from under it.
 * Rescheduling is an explicit DELETE + INSERT; nothing in this class ever moves a row between
 * partitions.
 *
 * <p><strong>Required schema.</strong> This class targets {@code notif.scheduled_notification},
 * which the design specifies (§6.3, §8.6) but which is <em>not</em> in {@code V1__baseline.sql}.
 * The DDL belongs to {@code platform-persistence}, so it is reported as a gap rather than smuggled
 * in here. The contract relied on:
 *
 * <pre>{@code
 * CREATE TABLE notif.scheduled_notification (
 *     due_bucket              date         NOT NULL,          -- partition key, immutable
 *     id                      uuid         NOT NULL,          -- UUIDv7
 *     due_at                  timestamptz  NOT NULL,
 *     shard                   smallint     NOT NULL,          -- 0..255, fixed at insert
 *     tenant_id               bigint       NOT NULL,
 *     notification_id         uuid         NOT NULL,
 *     notification_created_at timestamptz  NOT NULL,
 *     recipient_id            uuid         NOT NULL,
 *     channel                 varchar(16)  NOT NULL,
 *     traffic_class           varchar(16)  NOT NULL,
 *     state                   varchar(16)  NOT NULL DEFAULT 'READY',
 *     claimed_by              varchar(96),
 *     claim_expires_at        timestamptz,
 *     claim_count             smallint     NOT NULL DEFAULT 0,
 *     payload                 jsonb        NOT NULL,
 *     PRIMARY KEY (due_bucket, id),
 *     CONSTRAINT sn_state_ck CHECK (state IN ('READY','CLAIMED','DISPATCHED','CANCELLED','FAILED')),
 *     CONSTRAINT sn_shard_ck CHECK (shard BETWEEN 0 AND 255),
 *     -- makes "CLAIMED with no owner" unrepresentable
 *     CONSTRAINT sn_owner_ck CHECK ((state = 'CLAIMED') = (claimed_by IS NOT NULL))
 * ) PARTITION BY RANGE (due_bucket);
 *
 * CREATE INDEX sn_due_shard_ix ON notif.scheduled_notification (due_at, shard)
 *     INCLUDE (id) WHERE state = 'READY';
 * CREATE INDEX sn_lease_expiry_ix ON notif.scheduled_notification (claim_expires_at)
 *     WHERE state = 'CLAIMED';
 * }</pre>
 */
@Repository
public class JdbcScheduledWorkStore implements ScheduledWorkStore {

    /**
     * How far back the due scan looks for overdue READY rows.
     *
     * <p>Bounded rather than open-ended so the scan prunes to eight partitions instead of the full
     * retention window. Seven days is generous: the longest TTL in the platform is BULK's 72 hours,
     * so a READY row older than that has already outlived the message it would have sent. Those are
     * a bug, and they belong to the nightly reconciler, which can afford a full scan.
     */
    private static final int SCAN_BUCKET_LOOKBACK_DAYS = 7;

    private static final String COLUMNS = """
            id, due_at, shard, tenant_id, notification_id, notification_created_at,
            recipient_id, channel, traffic_class, claim_count, claimed_by, claim_expires_at,
            payload
            """;

    /**
     * The near-horizon read. Index-only against {@code sn_due_shard_ix} — measured at 75 buffers /
     * 0.275 ms for 500 rows — because the predicate matches the index's own {@code WHERE} clause
     * and {@code id} is in the INCLUDE list.
     *
     * <p>The {@code due_bucket} bound looks redundant next to the {@code due_at} bound and is not:
     * only the bucket predicate prunes partitions, and only the {@code due_at} predicate gives the
     * index scan a stop point.
     *
     * <p>{@code AT TIME ZONE 'UTC'} rather than a bare {@code ::date}. Casting a {@code timestamptz}
     * to {@code date} resolves against the <em>session</em> TimeZone, so the same query would prune
     * to a different partition depending on which pod ran it — and would be right in CI, where
     * everything is UTC, and wrong wherever it is not.
     */
    private static final String SCAN_READY_SQL = """
            SELECT %s
              FROM notif.scheduled_notification
             WHERE state = 'READY'
               AND due_bucket >= ((CAST(:horizonEnd AS timestamptz) AT TIME ZONE 'UTC')
                                   - make_interval(days => :lookbackDays))::date
               AND due_bucket <=  (CAST(:horizonEnd AS timestamptz) AT TIME ZONE 'UTC')::date
               AND due_at <= :horizonEnd
             ORDER BY due_at
             LIMIT :limit
            """.formatted(COLUMNS);

    /**
     * Claim. The CTE takes the locks and the outer statement does the write, which is the only
     * shape in PostgreSQL that combines {@code SKIP LOCKED} with {@code RETURNING}. A plain
     * {@code UPDATE … WHERE id IN (…)} would <em>block</em> on a row another claimer holds rather
     * than stepping over it, and blocking is exactly the 159-tps stampede this design exists to
     * avoid.
     */
    private static final String CLAIM_SQL = """
            WITH claimable AS (
                SELECT sn.due_bucket, sn.id
                  FROM notif.scheduled_notification sn
                 WHERE sn.id IN (:ids)
                   AND sn.shard = :shard
                   AND sn.state = 'READY'
                   AND sn.due_at <= :now
                   FOR UPDATE SKIP LOCKED
            )
            UPDATE notif.scheduled_notification t
               SET state            = 'CLAIMED',
                   claimed_by       = :owner,
                   claim_expires_at = :leaseExpiresAt
              FROM claimable c
             WHERE t.due_bucket = c.due_bucket
               AND t.id         = c.id
            RETURNING t.id, t.due_at, t.shard, t.tenant_id, t.notification_id,
                      t.notification_created_at, t.recipient_id, t.channel, t.traffic_class,
                      t.claim_count, t.claimed_by, t.claim_expires_at, t.payload
            """;

    private static final String MARK_DISPATCHED_SQL = """
            UPDATE notif.scheduled_notification
               SET state            = 'DISPATCHED',
                   claimed_by       = NULL,
                   claim_expires_at = NULL
             WHERE id IN (:ids)
               AND state      = 'CLAIMED'
               AND claimed_by = :owner
            """;

    private static final String EXPIRED_LEASES_SQL = """
            SELECT %s
              FROM notif.scheduled_notification
             WHERE state = 'CLAIMED'
               AND claim_expires_at < :now
             ORDER BY claim_expires_at
             LIMIT :limit
            """.formatted(COLUMNS);

    /**
     * The {@code claim_expires_at < :now} predicate is repeated even though the caller already
     * filtered on it. Between the read and this write another reaper may have reclaimed the row and
     * a claimer may have re-leased it; without the re-check this statement rips a live lease out
     * from under a pod that is mid-publish, and produces the duplicate the lease exists to bound.
     */
    private static final String RECLAIM_SQL = """
            UPDATE notif.scheduled_notification
               SET state            = 'READY',
                   claimed_by       = NULL,
                   claim_expires_at = NULL,
                   claim_count      = claim_count + 1
             WHERE id IN (:ids)
               AND state = 'CLAIMED'
               AND claim_expires_at < :now
            """;

    private static final String ABANDON_SQL = """
            UPDATE notif.scheduled_notification
               SET state            = 'FAILED',
                   claimed_by       = NULL,
                   claim_expires_at = NULL
             WHERE id IN (:ids)
               AND state = 'CLAIMED'
            """;

    private static final RowMapper<ScheduledWork> MAPPER = JdbcScheduledWorkStore::mapRow;

    private final NamedParameterJdbcTemplate jdbc;

    public JdbcScheduledWorkStore(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    @Transactional(readOnly = true)
    public List<ScheduledWork> scanReady(Instant horizonEnd, int limit) {
        return jdbc.query(SCAN_READY_SQL, Map.of(
                "horizonEnd", utc(horizonEnd),
                "lookbackDays", SCAN_BUCKET_LOOKBACK_DAYS,
                "limit", limit), MAPPER);
    }

    @Override
    @Transactional
    public List<ScheduledWork> claim(int shard, Collection<UUID> ids, String owner,
                                     Instant now, Instant leaseExpiresAt) {
        if (ids.isEmpty()) {
            return List.of();
        }
        var params = new MapSqlParameterSource()
                .addValue("ids", ids)
                .addValue("shard", shard)
                .addValue("owner", owner)
                .addValue("now", utc(now))
                .addValue("leaseExpiresAt", utc(leaseExpiresAt));
        return jdbc.query(CLAIM_SQL, params, MAPPER);
    }

    @Override
    @Transactional
    public int markDispatched(Collection<UUID> ids, String owner) {
        if (ids.isEmpty()) {
            return 0;
        }
        return jdbc.update(MARK_DISPATCHED_SQL, new MapSqlParameterSource()
                .addValue("ids", ids)
                .addValue("owner", owner));
    }

    @Override
    @Transactional(readOnly = true)
    public List<ScheduledWork> findExpiredLeases(Instant now, int limit) {
        return jdbc.query(EXPIRED_LEASES_SQL, Map.of("now", utc(now), "limit", limit), MAPPER);
    }

    @Override
    @Transactional
    public int reclaim(Collection<UUID> ids, Instant now) {
        if (ids.isEmpty()) {
            return 0;
        }
        return jdbc.update(RECLAIM_SQL, new MapSqlParameterSource()
                .addValue("ids", ids)
                .addValue("now", utc(now)));
    }

    @Override
    @Transactional
    public int abandon(Collection<UUID> ids) {
        if (ids.isEmpty()) {
            return 0;
        }
        return jdbc.update(ABANDON_SQL, new MapSqlParameterSource().addValue("ids", ids));
    }

    /**
     * Binds as {@code timestamptz}. A {@code java.sql.Timestamp} arrives as an unzoned
     * {@code timestamp} and is reinterpreted in the server's timezone, which on any server not
     * running in UTC silently shifts a lease expiry by hours.
     */
    private static OffsetDateTime utc(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static ScheduledWork mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new ScheduledWork(
                rs.getObject("id", UUID.class),
                rs.getObject("due_at", OffsetDateTime.class).toInstant(),
                rs.getInt("shard"),
                rs.getLong("tenant_id"),
                rs.getObject("notification_id", UUID.class),
                rs.getObject("notification_created_at", OffsetDateTime.class).toInstant(),
                rs.getObject("recipient_id", UUID.class),
                Channel.valueOf(rs.getString("channel")),
                TrafficClass.valueOf(rs.getString("traffic_class")),
                rs.getInt("claim_count"),
                rs.getString("claimed_by"),
                instantOrNull(rs.getObject("claim_expires_at", OffsetDateTime.class)),
                rs.getString("payload"));
    }

    private static Instant instantOrNull(OffsetDateTime timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
