package dev.gaurav.notification.persistence.schedule;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * The only {@code INSERT} into {@code notif.scheduled_notification}.
 *
 * <p><strong>{@code due_bucket} is computed by the database, never by Java.</strong> The column is
 * the partition key and the due scan bounds it as
 * {@code ((:horizonEnd AT TIME ZONE 'UTC')::date)}. If this class derived the same date from a
 * {@link java.time.LocalDate} in the JVM's default zone, then every row written by a pod running
 * anywhere other than UTC would land in a partition the scan prunes away — and would do so
 * silently, because there is no error in "the query returned no rows". So the statement below
 * writes {@code (:dueAt AT TIME ZONE 'UTC')::date} and the two expressions are textually the same
 * thing. A trigger installed by {@code V2__scheduled_notification.sql} rejects any row where they
 * disagree, so a future writer that gets this wrong fails loudly at the insert.
 *
 * <p><strong>Rows are always born {@code READY} with no lease.</strong> {@code state},
 * {@code claimed_by}, {@code claim_expires_at}, {@code claim_count} and {@code fencing_token} are
 * left to their column defaults rather than being parameters, because there is no legitimate
 * caller that wants to create work already claimed by somebody. Making it inexpressible here is
 * cheaper than the constraint violation it would otherwise become.
 */
@Repository
public class ScheduledNotificationWriter {

    /**
     * The shard space, fixed at 256 forever.
     *
     * <p>Declared here because this is where a shard is first stamped onto a row, and a shard is
     * immutable from that moment: it decides which pod owns the work, and a shard that moves is a
     * row two pods reach for at once. Changing this number orphans every row already written and
     * every Redis key already hydrated, so it is a constant and not configuration.
     */
    public static final int SHARD_COUNT = 256;

    /**
     * {@code ON CONFLICT DO NOTHING} on {@code (due_bucket, id)}.
     *
     * <p>Scoped honestly: this makes a <em>retried statement</em> safe, not a re-expansion. The
     * caller's id is the recipient row's id, and a genuine re-expansion mints a new recipient row
     * and therefore a new id — that case is caught upstream by the consumer's dedup key, not here.
     * What this covers is the batch being replayed inside a retried transaction, where a
     * unique-violation would abort the whole expansion and turn a harmless retry into a request
     * that is never expanded at all.
     */
    private static final String INSERT_SQL = """
            INSERT INTO notif.scheduled_notification (
                due_bucket, id, due_at, shard, tenant_id, notification_id,
                notification_created_at, recipient_id, channel, traffic_class, payload)
            VALUES (
                (CAST(:dueAt AS timestamptz) AT TIME ZONE 'UTC')::date,
                :id, :dueAt, :shard, :tenantId, :notificationId,
                :notificationCreatedAt, :recipientId, :channel, :trafficClass,
                CAST(:payload AS jsonb))
            ON CONFLICT (due_bucket, id) DO NOTHING
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public ScheduledNotificationWriter(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * One piece of future work, as the writer sees it.
     *
     * <p>{@code channel} and {@code trafficClass} are the enum <em>names</em> rather than the enum
     * types: this module must not depend on how the scheduler models them, and the column is a
     * {@code varchar} with a {@code CHECK} either way.
     *
     * @param payload the serialised {@code NotificationDispatchEvent}, as JSON. Carried whole so
     *                the claim path never joins; a lookup per row at 74,600 rows/s would put the
     *                dispatch fan-out back on the database the Redis near-horizon exists to spare
     */
    public record ScheduledInsert(UUID id,
                                  Instant dueAt,
                                  int shard,
                                  long tenantId,
                                  UUID notificationId,
                                  Instant notificationCreatedAt,
                                  UUID recipientId,
                                  String channel,
                                  String trafficClass,
                                  String payload) {

        public ScheduledInsert {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(dueAt, "dueAt");
            Objects.requireNonNull(notificationId, "notificationId");
            Objects.requireNonNull(notificationCreatedAt, "notificationCreatedAt");
            Objects.requireNonNull(recipientId, "recipientId");
            Objects.requireNonNull(channel, "channel");
            Objects.requireNonNull(trafficClass, "trafficClass");
            Objects.requireNonNull(payload, "payload");
            if (shard < 0 || shard >= SHARD_COUNT) {
                throw new IllegalArgumentException(
                        "shard %d is outside 0..%d".formatted(shard, SHARD_COUNT - 1));
            }
        }
    }

    /** @return 1 when the row was written, 0 when an identical one already existed */
    @Transactional
    public int insert(ScheduledInsert work) {
        return jdbc.update(INSERT_SQL, parameters(work));
    }

    /**
     * Batch form, for a fan-out that defers many recipients at once.
     *
     * <p>One round trip instead of one per recipient. A campaign expanded into a quiet-hours
     * window is thousands of rows, and at that size the network is the cost, not the insert.
     *
     * @return rows actually written, which is fewer than {@code work.size()} when the batch
     *         contains a replay
     */
    @Transactional
    public int insertAll(Collection<ScheduledInsert> work) {
        if (work.isEmpty()) {
            return 0;
        }
        var batch = work.stream().map(ScheduledNotificationWriter::parameters)
                .toArray(SqlParameterSource[]::new);
        int written = 0;
        for (int affected : jdbc.batchUpdate(INSERT_SQL, batch)) {
            // A batched statement can report SUCCESS_NO_INFO (-2) rather than a row count. Adding
            // it blindly would make the caller's metric drift negative, so only real counts count.
            written += Math.max(affected, 0);
        }
        return written;
    }

    /**
     * Which of the 256 shards a piece of work belongs to.
     *
     * <p>Hashed from the same {@code tenantId|recipientId|CHANNEL} triple that keys the dispatch
     * topics, so a recipient's scheduled work and their immediate work spread the same way. Two
     * properties matter and neither is obvious:
     *
     * <ul>
     *   <li><strong>No time component.</strong> Sharding on the due instant would put every
     *       message scheduled for 09:00 on one shard, which is one pod doing all the work at the
     *       exact moment the platform is busiest — a hot shard created by the thing that was
     *       supposed to spread the load.</li>
     *   <li><strong>{@link String#hashCode()} and not {@link Object#hashCode()}.</strong> The
     *       String contract is specified by the JDK and identical in every JVM; {@code UUID}'s is
     *       too, but a record's or an enum's is not. A shard that depends on identity hash codes
     *       would be recomputed differently after a restart, and the row's stamped shard would
     *       stop matching the pod that owns it.</li>
     * </ul>
     */
    public static int shardFor(long tenantId, UUID recipientId, String channel) {
        Objects.requireNonNull(recipientId, "recipientId");
        Objects.requireNonNull(channel, "channel");
        var key = tenantId + "|" + recipientId + "|" + channel;
        return Math.floorMod(key.hashCode(), SHARD_COUNT);
    }

    private static SqlParameterSource parameters(ScheduledInsert work) {
        return new MapSqlParameterSource()
                .addValue("id", work.id())
                // Bound as OffsetDateTime so PgJDBC sends a timestamptz. A java.sql.Timestamp
                // arrives as an unzoned timestamp and is reinterpreted in the server's timezone,
                // which on any server not running in UTC moves the row to the wrong partition.
                .addValue("dueAt", utc(work.dueAt()))
                .addValue("shard", work.shard())
                .addValue("tenantId", work.tenantId())
                .addValue("notificationId", work.notificationId())
                .addValue("notificationCreatedAt", utc(work.notificationCreatedAt()))
                .addValue("recipientId", work.recipientId())
                .addValue("channel", work.channel())
                .addValue("trafficClass", work.trafficClass())
                .addValue("payload", work.payload());
    }

    private static OffsetDateTime utc(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
