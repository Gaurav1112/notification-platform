package dev.gaurav.notification.scheduler.partition;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import dev.gaurav.notification.persistence.config.PartitionMaintenanceService;
import dev.gaurav.notification.scheduler.config.SchedulerProperties;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Premakes partitions, and — more importantly — alarms when they are missing.
 *
 * <p><strong>Production does not use this job to create partitions.</strong> Production uses
 * {@code pg_partman} driven by {@code pg_cron}, with {@code premake = 14} and a pre-drop export to
 * S3. A Spring {@code @Scheduled} job was considered for that role and rejected for a reason that
 * is hard to argue with: it cannot run while the application is being deployed, which is exactly
 * when a long-running platform is most likely to skip a day. This class exists so that local
 * development and CI can provision partitions against the stock postgres image without installing
 * {@code pg_partman}.
 *
 * <p><strong>The canary is the part that earns its place in production too.</strong> The failure
 * mode of a missing partition is not an error — it is every insert quietly landing in the DEFAULT
 * partition. Nothing fails, nothing is logged, and the queries that matter get slower in a way
 * nobody attributes. It then gets worse: while a row sits in the DEFAULT partition, creating the
 * real partition for that range must scan the default under an {@code ACCESS EXCLUSIVE} lock and
 * <em>fails</em> if it finds a conflicting row. So the silent failure converts routine partition
 * creation into an outage, and it does so at whatever hour someone next tries to fix it. The canary
 * turns a silent, compounding fault into a page while it is still cheap.
 *
 * <p>Two independent alarms, because they catch different things: tomorrow's partition not existing
 * catches a stalled {@code pg_cron}; rows already in a DEFAULT partition catch the case where it
 * stalled some time ago and nobody noticed.
 */
@Component
public class PartitionMaintenanceJob {

    /** Every RANGE-partitioned parent in {@code V1__baseline.sql} that is partitioned by day. */
    private static final List<String> DAILY_TABLES = List.of(
            "notification_request",
            "notification",
            "notification_recipient",
            "delivery_attempt",
            "notification_event");

    private static final String PARTITION_EXISTS_SQL =
            "SELECT to_regclass('notif.' || ? || '_' || ?) IS NOT NULL";

    private static final Logger log = LoggerFactory.getLogger(PartitionMaintenanceJob.class);

    private final PartitionMaintenanceService partitions;
    private final JdbcTemplate jdbc;
    private final SchedulerProperties.Partition settings;
    private final Clock clock;
    private final MeterRegistry meters;

    /**
     * Gauge state held in fields and registered once.
     *
     * <p>{@code MeterRegistry.gauge(name, number)} keeps a <em>weak</em> reference to the number it
     * is handed, and re-registering under the same name is a no-op that returns the original. Call
     * it with a fresh {@code Long} on every run and the first one is garbage collected within the
     * hour, after which the gauge reports {@code NaN} forever — a partition alarm that silently
     * stops alarming, which is the worst possible failure for this particular metric.
     */
    private final AtomicLong missingTomorrow = new AtomicLong();
    private final AtomicLong rowsInDefault = new AtomicLong();

    public PartitionMaintenanceJob(PartitionMaintenanceService partitions, JdbcTemplate jdbc,
                                   SchedulerProperties properties, Clock clock,
                                   MeterRegistry meters) {
        this.partitions = partitions;
        this.jdbc = jdbc;
        this.settings = properties.partition();
        this.clock = clock;
        this.meters = meters;

        Gauge.builder("scheduler.partition.missing.tomorrow", missingTomorrow, AtomicLong::get)
                .description("Daily tables with no partition for tomorrow. Non-zero past the "
                        + "canary hour is an outage scheduled for midnight UTC.")
                .register(meters);
        Gauge.builder("scheduler.partition.default.rows", rowsInDefault, AtomicLong::get)
                .description("Rows sitting in a DEFAULT partition. One is enough to make creating "
                        + "the real partition fail under an ACCESS EXCLUSIVE lock.")
                .register(meters);
    }

    /**
     * Creates the partition window.
     *
     * <p>03:00 UTC, not midnight. Midnight is when the daily partition boundary is crossed, the
     * retention drop runs, and half the platform's cron jobs fire; adding DDL that takes brief
     * {@code ACCESS EXCLUSIVE} locks to that moment is how a partition job becomes a nightly
     * latency spike.
     *
     * <p>The underlying SQL functions are idempotent ({@code IF to_regclass(...) IS NULL}), so a
     * second replica running this concurrently is free rather than an error.
     */
    @Scheduled(cron = "${notification.scheduler.partition.cron:0 0 3 * * *}", zone = "UTC")
    public void premakePartitions() {
        try {
            partitions.ensureWindow(settings.daysAhead());
            meters.counter("scheduler.partition.maintenance.ok").increment();
            log.info("ensured partitions through +{} days", settings.daysAhead());
        } catch (DataAccessException e) {
            meters.counter("scheduler.partition.maintenance.failed").increment();
            log.error("partition premake failed; the canary will page if tomorrow's partition is "
                    + "still missing at {}:00 UTC", settings.canaryAt(), e);
        }
    }

    /**
     * The canary.
     *
     * <p>Hourly, so the check itself cannot be the thing that was skipped, but it only escalates
     * once the configured hour has passed — before that, a missing tomorrow is merely a job that
     * has not run yet. Twelve hours of margin is enough for a human to fix it during a working day
     * and far short of the midnight boundary where it becomes an incident.
     */
    @Scheduled(cron = "${notification.scheduler.partition.canary-cron:0 5 * * * *}", zone = "UTC")
    public void canary() {
        var now = clock.instant().atZone(ZoneOffset.UTC);
        var tomorrow = now.toLocalDate().plusDays(1);

        long missing = DAILY_TABLES.stream().filter(table -> !partitionExists(table, tomorrow)).count();
        missingTomorrow.set(missing);

        if (missing > 0 && now.getHour() >= settings.canaryAt()) {
            meters.counter("scheduler.partition.canary.failed").increment();
            log.error("CANARY: {} of {} daily tables have no partition for {} and it is past "
                    + "{}:00 UTC. Every insert for that day will land in the DEFAULT partition, "
                    + "and a single row there blocks creation of the real partition under an "
                    + "ACCESS EXCLUSIVE lock. Fix before midnight UTC.",
                    missing, DAILY_TABLES.size(), tomorrow, settings.canaryAt());
        }

        long stray = DAILY_TABLES.stream().mapToLong(this::countDefaultRows).sum();
        rowsInDefault.set(stray);
        if (stray > 0) {
            log.error("CANARY: {} rows are sitting in a DEFAULT partition. Partition creation for "
                    + "the covering range will now fail until they are moved.", stray);
        }
    }

    private boolean partitionExists(String table, LocalDate day) {
        try {
            // Matches the naming the ensure_daily_partition function uses: notification_20260901.
            var suffix = "%04d%02d%02d".formatted(day.getYear(), day.getMonthValue(), day.getDayOfMonth());
            return Boolean.TRUE.equals(jdbc.queryForObject(PARTITION_EXISTS_SQL, Boolean.class, table, suffix));
        } catch (DataAccessException e) {
            // A canary that cannot read the catalogue reports "missing", never "fine". A silent
            // pass here would defeat the entire point of the check.
            log.warn("could not check for the {} partition of {}", day, table, e);
            return false;
        }
    }

    private long countDefaultRows(String table) {
        try {
            return partitions.countRowsInDefaultPartition(table);
        } catch (DataAccessException e) {
            log.warn("could not count rows in the DEFAULT partition of {}", table, e);
            return 0L;
        }
    }
}
