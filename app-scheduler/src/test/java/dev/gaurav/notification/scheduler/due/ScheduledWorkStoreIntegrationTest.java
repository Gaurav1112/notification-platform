package dev.gaurav.notification.scheduler.due;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.messaging.event.DeadLetterEvent;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.persistence.schedule.ScheduledNotificationWriter;
import dev.gaurav.notification.scheduler.config.SchedulerProperties;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The claim path, against a real PostgreSQL, with real concurrency.
 *
 * <p>This is the test that {@code V2__scheduled_notification.sql} exists to make possible, and it
 * is the only place the platform's central safety property is actually demonstrated rather than
 * asserted in a javadoc: <strong>a due row is dispatched exactly once, even when several pods
 * reach for it at the same instant.</strong> Everything else in the scheduler — the shard
 * affinity, the Redis near-horizon, the leader-elected hydrator — is throughput. This is
 * correctness.
 *
 * <p><strong>The {@link CyclicBarrier} is the whole test.</strong> Without it the first claimer's
 * transaction commits before the second one starts, every claimer sees a clean table, and the
 * assertions below pass on plain {@code FOR UPDATE} — or on no locking at all. The barrier forces
 * every claimer to hold its locks until all of them have claimed, so the only thing that can make
 * the result disjoint is {@code SKIP LOCKED} genuinely stepping over another session's rows. A
 * claimer that blocks instead of skipping never reaches the barrier and the test fails on a
 * timeout rather than passing by accidental serialisation. {@code OutboxRepositoryTest} does the
 * same thing for the same reason.
 *
 * <p><strong>Not a {@code @SpringBootTest}.</strong> Booting the scheduler context would start the
 * Kafka listeners, the Redis leader election and six {@code @Scheduled} jobs, all of which would
 * be racing the assertions on the very table under test. The two collaborators that matter are
 * wired by hand instead, from the same classes production uses — including
 * {@link ScheduledNotificationWriter}, so the rows these tests claim were inserted by the real
 * writer and not by hand-written SQL that could disagree with it.
 */
@Tag("integration")
class ScheduledWorkStoreIntegrationTest {

    /** Enough concurrency that a serialised implementation cannot fake the result. */
    private static final int CLAIMERS = 6;

    /** Comfortably more than one claimer's batch, so no single pass can trivially take them all. */
    private static final int DUE_ROWS = 240;

    /**
     * One shard for every row, which is the <em>hostile</em> case rather than the normal one.
     * In steady state shard affinity means two pods never reach for the same row and there is
     * nothing to skip; this reproduces the rebalance window, when a shard has briefly moved
     * between pods and both believe they own it. That window is the only reason
     * {@code SKIP LOCKED} is in the claim at all, so it is the window worth testing.
     */
    private static final int SHARD = 7;

    /** Matches {@code notification.scheduler.claimer.max-claims-per-row}. */
    private static final int MAX_CLAIMS = 5;

    private static final String PAYLOAD = "{\"eventType\":\"notification.dispatch\"}";

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6");

    private static DataSource dataSource;

    static {
        POSTGRES.start();
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas("notif")
                .locations("classpath:db/migration")
                .load()
                .migrate();

        var source = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        source.setDriverClassName("org.postgresql.Driver");
        dataSource = source;
    }

    private final JdbcTemplate jdbc = new JdbcTemplate(dataSource);
    private final NamedParameterJdbcTemplate namedJdbc = new NamedParameterJdbcTemplate(dataSource);
    private final TransactionTemplate transactions =
            new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    private final ScheduledWorkStore store =
            new JdbcScheduledWorkStore(namedJdbc, new ScheduledNotificationWriter(namedJdbc));

    @BeforeEach
    void emptyTheLedger() {
        // TRUNCATE on the partitioned parent cascades to its partitions and leaves the partition
        // set intact. A DROP would take the partitions with it and the next test would write into
        // the DEFAULT partition — passing, while proving nothing about the real ones.
        jdbc.execute("TRUNCATE notif.scheduled_notification");
    }

    @Test
    @DisplayName("six claimers with overlapping candidate lists lease each of 240 due rows exactly once — never twice, never zero times")
    void concurrentClaimersPartitionTheDueSetExactlyOnce() throws Exception {
        var now = Instant.now();
        List<UUID> due = seedDue(DUE_ROWS, now.minusSeconds(60));

        var everyoneHasClaimed = new CyclicBarrier(CLAIMERS);
        var pool = Executors.newFixedThreadPool(CLAIMERS);
        try {
            var futures = new ArrayList<Future<List<UUID>>>(CLAIMERS);
            for (int i = 0; i < CLAIMERS; i++) {
                var owner = "claimer-" + i;
                var candidates = overlappingWindow(due, i);
                futures.add(pool.submit(() -> claimHolding(owner, candidates, now, everyoneHasClaimed)));
            }

            var union = new HashSet<UUID>();
            int handedOut = 0;
            for (var future : futures) {
                // Generous, and deliberately finite. A claimer that blocks on another's row
                // instead of skipping it never reaches the barrier, and this is where that shows
                // up — as a failure, rather than as a suspiciously tidy pass.
                List<UUID> batch = future.get(60, TimeUnit.SECONDS);
                union.addAll(batch);
                handedOut += batch.size();
            }

            assertThat(handedOut)
                    .as("every row the claimers were handed, counted once per claimer that got "
                            + "it — a total above %d means one row was leased to two pods and the "
                            + "recipient gets the message twice", DUE_ROWS)
                    .isEqualTo(DUE_ROWS);
            assertThat(union)
                    .as("the union of every claimer's batch — a total below %d means a due row was "
                            + "skipped by every claimer whose window contained it, and is now "
                            + "waiting for a hydrator pass that has already happened", DUE_ROWS)
                    .hasSize(DUE_ROWS);
        } finally {
            pool.shutdownNow();
        }

        assertThat(countWhere("state = 'READY'"))
                .as("nothing may still be READY once the claims committed")
                .isZero();
        assertThat(countWhere("state = 'CLAIMED' AND claimed_by IS NOT NULL"))
                .isEqualTo(DUE_ROWS);
        assertThat(distinctOwners())
                .as("the exclusive ends of the first and last windows cannot both go to one pod, "
                        + "so anything less than two owners means the claim serialised and the "
                        + "exactly-once result above was reached without any contention at all")
                .isGreaterThan(1);
        assertThat(countWhere("claim_count <> 0"))
                .as("claim_count counts leases that expired, not leases that were taken; a "
                        + "successful claim must leave it alone or it stops being a poison signal")
                .isZero();
    }

    /**
     * The candidate list one claimer polls: a window twice the stride, so consecutive claimers
     * overlap by half.
     *
     * <p>Handing every claimer the whole id set was the first version of this test and it was
     * weaker than it looked — whichever thread reached the database first locked all 240 rows and
     * the other five legitimately got nothing, so the run proved exactly-once without ever proving
     * that two claimers can make progress at the same time. Overlapping windows reproduce what a
     * rebalance actually looks like (two pods polling shard 7 with partly-shared Redis candidate
     * lists) and leave each end of the range reachable by only one claimer, which is what makes
     * the two-owner assertion above deterministic rather than lucky.
     */
    private static List<UUID> overlappingWindow(List<UUID> due, int claimer) {
        int stride = due.size() / CLAIMERS;
        int from = claimer * stride;
        int to = Math.min(due.size(), from + 2 * stride);
        return List.copyOf(due.subList(from, to));
    }

    @Test
    @DisplayName("a lease that expired because the pod holding it died returns to READY, so the work is not lost with the pod")
    void expiredLeaseIsReclaimed() {
        var now = Instant.now();
        List<UUID> due = seedDue(1, now.minusSeconds(60));
        var publisher = publisherThatAlwaysAcks();
        var reaper = reaperWith(publisher, now);

        // Claiming with a lease that has already elapsed is what a pod OOM-killed mid-dispatch
        // leaves behind: the row is CLAIMED, the owner is gone, and nothing but the deadline says
        // so. There is no other way to reach this state, which is exactly the point of the lease.
        assertThat(store.claim(SHARD, due, "pod-that-died", now, now.minusSeconds(1))).hasSize(1);

        reaper.reap();

        assertThat(stateOf(due.get(0))).isEqualTo("READY");
        assertThat(countWhere("claimed_by IS NOT NULL OR claim_expires_at IS NOT NULL"))
                .as("a reclaimed row must drop both halves of the lease; the biconditional CHECK "
                        + "would reject one without the other, and this proves we never try")
                .isZero();
        assertThat(claimCountOf(due.get(0)))
                .as("the reclaim is what increments the counter — that is what makes it a count "
                        + "of pods this row has killed rather than a retry tally")
                .isEqualTo(1);
        verify(publisher, never()).publishDeadLetter(any());
    }

    @Test
    @DisplayName("a row whose claim has expired six times goes to the DLQ instead of being handed to a seventh pod to kill")
    void poisonRowIsDeadLetteredRatherThanReclaimedForever() {
        var now = Instant.now();
        List<UUID> due = seedDue(1, now.minusSeconds(60));
        var id = due.get(0);
        var publisher = publisherThatAlwaysAcks();
        var reaper = reaperWith(publisher, now);

        // Six real claim-and-expire cycles rather than an UPDATE that sets the counter. The
        // threshold is only meaningful if it is reached the way production reaches it, and this
        // also proves the counter is driven by reclaim and by nothing else.
        for (int attempt = 0; attempt < MAX_CLAIMS + 1; attempt++) {
            store.claim(SHARD, due, "pod-" + attempt, now, now.minusSeconds(1));
            reaper.reap();
        }
        assertThat(claimCountOf(id)).isEqualTo(MAX_CLAIMS + 1);
        assertThat(stateOf(id))
                .as("up to and including the threshold the row is still recoverable")
                .isEqualTo("READY");

        // The seventh victim.
        store.claim(SHARD, due, "pod-7", now, now.minusSeconds(1));
        reaper.reap();

        assertThat(stateOf(id))
                .as("past the threshold the row must leave circulation, or it claims, crashes, "
                        + "expires and reclaims forever while every pod restarts cleanly and no "
                        + "metric anywhere counts the loop")
                .isEqualTo("FAILED");
        assertThat(claimCountOf(id))
                .as("abandoning must not look like another reclaim")
                .isEqualTo(MAX_CLAIMS + 1);

        var dlq = ArgumentCaptor.forClass(DeadLetterEvent.class);
        verify(publisher, times(1)).publishDeadLetter(dlq.capture());
        assertThat(dlq.getValue().sourceKey())
                .as("the triage coordinate has to be the row, not an invented Kafka offset")
                .isEqualTo(id.toString());
        assertThat(dlq.getValue().exceptionClass()).isEqualTo("ScheduleClaimCountExceeded");
    }

    @Test
    @DisplayName("a row parked for a future instant is invisible to a due scan until that instant arrives")
    void futureWorkIsNotScannedEarly() {
        var now = Instant.now();
        seedDue(3, now.minusSeconds(60));
        seedDue(5, now.plus(Duration.ofHours(6)));

        assertThat(store.scanReady(now, 100))
                .as("the scan bounds due_at as well as the partition, and a scheduled send that "
                        + "leaks out early is worse than one that is late")
                .hasSize(3);
        assertThat(store.scanReady(now.plus(Duration.ofHours(7)), 100)).hasSize(8);
    }

    private List<UUID> claimHolding(String owner, List<UUID> ids, Instant now, CyclicBarrier barrier) {
        return transactions.execute(status -> {
            var claimed = store.claim(SHARD, ids, owner, now, now.plusSeconds(60)).stream()
                    .map(ScheduledWork::id)
                    .toList();
            try {
                barrier.await(45, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException(
                        owner + " reached the barrier but another claimer never did — which is "
                                + "what a claimer blocking on a locked row looks like", e);
            }
            return claimed;
        });
    }

    /** Writes {@code count} rows through the production writer and returns their ids. */
    private List<UUID> seedDue(int count, Instant dueAt) {
        var ids = new ArrayList<UUID>(count);
        for (int i = 0; i < count; i++) {
            var id = UUID.randomUUID();
            int written = store.schedule(new ScheduledWork(
                    id, dueAt, SHARD, 1L, UUID.randomUUID(), dueAt, UUID.randomUUID(),
                    Channel.SMS, TrafficClass.TRANSACTIONAL, 0, null, null, PAYLOAD));
            assertThat(written).as("seeding row %d", i).isEqualTo(1);
            ids.add(id);
        }
        return List.copyOf(ids);
    }

    private LeaseReaper reaperWith(NotificationEventPublisher publisher, Instant now) {
        var properties = new SchedulerProperties(null,
                new SchedulerProperties.Claimer(null, null, null, MAX_CLAIMS),
                null, null, null, null);
        return new LeaseReaper(store, publisher, properties,
                Clock.fixed(now, ZoneOffset.UTC), new SimpleMeterRegistry());
    }

    private static NotificationEventPublisher publisherThatAlwaysAcks() {
        var publisher = mock(NotificationEventPublisher.class);
        when(publisher.publishDeadLetter(any()))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));
        return publisher;
    }

    private long countWhere(String predicate) {
        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM notif.scheduled_notification WHERE " + predicate, Long.class);
        return count == null ? 0L : count;
    }

    private long distinctOwners() {
        Long count = jdbc.queryForObject(
                "SELECT count(DISTINCT claimed_by) FROM notif.scheduled_notification", Long.class);
        return count == null ? 0L : count;
    }

    private String stateOf(UUID id) {
        return jdbc.queryForObject(
                "SELECT state FROM notif.scheduled_notification WHERE id = ?", String.class, id);
    }

    private int claimCountOf(UUID id) {
        Integer count = jdbc.queryForObject(
                "SELECT claim_count FROM notif.scheduled_notification WHERE id = ?", Integer.class, id);
        return count == null ? 0 : count;
    }
}
