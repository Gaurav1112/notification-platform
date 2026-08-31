package dev.gaurav.notification.scheduler.outbox;

import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import dev.gaurav.notification.messaging.config.KafkaProducerConfig;
import dev.gaurav.notification.messaging.event.NotificationEvent;
import dev.gaurav.notification.persistence.entity.OutboxMessage;
import dev.gaurav.notification.persistence.repository.OutboxRepository;
import dev.gaurav.notification.scheduler.config.SchedulerProperties;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Drains {@code notif.outbox_message} to Kafka.
 *
 * <p><strong>This is the safety net, not the delivery path.</strong> The API publishes on the fast
 * path immediately after its transaction commits, so in a healthy system this sweeper finds an
 * empty table almost every tick. It exists for the cases the fast path cannot cover: the JVM dies
 * between commit and publish, the broker is unreachable for ninety seconds, a producer buffer
 * fills. Without it those messages are accepted-then-lost, which is the one failure mode a
 * notification platform cannot have — the caller already got a 202.
 *
 * <p><strong>A double publish is harmless and expected.</strong> The fast path and this sweeper
 * will sometimes both send the same record; that is the price of not making the API wait for an
 * ack. Every consumer is an idempotent receiver keyed on {@code eventId}, and every status write
 * goes through the monotonic guard, so the second copy is dropped. Designing to avoid the
 * duplicate — a publish inside the database transaction, a distributed transaction, a lock — costs
 * far more than tolerating it.
 *
 * <p><strong>Published rows are DELETEd, never UPDATEd.</strong> Stamping {@code published_at} and
 * leaving the row behind makes the table and its index grow without bound: at 100M messages a day
 * the outbox becomes one of the largest tables in the database, holding data whose only purpose
 * was to be forwarded, and the partial index that keeps the claim query constant-time degrades
 * along with it. {@code published_at} exists for crash forensics — a publish that succeeded and a
 * delete that did not — never as the normal terminal state.
 *
 * <p><strong>The relay does not re-derive topic or key.</strong> Both were decided and stored when
 * the row was written. Recomputing them here would let the fast path and the sweeper disagree
 * after any refactor, and a disagreement means the same event on two topics — invisible until a
 * consumer group reports zero lag on a topic nobody produces to any more.
 */
@Component
public class OutboxSweeper {

    /** The oldest unpublished row. Reads one row through {@code outbox_unpublished_ix}. */
    private static final String OLDEST_UNPUBLISHED_SQL = """
            SELECT created_at
              FROM notif.outbox_message
             WHERE published_at IS NULL
             ORDER BY id
             LIMIT 1
            """;

    private static final Logger log = LoggerFactory.getLogger(OutboxSweeper.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, NotificationEvent> kafka;
    private final JsonMapper mapper;
    private final JdbcTemplate jdbc;
    private final SchedulerProperties.Outbox settings;
    private final Duration publishTimeout;
    private final Clock clock;
    private final MeterRegistry meters;

    /**
     * Age of the oldest unpublished row, in seconds. Held in a field rather than computed by the
     * gauge callback so the query runs on the sweep thread — a Micrometer gauge that hits the
     * database runs on the Prometheus scrape thread, and a scrape storm then becomes database
     * load on a system already in trouble.
     */
    private final AtomicLong oldestUnpublishedAgeSeconds = new AtomicLong();

    public OutboxSweeper(OutboxRepository outbox,
                         KafkaTemplate<String, NotificationEvent> kafka,
                         @Qualifier(KafkaProducerConfig.EVENT_JSON_MAPPER) JsonMapper mapper,
                         JdbcTemplate jdbc,
                         SchedulerProperties properties,
                         Clock clock,
                         MeterRegistry meters) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.mapper = mapper;
        this.jdbc = jdbc;
        this.settings = properties.outbox();
        this.publishTimeout = properties.dispatch().publishTimeout();
        this.clock = clock;
        this.meters = meters;

        Gauge.builder("scheduler.outbox.oldest.age", oldestUnpublishedAgeSeconds, AtomicLong::get)
                .description("Age of the oldest unpublished outbox row. Every second of this is a "
                        + "notification the caller believes was accepted and that has not left the JVM.")
                .baseUnit("seconds")
                .register(meters);
    }

    /**
     * One drain pass.
     *
     * <p>{@code fixedDelay}, not {@code fixedRate}. A fixed rate queues missed executions when a
     * pass runs long, so a slow broker produces a burst of concurrent sweeps against a database
     * that is already the reason the pass was slow.
     *
     * <p>The whole pass is one transaction because {@code claimUnpublished} takes row locks that
     * only exist until commit. Claim in one transaction and delete in another and the gap between
     * them is a window where a second relay claims and republishes the same batch — the reason
     * {@link OutboxRepository} declares {@code Propagation.MANDATORY}.
     *
     * <p>This does mean the Kafka produce happens with row locks held. That is a deliberate,
     * bounded cost: the batch is small, {@code linger.ms} is 25 ms, and the publish is capped by
     * {@link SchedulerProperties.Dispatch#publishTimeout()}. The alternative — commit the delete
     * before the ack — trades a lock for message loss.
     */
    @Scheduled(fixedDelayString = "${notification.scheduler.outbox.sweep-interval:100ms}")
    @Transactional
    public void sweep() {
        recordOldestUnpublishedAge();

        var batch = outbox.claimUnpublished(settings.batchSize());
        if (batch.isEmpty()) {
            return;
        }

        var publishedIds = new ArrayList<Long>(batch.size());
        var inFlight = new ArrayList<CompletableFuture<SendResult<String, NotificationEvent>>>(batch.size());
        var inFlightIds = new ArrayList<Long>(batch.size());

        for (var row : batch) {
            NotificationEvent event;
            try {
                event = mapper.readValue(row.getPayload(), NotificationEvent.class);
            } catch (JacksonException e) {
                // An unparseable payload cannot be published and will never become parseable.
                // Leaving it claims the head of the queue forever and every message behind it
                // stops — the outbox equivalent of a poison pill. Drop it loudly instead.
                log.error("outbox row {} has an unparseable payload; deleting it so the relay can "
                        + "make progress. topic={} aggregate={}/{}",
                        row.getId(), row.getTopic(), row.getAggregateType(), row.getAggregateId(), e);
                meters.counter("scheduler.outbox.unparseable").increment();
                publishedIds.add(row.getId());
                continue;
            }
            inFlight.add(kafka.send(toRecord(row, event)));
            inFlightIds.add(row.getId());
        }

        for (int i = 0; i < inFlight.size(); i++) {
            try {
                inFlight.get(i).get(publishTimeout.toMillis(), TimeUnit.MILLISECONDS);
                publishedIds.add(inFlightIds.get(i));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Not published: leave the row. The lock is released at commit and the next pass
                // — or another replica — picks it up. Never delete on a failed ack.
                log.warn("outbox row {} failed to publish; leaving it for the next pass",
                        inFlightIds.get(i), e);
                meters.counter("scheduler.outbox.publish.failed").increment();
            }
        }

        if (!publishedIds.isEmpty()) {
            outbox.deleteByIdIn(publishedIds);
            meters.counter("scheduler.outbox.published").increment(publishedIds.size());
        }
    }

    /** For the {@code outbox_age > 120s} alert; also the value the gauge exposes. */
    public long oldestUnpublishedAgeSeconds() {
        return oldestUnpublishedAgeSeconds.get();
    }

    private ProducerRecord<String, NotificationEvent> toRecord(OutboxMessage row, NotificationEvent event) {
        return new ProducerRecord<>(row.getTopic(), row.getPartitionKey(), event);
    }

    /**
     * {@code OffsetDateTime}, not {@code Instant}: PgJDBC's {@code getObject(int, Class)} has no
     * mapping for {@code java.time.Instant} and throws rather than converting, so asking for one
     * turns the age metric into an exception on every sweep.
     */
    private void recordOldestUnpublishedAge() {
        List<OffsetDateTime> oldest = jdbc.queryForList(OLDEST_UNPUBLISHED_SQL, OffsetDateTime.class);
        if (oldest.isEmpty()) {
            oldestUnpublishedAgeSeconds.set(0L);
            return;
        }
        var age = Duration.between(oldest.get(0).toInstant(), clock.instant());
        oldestUnpublishedAgeSeconds.set(Math.max(0L, age.toSeconds()));
    }
}
