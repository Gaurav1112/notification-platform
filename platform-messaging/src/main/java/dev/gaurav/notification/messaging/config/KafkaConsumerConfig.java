package dev.gaurav.notification.messaging.config;

import dev.gaurav.notification.messaging.consumer.DeduplicationStore;
import dev.gaurav.notification.messaging.consumer.InMemoryDeduplicationStore;
import dev.gaurav.notification.messaging.event.NotificationEvent;

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * The consumer side. Three settings here are the difference between a platform that degrades and
 * one that collapses silently.
 *
 * <p><strong>{@code enable.auto.commit=false}.</strong> Auto-commit advances the offset on a timer
 * that knows nothing about whether the database write succeeded. A pod evicted between the commit
 * and the commit is a notification that Kafka believes was handled and PostgreSQL never saw — a
 * loss with no error anywhere. With manual commit the offset is committed <em>after</em> the DB
 * transaction, so the worst case is redelivery, which layer 2 deduplicates and the monotonic
 * status guard makes harmless. Never loss.
 *
 * <p><strong>{@code max.poll.records=100}, not the default 500.</strong> The dispatch consumer
 * makes one network call per record. At 500 records and a provider degraded to 600 ms per call,
 * one poll needs five minutes and blows through {@code max.poll.interval.ms}. Then: the consumer
 * is evicted, the group rebalances, the backlog grows, every consumer pulls a full batch against a
 * larger backlog, the hanging record is reassigned, repeat. Throughput reaches zero while the
 * Kafka cluster reports perfect health, which is why teams debug the broker for hours.
 * <strong>Raising {@code max.poll.interval.ms} is not the fix</strong> — it costs failover time,
 * and worst-case rebalance detection then takes up to twice the interval.
 *
 * <p><strong>{@code isolation.level=read_committed}.</strong> The expander produces inside a Kafka
 * transaction. Without this the dispatch consumers read aborted records and send messages for
 * requests that were rolled back.
 *
 * <p>KIP-62 moved heartbeats to a background thread, so a consumer stuck in a slow provider call
 * <em>still heartbeats and looks alive</em> while the poll-interval clock runs out. That is why
 * liveness monitoring cannot see a rebalance storm, and why the bound has to be on the provider
 * call itself.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaConsumerConfig {

    /**
     * KIP-848, GA in Kafka 4.0. Assignment moves to the broker-side coordinator, which removes the
     * group-wide synchronisation barrier — the thing that makes a rebalance stop every consumer in
     * the group rather than just the ones losing partitions.
     *
     * <p>Set to {@code classic} to fall back; the config below then installs
     * {@link CooperativeStickyAssignor}, which keeps unaffected partitions assigned through a
     * rebalance instead of revoking everything. The two are mutually exclusive: the new protocol
     * rejects {@code partition.assignment.strategy} outright.
     */
    public static final String DEFAULT_GROUP_PROTOCOL = "consumer";

    private final String bootstrapServers;
    private final String groupProtocol;

    public KafkaConsumerConfig(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${notification.kafka.consumer.group-protocol:" + DEFAULT_GROUP_PROTOCOL + "}")
            String groupProtocol) {
        this.bootstrapServers = bootstrapServers;
        this.groupProtocol = groupProtocol;
    }

    @Bean
    public ConsumerFactory<String, NotificationEvent> notificationConsumerFactory(
            @Qualifier(KafkaProducerConfig.EVENT_JSON_MAPPER) JsonMapper mapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        // A consumer that starts at "latest" on a brand-new group silently skips the backlog it
        // was deployed to drain.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
        if (!DEFAULT_GROUP_PROTOCOL.equalsIgnoreCase(groupProtocol)) {
            // Only legal on the classic protocol; the consumer protocol fails startup if it is set.
            props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                    CooperativeStickyAssignor.class.getName());
        }

        var delegate = new JacksonJsonDeserializer<NotificationEvent>(NotificationEvent.class, mapper);
        // Without this wrapper a single unparseable record throws inside the poll loop, the
        // container seeks back to it, and the partition stops forever with no consumer error rate
        // to alarm on. Wrapped, the failure becomes a record the error handler can dead-letter.
        var valueDeserializer =
                new ErrorHandlingDeserializer<NotificationEvent>(delegate);
        return new DefaultKafkaConsumerFactory<>(
                props, new StringDeserializer(), valueDeserializer);
    }

    /**
     * {@code MANUAL_IMMEDIATE}: the listener calls {@code ack.acknowledge()} itself, after its
     * database transaction has committed, and the offset is sent to the broker at that moment
     * rather than batched to the end of the poll.
     *
     * <p>Immediate rather than {@code MANUAL} because a batched acknowledgement lost to a pod
     * eviction redelivers the whole poll, not one record — 100 duplicate provider calls instead
     * of one.
     *
     * <p>Concurrency is 1 by default and set per listener. A container-wide value would give the
     * SMS lane (12 partitions) the same thread count as bulk push (54), which either starves one
     * or wastes threads on the other.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NotificationEvent>
            kafkaListenerContainerFactory(
                    ConsumerFactory<String, NotificationEvent> consumerFactory,
                    CommonErrorHandler notificationErrorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, NotificationEvent>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(notificationErrorHandler);
        ContainerProperties containerProperties = factory.getContainerProperties();
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Observability needs to distinguish "the consumer is slow" from "the consumer is idle".
        containerProperties.setMicrometerEnabled(true);
        containerProperties.setObservationEnabled(true);
        return factory;
    }

    /**
     * A single-JVM dedup store, registered only when nothing better is on the classpath.
     *
     * <p>The production implementation is Valkey-backed and lives in the deployable that has the
     * Redis client. Defaulting to something rather than failing startup is deliberate: a developer
     * running one worker locally should not need Valkey, and
     * {@code IdempotentConsumer.isFullyProtected()} reports the difference so the degraded mode is
     * visible rather than assumed.
     */
    @Bean
    @ConditionalOnMissingBean(DeduplicationStore.class)
    public DeduplicationStore inMemoryDeduplicationStore() {
        return new InMemoryDeduplicationStore();
    }
}
