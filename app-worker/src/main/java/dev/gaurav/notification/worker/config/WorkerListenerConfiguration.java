package dev.gaurav.notification.worker.config;

import dev.gaurav.notification.messaging.event.NotificationEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * Three listener container factories, because the three kinds of work have nothing in common.
 *
 * <p>A single container-wide concurrency would give the SMS transactional lane (12 partitions, a
 * 5-second dispatch objective) the same thread count as bulk push (54 partitions, 15 minutes), and
 * would give both of them to the retry tiers, which do no I/O at all. One of the three is always
 * starved and another is always wasting threads.
 *
 * <p>What every factory shares, and why:
 *
 * <ul>
 *   <li><strong>{@code MANUAL_IMMEDIATE}.</strong> The listener acknowledges after its database
 *       commit, and the offset goes to the broker at that moment rather than batched to the end of
 *       the poll. Batched acknowledgement lost to a pod eviction redelivers the whole poll — 100
 *       duplicate provider calls instead of one.</li>
 *   <li><strong>The shared error handler.</strong> Three in-place attempts, then the DLQ, then
 *       <em>commit the offset</em>. A poison message must never block a partition: offsets commit
 *       only after successful processing, so a consumer that keeps throwing seeks back to the same
 *       offset forever while heartbeating, staying in the group and reporting healthy.</li>
 *   <li><strong>Observation enabled.</strong> Needed to tell "the consumer is slow" from "the
 *       consumer is idle", which are the two hypotheses at 3 a.m. and look identical without it.</li>
 * </ul>
 *
 * <p><strong>{@code @EnableKafka} is declared here and it is not decoration.</strong>
 * {@code platform-messaging} depends on {@code spring-kafka} directly rather than on Boot's
 * {@code spring-boot-kafka} auto-configuration module, so nothing else in this application
 * registers {@code KafkaListenerAnnotationBeanPostProcessor} or the
 * {@code KafkaListenerEndpointRegistry}. Without it the factories below are built, every
 * {@code @KafkaListener} in this module is quietly ignored, and {@code ProviderHealthGate} — which
 * pauses and resumes containers through that registry — cannot be constructed at all. This module
 * is the only one that consumes, which is why the annotation belongs on this class rather than on
 * the shared messaging configuration.
 */
@Configuration(proxyBeanMethods = false)
@EnableKafka
public class WorkerListenerConfiguration {

    /** Transactional lanes and the control topics: the latency-sensitive work. */
    public static final String TRANSACTIONAL_FACTORY = "txListenerContainerFactory";

    /** Campaign lanes: throughput-sensitive, latency-tolerant, and shed first under pressure. */
    public static final String BULK_FACTORY = "bulkListenerContainerFactory";

    /** The delay tiers: no external I/O, so a handful of threads is plenty. */
    public static final String RETRY_TIER_FACTORY = "retryTierListenerContainerFactory";

    private final ConsumerFactory<String, NotificationEvent> consumerFactory;
    private final CommonErrorHandler errorHandler;
    private final int txConcurrency;
    private final int bulkConcurrency;
    private final int retryConcurrency;
    private final boolean autoStartup;

    /**
     * @param autoStartup honours {@code spring.kafka.listener.auto-startup}, which Boot's own
     *                    factory reads and these hand-built ones otherwise would not. Without it
     *                    the property is documented and inert, and every context that loads this
     *                    module opens consumer connections — including a test that only wants to
     *                    know whether the bean graph is complete
     */
    public WorkerListenerConfiguration(
            ConsumerFactory<String, NotificationEvent> consumerFactory,
            CommonErrorHandler notificationErrorHandler,
            @Value("${notification.worker.concurrency.tx:6}") int txConcurrency,
            @Value("${notification.worker.concurrency.bulk:3}") int bulkConcurrency,
            @Value("${notification.worker.concurrency.retry:2}") int retryConcurrency,
            @Value("${spring.kafka.listener.auto-startup:true}") boolean autoStartup) {
        this.consumerFactory = consumerFactory;
        this.errorHandler = notificationErrorHandler;
        this.txConcurrency = txConcurrency;
        this.bulkConcurrency = bulkConcurrency;
        this.retryConcurrency = retryConcurrency;
        this.autoStartup = autoStartup;
    }

    @Bean(TRANSACTIONAL_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> txListenerContainerFactory() {
        return factory(txConcurrency, false);
    }

    @Bean(BULK_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> bulkListenerContainerFactory() {
        return factory(bulkConcurrency, false);
    }

    /**
     * The tier factory, with one setting the other two must not have.
     *
     * <p>{@code setIdlePartitionEventInterval} is what makes a <em>paused</em> partition observable.
     * A tier consumer parks a not-yet-due message by pausing its partition, and a partition that is
     * paused looks exactly like a partition with no traffic: no lag alarm, no error, no log line.
     * Emitting idle events gives the difference a name.
     */
    @Bean(RETRY_TIER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> retryTierListenerContainerFactory() {
        return factory(retryConcurrency, true);
    }

    private ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> factory(
            int concurrency, boolean reportIdlePartitions) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, NotificationEvent>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.setConcurrency(concurrency);
        factory.setAutoStartup(autoStartup);
        ContainerProperties properties = factory.getContainerProperties();
        properties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        properties.setMicrometerEnabled(true);
        properties.setObservationEnabled(true);
        if (reportIdlePartitions) {
            properties.setIdlePartitionEventInterval(30_000L);
        }
        return factory;
    }
}
