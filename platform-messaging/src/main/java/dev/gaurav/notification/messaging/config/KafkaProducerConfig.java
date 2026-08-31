package dev.gaurav.notification.messaging.config;

import dev.gaurav.notification.messaging.event.NotificationEvent;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * The producer. Every setting here exists to stop a specific way messages get lost or duplicated.
 *
 * <p>Spring Boot's defaults are not these. {@code acks=1} loses acknowledged records when the
 * leader dies before the followers catch up, and that loss is silent — the producer already
 * returned success.
 *
 * <table border="1">
 *   <caption>Producer settings and the failure each prevents</caption>
 *   <tr><th>Setting</th><th>Failure it prevents</th></tr>
 *   <tr><td>{@code acks=all}</td>
 *       <td>Acknowledged-then-lost on leader failover. With {@code minISR=2} this survives one
 *           broker loss; {@code sms.tx} runs {@code minISR=3} because an OTP is worth more than
 *           the availability it costs</td></tr>
 *   <tr><td>{@code enable.idempotence=true}</td>
 *       <td>Producer-side retries writing the record twice. The broker de-duplicates on the
 *           producer's sequence number, so a retry after a lost ack is a no-op</td></tr>
 *   <tr><td>{@code max.in.flight=5}</td>
 *       <td>Nothing on its own — it is safe <em>because</em> idempotence is on. The idempotent
 *           producer re-sequences on retry, so five in-flight batches cannot reorder. Turn
 *           idempotence off and this same value silently breaks per-key ordering</td></tr>
 *   <tr><td>{@code retries=MAX_VALUE} + {@code delivery.timeout.ms=120000}</td>
 *       <td>The retry count being the thing that gives up. The wall-clock deadline is the real
 *           bound; a numeric retry limit expires unpredictably depending on how fast each attempt
 *           failed</td></tr>
 *   <tr><td>{@code compression.type=zstd}</td>
 *       <td>Paying for bandwidth on highly repetitive JSON. Compression happens once on the
 *           producer and the broker stores the compressed batch as-is</td></tr>
 *   <tr><td>{@code linger.ms=25} + {@code batch.size=262144}</td>
 *       <td>One-record batches. 25 ms is well inside the 5-second CRITICAL dispatch objective and
 *           buys an order of magnitude in throughput</td></tr>
 * </table>
 *
 * <p><strong>No custom partitioner.</strong> murmur2 stays, so key → partition is reproducible
 * from outside the JVM during an incident, and a future partition increase behaves the way the
 * repartitioning runbook assumes. Custom partitioners are rejected in code review.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaProducerConfig {

    /** Bean name, so a deployable can inject this mapper without pulling in the web one. */
    public static final String EVENT_JSON_MAPPER = "notificationEventJsonMapper";

    private final String bootstrapServers;

    public KafkaProducerConfig(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * The mapper used for the wire format, kept separate from the HTTP one.
     *
     * <p>{@code FAIL_ON_UNKNOWN_PROPERTIES} is off because producers and consumers deploy
     * independently: adding a field to an event would otherwise break every consumer that has not
     * been redeployed yet, turning an additive change into a coordinated release.
     *
     * <p>{@code WRITE_DATES_AS_TIMESTAMPS} is off so instants are ISO-8601 on the wire. Epoch
     * numbers are unreadable in a DLQ dump, and their precision has changed between Jackson
     * versions before.
     */
    @Bean(EVENT_JSON_MAPPER)
    public JsonMapper notificationEventJsonMapper() {
        return JsonMapper.builder()
                .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
    }

    @Bean
    public ProducerFactory<String, NotificationEvent> notificationProducerFactory(
            @Qualifier(EVENT_JSON_MAPPER) JsonMapper mapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 25);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 262_144);

        // Broker client quotas throttle by DELAY, not by error: the broker holds the response and
        // mutes the channel, so a throttled producer sees latency. Without enough buffer and a
        // matching block timeout, that latency surfaces as BufferExhaustedException instead of
        // graceful degradation.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64L * 1024 * 1024);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);

        var valueSerializer = new JacksonJsonSerializer<NotificationEvent>(mapper)
                // The subtype discriminator is in the payload as `eventType`. Spring's header
                // type id would put a Java FQCN on the wire, coupling the format to our package
                // layout and breaking every consumer the day a class moves.
                .noTypeInfo();
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), valueSerializer);
    }

    @Bean
    public KafkaTemplate<String, NotificationEvent> notificationKafkaTemplate(
            ProducerFactory<String, NotificationEvent> factory) {
        return new KafkaTemplate<>(factory);
    }
}
