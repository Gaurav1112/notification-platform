package dev.gaurav.notification.messaging;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.messaging.config.KafkaConsumerConfig;
import dev.gaurav.notification.messaging.config.KafkaErrorHandlingConfig;
import dev.gaurav.notification.messaging.config.KafkaProducerConfig;
import dev.gaurav.notification.messaging.event.NotificationDispatchEvent;
import dev.gaurav.notification.messaging.event.NotificationEvent;
import dev.gaurav.notification.messaging.event.TestEvents;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.messaging.topic.Topics;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The parts unit tests cannot reach: the real serializers, the real broker, and the poison-message
 * path where "the offset was committed" is the only thing that matters.
 */
@SpringJUnitConfig(KafkaPipelineIntegrationTest.TestConfig.class)
@EmbeddedKafka(
        partitions = 1,
        topics = {
                Topics.DISPATCH_SMS_TX,
                Topics.DISPATCH_PUSH_BULK,
                Topics.DLQ
        },
        brokerProperties = {"auto.create.topics.enable=false"})
class KafkaPipelineIntegrationTest {

    static final String POISON_GROUP = "poison-listener";

    @Autowired
    NotificationEventPublisher publisher;

    @Autowired
    ConsumerFactory<String, NotificationEvent> consumerFactory;

    @Autowired
    PoisonListener listener;

    @Test
    @DisplayName("a dispatch event survives the real producer and consumer with its subtype, its "
            + "partition key and its eventId header intact")
    void aDispatchEventSurvivesTheRealWire() {
        NotificationDispatchEvent sent =
                TestEvents.dispatch(TrafficClass.BULK, Channel.PUSH);

        publisher.publishDispatch(sent);

        ConsumerRecord<String, NotificationEvent> received =
                consumeOne(Topics.DISPATCH_PUSH_BULK, "wire-assertions");

        assertThat(received.key())
                .isEqualTo(sent.tenantId() + "|" + sent.recipientId() + "|PUSH");
        assertThat(received.value()).isInstanceOf(NotificationDispatchEvent.class);
        assertThat(received.value().eventId()).isEqualTo(sent.eventId());
        assertThat(header(received, NotificationEventPublisher.HEADER_EVENT_ID))
                .isEqualTo(sent.eventId().toString());
        assertThat(((NotificationDispatchEvent) received.value()).addressCipher())
                .isEqualTo(sent.addressCipher());
    }

    @Test
    @DisplayName("a poison message is retried three times, dead-lettered and its offset committed "
            + "— without the commit the partition delivers zero records forever while the "
            + "consumer heartbeats and every dashboard stays green")
    void aPoisonMessageDoesNotBlockThePartition() throws Exception {
        var poison = TestEvents.dispatch(TrafficClass.CRITICAL, Channel.SMS);
        var good = TestEvents.dispatch(TrafficClass.CRITICAL, Channel.SMS);
        listener.poisonEventId = poison.eventId().toString();
        listener.survivorLatch = new CountDownLatch(1);

        publisher.publishDispatch(poison);
        publisher.publishDispatch(good);

        // The record behind the poison is the proof: it can only arrive if the poison's offset
        // was committed rather than seeked back to.
        assertThat(listener.survivorLatch.await(30, TimeUnit.SECONDS))
                .as("the record behind the poison message never arrived — the partition is blocked")
                .isTrue();

        ConsumerRecord<String, NotificationEvent> deadLettered = consumeOne(Topics.DLQ, "dlq-reader");
        assertThat(deadLettered.value().eventId()).isEqualTo(poison.eventId());
        assertThat(header(deadLettered, "kafka_dlt-original-topic"))
                .isEqualTo(Topics.DISPATCH_SMS_TX);
        assertThat(header(deadLettered, KafkaErrorHandlingConfig.HEADER_REPLAY_ATTEMPTS))
                .isEqualTo("0");
        assertThat(listener.attempts)
                .as("three in-place attempts, then the dead-letter channel")
                .hasSize((int) KafkaErrorHandlingConfig.IN_PLACE_RETRIES + 1);
    }

    private ConsumerRecord<String, NotificationEvent> consumeOne(String topic, String group) {
        var overrides = new Properties();
        overrides.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, NotificationEvent> consumer =
                     consumerFactory.createConsumer(group, null, null, overrides)) {
            consumer.subscribe(List.of(topic));
            return KafkaTestUtils.getSingleRecord(consumer, topic, Duration.ofSeconds(30));
        }
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    @EnableKafka
    @Configuration(proxyBeanMethods = false)
    @Import({KafkaProducerConfig.class, KafkaConsumerConfig.class,
             KafkaErrorHandlingConfig.class, NotificationEventPublisher.class})
    static class TestConfig {
        @Bean
        PoisonListener poisonListener() {
            return new PoisonListener();
        }
    }

    /** Throws for one nominated event id and acknowledges everything else. */
    static class PoisonListener {

        final List<String> attempts = new CopyOnWriteArrayList<>();
        volatile String poisonEventId = "";
        volatile CountDownLatch survivorLatch = new CountDownLatch(1);

        @KafkaListener(topics = Topics.DISPATCH_SMS_TX, groupId = POISON_GROUP)
        void onDispatch(NotificationEvent event, Acknowledgment ack) {
            if (event.eventId().toString().equals(poisonEventId)) {
                attempts.add(event.eventId().toString());
                throw new IllegalStateException("template variable 'code' is missing");
            }
            ack.acknowledge();
            survivorLatch.countDown();
        }
    }
}
