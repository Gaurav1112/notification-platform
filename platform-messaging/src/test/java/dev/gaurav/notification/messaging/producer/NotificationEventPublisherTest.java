package dev.gaurav.notification.messaging.producer;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.messaging.event.NotificationEvent;
import dev.gaurav.notification.messaging.event.TestEvents;
import dev.gaurav.notification.messaging.topic.Topics;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NotificationEventPublisherTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, NotificationEvent> template = mock(KafkaTemplate.class);

    private NotificationEventPublisher publisher;

    @BeforeEach
    void setUp() {
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        mock(SendResult.class)));
        publisher = new NotificationEventPublisher(template);
    }

    @Test
    @DisplayName("an OTP is published to the tx lane and a campaign to the bulk lane — the lane "
            + "comes from the traffic class, so a caller cannot queue an OTP behind 10M records")
    void theTrafficClassChoosesTheLaneNotTheCaller() {
        publisher.publishDispatch(TestEvents.dispatch(TrafficClass.CRITICAL, Channel.SMS));
        assertThat(captured().topic()).isEqualTo(Topics.DISPATCH_SMS_TX);

        publisher.publishDispatch(TestEvents.dispatch(TrafficClass.BULK, Channel.PUSH));
        assertThat(captured().topic()).isEqualTo(Topics.DISPATCH_PUSH_BULK);
    }

    @Test
    @DisplayName("the dispatch record is keyed tenantId|recipientId|CHANNEL — the key that keeps "
            + "50M recipients spread across partitions instead of piled on one")
    void theDispatchRecordCarriesTheOrderingKey() {
        var event = TestEvents.dispatch();

        publisher.publishDispatch(event);

        assertThat(captured().key())
                .isEqualTo(event.tenantId() + "|" + event.recipientId() + "|SMS");
    }

    @Test
    @DisplayName("delivery and status records are keyed on notificationId alone, so the worker "
            + "write, the provider webhook and the reconciler all land on the same partition")
    void statusObservationsShareAPartitionPerNotification() {
        var event = TestEvents.status(DeliveryStatus.SENT, 60);

        publisher.publishDelivery(event);
        var delivery = captured();
        publisher.publishStatusProjection(event);
        var status = captured();

        assertThat(delivery.topic()).isEqualTo(Topics.DELIVERY);
        assertThat(status.topic()).isEqualTo(Topics.STATUS);
        assertThat(delivery.key()).isEqualTo(event.notificationId().toString());
        assertThat(status.key()).isEqualTo(delivery.key());
    }

    @Test
    @DisplayName("a retry keeps the original dispatch key across the tier hop — re-keying would "
            + "spray one recipient's retries over five partitions")
    void aRetryKeepsTheOriginalDispatchKey() {
        var retry = TestEvents.retry();

        publisher.publishDispatch(retry.dispatch());
        String dispatchKey = captured().key();
        publisher.publishRetry(retry);
        var record = captured();

        assertThat(record.topic()).isEqualTo(Topics.RETRY_5S);
        assertThat(record.key()).isEqualTo(dispatchKey);
    }

    @Test
    @DisplayName("the eventId is mirrored into a header, so the idempotent receiver can drop a "
            + "redelivery without deserialising 100 records per poll")
    void theEventIdIsReadableWithoutParsingThePayload() {
        var event = TestEvents.dispatch();

        publisher.publishDispatch(event);
        var record = captured();

        assertThat(headerValue(record, NotificationEventPublisher.HEADER_EVENT_ID))
                .isEqualTo(event.eventId().toString());
        assertThat(headerValue(record, NotificationEventPublisher.HEADER_EVENT_TYPE))
                .isEqualTo(NotificationEvent.TYPE_DISPATCH);
        assertThat(headerValue(record, NotificationEventPublisher.HEADER_TENANT_ID))
                .isEqualTo(Long.toString(event.tenantId()));
    }

    @Test
    @DisplayName("a dead letter with no parseable notificationId still gets a key — a null key "
            + "would fall back to sticky partitioning and pile a poison storm on one partition")
    void aDeadLetterIsNeverProducedWithANullKey() {
        var event = TestEvents.deadLetter();
        var anonymous = new dev.gaurav.notification.messaging.event.DeadLetterEvent(
                event.eventId(), event.occurredAt(), NotificationEvent.UNKNOWN_TENANT, null,
                event.sourceTopic(), event.sourcePartition(), event.sourceOffset(),
                event.sourceKey(), event.consumerGroup(), null, null, null,
                event.exceptionClass(), event.exceptionMessage(), event.stackTrace(),
                event.attemptCount(), event.replayAttempts(), event.payload());

        publisher.publishDeadLetter(anonymous);
        var record = captured();

        assertThat(record.topic()).isEqualTo(Topics.DLQ);
        assertThat(record.key()).isNotNull().contains(Long.toString(event.sourceOffset()));
    }

    @Test
    @DisplayName("a traceparent absent on an internally-generated event does not produce a "
            + "header with the literal string \"null\"")
    void anAbsentTraceparentIsOmittedRatherThanStringified() {
        var event = TestEvents.dispatchWithoutTrace();

        publisher.publishDispatch(event);

        assertThat(captured().headers()
                .lastHeader(NotificationEventPublisher.HEADER_TRACEPARENT)).isNull();
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, NotificationEvent> captured() {
        var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template, org.mockito.Mockito.atLeastOnce()).send(captor.capture());
        return captor.getValue();
    }

    private static String headerValue(ProducerRecord<String, NotificationEvent> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
