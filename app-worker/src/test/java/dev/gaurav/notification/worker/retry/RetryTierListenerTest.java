package dev.gaurav.notification.worker.retry;

import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.messaging.event.NotificationEvent;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.messaging.producer.PublishNotConfirmedException;
import dev.gaurav.notification.messaging.topic.Topics;
import dev.gaurav.notification.persistence.entity.NotificationRecipient;
import dev.gaurav.notification.persistence.repository.NotificationRecipientRepository;
import dev.gaurav.notification.worker.TestDispatchEvents;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The tier's republish <em>is</em> the retry, so an unacknowledged one that gets its offset
 * committed anyway ends the retry chain in silence: the message is not re-parked, not dead
 * lettered and not counted anywhere.
 */
class RetryTierListenerTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, NotificationEvent> template = mock(KafkaTemplate.class);
    private final NotificationRecipientRepository recipients = mock(NotificationRecipientRepository.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);
    private final Consumer<?, ?> consumer = mock(Consumer.class);

    private RetryTierListener listener;

    @BeforeEach
    void setUp() {
        listener = new RetryTierListener(mock(KafkaListenerEndpointRegistry.class), recipients,
                new NotificationEventPublisher(template),
                Clock.fixed(TestDispatchEvents.NOW, ZoneOffset.UTC), new SimpleMeterRegistry());

        var recipient = mock(NotificationRecipient.class);
        when(recipient.getStatus()).thenReturn(DeliveryStatus.SEND_FAILED);
        when(recipients.findInWindow(any(), anyLong(), any(), any())).thenReturn(Optional.of(recipient));
    }

    @Test
    @DisplayName("a republish the broker never acknowledged does not commit the tier offset — "
            + "committing it would end the retry chain with no DLQ record and no alert")
    void anUnacknowledgedRepublishLeavesTheParkedMessageOnTheTier() {
        when(template.send(any(ProducerRecord.class))).thenReturn(
                CompletableFuture.failedFuture(new NotLeaderOrFollowerException("leader moved")));

        assertThatThrownBy(() -> listener.onParked(TestDispatchEvents.parked(),
                Topics.RETRY_5S, 0, 17L, consumer, ack))
                .isInstanceOf(PublishNotConfirmedException.class);

        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("a confirmed republish is acknowledged after the send, not before, so the tier "
            + "hands the message on exactly once it is safely on the dispatch lane")
    void aConfirmedRepublishIsAcknowledgedAfterTheBrokerAnswers() {
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        listener.onParked(TestDispatchEvents.parked(), Topics.RETRY_5S, 0, 17L, consumer, ack);

        var order = inOrder(template, ack);
        order.verify(template).send(any(ProducerRecord.class));
        order.verify(ack).acknowledge();
    }

    @Test
    @DisplayName("a message cancelled while it was parked is dropped and acknowledged without a "
            + "publish, so the broker is never on the path of work nobody wants")
    void aCancelledMessageIsStillAcknowledgedWithoutPublishing() {
        var cancelled = mock(NotificationRecipient.class);
        when(cancelled.getStatus()).thenReturn(DeliveryStatus.CANCELLED);
        when(recipients.findInWindow(any(), anyLong(), any(), any())).thenReturn(Optional.of(cancelled));

        listener.onParked(TestDispatchEvents.parked(), Topics.RETRY_5S, 0, 17L, consumer, ack);

        verify(template, never()).send(any(ProducerRecord.class));
        verify(ack).acknowledge();
    }
}
