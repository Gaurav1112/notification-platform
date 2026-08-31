package dev.gaurav.notification.worker.orchestrator;

import dev.gaurav.notification.messaging.consumer.IdempotentConsumer;
import dev.gaurav.notification.messaging.event.NotificationEvent;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.messaging.producer.PublishNotConfirmedException;
import dev.gaurav.notification.worker.TestDispatchEvents;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The expander's one job under failure: never commit an offset for a campaign whose dispatch
 * events did not reach the broker.
 *
 * <p>{@code kafkaTemplate.send} does not throw when Kafka is unreachable — it completes the future
 * it returned with the exception, and it does so after the listener has already returned. That is
 * why these tests drive a real {@link NotificationEventPublisher} over a fake {@link KafkaTemplate}
 * instead of mocking the publisher: the bug being guarded against lives entirely in the difference
 * between "send returned" and "the broker acknowledged".
 */
class RequestedEventListenerTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, NotificationEvent> template = mock(KafkaTemplate.class);
    private final IdempotentConsumer idempotentConsumer = mock(IdempotentConsumer.class);
    private final RequestFanOut fanOut = mock(RequestFanOut.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);

    private RequestedEventListener listener;

    @BeforeEach
    void setUp() {
        listener = new RequestedEventListener(idempotentConsumer, fanOut,
                new NotificationEventPublisher(template), new SimpleMeterRegistry());
        when(idempotentConsumer.isFirstSighting(anyString(), any())).thenReturn(true);
        when(fanOut.expand(any())).thenReturn(new RequestFanOut.FanOutResult(
                List.of(TestDispatchEvents.dispatch(), TestDispatchEvents.dispatch()), 0, 0));
    }

    @Test
    @DisplayName("a broker outage during fan-out leaves the offset uncommitted: acknowledging a "
            + "campaign whose dispatch events never left the JVM strands every recipient in QUEUED")
    void anUnacknowledgedFanOutIsNeverAcknowledgedToKafka() {
        when(template.send(any(ProducerRecord.class))).thenReturn(
                CompletableFuture.failedFuture(new TimeoutException("no leader for partition")));

        assertThatThrownBy(() -> listener.onRequested(TestDispatchEvents.requested(), ack))
                .isInstanceOf(PublishNotConfirmedException.class);

        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("a failed publish also drops the dedup key, or our own idempotent receiver "
            + "suppresses the redelivery Kafka is about to perform and the request is lost for 7 days")
    void aFailedPublishForgetsTheDedupKeySoTheRedeliveryIsNotSuppressed() {
        when(template.send(any(ProducerRecord.class))).thenReturn(
                CompletableFuture.failedFuture(new TimeoutException("no leader for partition")));
        var event = TestDispatchEvents.requested();

        assertThatThrownBy(() -> listener.onRequested(event, ack))
                .isInstanceOf(PublishNotConfirmedException.class);

        verify(idempotentConsumer).forget(RequestedEventListener.CONSUMER_GROUP, event.eventId());
    }

    @Test
    @DisplayName("with the broker healthy every dispatch is published and only then is the offset "
            + "committed — the ack is the last thing that happens, not the first")
    void theOffsetIsCommittedOnlyAfterTheBrokerAcknowledgedEveryDispatch() {
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        listener.onRequested(TestDispatchEvents.requested(), ack);

        var order = inOrder(template, ack);
        order.verify(template, times(2)).send(any(ProducerRecord.class));
        order.verify(ack).acknowledge();
    }
}
