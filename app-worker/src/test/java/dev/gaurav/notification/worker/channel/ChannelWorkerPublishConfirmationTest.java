package dev.gaurav.notification.worker.channel;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.messaging.consumer.IdempotentConsumer;
import dev.gaurav.notification.messaging.event.NotificationDispatchEvent;
import dev.gaurav.notification.messaging.event.NotificationEvent;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.messaging.producer.PublishNotConfirmedException;
import dev.gaurav.notification.messaging.topic.Topics;
import dev.gaurav.notification.persistence.entity.NotificationRecipient;
import dev.gaurav.notification.persistence.repository.NotificationRecipientRepository;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendResult;
import dev.gaurav.notification.resilience.retry.RetryTier;
import dev.gaurav.notification.worker.TestDispatchEvents;
import dev.gaurav.notification.worker.config.WorkerProperties;
import dev.gaurav.notification.worker.retry.RetryDecision;
import dev.gaurav.notification.worker.retry.RetryRouter;
import dev.gaurav.notification.worker.status.ApplyDeliveryStatusUseCase;
import dev.gaurav.notification.worker.status.SuppressionWriter;
import dev.gaurav.notification.worker.support.ProviderIds;
import dev.gaurav.notification.worker.support.RecipientAddressVault;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The channel worker acknowledges a record only after the broker has acknowledged every event that
 * record produced.
 *
 * <p>Each failure below is a different way the platform used to lose a message in silence, and the
 * silence is the point: {@code kafkaTemplate.send} never throws for a broker outage, so a worker
 * that dropped the future saw a clean run and committed the offset. The retry event, the failover
 * dispatch or the dead letter then existed nowhere, and the recipient had no owner — no lane, no
 * tier, no DLQ, no alert, and a status row that says the send is still in progress.
 *
 * <p>Each test fails exactly one topic, so it proves that <em>that</em> publish is inside the
 * confirmation and not merely that some publish is.
 */
class ChannelWorkerPublishConfirmationTest {

    private static final ProviderCode PRIMARY = ProviderCode.of("mock-sms-primary");
    private static final ProviderCode SECONDARY = ProviderCode.of("mock-sms-secondary");

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, NotificationEvent> template = mock(KafkaTemplate.class);
    private final IdempotentConsumer idempotentConsumer = mock(IdempotentConsumer.class);
    private final NotificationRecipientRepository recipients = mock(NotificationRecipientRepository.class);
    private final DeliveryAttemptRecorder attempts = mock(DeliveryAttemptRecorder.class);
    private final ChannelProviderRouter router = mock(ChannelProviderRouter.class);
    private final NotificationProvider provider = mock(NotificationProvider.class);
    private final RetryRouter retryRouter = mock(RetryRouter.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);
    private final ConsumerRecordMetadata metadata = mock(ConsumerRecordMetadata.class);

    private TestChannelWorker worker;

    @BeforeEach
    void setUp() {
        var providerIds = mock(ProviderIds.class);
        var vault = mock(RecipientAddressVault.class);
        var support = new ChannelWorkerSupport(idempotentConsumer, recipients, attempts, router,
                providerIds, vault, new NotificationEventPublisher(template),
                mock(ApplyDeliveryStatusUseCase.class), mock(SuppressionWriter.class), retryRouter,
                new WorkerProperties(0.10, 100, Duration.ofSeconds(8), 16, 64, true),
                Clock.fixed(TestDispatchEvents.NOW, ZoneOffset.UTC), new SimpleMeterRegistry());
        worker = new TestChannelWorker(support);

        when(idempotentConsumer.isFirstSighting(anyString(), any())).thenReturn(true);
        var recipient = mock(NotificationRecipient.class);
        when(recipient.getId()).thenReturn(TestDispatchEvents.RECIPIENT_ID);
        when(recipient.getStatus()).thenReturn(DeliveryStatus.QUEUED);
        when(recipients.findInWindow(any(), anyLong(), any(), any())).thenReturn(Optional.of(recipient));
        when(providerIds.idOf(any())).thenReturn((short) 1);
        when(vault.open(anyString(), anyString())).thenReturn("+447700900123");
        when(attempts.registerPending(any(), anyLong(), anyInt(), anyShort(), anyString()))
                .thenReturn(new DeliveryAttemptRecorder.AttemptRef(
                        1L, TestDispatchEvents.NOW, TestDispatchEvents.NOW));
        when(router.select(any(), anyLong(), any(), any()))
                .thenReturn(Optional.of(new ChannelProviderRouter.Selection(provider, PRIMARY, true)));
        when(provider.send(any()))
                .thenReturn(new SendResult.Accepted("SM123", Duration.ofMillis(250), 750L));
    }

    @Test
    @DisplayName("a delivery-status event the broker never acknowledged blocks the ack — the "
            + "ledger, the tenant webhook and the projection would otherwise never learn the outcome")
    void anUnconfirmedDeliveryEventStopsTheOffsetFromBeingCommitted() {
        brokerRejects(topic -> topic.equals(Topics.DELIVERY));

        assertThatThrownBy(() -> worker.run(TestDispatchEvents.dispatch(), metadata, ack))
                .isInstanceOf(PublishNotConfirmedException.class);

        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("a retry event the broker never acknowledged blocks the ack — committing it "
            + "would leave the recipient on no lane and no tier, retried by nothing, forever")
    void anUnconfirmedRetryEventStopsTheOffsetFromBeingCommitted() {
        providerRejectsWith(FailureType.TRANSIENT_NETWORK);
        when(retryRouter.decide(any())).thenReturn(new RetryDecision.RetrySameProvider(
                RetryTier.T5S, Duration.ofSeconds(5), FailureType.TRANSIENT_NETWORK));
        brokerRejects(topic -> topic.equals(Topics.RETRY_5S));

        assertThatThrownBy(() -> worker.run(TestDispatchEvents.dispatch(), metadata, ack))
                .isInstanceOf(PublishNotConfirmedException.class);

        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("a failover dispatch the broker never acknowledged blocks the ack — the primary "
            + "has already rejected the message, so losing the failover loses the last attempt")
    void anUnconfirmedFailoverDispatchStopsTheOffsetFromBeingCommitted() {
        providerRejectsWith(FailureType.AUTH_FAILURE);
        when(retryRouter.decide(any()))
                .thenReturn(new RetryDecision.FailoverNow(FailureType.AUTH_FAILURE, true));
        when(router.select(any(), anyLong(), any(), eqExcluding()))
                .thenReturn(Optional.of(new ChannelProviderRouter.Selection(provider, SECONDARY, false)));
        brokerRejects(topic -> topic.equals(Topics.DISPATCH_SMS_TX));

        assertThatThrownBy(() -> worker.run(TestDispatchEvents.dispatch(), metadata, ack))
                .isInstanceOf(PublishNotConfirmedException.class);

        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("a dead letter the broker never acknowledged blocks the ack — a DLQ record that "
            + "was never written is the one place nobody thinks to look for the message")
    void anUnconfirmedDeadLetterStopsTheOffsetFromBeingCommitted() {
        providerRejectsWith(FailureType.PROVIDER_5XX);
        when(retryRouter.decide(any())).thenReturn(
                new RetryDecision.DeadLetter(FailureType.PROVIDER_5XX, "retry budget exhausted"));
        when(metadata.topic()).thenReturn(Topics.DISPATCH_SMS_TX);
        when(metadata.partition()).thenReturn(3);
        when(metadata.offset()).thenReturn(918_273L);
        brokerRejects(topic -> topic.equals(Topics.DLQ));

        assertThatThrownBy(() -> worker.run(TestDispatchEvents.dispatch(), metadata, ack))
                .isInstanceOf(PublishNotConfirmedException.class);

        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("an unconfirmed publish also drops the attempt-scoped dedup key, so the "
            + "redelivery is reprocessed instead of being skipped as a duplicate and lost")
    void anUnconfirmedPublishForgetsTheDedupKey() {
        brokerRejects(topic -> true);
        var event = TestDispatchEvents.dispatch();

        assertThatThrownBy(() -> worker.run(event, metadata, ack))
                .isInstanceOf(PublishNotConfirmedException.class);

        verify(idempotentConsumer).forget(anyString(), any());
    }

    @Test
    @DisplayName("with the broker healthy the offset is committed, and only after every event of "
            + "the record has been acknowledged")
    void aHealthyRecordIsAcknowledgedAfterItsEventsAreConfirmed() {
        brokerRejects(topic -> false);

        worker.run(TestDispatchEvents.dispatch(), metadata, ack);

        var order = inOrder(template, ack);
        order.verify(template, atLeastOnce()).send(any(ProducerRecord.class));
        order.verify(ack).acknowledge();
    }

    /** Fails the produce for the topics the predicate names and acknowledges every other one. */
    @SuppressWarnings("unchecked")
    private void brokerRejects(Predicate<String> unreachableTopic) {
        when(template.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            ProducerRecord<String, NotificationEvent> record = invocation.getArgument(0);
            return unreachableTopic.test(record.topic())
                    ? CompletableFuture.failedFuture(new NotLeaderOrFollowerException(
                            "no leader for " + record.topic()))
                    : CompletableFuture.completedFuture(mock(org.springframework.kafka.support.SendResult.class));
        });
    }

    private void providerRejectsWith(FailureType type) {
        when(provider.send(any())).thenReturn(new SendResult.Rejected(
                type, "500", "upstream unavailable", Duration.ofMillis(250), Optional.empty()));
    }

    /** The failover lookup, distinguished from the first selection by its exclusion set. */
    private static Set<ProviderCode> eqExcluding() {
        return org.mockito.ArgumentMatchers.eq(Set.of(PRIMARY));
    }

    /**
     * A fourth channel worker that exists only here: the three real ones differ from each other
     * only in topics and channel, and none of them may expose {@code dispatch} to a test.
     */
    private static final class TestChannelWorker extends AbstractChannelWorker {

        TestChannelWorker(ChannelWorkerSupport support) {
            super(Channel.SMS, support);
        }

        void run(NotificationDispatchEvent event, ConsumerRecordMetadata metadata, Acknowledgment ack) {
            dispatch(event, metadata, ack);
        }

        @Override
        protected String consumerGroup() {
            return "notification-worker.test.sms";
        }
    }
}
