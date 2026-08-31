package dev.gaurav.notification.worker.channel;

import dev.gaurav.notification.messaging.consumer.IdempotentConsumer;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.persistence.repository.NotificationRecipientRepository;
import dev.gaurav.notification.worker.config.WorkerProperties;
import dev.gaurav.notification.worker.retry.RetryRouter;
import dev.gaurav.notification.worker.status.ApplyDeliveryStatusUseCase;
import dev.gaurav.notification.worker.status.SuppressionWriter;
import dev.gaurav.notification.worker.support.ProviderIds;
import dev.gaurav.notification.worker.support.RecipientAddressVault;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Clock;

/**
 * The eleven collaborators every channel worker needs, injected once.
 *
 * <p>{@link AbstractChannelWorker} fixes the dispatch sequence and the three subclasses supply
 * only what differs — the topics, the channel and the per-channel semantics. Threading eleven
 * constructor parameters through each subclass would mean every new collaborator edits four files
 * and three of the edits are mechanical. Worse, it invites a subclass to quietly take a
 * <em>different</em> dependency, which is how one channel ends up with its own retry rules.
 *
 * <p>A record rather than a class with getters: it is a bundle of references with no behaviour,
 * and making it immutable means it is safe to share across every consumer thread.
 */
@Component
public record ChannelWorkerSupport(IdempotentConsumer idempotentConsumer,
                                   NotificationRecipientRepository recipients,
                                   DeliveryAttemptRecorder attempts,
                                   ChannelProviderRouter router,
                                   ProviderIds providerIds,
                                   RecipientAddressVault vault,
                                   NotificationEventPublisher publisher,
                                   ApplyDeliveryStatusUseCase applyStatus,
                                   SuppressionWriter suppressions,
                                   RetryRouter retryRouter,
                                   WorkerProperties properties,
                                   Clock clock,
                                   MeterRegistry meters) {
}
