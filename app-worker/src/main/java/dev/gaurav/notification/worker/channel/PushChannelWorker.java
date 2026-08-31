package dev.gaurav.notification.worker.channel;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.messaging.event.NotificationDispatchEvent;
import dev.gaurav.notification.messaging.topic.Topics;
import dev.gaurav.notification.worker.config.WorkerListenerConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.stereotype.Component;

/**
 * Push dispatch. The channel with the widest topic and nowhere to fail over to.
 *
 * <p>FCM and APNs are the only routes to their respective devices, so there is no competitor to
 * switch to — the honest response to a push outage is backoff until the TTL expires, not failover.
 * Modelling a secondary here would be modelling a resilience option that does not exist.
 *
 * <p>Two facts shape the volume. There is <strong>no batch endpoint</strong>: FCM's {@code /batch}
 * was deprecated in 2023 and stopped working in 2024, and {@code sendEachForMulticast} fans out to
 * individual HTTP/2 requests — the 500-token limit is client-side chunking, not a server batch. And
 * dead tokens are the steady state, not an incident: {@code DEVICE_UNREGISTERED} is the dominant
 * failure, which is why it suppresses the address rather than retrying.
 *
 * <p>Duplicate cost is close to zero, so the semantics are at-least-once, with
 * {@code apns-collapse-id} / {@code collapse_key} set to our dedup key so a duplicate
 * <em>replaces</em> the earlier notification on the device rather than stacking beneath it.
 * TODO(phase-9): set the collapse key on the send command once the real adapters land.
 */
@Component
public class PushChannelWorker extends AbstractChannelWorker {

    public static final String TX_GROUP = "notification-worker.push.tx";

    public static final String BULK_GROUP = "notification-worker.push.bulk";

    public PushChannelWorker(ChannelWorkerSupport support) {
        super(Channel.PUSH, support);
    }

    @KafkaListener(
            id = "dispatch-push-tx",
            topics = Topics.DISPATCH_PUSH_TX,
            groupId = TX_GROUP,
            containerFactory = WorkerListenerConfiguration.TRANSACTIONAL_FACTORY)
    public void onTransactional(NotificationDispatchEvent event, ConsumerRecordMetadata metadata,
                                Acknowledgment ack) {
        dispatch(event, metadata, ack);
    }

    @KafkaListener(
            id = "dispatch-push-bulk",
            topics = Topics.DISPATCH_PUSH_BULK,
            groupId = BULK_GROUP,
            containerFactory = WorkerListenerConfiguration.BULK_FACTORY)
    public void onBulk(NotificationDispatchEvent event, ConsumerRecordMetadata metadata,
                       Acknowledgment ack) {
        dispatch(event, metadata, ack);
    }

    @Override
    protected String consumerGroup() {
        return "notification-worker.push";
    }
}
