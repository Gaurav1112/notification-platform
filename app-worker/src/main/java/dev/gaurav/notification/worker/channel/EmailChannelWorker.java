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
 * Email dispatch. The channel where a duplicate is cheap and reconciliation is actually possible.
 *
 * <p>SES echoes {@code EmailTags} and SendGrid echoes {@code custom_args} on every event webhook,
 * so a send whose outcome we did not see can be resolved later against our own reference. That is
 * what makes <strong>at-least-once plus reconciliation</strong> the right choice here and the wrong
 * one for SMS — and AWS documents the exact case that forces it: a timeout may follow acceptance,
 * and a retry then sends a second email with a different message id.
 *
 * <p>The consequence that catches people out is that acceptance is not delivery. An SMTP 250 means
 * "queued", and a hard bounce legitimately arrives minutes later on a completely different path.
 * {@code DELIVERED} is deliberately not a terminal status for exactly this reason; a state machine
 * that treats it as final drops the bounce, the address never reaches the suppression list, and
 * sender reputation degrades for every tenant sharing the IP pool.
 */
@Component
public class EmailChannelWorker extends AbstractChannelWorker {

    public static final String TX_GROUP = "notification-worker.email.tx";

    public static final String BULK_GROUP = "notification-worker.email.bulk";

    public EmailChannelWorker(ChannelWorkerSupport support) {
        super(Channel.EMAIL, support);
    }

    @KafkaListener(
            id = "dispatch-email-tx",
            topics = Topics.DISPATCH_EMAIL_TX,
            groupId = TX_GROUP,
            containerFactory = WorkerListenerConfiguration.TRANSACTIONAL_FACTORY)
    public void onTransactional(NotificationDispatchEvent event, ConsumerRecordMetadata metadata,
                                Acknowledgment ack) {
        dispatch(event, metadata, ack);
    }

    @KafkaListener(
            id = "dispatch-email-bulk",
            topics = Topics.DISPATCH_EMAIL_BULK,
            groupId = BULK_GROUP,
            containerFactory = WorkerListenerConfiguration.BULK_FACTORY)
    public void onBulk(NotificationDispatchEvent event, ConsumerRecordMetadata metadata,
                       Acknowledgment ack) {
        dispatch(event, metadata, ack);
    }

    @Override
    protected String consumerGroup() {
        return "notification-worker.email";
    }
}
