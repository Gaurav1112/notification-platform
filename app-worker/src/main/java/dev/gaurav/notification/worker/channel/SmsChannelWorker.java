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
 * SMS dispatch. The channel where a duplicate costs more than a loss.
 *
 * <p>That asymmetry is not a preference, it is the verified state of the market: Twilio offers no
 * client idempotency key, and no reconciliation by our own reference either — {@code GET /Messages}
 * filters on {@code To}/{@code From}/{@code DateSent} at whole-day granularity and Messages carry
 * no client reference field. So when a send times out there is no way, ever, to find out what
 * happened to it. A retry is an unrecoverable duplicate: real money, a confused user, and a carrier
 * spam signal that degrades the sender for every tenant. A lost one-time passcode, by contrast, is
 * recovered by the user pressing "resend".
 *
 * <p>The platform therefore chooses <strong>at-most-once</strong> for SMS on an indeterminate
 * outcome, and the choice is enforced in
 * {@link dev.gaurav.notification.worker.retry.RetryRouter} rather than here, so it cannot be
 * accidentally different between the {@code tx} and {@code bulk} lanes.
 *
 * <p><strong>Two listener methods, not one with two topics.</strong> Separate containers mean
 * separate consumer threads and separate pause state: a campaign backlog on the bulk lane cannot
 * delay a password reset, and the health gate can pause one lane without the other. Kafka
 * partitions are strictly FIFO, so no priority field on a shared topic can achieve this — only
 * physically separate topics with physically separate consumers can.
 */
@Component
public class SmsChannelWorker extends AbstractChannelWorker {

    /** Distinct from the bulk group so the two lanes commit offsets independently. */
    public static final String TX_GROUP = "notification-worker.sms.tx";

    public static final String BULK_GROUP = "notification-worker.sms.bulk";

    public SmsChannelWorker(ChannelWorkerSupport support) {
        super(Channel.SMS, support);
    }

    @KafkaListener(
            id = "dispatch-sms-tx",
            topics = Topics.DISPATCH_SMS_TX,
            groupId = TX_GROUP,
            containerFactory = WorkerListenerConfiguration.TRANSACTIONAL_FACTORY)
    public void onTransactional(NotificationDispatchEvent event, ConsumerRecordMetadata metadata,
                                Acknowledgment ack) {
        dispatch(event, metadata, ack);
    }

    @KafkaListener(
            id = "dispatch-sms-bulk",
            topics = Topics.DISPATCH_SMS_BULK,
            groupId = BULK_GROUP,
            containerFactory = WorkerListenerConfiguration.BULK_FACTORY)
    public void onBulk(NotificationDispatchEvent event, ConsumerRecordMetadata metadata,
                       Acknowledgment ack) {
        dispatch(event, metadata, ack);
    }

    @Override
    protected String consumerGroup() {
        return "notification-worker.sms";
    }
}
