package dev.gaurav.notification.messaging.config;

import dev.gaurav.notification.messaging.event.NotificationEvent;
import dev.gaurav.notification.messaging.topic.Topics;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;

/**
 * What happens when a record cannot be processed: three attempts, then the dead-letter channel,
 * <strong>then commit the offset</strong>.
 *
 * <p>The last clause is the whole point. Offsets commit only after successful processing, so a
 * consumer that keeps throwing seeks back to the same offset forever. Throughput for that
 * partition is exactly zero while the consumer heartbeats, stays in the group, and reports healthy
 * — group-level dashboards stay green while a deterministic subset of tenants receives nothing.
 * <strong>A poison message must never block a partition.</strong>
 *
 * <p>Three in-place attempts with no backoff, not five with an exponential one. Backoff inside the
 * listener holds the partition and eats the poll interval; delay belongs on the retry tiers, where
 * it costs nothing. These three attempts exist only to ride out a hiccup that resolves in
 * milliseconds — a connection-pool blip, a leader election. Anything slower is a retry-tier
 * concern.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaErrorHandlingConfig {

    /** In-place attempts before dead-lettering: the original plus {@code RETRIES}. */
    public static final long IN_PLACE_RETRIES = 2L;

    /** Consumer group that gave up on the record. Not in Spring's default header set. */
    public static final String HEADER_FAILED_GROUP = "notification-dlq-consumer-group";

    /** Ceiling enforcement for replay — see {@code DeadLetterEvent.MAX_REPLAY_ATTEMPTS}. */
    public static final String HEADER_REPLAY_ATTEMPTS = "notification-dlq-replay-attempts";

    /**
     * Marks a record that failed to <em>deserialise</em>, as opposed to one that failed to process.
     *
     * <p>"Bad data" and "the provider was down" have nothing in common operationally, and mixing
     * them in one queue makes triage guesswork. The design calls for a separate invalid-message
     * channel; until that topic exists, this header keeps the two distinguishable inside the DLQ.
     */
    public static final String HEADER_INVALID_MESSAGE = "notification-dlq-invalid-message";

    /**
     * Routes everything to {@code notification.dlq}, always with partition {@code -1}.
     *
     * <p>Spring's default resolver sends to {@code <sourceTopic>.DLT} at the <em>same partition
     * number</em>. Both halves are wrong here. The topic table has one DLQ, not sixteen; and
     * {@code notification.dispatch.push.bulk} has 54 partitions against the DLQ's 6, so a
     * same-partition send from partition 40 targets a partition that does not exist and the
     * recoverer throws — which puts us straight back to a blocked partition, from the code whose
     * job was to unblock it. {@code -1} hands the choice to the murmur2 partitioner.
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, NotificationEvent> kafkaTemplate) {

        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(Topics.DLQ, -1));

        // Spring already adds original topic/partition/offset/timestamp and the exception FQCN,
        // message and stack trace. These three are the ones triage needs and Spring does not know.
        recoverer.addHeadersFunction((record, exception) -> {
            var headers = new RecordHeaders();
            headers.add(header(HEADER_REPLAY_ATTEMPTS, "0"));
            if (isDeserializationFailure(exception)) {
                headers.add(header(HEADER_INVALID_MESSAGE, "true"));
            }
            return headers;
        });

        // The DLQ send is a normal produce with acks=all. If it fails we must NOT ack the source
        // record: acking would drop the message entirely, which is the one outcome worse than a
        // blocked partition.
        recoverer.setFailIfSendResultIsError(true);
        return recoverer;
    }

    /**
     * The handler wired into every listener container.
     *
     * <p>{@code FixedBackOff(0, 2)} is three total attempts with no delay, and
     * {@code isAckAfterHandle()} is true by default — that is what commits the offset once the
     * record has been dead-lettered, so the partition moves on.
     */
    @Bean
    public CommonErrorHandler notificationErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, IN_PLACE_RETRIES));
        // Retrying a payload that cannot be parsed produces the identical exception three times
        // and delays the DLQ hop by nothing useful. Go straight there.
        handler.addNotRetryableExceptions(DeserializationException.class);
        return handler;
    }

    private static boolean isDeserializationFailure(Exception exception) {
        for (Throwable t = exception; t != null; t = t.getCause()) {
            if (t instanceof DeserializationException) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }

    private static RecordHeader header(String name, String value) {
        return new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
