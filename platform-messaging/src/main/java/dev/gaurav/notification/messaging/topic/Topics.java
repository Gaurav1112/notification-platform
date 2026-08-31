package dev.gaurav.notification.messaging.topic;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.TrafficClass;

import java.util.List;

/**
 * Every Kafka topic name in the platform, in one place.
 *
 * <p>Topic names are typed as string literals in {@code @KafkaListener} annotations, which means a
 * typo is not a compile error — it is a consumer group that subscribes to a topic nobody produces
 * to and reports zero lag forever. Constants make that a compile error instead.
 *
 * <p>The set is deliberately closed and matches {@code docs/KAFKA.md} exactly: 16 topics.
 * {@code auto.create.topics.enable=false} on the broker, so producing to a name that is not in
 * this list fails loudly at deploy time rather than silently creating a one-partition topic with
 * default retention.
 *
 * <p><strong>Why per-channel dispatch topics and not one shared topic:</strong> a stalled SMS
 * record — a Twilio 429 with a 30-second backoff — head-of-line blocks every push and email
 * record behind it in the same partition. One provider incident would degrade all three channels.
 *
 * <p><strong>Why {@code .tx} and {@code .bulk} lanes:</strong> without them a 10M-recipient blast
 * puts ~11,000 records ahead of a password reset in every partition, which at 300 msg/s is a
 * 37-second delay on a password reset. Kafka partitions are strictly FIFO, so a priority
 * <em>field</em> cannot fix this — only a physically separate topic can.
 *
 * @see TopicPartitions
 * @see RetryTier
 */
public final class Topics {

    /** Accepted requests, before fan-out. Key {@code tenantId|idempotencyKey}. */
    public static final String REQUESTED = "notification.requested";

    /** Requests whose send time is in the future. Key {@code tenantId|requestId}. */
    public static final String SCHEDULED = "notification.scheduled";

    /** OTP and transactional SMS. The only topic with {@code minISR=3}. */
    public static final String DISPATCH_SMS_TX = "notification.dispatch.sms.tx";
    /** Campaign SMS. */
    public static final String DISPATCH_SMS_BULK = "notification.dispatch.sms.bulk";
    /** Receipts, password resets. */
    public static final String DISPATCH_EMAIL_TX = "notification.dispatch.email.tx";
    /** Marketing email. */
    public static final String DISPATCH_EMAIL_BULK = "notification.dispatch.email.bulk";
    /** Transactional push. */
    public static final String DISPATCH_PUSH_TX = "notification.dispatch.push.tx";
    /** Campaign push. The widest topic in the platform — FCM has no batch endpoint. */
    public static final String DISPATCH_PUSH_BULK = "notification.dispatch.push.bulk";

    /** Raw send outcomes from the workers. Key {@code notificationId}. */
    public static final String DELIVERY = "notification.delivery";

    /**
     * The projected current status per notification. {@code cleanup.policy=compact,delete}.
     *
     * <p>Compaction keeps the record with the <em>highest offset</em> per key, not the one with
     * the latest state. A late {@code SENT} produced after {@code DELIVERED} therefore survives
     * compaction and a naive projector would regress the status. Consumers must compare the
     * payload {@code version} and drop anything {@code <= current}.
     */
    public static final String STATUS = "notification.status";

    /** Retry tier 1. Key {@code tenantId|recipientId|channel}. */
    public static final String RETRY_5S = "notification.retry.5s";
    /** Retry tier 2. */
    public static final String RETRY_30S = "notification.retry.30s";
    /** Retry tier 3. */
    public static final String RETRY_2M = "notification.retry.2m";
    /** Retry tier 4. */
    public static final String RETRY_10M = "notification.retry.10m";
    /** Retry tier 5, the last one. Exhausting it means the DLQ. */
    public static final String RETRY_1H = "notification.retry.1h";

    /**
     * Terminal parking for anything that failed three in-place attempts, retained 30 days.
     *
     * <p>Replay targets {@link #RETRY_5S}, never the source topic: naive bulk replay of a
     * deterministic poison message recreates the identical storm that produced it.
     */
    public static final String DLQ = "notification.dlq";

    private static final List<String> DISPATCH = List.of(
            DISPATCH_SMS_TX, DISPATCH_SMS_BULK,
            DISPATCH_EMAIL_TX, DISPATCH_EMAIL_BULK,
            DISPATCH_PUSH_TX, DISPATCH_PUSH_BULK);

    private static final List<String> RETRY = List.of(
            RETRY_5S, RETRY_30S, RETRY_2M, RETRY_10M, RETRY_1H);

    private static final List<String> ALL = List.of(
            REQUESTED, SCHEDULED,
            DISPATCH_SMS_TX, DISPATCH_SMS_BULK,
            DISPATCH_EMAIL_TX, DISPATCH_EMAIL_BULK,
            DISPATCH_PUSH_TX, DISPATCH_PUSH_BULK,
            DELIVERY, STATUS,
            RETRY_5S, RETRY_30S, RETRY_2M, RETRY_10M, RETRY_1H,
            DLQ);

    private Topics() {
    }

    /**
     * Resolves the dispatch topic for a (channel, class) pair.
     *
     * <p>Delegates to {@link TrafficClass#dispatchTopic(Channel)} on purpose. The mapping from
     * "this is an OTP" to "therefore it goes on this physical topic" is a domain rule; duplicating
     * it here as a switch would let the two drift, and the drift would show up as a topic nobody
     * consumes rather than as a failure.
     *
     * @return e.g. {@code notification.dispatch.sms.tx}
     */
    public static String dispatchTopic(Channel channel, TrafficClass trafficClass) {
        return trafficClass.dispatchTopic(channel);
    }

    /** The six dispatch topics, in the order they appear in {@code docs/KAFKA.md}. */
    public static List<String> dispatchTopics() {
        return DISPATCH;
    }

    /** The five retry tiers, shortest delay first. */
    public static List<String> retryTopics() {
        return RETRY;
    }

    /** All 16 topics. Used by the local bootstrap script and by the topic-existence health check. */
    public static List<String> all() {
        return ALL;
    }

    /** True if the name is one we own. Guards against producing to an auto-created typo topic. */
    public static boolean isKnown(String topic) {
        return ALL.contains(topic);
    }
}
