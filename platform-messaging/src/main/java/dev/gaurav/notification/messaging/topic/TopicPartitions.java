package dev.gaurav.notification.messaging.topic;

import java.util.Map;

/**
 * Partition counts per topic. 288 partitions total.
 *
 * <p>Partition count is the one Kafka decision you cannot cheaply change later, so none of these
 * numbers is a round number picked by feel. Each is:
 *
 * <pre>
 *   P = ceil( peakRate / consumerRatePerPartition x 1.30 x K )   rounded up to a multiple of 3
 *
 *   1.30 = operational headroom (broker restart, rolling upgrade, rebalance)
 *   K    = 1.5 for KEYED topics   - increasing partitions re-maps murmur2(key) % N and
 *                                   permanently breaks per-key ordering across the boundary,
 *                                   so keyed topics are over-provisioned up front
 *        = 1.0 for unkeyed topics - "--alter --partitions" is safe and online
 *   3    = availability zones, so partitions spread evenly
 * </pre>
 *
 * <p>The binding input is <strong>measured work per message</strong>, never bytes. Even at 100M
 * notifications/day with a 10M campaign superimposed the cluster ingests ~16.5 MB/s compressed —
 * a single broker handles that. Sizing Kafka by MB/s produces a cluster roughly 5x short on
 * partitions and 2x oversized on brokers.
 *
 * <p>Measured consumer rates that feed the formula:
 * dispatch SMS 80/s (hard provider caps, ~250 ms round trip), dispatch EMAIL 200/s (SES bulk,
 * 50 destinations), dispatch PUSH 300/s (one HTTP/2 request per token — FCM removed its batch
 * endpoint in June 2024), requested/expander 400/s, delivery writer 1,500/s, status projector
 * 2,000/s, retry promoter 1,000/s (no external I/O — it pauses and republishes).
 *
 * <p>{@code notification.retry.5s} is sized for a <strong>total provider outage</strong>
 * (9,861/s), not for the 12.8% steady retry rate. Retry tiers are the shock absorber; sizing them
 * for an average day means they are useless on the day you need them.
 */
public final class TopicPartitions {

    /** 400/s expander rate, keyed on {@code tenantId|idempotencyKey}. */
    public static final int REQUESTED = 12;

    /** Low volume — only the fraction of requests with a future send time. */
    public static final int SCHEDULED = 6;

    /** 300/s per partition, but transactional push is latency-critical. */
    public static final int DISPATCH_PUSH_TX = 18;

    /** The widest topic in the platform: campaign push at one HTTP/2 request per device token. */
    public static final int DISPATCH_PUSH_BULK = 54;

    /** 200/s per partition thanks to SES's 50-destination bulk API. */
    public static final int DISPATCH_EMAIL_TX = 12;

    /** Campaign email; still cheap per message because of bulk send. */
    public static final int DISPATCH_EMAIL_BULK = 36;

    /** 80/s per partition — the slowest consumer in the system. Also the only {@code minISR=3}. */
    public static final int DISPATCH_SMS_TX = 12;

    /** Bulk SMS is rare and expensive, so it is deliberately the narrowest dispatch topic. */
    public static final int DISPATCH_SMS_BULK = 6;

    /** 1,500/s per partition via batched multi-row INSERT. */
    public static final int DELIVERY = 24;

    /** 2,000/s per partition via batched upsert, but compacted — over-provisioned on purpose. */
    public static final int STATUS = 48;

    /** Sized for a total provider outage at 9,861/s, not the 12.8% steady retry rate. */
    public static final int RETRY_5S = 24;

    /** Second tier: only what still fails after 5 s. */
    public static final int RETRY_30S = 12;

    /** Third tier. */
    public static final int RETRY_2M = 6;

    /** Fourth tier. */
    public static final int RETRY_10M = 6;

    /** Fifth and last tier. */
    public static final int RETRY_1H = 6;

    /** Low volume by design — a busy DLQ is an incident, not a capacity problem. */
    public static final int DLQ = 6;

    /**
     * 288 leaders x RF 3 = 864 replicas / 6 brokers = 144 per broker, which is 14% of the
     * AWS-recommended ceiling. The slack is deliberate: see the class javadoc on why partition
     * count is the expensive decision.
     */
    public static final int TOTAL = 288;

    private static final Map<String, Integer> BY_TOPIC = Map.ofEntries(
            Map.entry(Topics.REQUESTED, REQUESTED),
            Map.entry(Topics.SCHEDULED, SCHEDULED),
            Map.entry(Topics.DISPATCH_PUSH_TX, DISPATCH_PUSH_TX),
            Map.entry(Topics.DISPATCH_PUSH_BULK, DISPATCH_PUSH_BULK),
            Map.entry(Topics.DISPATCH_EMAIL_TX, DISPATCH_EMAIL_TX),
            Map.entry(Topics.DISPATCH_EMAIL_BULK, DISPATCH_EMAIL_BULK),
            Map.entry(Topics.DISPATCH_SMS_TX, DISPATCH_SMS_TX),
            Map.entry(Topics.DISPATCH_SMS_BULK, DISPATCH_SMS_BULK),
            Map.entry(Topics.DELIVERY, DELIVERY),
            Map.entry(Topics.STATUS, STATUS),
            Map.entry(Topics.RETRY_5S, RETRY_5S),
            Map.entry(Topics.RETRY_30S, RETRY_30S),
            Map.entry(Topics.RETRY_2M, RETRY_2M),
            Map.entry(Topics.RETRY_10M, RETRY_10M),
            Map.entry(Topics.RETRY_1H, RETRY_1H),
            Map.entry(Topics.DLQ, DLQ));

    private TopicPartitions() {
    }

    /**
     * @throws IllegalArgumentException if the topic is not one of ours — an unknown name here
     *         means somebody is about to produce to a topic the broker will refuse to create
     */
    public static int forTopic(String topic) {
        Integer partitions = BY_TOPIC.get(topic);
        if (partitions == null) {
            throw new IllegalArgumentException("unknown topic: " + topic);
        }
        return partitions;
    }

    /** Immutable view, for the local bootstrap script and the topic-provisioning check. */
    public static Map<String, Integer> asMap() {
        return BY_TOPIC;
    }
}
