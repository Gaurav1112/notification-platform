package dev.gaurav.notification.domain.enums;

import java.time.Duration;

/**
 * The load-bearing concept in this platform.
 *
 * <p>Notifications are not one workload. A login OTP dies worthless after 60 seconds; a marketing
 * digest can wait an hour. Kafka partitions are strictly FIFO, so a priority <em>field</em> on a
 * shared topic is a lie — the consumer still has to read past ten million bulk records to reach
 * the OTP behind them. Only physically separate topics give real isolation, and this enum is what
 * selects them.
 *
 * @see #topicSuffix()
 */
public enum TrafficClass {

    /** OTP, 2FA, fraud alerts. Never queued behind anything. Shed last. */
    CRITICAL(Duration.ofSeconds(60), Duration.ofSeconds(5), "tx", 0),

    /** Receipts, password resets, shipping updates. */
    TRANSACTIONAL(Duration.ofHours(24), Duration.ofSeconds(30), "tx", 1),

    /** Marketing, digests, campaigns. Shed first under pressure. */
    BULK(Duration.ofHours(72), Duration.ofMinutes(15), "bulk", 2);

    private final Duration defaultTtl;
    private final Duration dispatchLatencyObjective;
    private final String topicSuffix;
    private final int shedOrder;

    TrafficClass(Duration defaultTtl, Duration dispatchLatencyObjective,
                 String topicSuffix, int shedOrder) {
        this.defaultTtl = defaultTtl;
        this.dispatchLatencyObjective = dispatchLatencyObjective;
        this.topicSuffix = topicSuffix;
        this.shedOrder = shedOrder;
    }

    /** How long this class stays sendable before becoming EXPIRED. */
    public Duration defaultTtl() {
        return defaultTtl;
    }

    /** The p99 accept-to-provider-call target. Drives alerting, not runtime behaviour. */
    public Duration dispatchLatencyObjective() {
        return dispatchLatencyObjective;
    }

    /**
     * The Kafka lane. CRITICAL and TRANSACTIONAL share {@code tx} because both drain in seconds;
     * BULK is physically separate because it does not.
     */
    public String topicSuffix() {
        return topicSuffix;
    }

    /** Lower sheds first. BULK (2) is dropped before CRITICAL (0) is touched. */
    public int shedOrder() {
        return shedOrder;
    }

    /** {@code notification.dispatch.sms.tx} */
    public String dispatchTopic(Channel channel) {
        return "notification.dispatch." + channel.name().toLowerCase() + "." + topicSuffix;
    }
}
