package dev.gaurav.notification.messaging.topic;

import java.time.Duration;
import java.util.Optional;

/**
 * The five delayed-message tiers a failed dispatch hops through: 5 s, 30 s, 2 m, 10 m, 1 h.
 *
 * <p><strong>Why tiers instead of {@code Thread.sleep()} in the consumer:</strong> sleeping holds
 * the partition. Every record behind it waits, and if the sleep outlasts
 * {@code max.poll.interval.ms} the consumer is evicted, the group rebalances, the backlog grows
 * and the same record is reassigned — a rebalance storm that looks like perfect broker health.
 *
 * <p><strong>Why messages hop tiers instead of waiting in one:</strong> a partition is FIFO, so a
 * message waiting one hour at the head of a "5-second" partition stalls everything behind it. The
 * consumer for a tier therefore pauses only for that tier's delay and republishes to the next
 * tier. Each tier stays tight, and the total budget is bounded at
 * {@value #TOTAL_BUDGET_SECONDS} seconds (1 h 12 m 35 s).
 *
 * <p>Tiers are shared across channels, which is only safe because the retry consumer performs
 * <strong>no external I/O</strong>. Head-of-line blocking inside a tier is bounded by the tier
 * delay itself, never by provider latency.
 */
public enum RetryTier {

    /** First retry. Absorbs transient network blips and single-instance provider hiccups. */
    T5S(Duration.ofSeconds(5), Topics.RETRY_5S),

    /** Second retry. Typical recovery window for a provider's own internal failover. */
    T30S(Duration.ofSeconds(30), Topics.RETRY_30S),

    /** Third retry. Past this point the provider is having a real incident, not a blip. */
    T2M(Duration.ofMinutes(2), Topics.RETRY_2M),

    /** Fourth retry. */
    T10M(Duration.ofMinutes(10), Topics.RETRY_10M),

    /** Last retry. Exhausting this tier means the dead-letter channel. */
    T1H(Duration.ofHours(1), Topics.RETRY_1H);

    /** 5 + 30 + 120 + 600 + 3600. Stated as a constant so the budget cannot silently grow. */
    public static final long TOTAL_BUDGET_SECONDS = 4355L;

    private final Duration delay;
    private final String topic;

    RetryTier(Duration delay, String topic) {
        this.delay = delay;
        this.topic = topic;
    }

    /** How long the tier consumer waits before republishing. */
    public Duration delay() {
        return delay;
    }

    /** The Kafka topic that backs this tier. */
    public String topic() {
        return topic;
    }

    /** The tier a message moves to after this one, or empty when the budget is spent. */
    public Optional<RetryTier> next() {
        int i = ordinal() + 1;
        return i < values().length ? Optional.of(values()[i]) : Optional.empty();
    }

    /** Where a first failure goes. */
    public static RetryTier first() {
        return T5S;
    }

    /**
     * The tier for a given attempt number (1-based).
     *
     * <p>Returns empty once attempts exceed the five tiers, which is the signal to dead-letter
     * rather than to keep retrying forever. Callers that treat "no tier" as "retry in the last
     * tier again" reinvent the infinite retry loop the budget exists to prevent.
     */
    public static Optional<RetryTier> forAttempt(int attemptNumber) {
        if (attemptNumber < 1 || attemptNumber > values().length) {
            return Optional.empty();
        }
        return Optional.of(values()[attemptNumber - 1]);
    }

    /** Resolves a tier from its topic name, for the tier consumer's own configuration. */
    public static Optional<RetryTier> forTopic(String topic) {
        for (RetryTier tier : values()) {
            if (tier.topic.equals(topic)) {
                return Optional.of(tier);
            }
        }
        return Optional.empty();
    }
}
