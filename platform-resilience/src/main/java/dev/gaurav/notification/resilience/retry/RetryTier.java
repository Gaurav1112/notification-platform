package dev.gaurav.notification.resilience.retry;

import java.time.Duration;

/**
 * The five delay lanes a failed message is republished into: 5 s → 30 s → 2 m → 10 m → 1 h.
 *
 * <p><strong>Why topics and not {@code Thread.sleep()}.</strong> The obvious implementation of
 * backoff inside a Kafka consumer is to sleep and try again. It is wrong for two independent
 * reasons:
 *
 * <ol>
 *   <li><em>It holds the partition.</em> The consumer owns that partition for the whole sleep, so
 *       every other message behind the sleeping one is also delayed. One recipient stalled on a
 *       Twilio 429 with a 30 s backoff blocks the thousands of healthy messages queued behind it.
 *   <li><em>It breaks {@code max.poll.interval.ms}.</em> Kafka measures liveness by the gap between
 *       {@code poll()} calls. Sleep past it — the default is 5 minutes, and the 10 m and 1 h tiers
 *       obviously do — and the broker declares the consumer dead and rebalances the group. The
 *       rebalance revokes partitions mid-flight, the in-flight messages are redelivered elsewhere,
 *       and under a provider outage every consumer sleeps at once, so the whole group thrashes at
 *       precisely the moment it must not.
 * </ol>
 *
 * <p>Publishing to a delay topic instead makes the wait durable, survivable across a pod restart,
 * and invisible to the dispatch lanes. The tier consumer does no external I/O at all — it pauses
 * the partition, re-polls until {@code ready_at}, re-checks eligibility (cancelled? expired?) and
 * republishes — so head-of-line blocking within a tier is bounded by the tier's own delay.
 *
 * <p>Five shared tiers rather than a topic per delay per channel: 5 topics instead of 5 × 3, and
 * sharing is safe precisely because the tier consumer never calls a provider.
 */
public enum RetryTier {

    /** Sized for a total provider outage, not the steady-state failure rate. */
    T5S(Duration.ofSeconds(5), "notification.retry.5s"),
    T30S(Duration.ofSeconds(30), "notification.retry.30s"),
    T2M(Duration.ofMinutes(2), "notification.retry.2m"),
    T10M(Duration.ofMinutes(10), "notification.retry.10m"),

    /** The last lane. Anything still failing after this is DLQ material. */
    T1H(Duration.ofHours(1), "notification.retry.1h");

    private final Duration delay;
    private final String topic;

    RetryTier(Duration delay, String topic) {
        this.delay = delay;
        this.topic = topic;
    }

    /** How long a message waits in this lane. */
    public Duration delay() {
        return delay;
    }

    /** {@code notification.retry.30s} */
    public String topic() {
        return topic;
    }

    /**
     * The lane a computed backoff should be published to: the shortest tier that is <em>not
     * shorter</em> than the requested delay.
     *
     * <p>Rounding to the arithmetically closest tier would be wrong. A 17 s backoff is closer to
     * 5 s than to 30 s, so "closest" would fire the retry 12 s early and hand the failing provider
     * back the load the backoff was calculated to withhold. Rounding up costs a little latency;
     * rounding down discards the backoff. Anything beyond an hour clamps to {@link #T1H}, which is
     * the last lane before the DLQ.
     */
    public static RetryTier nearestFor(Duration delay) {
        for (RetryTier tier : values()) {
            if (tier.delay.compareTo(delay) >= 0) {
                return tier;
            }
        }
        return T1H;
    }
}
