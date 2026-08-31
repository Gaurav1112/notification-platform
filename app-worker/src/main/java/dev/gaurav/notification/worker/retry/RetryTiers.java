package dev.gaurav.notification.worker.retry;

import dev.gaurav.notification.messaging.topic.RetryTier;

/**
 * Bridges the two {@code RetryTier} enums.
 *
 * <p>There are deliberately two: {@code platform-resilience} owns the tier as a <em>backoff
 * ladder</em> (a duration, chosen by a policy that knows nothing about Kafka), and
 * {@code platform-messaging} owns it as a <em>topic</em> (a physical lane with a partition count
 * and a retention). Merging them would make the resilience module depend on the messaging module,
 * and a retry policy that cannot be unit-tested without a topic list is a retry policy nobody
 * unit-tests.
 *
 * <p>The cost of the split is that the two can drift, and drift here is silent: a delay that maps
 * to no topic would produce a message published nowhere. {@link #forTopicOf} converts through the
 * topic <em>name</em> rather than through the ordinal, so a mismatch is an exception on the first
 * message rather than a message parked on the wrong lane — an ordinal mapping would happily send a
 * one-hour backoff to the five-second topic.
 */
public final class RetryTiers {

    private RetryTiers() {
    }

    /** The messaging tier that backs a resilience tier, matched on topic name. */
    public static RetryTier forTopicOf(dev.gaurav.notification.resilience.retry.RetryTier tier) {
        return RetryTier.forTopic(tier.topic()).orElseThrow(() -> new IllegalStateException(
                "resilience tier " + tier + " names topic '" + tier.topic()
                        + "', which is not one of the platform's retry topics"));
    }
}
