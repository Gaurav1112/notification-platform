package dev.gaurav.notification.messaging.producer;

import dev.gaurav.notification.domain.enums.Channel;

import java.util.Objects;
import java.util.UUID;

/**
 * How a partition key is built for each topic, and why.
 *
 * <p>The key decides three things at once: which partition a record lands on, therefore what is
 * ordered relative to what, and therefore how evenly the work spreads. Getting it wrong does not
 * produce an error — it produces a hot partition, which looks like "Kafka is slow".
 *
 * <p><strong>{@code recipientId} must be in the dispatch key.</strong> The published ordering
 * contract is FIFO per {@code (tenantId, recipientId, channel)}, and that triple is also what
 * gives ~50M distinct keys. Over 54 partitions:
 *
 * <pre>
 *   mean = 100,000,000 / 72 = 1,388,889 records/partition
 *   sd   = sqrt(n · (1/p) · (1 − 1/p)) = 1,170
 *   4σ / mean = 0.337%                  ← statistically negligible
 * </pre>
 *
 * <p>Key on {@code tenantId} alone and a mega-tenant at 40% of platform volume puts 40% of the
 * traffic on one partition. That is not a hashing problem and no partitioner fixes it —
 * <strong>every real hot partition is a bug or an adversary.</strong> Related bans, all of which
 * this class exists to make impossible: no time component in a key (it serialises the whole
 * platform onto whichever partition owns "now"), and no custom partitioner (murmur2 stays, so that
 * key → partition is reproducible from outside the JVM during an incident).
 *
 * <p>Sticky partitioning (KIP-480) is irrelevant here: it applies only to null-key records, and
 * every record this platform produces has a key.
 */
public final class PartitionKeys {

    /** Chosen because it cannot appear inside a UUID, a channel name or a numeric tenant id. */
    static final String SEPARATOR = "|";

    private PartitionKeys() {
    }

    /**
     * {@code tenantId|idempotencyKey} for {@code notification.requested}.
     *
     * <p>Falls back to the request id when the caller supplied no idempotency key — it is optional
     * on the API. Hashing the literal {@code "42|null"} instead would collapse every keyless
     * request from a tenant onto a single partition: a hot partition created by a null check
     * nobody wrote.
     */
    public static String forRequested(long tenantId, String idempotencyKey, UUID requestId) {
        Objects.requireNonNull(requestId, "requestId");
        String discriminator = (idempotencyKey == null || idempotencyKey.isBlank())
                ? requestId.toString()
                : idempotencyKey;
        return tenantId + SEPARATOR + discriminator;
    }

    /** {@code tenantId|requestId} for {@code notification.scheduled}. */
    public static String forScheduled(long tenantId, UUID requestId) {
        Objects.requireNonNull(requestId, "requestId");
        return tenantId + SEPARATOR + requestId;
    }

    /**
     * {@code tenantId|recipientId|CHANNEL} for the six dispatch topics and the five retry tiers.
     *
     * <p>This is the ordering contract, expressed as a string. The channel is part of it because
     * dispatch topics are already per-channel; including it keeps the same key usable on the
     * shared retry tiers without two recipients' SMS and push traffic interleaving unpredictably.
     */
    public static String forDispatch(long tenantId, UUID recipientId, Channel channel) {
        Objects.requireNonNull(recipientId, "recipientId");
        Objects.requireNonNull(channel, "channel");
        return tenantId + SEPARATOR + recipientId + SEPARATOR + channel.name();
    }

    /**
     * {@code notificationId} for {@code notification.delivery}, {@code notification.status} and
     * {@code notification.dlq}.
     *
     * <p>Deliberately not tenant-scoped. The status projector's correctness depends on all
     * observations for one notification — worker write, provider webhook, reconciler sweep —
     * landing on the same partition, and {@code notification.status} is compacted, where the key
     * <em>is</em> the identity of the compacted value. Prefixing the tenant would change nothing
     * about the distribution and would break both properties on any tenant re-key.
     */
    public static String forNotification(UUID notificationId) {
        Objects.requireNonNull(notificationId, "notificationId");
        return notificationId.toString();
    }
}
