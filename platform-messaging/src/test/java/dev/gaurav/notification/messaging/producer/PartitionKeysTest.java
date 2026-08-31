package dev.gaurav.notification.messaging.producer;

import dev.gaurav.notification.domain.enums.Channel;

import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionKeysTest {

    private static final UUID RECIPIENT =
            UUID.fromString("018f3c2a-0000-7000-8000-000000000001");
    private static final UUID REQUEST =
            UUID.fromString("018f3c2a-0000-7000-8000-000000000002");
    private static final UUID NOTIFICATION =
            UUID.fromString("018f3c2a-0000-7000-8000-000000000003");

    @Test
    @DisplayName("the dispatch key is tenantId|recipientId|CHANNEL — dropping recipientId is what "
            + "puts a mega-tenant's entire volume on one partition")
    void theDispatchKeyCarriesTenantRecipientAndChannel() {
        String key = PartitionKeys.forDispatch(42L, RECIPIENT, Channel.SMS);

        assertThat(key).isEqualTo("42|018f3c2a-0000-7000-8000-000000000001|SMS");
        assertThat(key.split("\\|")).hasSize(3);
        assertThat(key).contains(RECIPIENT.toString());
    }

    @Test
    @DisplayName("the requested key is tenantId|idempotencyKey, and the delivery key is the bare "
            + "notificationId so worker, webhook and reconciler land on the same partition")
    void theOtherKeyFormatsMatchTheTopicTable() {
        assertThat(PartitionKeys.forRequested(42L, "order-9911", REQUEST))
                .isEqualTo("42|order-9911");
        assertThat(PartitionKeys.forScheduled(42L, REQUEST))
                .isEqualTo("42|018f3c2a-0000-7000-8000-000000000002");
        assertThat(PartitionKeys.forNotification(NOTIFICATION))
                .isEqualTo("018f3c2a-0000-7000-8000-000000000003");
    }

    @Test
    @DisplayName("a request with no idempotency key falls back to the requestId — hashing the "
            + "literal \"42|null\" would collapse every keyless request onto one partition")
    void aMissingIdempotencyKeyDoesNotCollapseOntoOnePartition() {
        String first = PartitionKeys.forRequested(42L, null, REQUEST);
        String second = PartitionKeys.forRequested(42L, "  ", UUID.randomUUID());

        assertThat(first).isEqualTo("42|" + REQUEST);
        assertThat(second).isNotEqualTo(first);
        assertThat(first).doesNotContain("null");
    }

    @Test
    @DisplayName("two tenants can reuse the same idempotency key without colliding — the key is "
            + "scoped by tenant, so tenant A cannot suppress tenant B's request")
    void idempotencyKeysAreTenantScoped() {
        assertThat(PartitionKeys.forRequested(1L, "order-1", REQUEST))
                .isNotEqualTo(PartitionKeys.forRequested(2L, "order-1", REQUEST));
    }

    @Test
    @DisplayName("the same recipient and channel always produce the same key, which is what makes "
            + "the (tenant, recipient, channel) FIFO contract expressible in Kafka at all")
    void theKeyIsStableForTheOrderingContract() {
        assertThat(PartitionKeys.forDispatch(7L, RECIPIENT, Channel.PUSH))
                .isEqualTo(PartitionKeys.forDispatch(7L, RECIPIENT, Channel.PUSH));
        assertThat(PartitionKeys.forDispatch(7L, RECIPIENT, Channel.PUSH))
                .isNotEqualTo(PartitionKeys.forDispatch(7L, RECIPIENT, Channel.SMS));
    }

    @Test
    @DisplayName("100k recipients of one tenant spread evenly over 54 partitions under murmur2 — "
            + "the hot partition people blame on hashing does not exist")
    void oneTenantsRecipientsSpreadEvenlyAcrossTheWidestTopic() {
        int partitions = 54;
        int recipients = 100_000;
        int[] counts = new int[partitions];
        Set<String> distinctKeys = new HashSet<>();

        for (int i = 0; i < recipients; i++) {
            String key = PartitionKeys.forDispatch(
                    42L, new UUID(0x018f3c2a00007000L, i), Channel.PUSH);
            distinctKeys.add(key);
            counts[partitionFor(key, partitions)]++;
        }

        assertThat(distinctKeys).hasSize(recipients);
        double mean = (double) recipients / partitions;
        int max = 0;
        int min = Integer.MAX_VALUE;
        for (int count : counts) {
            max = Math.max(max, count);
            min = Math.min(min, count);
        }
        assertThat(max / mean).isLessThan(1.10);
        assertThat(min / mean).isGreaterThan(0.90);
    }

    @Test
    @DisplayName("keying on tenantId alone would put a whole tenant on one partition — the "
            + "counter-example the dispatch key format exists to rule out")
    void keyingOnTenantAloneIsTheHotPartitionBug() {
        int partitions = 54;
        int[] counts = new int[partitions];
        for (int i = 0; i < 100_000; i++) {
            counts[partitionFor("42", partitions)]++;
        }

        long partitionsUsed = java.util.Arrays.stream(counts).filter(c -> c > 0).count();
        assertThat(partitionsUsed).isEqualTo(1);
    }

    /** The default partitioner's formula. Reproduced here because it must stay murmur2. */
    private static int partitionFor(String key, int partitions) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        return Utils.toPositive(Utils.murmur2(bytes)) % partitions;
    }
}
