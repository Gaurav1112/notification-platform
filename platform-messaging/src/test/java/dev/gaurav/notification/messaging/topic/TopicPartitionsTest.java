package dev.gaurav.notification.messaging.topic;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicPartitionsTest {

    @Test
    @DisplayName("the declared total is the actual sum — a drifting total mis-sizes the broker "
            + "replica budget, which is 864 replicas across 6 brokers")
    void thePartitionCountsSumToTheDeclaredTotal() {
        int sum = TopicPartitions.asMap().values().stream().mapToInt(Integer::intValue).sum();
        assertThat(sum).isEqualTo(TopicPartitions.TOTAL).isEqualTo(288);
    }

    @Test
    @DisplayName("every topic has a partition count — a missing one means provisioning silently "
            + "creates a one-partition topic with default retention")
    void everyDeclaredTopicHasAPartitionCount() {
        assertThat(TopicPartitions.asMap().keySet())
                .containsExactlyInAnyOrderElementsOf(Topics.all());
    }

    @Test
    @DisplayName("bulk push is the widest topic, because FCM removed its batch endpoint and every "
            + "device token costs one HTTP/2 request")
    void bulkPushIsTheWidestTopic() {
        int widest = TopicPartitions.asMap().values().stream()
                .mapToInt(Integer::intValue).max().orElseThrow();
        assertThat(TopicPartitions.DISPATCH_PUSH_BULK).isEqualTo(widest).isEqualTo(54);
        assertThat(TopicPartitions.DISPATCH_PUSH_BULK)
                .isGreaterThan(TopicPartitions.DISPATCH_SMS_BULK);
    }

    @Test
    @DisplayName("retry.5s is wider than every other tier, because it is sized for a total "
            + "provider outage and not for the 12.8% steady retry rate")
    void theFirstRetryTierAbsorbsAFullProviderOutage() {
        assertThat(TopicPartitions.RETRY_5S)
                .isGreaterThan(TopicPartitions.RETRY_30S)
                .isGreaterThan(TopicPartitions.RETRY_1H);
    }

    @Test
    @DisplayName("asking for the partition count of an unknown topic fails loudly rather than "
            + "defaulting to something plausible")
    void unknownTopicThrows() {
        assertThatThrownBy(() -> TopicPartitions.forTopic("notification.retry.5m"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown topic");
    }
}
