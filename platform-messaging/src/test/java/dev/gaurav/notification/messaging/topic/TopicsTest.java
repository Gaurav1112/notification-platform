package dev.gaurav.notification.messaging.topic;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.TrafficClass;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicsTest {

    @Test
    @DisplayName("the topic the domain computes is the topic the constant names — drift here "
            + "produces a lane nobody consumes and zero lag to alarm on")
    void dispatchTopicMatchesTheConstants() {
        assertThat(Topics.dispatchTopic(Channel.SMS, TrafficClass.CRITICAL))
                .isEqualTo(Topics.DISPATCH_SMS_TX);
        assertThat(Topics.dispatchTopic(Channel.SMS, TrafficClass.TRANSACTIONAL))
                .isEqualTo(Topics.DISPATCH_SMS_TX);
        assertThat(Topics.dispatchTopic(Channel.SMS, TrafficClass.BULK))
                .isEqualTo(Topics.DISPATCH_SMS_BULK);
        assertThat(Topics.dispatchTopic(Channel.EMAIL, TrafficClass.TRANSACTIONAL))
                .isEqualTo(Topics.DISPATCH_EMAIL_TX);
        assertThat(Topics.dispatchTopic(Channel.EMAIL, TrafficClass.BULK))
                .isEqualTo(Topics.DISPATCH_EMAIL_BULK);
        assertThat(Topics.dispatchTopic(Channel.PUSH, TrafficClass.CRITICAL))
                .isEqualTo(Topics.DISPATCH_PUSH_TX);
        assertThat(Topics.dispatchTopic(Channel.PUSH, TrafficClass.BULK))
                .isEqualTo(Topics.DISPATCH_PUSH_BULK);
    }

    @Test
    @DisplayName("CRITICAL and TRANSACTIONAL share the tx lane, and BULK never joins them — "
            + "mixing a 10M blast into the OTP lane is a 37-second password reset")
    void bulkIsPhysicallySeparateFromEveryTransactionalLane() {
        for (Channel channel : Channel.values()) {
            String critical = Topics.dispatchTopic(channel, TrafficClass.CRITICAL);
            String transactional = Topics.dispatchTopic(channel, TrafficClass.TRANSACTIONAL);
            String bulk = Topics.dispatchTopic(channel, TrafficClass.BULK);
            assertThat(critical).isEqualTo(transactional);
            assertThat(bulk).isNotEqualTo(critical);
        }
    }

    @Test
    @DisplayName("every dispatch topic the enum can produce is a topic we declared — an undeclared "
            + "name is an outage, because auto.create.topics.enable is false")
    void everyChannelClassPairResolvesToADeclaredTopic() {
        for (Channel channel : Channel.values()) {
            for (TrafficClass trafficClass : TrafficClass.values()) {
                assertThat(Topics.isKnown(Topics.dispatchTopic(channel, trafficClass))).isTrue();
            }
        }
    }

    @Test
    @DisplayName("a typo'd topic name is not silently accepted as one of ours")
    void isKnownRejectsATypo() {
        assertThat(Topics.isKnown("notification.dispatch.sms.txt")).isFalse();
        assertThat(Topics.isKnown("notification.retry.5m")).isFalse();
    }

    @Test
    @DisplayName("the topic set is exactly the 16 in docs/KAFKA.md, with no duplicates")
    void theTopicSetIsClosedAtSixteen() {
        assertThat(Topics.all()).hasSize(16).doesNotHaveDuplicates();
        assertThat(Topics.dispatchTopics()).hasSize(6);
        assertThat(Topics.retryTopics()).hasSize(5);
        assertThat(Topics.all()).containsAll(Topics.dispatchTopics())
                .containsAll(Topics.retryTopics());
    }

    @Test
    @DisplayName("the topic list is immutable — a caller mutating it would change routing at runtime")
    void theTopicListCannotBeMutatedByACaller() {
        assertThatThrownBy(() -> Topics.all().add("notification.rogue"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
