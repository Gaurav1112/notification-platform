package dev.gaurav.notification.messaging.event;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.domain.enums.Priority;
import dev.gaurav.notification.domain.enums.ScheduleType;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.messaging.topic.RetryTier;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Each of these rejects a message that would fail somewhere much further downstream. */
class EventInvariantsTest {

    @Test
    @DisplayName("a request carrying both an inline recipient list and an S3 claim check is "
            + "rejected — the expander would otherwise have to guess which one is authoritative")
    void aRequestCannotHaveTwoSourcesOfRecipients() {
        assertThatThrownBy(() -> new NotificationRequestedEvent(
                UUID.randomUUID(), TestEvents.NOW, 1L, null,
                TestEvents.REQUEST_ID, TestEvents.NOW, "k",
                Set.of(Channel.EMAIL), TrafficClass.BULK, Priority.P3_LOW,
                ScheduleType.IMMEDIATE, null, TestEvents.NOW.plusSeconds(60),
                "t", "en", Map.of(),
                "s3://bucket/manifest.jsonl", List.of("user-1"), 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one of");
    }

    @Test
    @DisplayName("a SCHEDULED request with no time is rejected at the event boundary, instead of "
            + "sitting in the hydrator forever waiting for a null")
    void aScheduledRequestWithoutATimeIsRejected() {
        assertThatThrownBy(() -> new NotificationRequestedEvent(
                UUID.randomUUID(), TestEvents.NOW, 1L, null,
                TestEvents.REQUEST_ID, TestEvents.NOW, "k",
                Set.of(Channel.EMAIL), TrafficClass.BULK, Priority.P3_LOW,
                ScheduleType.SCHEDULED, null, TestEvents.NOW.plusSeconds(3600),
                "t", "en", Map.of(),
                null, List.of("user-1"), 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scheduledAt");
    }

    @Test
    @DisplayName("a SEND_FAILED status with no failure type is rejected — an unclassified failure "
            + "forces the retry decision into the catch-all branch that retries dead numbers")
    void afailureStatusMustCarryAClassification() {
        assertThatThrownBy(() -> new DeliveryStatusEvent(
                UUID.randomUUID(), TestEvents.NOW, 1L, null,
                TestEvents.NOTIFICATION_ID, TestEvents.NOW, TestEvents.RECIPIENT_ID,
                Channel.SMS, DeliveryStatus.SEND_FAILED, 55, 1, null,
                "mock", null, null, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failureType");
    }

    @Test
    @DisplayName("a SUPPRESSED status must say why — an unattributed suppression is unauditable, "
            + "and the reason is what tells support whether the user opted out or hit a cap")
    void aSuppressionMustCarryItsReason() {
        assertThatThrownBy(() -> new DeliveryStatusEvent(
                UUID.randomUUID(), TestEvents.NOW, 1L, null,
                TestEvents.NOTIFICATION_ID, TestEvents.NOW, TestEvents.RECIPIENT_ID,
                Channel.SMS, DeliveryStatus.SUPPRESSED, 25, 0, null,
                null, null, null, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("suppressionReason");
    }

    @Test
    @DisplayName("a redelivered status event with the same version does not supersede — "
            + "greater-or-equal here double-counts every delivery metric on a rebalance")
    void anIdenticalVersionDoesNotSupersede() {
        var event = TestEvents.status(DeliveryStatus.DELIVERED, 80);

        assertThat(event.supersedes(79)).isTrue();
        assertThat(event.supersedes(80)).isFalse();
        assertThat(event.supersedes(85)).isFalse();
    }

    @Test
    @DisplayName("a retry cannot be scheduled for a permanent failure — retrying an invalid "
            + "number spends the whole 72-minute budget on a message that was dead on attempt one")
    void aPermanentFailureCannotBeScheduledForRetry() {
        assertThatThrownBy(() -> new RetryScheduledEvent(
                UUID.randomUUID(), TestEvents.NOW, 1L, null,
                RetryTier.T5S, TestEvents.NOW.plusSeconds(5), 1,
                FailureType.INVALID_RECIPIENT, TestEvents.dispatch()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not retryable");
    }

    @Test
    @DisplayName("a retry that would land after the notification's TTL is detectable before it "
            + "costs a provider call for a message the user no longer wants")
    void aRetryPastTheDeadlineIsVisible() {
        var dispatch = TestEvents.dispatch();
        var late = new RetryScheduledEvent(
                UUID.randomUUID(), TestEvents.NOW, 1L, null,
                RetryTier.T1H, TestEvents.NOW.plusSeconds(3600), 5,
                FailureType.PROVIDER_5XX, dispatch);

        assertThat(late.outlivesDeadline()).isTrue();
        assertThat(TestEvents.retry().outlivesDeadline()).isFalse();
    }

    @Test
    @DisplayName("a 40-minute-old OTP is dropped rather than sent — by then the user has already "
            + "requested another one, and the late arrival only causes confusion")
    void anExpiredDispatchIsDroppedNotSent() {
        var dispatch = TestEvents.dispatch();

        assertThat(dispatch.isExpiredAt(TestEvents.NOW.plusSeconds(59))).isFalse();
        assertThat(dispatch.isExpiredAt(TestEvents.NOW.plusSeconds(60))).isTrue();
        assertThat(dispatch.isExpiredAt(Instant.parse("2026-08-31T09:55:00Z"))).isTrue();
    }

    @Test
    @DisplayName("a dead letter stops being replayable at the ceiling — naive bulk replay of a "
            + "deterministic poison recreates the identical storm that produced it")
    void replayIsBoundedByACeiling() {
        var event = TestEvents.deadLetter();
        var exhausted = new DeadLetterEvent(
                event.eventId(), event.occurredAt(), event.tenantId(), event.traceparent(),
                event.sourceTopic(), event.sourcePartition(), event.sourceOffset(),
                event.sourceKey(), event.consumerGroup(), event.notificationId(),
                event.recipientId(), event.failureType(), event.exceptionClass(),
                event.exceptionMessage(), event.stackTrace(), event.attemptCount(),
                DeadLetterEvent.MAX_REPLAY_ATTEMPTS, event.payload());

        assertThat(event.isReplayable()).isTrue();
        assertThat(exhausted.isReplayable()).isFalse();
    }

    @Test
    @DisplayName("attempt numbers are 1-based, so attempt 0 cannot silently offset the retry tier "
            + "lookup and give a failed message a sixth attempt")
    void attemptNumbersAreOneBased() {
        assertThatThrownBy(() -> new NotificationDispatchEvent(
                UUID.randomUUID(), TestEvents.NOW, 1L, null,
                TestEvents.NOTIFICATION_ID, TestEvents.NOW, TestEvents.RECIPIENT_ID,
                TestEvents.REQUEST_ID, Channel.SMS, TrafficClass.CRITICAL, Priority.P0_URGENT,
                "Y2lwaGVy", "h", "dek", null, "Ym9keQ==", null, "t", "tok", null,
                0, TestEvents.NOW.plusSeconds(60), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1-based");
    }
}
