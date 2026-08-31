package dev.gaurav.notification.messaging.event;

import dev.gaurav.notification.domain.enums.AttemptState;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.domain.enums.Priority;
import dev.gaurav.notification.domain.enums.ScheduleType;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.messaging.topic.RetryTier;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Valid sample events, so each test names one failure instead of twenty constructor arguments. */
public final class TestEvents {

    public static final long TENANT = 42L;
    public static final Instant NOW = Instant.parse("2026-08-31T09:15:00Z");
    public static final UUID NOTIFICATION_ID =
            UUID.fromString("018f3c2a-0000-7000-8000-00000000000a");
    public static final UUID RECIPIENT_ID =
            UUID.fromString("018f3c2a-0000-7000-8000-00000000000b");
    public static final UUID REQUEST_ID =
            UUID.fromString("018f3c2a-0000-7000-8000-00000000000c");

    private TestEvents() {
    }

    public static NotificationRequestedEvent requested() {
        return new NotificationRequestedEvent(
                UUID.randomUUID(), NOW, TENANT, "00-trace-span-01",
                REQUEST_ID, NOW, "order-9911",
                Set.of(Channel.SMS), TrafficClass.CRITICAL, Priority.P0_URGENT,
                ScheduleType.IMMEDIATE, null, NOW.plusSeconds(60),
                "otp.login", "en-GB", Map.of("code", "884120"),
                null, List.of("user-1"), 1);
    }

    public static NotificationDispatchEvent dispatch() {
        return dispatch(TrafficClass.CRITICAL, Channel.SMS);
    }

    public static NotificationDispatchEvent dispatch(TrafficClass trafficClass, Channel channel) {
        return dispatch(trafficClass, channel, "00-trace-span-01");
    }

    /** The reconciler and the outbox sweeper have no inbound trace to continue. */
    public static NotificationDispatchEvent dispatchWithoutTrace() {
        return dispatch(TrafficClass.CRITICAL, Channel.SMS, null);
    }

    private static NotificationDispatchEvent dispatch(
            TrafficClass trafficClass, Channel channel, String traceparent) {
        return new NotificationDispatchEvent(
                UUID.randomUUID(), NOW, TENANT, traceparent,
                NOTIFICATION_ID, NOW, RECIPIENT_ID, REQUEST_ID,
                channel, trafficClass, Priority.P0_URGENT,
                "Y2lwaGVydGV4dA==", "+4477***4210", "dek:user-1",
                null, "Ym9keQ==", null,
                "otp.login", "tok-018f3c2a-1", null,
                1, NOW.plusSeconds(60), Map.of());
    }

    public static DeliveryStatusEvent status(DeliveryStatus status, long version) {
        return new DeliveryStatusEvent(
                UUID.randomUUID(), NOW, TENANT, "00-trace-span-01",
                NOTIFICATION_ID, NOW, RECIPIENT_ID, Channel.SMS,
                status, version, 1, AttemptState.SUCCEEDED,
                "mock-sms-primary", "SM0123456789", null, null, null, null,
                750L, 231L);
    }

    public static RetryScheduledEvent retry() {
        return new RetryScheduledEvent(
                UUID.randomUUID(), NOW, TENANT, "00-trace-span-01",
                RetryTier.T5S, NOW.plusSeconds(5), 1,
                FailureType.PROVIDER_5XX, dispatch());
    }

    public static DeadLetterEvent deadLetter() {
        return new DeadLetterEvent(
                UUID.randomUUID(), NOW, TENANT, "00-trace-span-01",
                "notification.dispatch.sms.tx", 3, 918_273L, "42|" + RECIPIENT_ID + "|SMS",
                "dispatch-sms-tx", NOTIFICATION_ID, RECIPIENT_ID,
                FailureType.TEMPLATE_ERROR, "java.lang.IllegalStateException",
                "no such variable: code", "stack", 3, 0, "{\"eventType\":\"broken\"}");
    }
}
