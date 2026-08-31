package dev.gaurav.notification.worker;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.domain.enums.Priority;
import dev.gaurav.notification.domain.enums.ScheduleType;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.messaging.event.NotificationDispatchEvent;
import dev.gaurav.notification.messaging.event.NotificationRequestedEvent;
import dev.gaurav.notification.messaging.event.RetryScheduledEvent;
import dev.gaurav.notification.messaging.topic.RetryTier;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Valid sample events for the worker's listener tests, so each test names one failure instead of
 * twenty-three constructor arguments.
 *
 * <p>Duplicated from {@code platform-messaging}'s own fixtures rather than shared: test classes are
 * not published as a test-jar, and adding one to make a fixture visible would put test scaffolding
 * on the dependency graph of every module that consumes messaging.
 */
public final class TestDispatchEvents {

    public static final long TENANT = 42L;
    public static final Instant NOW = Instant.parse("2026-08-31T09:15:00Z");
    public static final UUID NOTIFICATION_ID = UUID.fromString("018f3c2a-0000-7000-8000-00000000000a");
    public static final UUID RECIPIENT_ID = UUID.fromString("018f3c2a-0000-7000-8000-00000000000b");
    public static final UUID REQUEST_ID = UUID.fromString("018f3c2a-0000-7000-8000-00000000000c");

    private TestDispatchEvents() {
    }

    public static NotificationRequestedEvent requested() {
        return new NotificationRequestedEvent(
                UUID.randomUUID(), NOW, TENANT, "00-trace-span-01",
                REQUEST_ID, NOW, "order-9911",
                Set.of(Channel.SMS), TrafficClass.CRITICAL, Priority.P0_URGENT,
                ScheduleType.IMMEDIATE, null, NOW.plusSeconds(600),
                "otp.login", "en-GB", Map.of("code", "884120"),
                null, List.of("user-1"), 1);
    }

    public static NotificationDispatchEvent dispatch() {
        return dispatch(RECIPIENT_ID);
    }

    public static NotificationDispatchEvent dispatch(UUID recipientId) {
        return new NotificationDispatchEvent(
                UUID.randomUUID(), NOW, TENANT, "00-trace-span-01",
                NOTIFICATION_ID, NOW, recipientId, REQUEST_ID,
                Channel.SMS, TrafficClass.CRITICAL, Priority.P0_URGENT,
                "Y2lwaGVydGV4dA==", "+4477***4210", "dek:user-1",
                null, "Ym9keQ==", null,
                "otp.login", "tok-018f3c2a-1", null,
                1, NOW.plusSeconds(600), Map.of());
    }

    /** Due now, so the tier republishes it rather than pausing the partition. */
    public static RetryScheduledEvent parked() {
        return new RetryScheduledEvent(
                UUID.randomUUID(), NOW, TENANT, "00-trace-span-01",
                RetryTier.T5S, NOW, 1, FailureType.PROVIDER_5XX, dispatch());
    }
}
