package dev.gaurav.notification.application.fake;

import dev.gaurav.notification.application.command.RecipientSelector;
import dev.gaurav.notification.application.command.Schedule;
import dev.gaurav.notification.application.command.SendNotificationCommand;
import dev.gaurav.notification.application.command.TemplateRef;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.Priority;
import dev.gaurav.notification.domain.enums.TrafficClass;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builders for a valid {@link SendNotificationCommand}, so each test states only the one field it
 * is about.
 *
 * <p>The fingerprint is derived from a caller-supplied string rather than hashed from the command,
 * because these tests need to control the "same key, different body" case directly — which is the
 * exact case the real edge computes over raw request bytes.
 */
public final class TestCommands {

    public static final String TENANT = "acme";

    private TestCommands() {}

    /** A two-channel transactional send to two users. */
    public static SendNotificationCommand send(String idempotencyKey, String body) {
        return send(idempotencyKey, body, Set.of(Channel.EMAIL, Channel.PUSH));
    }

    public static SendNotificationCommand send(
            String idempotencyKey, String body, Set<Channel> channels) {
        return new SendNotificationCommand(
                TENANT,
                idempotencyKey,
                fingerprintOf(body),
                TrafficClass.TRANSACTIONAL,
                channels,
                Priority.P1_HIGH,
                new TemplateRef("order-shipped", "en-US"),
                null,
                RecipientSelector.userIds(List.of("u_9f2a", "u_7b31")),
                Map.of("orderId", "A-4821"),
                Schedule.immediate(),
                null,
                Map.of("campaignId", "cmp_2026q3"),
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    }

    /** Stands in for the SHA-256 the API computes over the raw bytes that arrived. */
    public static byte[] fingerprintOf(String body) {
        return body.getBytes(StandardCharsets.UTF_8);
    }
}
