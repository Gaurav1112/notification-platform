package dev.gaurav.notification.application.command;

import dev.gaurav.notification.application.exception.ValidationException;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.Priority;
import dev.gaurav.notification.domain.enums.ScheduleType;
import dev.gaurav.notification.domain.enums.TrafficClass;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The command's compact constructor is the only validation gate on the accept path. Each test here
 * is a request that would otherwise be accepted and then quietly do the wrong thing.
 */
class SendNotificationCommandTest {

    private static final Instant NOW = Instant.parse("2026-08-31T09:14:22.481Z");

    @Test
    @DisplayName("a request with no channel is rejected, not accepted and delivered to nobody")
    void noChannelIsRejected() {
        assertThatThrownBy(() -> command(b -> b.channels = Set.of()))
                .isInstanceOf(ValidationException.class)
                .satisfies(e -> assertThat(((ValidationException) e).typeSlug()).isEqualTo("validation-failed"));
    }

    @Test
    @DisplayName("template and content together is rejected, because the two would eventually disagree")
    void templateAndContentTogetherIsRejected() {
        assertThatThrownBy(() -> command(b -> b.content = new InlineContent("Hi", "body")))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    @DisplayName("neither template nor content is rejected, because there would be nothing to send")
    void neitherTemplateNorContentIsRejected() {
        assertThatThrownBy(() -> command(b -> b.template = null))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    @DisplayName("SCHEDULED without a sendAt is rejected rather than sitting in the hydrator forever")
    void scheduledWithoutSendAtIsRejected() {
        assertThatThrownBy(() -> new Schedule(ScheduleType.SCHEDULED, null))
                .isInstanceOf(ValidationException.class)
                .satisfies(e -> assertThat(((ValidationException) e).typeSlug()).isEqualTo("schedule-invalid"));
    }

    @Test
    @DisplayName("a sendAt in the past is rejected — the due scanner would fire it on its next pass")
    void pastSendAtIsRejected() {
        assertThatThrownBy(() -> Schedule.at(Instant.now().minus(Duration.ofHours(1))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("in the past");
    }

    @Test
    @DisplayName("a sendAt beyond the one-year horizon is rejected, because retention would drop "
            + "the partition before it fires")
    void sendAtBeyondHorizonIsRejected() {
        assertThatThrownBy(() -> Schedule.at(Instant.now().plus(Duration.ofDays(400))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("horizon");
    }

    @Test
    @DisplayName("IMMEDIATE with a sendAt is rejected instead of silently ignoring the time")
    void immediateWithSendAtIsRejected() {
        assertThatThrownBy(() -> new Schedule(ScheduleType.IMMEDIATE, Instant.now().plusSeconds(3600)))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    @DisplayName("TTL runs from sendAt, not from acceptance, so a deferred send does not expire "
            + "before it is due")
    void ttlOnADeferredSendRunsFromSendAt() {
        var sendAt = Instant.now().plus(Duration.ofDays(7));
        var deferred = command(b -> {
            b.schedule = Schedule.at(sendAt);
            b.ttlSeconds = 3600;
        });

        assertThat(deferred.expiresAt(NOW)).isEqualTo(sendAt.plusSeconds(3600));
    }

    @Test
    @DisplayName("an omitted TTL falls back to the traffic class default rather than to no expiry")
    void omittedTtlUsesTheTrafficClassDefault() {
        var critical = command(b -> b.trafficClass = TrafficClass.CRITICAL);

        assertThat(critical.expiresAt(NOW)).isEqualTo(NOW.plus(TrafficClass.CRITICAL.defaultTtl()));
    }

    @Test
    @DisplayName("an inline recipient list above the claim-check threshold is rejected rather than "
            + "producing a Kafka record the broker will refuse")
    void oversizedInlineListIsRejected() {
        var tooMany = IntStream.rangeClosed(0, RecipientSelector.MAX_INLINE)
                .mapToObj(i -> "u_" + i)
                .toList();

        assertThatThrownBy(() -> RecipientSelector.userIds(tooMany))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("S3_MANIFEST");
    }

    @Test
    @DisplayName("a manifest audience with no declared count is rejected, because the 202 could "
            + "not report a recipient count without reading S3 first")
    void manifestWithoutCountIsRejected() {
        assertThatThrownBy(() -> RecipientSelector.manifest("s3://bucket/audience.ndjson", 0))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    @DisplayName("the fingerprint accessor hands back a copy, so a caller cannot mutate the "
            + "fingerprint a claim was made with")
    void fingerprintIsDefensivelyCopied() {
        var original = "{\"orderId\":\"A-4821\"}".getBytes(StandardCharsets.UTF_8);
        var cmd = command(b -> b.fingerprint = original);

        original[0] = 'X';
        cmd.requestFingerprint()[1] = 'Y';

        assertThat(cmd.requestFingerprint()).isEqualTo("{\"orderId\":\"A-4821\"}".getBytes(StandardCharsets.UTF_8));
    }

    // --- builder -------------------------------------------------------------------------------

    private static final class Fields {
        String tenantId = "acme";
        String idempotencyKey = "order-4821";
        byte[] fingerprint = "body".getBytes(StandardCharsets.UTF_8);
        TrafficClass trafficClass = TrafficClass.TRANSACTIONAL;
        Set<Channel> channels = Set.of(Channel.EMAIL);
        TemplateRef template = new TemplateRef("order-shipped", "en-US");
        InlineContent content = null;
        RecipientSelector recipients = RecipientSelector.userIds(List.of("u_9f2a"));
        Schedule schedule = Schedule.immediate();
        Integer ttlSeconds = null;
    }

    private static SendNotificationCommand command(Consumer<Fields> customise) {
        var f = new Fields();
        customise.accept(f);
        return new SendNotificationCommand(
                f.tenantId, f.idempotencyKey, f.fingerprint, f.trafficClass, f.channels,
                Priority.P2_NORMAL, f.template, f.content, f.recipients, Map.of(), f.schedule,
                f.ttlSeconds, Map.of(), null);
    }
}
