package dev.gaurav.notification.application.usecase;

import dev.gaurav.notification.application.fake.FakeDeliveryStatusWriter;
import dev.gaurav.notification.application.port.DeliveryStatusWriter.StatusTransition;
import dev.gaurav.notification.domain.enums.DeliveryStatus;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The status path is at-least-once and out-of-order by construction. These are the events that
 * must be dropped, and the one thing that must happen when they are.
 */
class ApplyDeliveryStatusUseCaseTest {

    private static final String TENANT = "acme";
    private static final Instant CREATED_AT = Instant.parse("2026-08-31T09:00:00Z");
    private static final UUID NOTIFICATION_ID = UUID.randomUUID();
    private static final UUID RECIPIENT_ID = UUID.randomUUID();

    @Test
    @DisplayName("a late SENT does not overwrite DELIVERED, and the drop is recorded rather than thrown")
    void lateSentDoesNotOverwriteDelivered() {
        var writer = new FakeDeliveryStatusWriter().startingAt(RECIPIENT_ID, DeliveryStatus.DELIVERED);
        var useCase = new ApplyDeliveryStatusUseCase(writer);

        boolean applied = useCase.apply(transition(DeliveryStatus.SENT, "late-sent"));

        assertThat(applied).isFalse();
        assertThat(writer.statusOf(RECIPIENT_ID)).isEqualTo(DeliveryStatus.DELIVERED);
        // Zero rows is not an error. It is an audit record — the webhook we correctly ignored.
        assertThat(writer.unapplied())
                .singleElement()
                .satisfies(u -> assertThat(u.reason()).isEqualTo(ApplyDeliveryStatusUseCase.REASON_NOT_MONOTONIC));
    }

    @Test
    @DisplayName("a duplicate DELIVERED webhook is dropped instead of becoming a DLQ entry and a page")
    void duplicateWebhookIsDropped() {
        var writer = new FakeDeliveryStatusWriter().startingAt(RECIPIENT_ID, DeliveryStatus.DELIVERED);
        var useCase = new ApplyDeliveryStatusUseCase(writer);

        assertThat(useCase.apply(transition(DeliveryStatus.DELIVERED, "dup"))).isFalse();
        assertThat(writer.unapplied()).hasSize(1);
    }

    @Test
    @DisplayName("a hard bounce after DELIVERED is applied, because DELIVERED is deliberately not terminal")
    void bounceAfterDeliveredIsApplied() {
        var writer = new FakeDeliveryStatusWriter().startingAt(RECIPIENT_ID, DeliveryStatus.DELIVERED);
        var useCase = new ApplyDeliveryStatusUseCase(writer);

        assertThat(useCase.apply(transition(DeliveryStatus.BOUNCED, "bounce"))).isTrue();
        assertThat(writer.statusOf(RECIPIENT_ID)).isEqualTo(DeliveryStatus.BOUNCED);
        // An applied event is not an unapplied one; a fake that recorded both would hide a real bug.
        assertThat(writer.unapplied()).isEmpty();
    }

    @Test
    @DisplayName("nothing is applied after a terminal status, so a DLQ replay is harmless")
    void nothingAppliesAfterTerminal() {
        var writer = new FakeDeliveryStatusWriter().startingAt(RECIPIENT_ID, DeliveryStatus.BOUNCED);
        var useCase = new ApplyDeliveryStatusUseCase(writer);

        assertThat(useCase.apply(transition(DeliveryStatus.COMPLAINED, "replay"))).isFalse();
        assertThat(writer.statusOf(RECIPIENT_ID)).isEqualTo(DeliveryStatus.BOUNCED);
    }

    private static StatusTransition transition(DeliveryStatus proposed, String dedup) {
        return new StatusTransition(
                TENANT,
                NOTIFICATION_ID,
                CREATED_AT,
                RECIPIENT_ID,
                CREATED_AT,
                proposed,
                Instant.parse("2026-08-31T09:14:41Z"),
                "mock-email-primary",
                null,
                null,
                dedup.getBytes(StandardCharsets.UTF_8));
    }
}
