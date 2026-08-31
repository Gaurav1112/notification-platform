package dev.gaurav.notification.application.usecase;

import dev.gaurav.notification.application.exception.NotificationNotFoundException;
import dev.gaurav.notification.application.fake.FakeNotificationQuery;
import dev.gaurav.notification.application.result.DeliveryAttemptView;
import dev.gaurav.notification.application.result.NotificationStatusView;
import dev.gaurav.notification.domain.enums.AttemptState;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.TrafficClass;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The read side leaks two things if you let it: other tenants' data, and the database. */
class GetNotificationStatusUseCaseTest {

    private static final Instant CREATED_AT = Instant.parse("2026-08-31T09:14:22.481Z");
    private static final UUID ID = UUID.fromString("01998f2a-7c31-7a04-9e12-6f0b3c1d5a89");

    @Test
    @DisplayName("another tenant's notification is a 404, not a 403 that confirms the id is real")
    void crossTenantReadIsNotFound() {
        var query = new FakeNotificationQuery().with("acme", view(2, 1, 0, 1, 0));
        var useCase = new GetNotificationStatusUseCase(query);

        assertThat(useCase.status("acme", ID)).isNotNull();
        assertThatThrownBy(() -> useCase.status("competitor", ID))
                .isInstanceOf(NotificationNotFoundException.class);
    }

    @Test
    @DisplayName("an unknown id on the attempts endpoint is a 404, not an empty list that reads "
            + "as 'we never tried'")
    void attemptsOnUnknownIdIsNotFound() {
        var useCase = new GetNotificationStatusUseCase(new FakeNotificationQuery());

        assertThatThrownBy(() -> useCase.attempts("acme", ID, 50))
                .isInstanceOf(NotificationNotFoundException.class);
    }

    @Test
    @DisplayName("an unbounded attempts request is clamped, so a polling client cannot read every "
            + "attempt of a 64-retry failover")
    void attemptLimitIsClamped() {
        var attempts = IntStream.rangeClosed(1, 400).mapToObj(GetNotificationStatusUseCaseTest::attempt).toList();
        var query = new FakeNotificationQuery()
                .with("acme", view(1, 0, 1, 0, 0))
                .withAttempts("acme", ID, attempts);
        var useCase = new GetNotificationStatusUseCase(query);

        var page = useCase.attempts("acme", ID, Integer.MAX_VALUE);

        assertThat(page).hasSize(GetNotificationStatusUseCase.MAX_ATTEMPTS_RETURNED);
        assertThat(query.requestedLimits())
                .containsExactly(GetNotificationStatusUseCase.MAX_ATTEMPTS_RETURNED);
    }

    @Test
    @DisplayName("a campaign where some recipients bounced is PARTIALLY_COMPLETED, not flattened "
            + "into a success or a failure")
    void partialOutcomeIsNotFlattened() {
        var partial = view(10, 7, 2, 1, 0);

        assertThat(partial.aggregateStatus()).isEqualTo("PARTIALLY_COMPLETED");
        assertThat(view(10, 10, 0, 0, 0).aggregateStatus()).isEqualTo("COMPLETED");
        assertThat(view(10, 0, 10, 0, 0).aggregateStatus()).isEqualTo("FAILED");
        assertThat(view(10, 3, 0, 0, 7).aggregateStatus()).isEqualTo("IN_PROGRESS");
    }

    private static NotificationStatusView view(
            int total, int delivered, int failed, int suppressed, int pending) {
        return new NotificationStatusView(
                ID, UUID.randomUUID(), Channel.EMAIL, TrafficClass.TRANSACTIONAL,
                DeliveryStatus.SENT, CREATED_AT, null, CREATED_AT.plusMillis(621), null,
                new NotificationStatusView.Counts(total, delivered, failed, suppressed, pending));
    }

    private static DeliveryAttemptView attempt(int number) {
        return new DeliveryAttemptView(
                number, "mock-email-primary", AttemptState.FAILED, null, null, null,
                212, null, CREATED_AT, CREATED_AT.plusMillis(212), null);
    }
}
