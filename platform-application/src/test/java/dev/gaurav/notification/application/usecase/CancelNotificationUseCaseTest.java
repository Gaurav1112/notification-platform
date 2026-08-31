package dev.gaurav.notification.application.usecase;

import dev.gaurav.notification.application.exception.AlreadyDispatchedException;
import dev.gaurav.notification.application.exception.NotificationNotFoundException;
import dev.gaurav.notification.application.fake.FakeCancellationWriter;
import dev.gaurav.notification.application.fake.FakeDispatchTombstoneStore;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Cancellation is a race, and these are the three ways it can end. */
class CancelNotificationUseCaseTest {

    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-08-31T09:14:22Z"), ZoneOffset.UTC);
    private static final UUID ID = UUID.fromString("01998f2a-7c31-7a04-9e12-6f0b3c1d5a89");

    @Test
    @DisplayName("cancelling an already dispatched notification is a 409, not a silent success")
    void alreadyDispatchedIsAConflict() {
        var tombstones = new FakeDispatchTombstoneStore();
        var useCase = new CancelNotificationUseCase(
                FakeCancellationWriter.cancelling().alreadyDispatched(), tombstones, CLOCK);

        assertThatThrownBy(() -> useCase.cancel("acme", ID))
                .isInstanceOf(AlreadyDispatchedException.class);

        // No tombstone: promising the workers we cancelled something we did not would be worse
        // than the 409, because the caller would believe the message was stopped.
        assertThat(tombstones.tombstoned()).isEmpty();
    }

    @Test
    @DisplayName("an id that belongs to another tenant is a 404, never a 403 that confirms it exists")
    void unknownOrCrossTenantIsNotFound() {
        var useCase = new CancelNotificationUseCase(
                FakeCancellationWriter.cancelling().notFound(), new FakeDispatchTombstoneStore(), CLOCK);

        assertThatThrownBy(() -> useCase.cancel("other-tenant", ID))
                .isInstanceOf(NotificationNotFoundException.class);
    }

    @Test
    @DisplayName("a successful cancel writes the tombstone workers check before the provider call")
    void successfulCancelWritesTombstone() {
        var tombstones = new FakeDispatchTombstoneStore();
        var useCase = new CancelNotificationUseCase(
                FakeCancellationWriter.cancelling(), tombstones, CLOCK);

        useCase.cancel("acme", ID);

        assertThat(tombstones.tombstoned()).containsExactly(ID);
    }

    @Test
    @DisplayName("a Valkey outage does not undo a cancellation that already committed")
    void tombstoneFailureDoesNotFailTheCancel() {
        var writer = FakeCancellationWriter.cancelling();
        var useCase = new CancelNotificationUseCase(
                writer, new FakeDispatchTombstoneStore().broken(), CLOCK);

        assertThatCode(() -> useCase.cancel("acme", ID)).doesNotThrowAnyException();
        assertThat(writer.calls()).isEqualTo(1);
    }
}
