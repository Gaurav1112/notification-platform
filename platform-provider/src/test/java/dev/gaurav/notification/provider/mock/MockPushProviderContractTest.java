package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.ProviderContractTest;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** FCM v1 + APNs semantics on top of the shared SPI contract. */
class MockPushProviderContractTest extends ProviderContractTest {

    private static final ProviderCode CODE = ProviderCode.of("mock-push-primary");

    /** Fixed, so the APNs 410 timestamp is reproducible and two identical sends compare equal. */
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-08-31T09:00:00Z"), ZoneOffset.UTC);

    private final ChaosState chaos = new ChaosState();

    private final MockProviderProperties properties = MockProviderProperties.defaults()
            .with(CODE, MockProviderProperties.ProviderSettings.defaults().withSuccessRate(0.5));

    private final MockPushProvider provider =
            new MockPushProvider(CODE, chaos, new FailureInjector(properties), Sleeper.NONE, 0L, CLOCK);

    @Override
    protected NotificationProvider provider() {
        return provider;
    }

    @Override
    protected String validAddress() {
        return "fcm:dGVzdC1kZXZpY2UtdG9rZW4tMDAx";
    }

    @Override
    protected NotificationProvider slowVariant() {
        var slow = MockProviderProperties.defaults().with(CODE,
                MockProviderProperties.ProviderSettings.defaults()
                        .withSuccessRate(1.0)
                        .withLatency(MockProviderProperties.Latency.of(Duration.ofSeconds(2), Duration.ofSeconds(3))));
        return new MockPushProvider(CODE, new ChaosState(), new FailureInjector(slow), Sleeper.REAL, 0L, CLOCK);
    }

    @Override
    protected void forceHardDown() {
        chaos.setMode(CODE, ChaosState.Mode.HARD_DOWN, Duration.ofMinutes(2));
    }

    @Test
    @DisplayName("FCM has no batch endpoint since 2024-06-21, so maxBatchSize is 1 and the fan-out cost is honest")
    void thereIsNoBatchEndpoint() {
        var capabilities = provider.capabilities();

        assertThat(capabilities.supportsBatching())
                .as("/batch was deprecated 2023-06-21 and stopped working 2024-06-21; "
                        + "sendEachForMulticast fans out to individual HTTP/2 requests")
                .isFalse();
        assertThat(capabilities.maxBatchSize())
                .as("pretending 500 tokens is one request hides the connection-pool cost that actually breaks")
                .isEqualTo(1);
        assertThat(provider.channel()).isEqualTo(Channel.PUSH);
    }

    @Test
    @DisplayName("apns-id is correlation only — APNs does not deduplicate on it, so we cannot claim an idempotency key")
    void apnsIdIsNotAnIdempotencyKey() {
        // Send the same apns-id twice and the device shows two notifications. Reporting true here
        // would let the platform skip its own deduplication and ship duplicate pushes.
        assertThat(provider.capabilities().supportsIdempotencyKey()).isFalse();
        assertThat(provider.capabilities().supportsStatusQuery()).isFalse();
        assertThat(provider.capabilities().supportsWebhook()).isTrue();
    }

    @Test
    @DisplayName("UNREGISTERED deletes the token and carries the instant it went invalid, so a re-registered device is not unsubscribed")
    void unregisteredDeactivatesTheToken() {
        var rejected = rejectionFor(FailureProfile.DEVICE_UNREGISTERED);

        assertThat(rejected.code()).isEqualTo("UNREGISTERED");
        assertThat(rejected.type()).isEqualTo(FailureType.DEVICE_UNREGISTERED);
        assertThat(rejected.type().shouldSuppressAddress()).isTrue();
        assertThat(rejected.type().isRetryable()).isFalse();
        assertThat(rejected.message())
                .as("the APNs 410 timestamp is what stops us deleting a token the device re-issued afterwards")
                .contains("token invalid since 2026-08-31T0");
    }

    @Test
    @DisplayName("a 429 never comes back with less than 60 seconds, because FCM says so and retrying sooner makes it worse")
    void quotaExceededHonoursTheOneMinuteFloor() {
        var rejected = rejectionFor(FailureProfile.RATE_LIMITED);

        assertThat(rejected.code()).isEqualTo("QUOTA_EXCEEDED");
        assertThat(rejected.retryAfter()).isPresent();
        assertThat(rejected.retryAfter().orElseThrow())
                .isGreaterThanOrEqualTo(MockPushProvider.MINIMUM_RETRY_AFTER);
    }

    @Test
    @DisplayName("a 503 also carries the one-minute floor, so an overloaded FCM is not hammered by 40 pods")
    void unavailableAlsoBacksOffAMinute() {
        var rejected = rejectionFor(FailureProfile.PROVIDER_5XX);

        assertThat(rejected.code()).isEqualTo("UNAVAILABLE");
        assertThat(rejected.type().isRetryable()).isTrue();
        assertThat(rejected.retryAfter().orElseThrow())
                .isGreaterThanOrEqualTo(MockPushProvider.MINIMUM_RETRY_AFTER);
    }

    @Test
    @DisplayName("an oversized payload is our bug and goes straight to the DLQ instead of failing over")
    void payloadTooLargeIsNotTheProvidersFault() {
        var rejected = rejectionFor(FailureProfile.PAYLOAD_TOO_LARGE);

        assertThat(rejected.type()).isEqualTo(FailureType.PAYLOAD_TOO_LARGE);
        assertThat(rejected.type().isRetryable()).isFalse();
        assertThat(rejected.type().shouldSuppressAddress())
                .as("the device is fine; it is our message that is wrong")
                .isFalse();
        assertThat(rejected.type().failoverAfterAttempts())
                .as("no other push provider will accept 4 KB + 1 either")
                .isEqualTo(-1);
    }

    private SendResult.Rejected rejectionFor(FailureProfile profile) {
        var forced = MockProviderProperties.defaults().with(CODE,
                MockProviderProperties.ProviderSettings.defaults()
                        .withSuccessRate(0.0)
                        .withFailures(List.of(new MockProviderProperties.WeightedProfile(profile, 1))));
        var pinned = new MockPushProvider(CODE, new ChaosState(), new FailureInjector(forced),
                Sleeper.NONE, 0L, CLOCK);
        return (SendResult.Rejected) pinned.send(command(UUID.randomUUID()));
    }
}
