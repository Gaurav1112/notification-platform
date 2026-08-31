package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.ProviderContractTest;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Twilio semantics on top of the shared SPI contract. */
class MockSmsProviderContractTest extends ProviderContractTest {

    private static final ProviderCode CODE = ProviderCode.of("mock-sms-primary");

    private final ChaosState chaos = new ChaosState();

    /** Half the sends fail, so the failure-mapping paths are actually walked by the contract run. */
    private final MockProviderProperties properties = MockProviderProperties.defaults()
            .with(CODE, MockProviderProperties.ProviderSettings.defaults().withSuccessRate(0.5));

    private final MockSmsProvider provider =
            new MockSmsProvider(CODE, chaos, new FailureInjector(properties), Sleeper.NONE);

    @Override
    protected NotificationProvider provider() {
        return provider;
    }

    @Override
    protected String validAddress() {
        return "+14155550123";
    }

    @Override
    protected NotificationProvider slowVariant() {
        var slow = MockProviderProperties.defaults().with(CODE,
                MockProviderProperties.ProviderSettings.defaults()
                        .withSuccessRate(1.0)
                        .withLatency(MockProviderProperties.Latency.of(Duration.ofSeconds(2), Duration.ofSeconds(3))));
        return new MockSmsProvider(CODE, new ChaosState(), new FailureInjector(slow), Sleeper.REAL);
    }

    @Override
    protected void forceHardDown() {
        chaos.setMode(CODE, ChaosState.Mode.HARD_DOWN, Duration.ofMinutes(2));
    }

    @Test
    @DisplayName("Twilio has no client idempotency key, which is why the UNKNOWN path has to exist at all")
    void noClientIdempotencyKey() {
        // If this ever flips to true, the reconciler, the SentTokenLog and the Indeterminate case
        // all become dead code — and someone will delete them. It is false because Twilio's
        // Messages resource genuinely has no client-reference field.
        assertThat(provider.capabilities().supportsIdempotencyKey()).isFalse();
        assertThat(provider.capabilities().supportsStatusQuery()).isFalse();
        assertThat(provider.capabilities().supportsWebhook())
                .as("StatusCallback is the only way we ever learn a message was delivered")
                .isTrue();
        assertThat(provider.channel()).isEqualTo(Channel.SMS);
    }

    @Test
    @DisplayName("21610 is classified UNSUBSCRIBED and never retried — re-texting a STOP is a regulatory problem")
    void unsubscribedIsPermanent() {
        var result = sendTo("+15005550004");

        assertThat(result).isInstanceOf(SendResult.Rejected.class);
        var rejected = (SendResult.Rejected) result;
        assertThat(rejected.code()).isEqualTo("21610");
        assertThat(rejected.type()).isEqualTo(FailureType.UNSUBSCRIBED);
        assertThat(rejected.type().isRetryable()).isFalse();
        assertThat(rejected.type().shouldSuppressAddress())
                .as("the number must reach the suppression list or the next campaign texts them again")
                .isTrue();
    }

    @Test
    @DisplayName("21614 deactivates the address rather than burning five retries on a number that does not exist")
    void invalidNumberIsPermanentAndSuppresses() {
        var rejected = (SendResult.Rejected) sendTo("+15005550009");

        assertThat(rejected.code()).isEqualTo("21614");
        assertThat(rejected.type()).isEqualTo(FailureType.INVALID_RECIPIENT);
        assertThat(rejected.type().isPermanent()).isTrue();
        assertThat(rejected.type().failoverAfterAttempts())
                .as("-1: no other provider can deliver to a number that is not real")
                .isEqualTo(-1);
    }

    @Test
    @DisplayName("20429 carries a Retry-After, because guessing the backoff is how you stay rate limited")
    void rateLimitedCarriesRetryAfter() {
        var rejected = rejectionFor(FailureProfile.RATE_LIMITED);

        assertThat(rejected.code()).isEqualTo("20429");
        assertThat(rejected.type()).isEqualTo(FailureType.RATE_LIMITED);
        assertThat(rejected.retryAfter()).isPresent();
        assertThat(rejected.type().shouldFailoverImmediately())
                .as("a throttled primary should hand the message to the secondary, not sit on it")
                .isTrue();
    }

    @Test
    @DisplayName("30003 is transient, so an unreachable handset is retried instead of written off")
    void unreachableHandsetIsRetryable() {
        var rejected = rejectionFor(FailureProfile.TRANSIENT_NETWORK);

        assertThat(rejected.code()).isEqualTo("30003");
        assertThat(rejected.type().isRetryable()).isTrue();
        assertThat(rejected.type().isPermanent()).isFalse();
    }

    @Test
    @DisplayName("a 5xx is the provider's fault and stays retryable, unlike a 4xx which is ours")
    void serverErrorsAreRetryable() {
        var rejected = rejectionFor(FailureProfile.PROVIDER_5XX);

        assertThat(rejected.code()).isEqualTo("50000");
        assertThat(rejected.type()).isEqualTo(FailureType.PROVIDER_5XX);
        assertThat(rejected.type().isRetryable()).isTrue();
    }

    @Test
    @DisplayName("a Twilio message SID keeps its SM + 32 hex shape, so a support ticket quoting one resolves")
    void messageIdLooksLikeATwilioSid() {
        SendResult.Accepted accepted = null;
        for (var i = 0; i < 200 && accepted == null; i++) {
            var result = provider.send(command(new UUID(i, i)));
            if (result instanceof SendResult.Accepted a) accepted = a;
        }
        assertThat(accepted).isNotNull();
        assertThat(accepted.providerMessageId()).matches("SM[0-9a-f]{32}");
    }

    private SendResult sendTo(String address) {
        var recipientId = UUID.nameUUIDFromBytes(address.getBytes(StandardCharsets.UTF_8));
        return provider.send(new SendCommand(recipientId, Channel.SMS, TrafficClass.TRANSACTIONAL,
                address, null, "code 314159", "tok-" + address, Map.of(), Duration.ofSeconds(5)));
    }

    /** Forces one specific profile by weighting the mix entirely onto it. */
    private SendResult.Rejected rejectionFor(FailureProfile profile) {
        var forced = MockProviderProperties.defaults().with(CODE,
                MockProviderProperties.ProviderSettings.defaults()
                        .withSuccessRate(0.0)
                        .withFailures(List.of(new MockProviderProperties.WeightedProfile(profile, 1))));
        var pinned = new MockSmsProvider(CODE, new ChaosState(), new FailureInjector(forced), Sleeper.NONE);
        return (SendResult.Rejected) pinned.send(command(UUID.randomUUID()));
    }
}
