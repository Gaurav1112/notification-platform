package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.ProviderContractTest;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Amazon SES semantics on top of the shared SPI contract. */
class MockEmailProviderContractTest extends ProviderContractTest {

    private static final ProviderCode CODE = ProviderCode.of("mock-email-primary");

    private final ChaosState chaos = new ChaosState();

    private final MockProviderProperties properties = MockProviderProperties.defaults()
            .with(CODE, MockProviderProperties.ProviderSettings.defaults().withSuccessRate(0.5));

    private final MockEmailProvider provider =
            new MockEmailProvider(CODE, chaos, new FailureInjector(properties), Sleeper.NONE);

    @Override
    protected NotificationProvider provider() {
        return provider;
    }

    @Override
    protected String validAddress() {
        return "recipient@example.com";
    }

    @Override
    protected NotificationProvider slowVariant() {
        var slow = MockProviderProperties.defaults().with(CODE,
                MockProviderProperties.ProviderSettings.defaults()
                        .withSuccessRate(1.0)
                        .withLatency(MockProviderProperties.Latency.of(Duration.ofSeconds(2), Duration.ofSeconds(3))));
        return new MockEmailProvider(CODE, new ChaosState(), new FailureInjector(slow), Sleeper.REAL);
    }

    @Override
    protected void forceHardDown() {
        chaos.setMode(CODE, ChaosState.Mode.HARD_DOWN, Duration.ofMinutes(2));
    }

    @Test
    @DisplayName("the batch limit is 50 because SendBulkEmail rejects 51 — it is an API limit, not a tuning knob")
    void batchingIsRealAndCappedAtFifty() {
        var capabilities = provider.capabilities();

        assertThat(capabilities.supportsBatching()).isTrue();
        assertThat(capabilities.maxBatchSize()).isEqualTo(MockEmailProvider.SES_BULK_DESTINATION_LIMIT);
        assertThat(capabilities.maxBatchSize())
                .as("a chunker that reads a configured size instead of this one will eventually send 51")
                .isEqualTo(50);
        assertThat(provider.channel()).isEqualTo(Channel.EMAIL);
    }

    @Test
    @DisplayName("SES gives a correlatable MessageId, so an UNKNOWN attempt can be resolved without a duplicate send")
    void statusIsQueryableEvenWithoutAnIdempotencyKey() {
        // The distinction that matters: SES cannot deduplicate a retried SendEmail (two emails),
        // but the MessageId it returns is echoed on every event-destination notification, so the
        // reconciler can find out what happened. Twilio offers neither.
        assertThat(provider.capabilities().supportsIdempotencyKey()).isFalse();
        assertThat(provider.capabilities().supportsStatusQuery()).isTrue();
    }

    @Test
    @DisplayName("a permanent bounce suppresses the address, because SES pauses an account above a 5% bounce rate")
    void permanentBounceSuppressesTheAddress() {
        var rejected = rejectionFor(FailureProfile.INVALID_RECIPIENT);

        assertThat(rejected.code()).isEqualTo("Bounce.Permanent.General");
        assertThat(rejected.type()).isEqualTo(FailureType.INVALID_RECIPIENT);
        assertThat(rejected.type().shouldSuppressAddress())
                .as("retrying a dead mailbox is how a sending domain gets shut off")
                .isTrue();
    }

    @Test
    @DisplayName("Throttling is retryable and carries a Retry-After, unlike AccountSendingPaused which is not")
    void throttlingAndQuotaAreClassifiedDifferently() {
        var throttled = rejectionFor(FailureProfile.RATE_LIMITED);
        assertThat(throttled.code()).isEqualTo("Throttling");
        assertThat(throttled.type().isRetryable()).isTrue();
        assertThat(throttled.retryAfter()).isPresent();

        var paused = rejectionFor(FailureProfile.QUOTA_EXCEEDED);
        assertThat(paused.code()).isEqualTo("AccountSendingPausedException");
        assertThat(paused.type().isRetryable())
                .as("no amount of waiting un-pauses an account; this has to fail over and page someone")
                .isFalse();
        assertThat(paused.type().shouldFailoverImmediately()).isTrue();
    }

    @Test
    @DisplayName("an unverified MAIL FROM domain is an auth failure, so it pages rather than silently retrying")
    void unverifiedDomainIsAnAuthFailure() {
        var rejected = rejectionFor(FailureProfile.AUTH_FAILURE);

        assertThat(rejected.code()).isEqualTo("MailFromDomainNotVerifiedException");
        assertThat(rejected.type()).isEqualTo(FailureType.AUTH_FAILURE);
        assertThat(rejected.type().isRetryable()).isFalse();
        assertThat(rejected.type().shouldFailoverImmediately()).isTrue();
    }

    private SendResult.Rejected rejectionFor(FailureProfile profile) {
        var forced = MockProviderProperties.defaults().with(CODE,
                MockProviderProperties.ProviderSettings.defaults()
                        .withSuccessRate(0.0)
                        .withFailures(List.of(new MockProviderProperties.WeightedProfile(profile, 1))));
        var pinned = new MockEmailProvider(CODE, new ChaosState(), new FailureInjector(forced), Sleeper.NONE);
        return (SendResult.Rejected) pinned.send(command(UUID.randomUUID()));
    }
}
