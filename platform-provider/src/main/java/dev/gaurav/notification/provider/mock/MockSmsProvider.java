package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.spi.ProviderCapabilities;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Twilio-shaped SMS adapter.
 *
 * <h2>The capability that shapes the whole platform: no client idempotency key</h2>
 *
 * <p>{@code supportsIdempotencyKey} and {@code supportsStatusQuery} are both <strong>false</strong>,
 * and that is not a simplification. Twilio's {@code Messages} resource genuinely has no
 * client-supplied reference field: you {@code POST} and you get back a {@code MessageSid} that
 * <em>Twilio</em> chose. If the response is lost — a socket reset, a pod eviction, a read timeout —
 * there is no key you can present to ask "did you already accept my message X?". You can list
 * messages by {@code To} and date range and guess, which is not an answer at scale when the same
 * number legitimately receives several messages a minute.
 *
 * <p>Everything downstream follows from that one fact: the {@code UNKNOWN} attempt state, the
 * reconciler, the {@link dev.gaurav.notification.provider.decorator.SentTokenLog}, and the choice
 * to model {@link SendResult.Indeterminate} as a first-class case rather than folding it into
 * failure. A mock that pretended Twilio had an idempotency key would delete the hardest and most
 * interesting part of the design.
 *
 * <h2>Error codes</h2>
 *
 * <table>
 *   <caption>Injected profile to Twilio wire representation</caption>
 *   <tr><th>Code</th><th>Twilio meaning</th><th>Classification</th></tr>
 *   <tr><td>21610</td><td>Attempt to send to an unsubscribed recipient (STOP)</td><td>UNSUBSCRIBED</td></tr>
 *   <tr><td>21614</td><td>'To' number is not a valid mobile number</td><td>INVALID_RECIPIENT</td></tr>
 *   <tr><td>20429</td><td>Too many requests</td><td>RATE_LIMITED, with Retry-After</td></tr>
 *   <tr><td>30003</td><td>Unreachable destination handset</td><td>TRANSIENT_NETWORK</td></tr>
 *   <tr><td>20003</td><td>Authentication failure</td><td>AUTH_FAILURE</td></tr>
 *   <tr><td>20005</td><td>Account suspended / out of balance</td><td>QUOTA_EXCEEDED</td></tr>
 *   <tr><td>30007</td><td>Carrier filtered as spam</td><td>CONTENT_REJECTED</td></tr>
 *   <tr><td>30006</td><td>Landline or unreachable carrier</td><td>INVALID_RECIPIENT</td></tr>
 *   <tr><td>50000</td><td>HTTP 5xx</td><td>PROVIDER_5XX</td></tr>
 * </table>
 *
 * <p>{@code 21610} and {@code 30007} matter most: both are <em>permanent</em>. Retrying an
 * unsubscribed number is a regulatory problem, not just a wasted send.
 */
public final class MockSmsProvider extends AbstractMockProvider {

    /**
     * Twilio's real magic test numbers, which behave identically on their live test credentials.
     * Having them here means an integration test can pin a specific failure without touching the
     * seed, which keeps everyone else's expected DLQ count stable.
     */
    private static final Map<String, FailureProfile> MAGIC_NUMBERS = Map.of(
            "+15005550001", FailureProfile.INVALID_RECIPIENT,   // 21211 invalid 'To'
            "+15005550003", FailureProfile.QUOTA_EXCEEDED,      // 21408 no permission / no balance
            "+15005550004", FailureProfile.UNSUBSCRIBED,        // 21610 blocklisted
            "+15005550009", FailureProfile.INVALID_RECIPIENT);  // 21614 not SMS-capable

    /** Twilio publishes 429 guidance in seconds; one second is their documented minimum backoff. */
    private static final Duration RATE_LIMIT_RETRY_AFTER = Duration.ofSeconds(1);

    private final long costMicros;

    public MockSmsProvider(ProviderCode code, ChaosState chaos, FailureInjector injector, Sleeper sleeper) {
        this(code, chaos, injector, sleeper, 7_900L); // ~$0.0079, US long-code list price
    }

    public MockSmsProvider(ProviderCode code, ChaosState chaos, FailureInjector injector,
                           Sleeper sleeper, long costMicros) {
        super(code, chaos, injector, sleeper);
        this.costMicros = costMicros;
    }

    @Override
    public Channel channel() {
        return Channel.SMS;
    }

    @Override
    public ProviderCapabilities capabilities() {
        // One message per request; no client reference; StatusCallback webhooks yes; status query no.
        return new ProviderCapabilities(false, 1, false, true, false);
    }

    @Override
    protected Optional<SendResult> vendorOverride(SendCommand command) {
        var profile = MAGIC_NUMBERS.get(command.address());
        if (profile == null) return Optional.empty();
        return Optional.of(reject(command, profile, Duration.ofMillis(35)));
    }

    @Override
    protected SendResult reject(SendCommand command, FailureProfile profile, Duration latency) {
        return switch (profile) {
            case UNSUBSCRIBED -> SendResult.Rejected.of(FailureType.UNSUBSCRIBED, "21610",
                    "Attempt to send to unsubscribed recipient", latency);
            case INVALID_RECIPIENT -> SendResult.Rejected.of(FailureType.INVALID_RECIPIENT, "21614",
                    "'To' number is not a valid mobile number", latency);
            case RATE_LIMITED -> new SendResult.Rejected(FailureType.RATE_LIMITED, "20429",
                    "Too Many Requests", latency, Optional.of(RATE_LIMIT_RETRY_AFTER));
            case TRANSIENT_NETWORK -> SendResult.Rejected.of(FailureType.TRANSIENT_NETWORK, "30003",
                    "Unreachable destination handset", latency);
            case PROVIDER_5XX -> SendResult.Rejected.of(FailureType.PROVIDER_5XX, "50000",
                    "HTTP 503 from the Messages resource", latency);
            case AUTH_FAILURE -> SendResult.Rejected.of(FailureType.AUTH_FAILURE, "20003",
                    "Authenticate: account SID or auth token is invalid", latency);
            case QUOTA_EXCEEDED -> SendResult.Rejected.of(FailureType.QUOTA_EXCEEDED, "20005",
                    "Account suspended or insufficient balance", latency);
            case CONTENT_REJECTED -> SendResult.Rejected.of(FailureType.CONTENT_REJECTED, "30007",
                    "Message filtered by the carrier", latency);
            case PAYLOAD_TOO_LARGE -> SendResult.Rejected.of(FailureType.PAYLOAD_TOO_LARGE, "21617",
                    "Message body exceeds 1600 characters", latency);
            case DEVICE_UNREGISTERED -> SendResult.Rejected.of(FailureType.INVALID_RECIPIENT, "30006",
                    "Landline or unreachable carrier", latency);
            // Handled before this method is reached; listed so adding a profile is a compile error
            // rather than a silent success.
            case SILENT_SUCCESS -> new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT,
                    "no response from the Messages resource", latency);
            case NONE -> throw new IllegalArgumentException("NONE is not a rejection");
        };
    }

    /** Twilio message SIDs are {@code SM} followed by 32 hex characters. */
    @Override
    protected String providerMessageId(SendCommand command) {
        return deterministicId("SM", 32, command.idempotencyToken());
    }

    @Override
    protected long costMicros() {
        return costMicros;
    }
}
