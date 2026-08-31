package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.spi.ProviderCapabilities;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;

import java.time.Duration;
import java.util.Optional;

/**
 * Amazon SES-shaped email adapter.
 *
 * <h2>Batching: real, and capped at 50</h2>
 *
 * <p>{@code supportsBatching = true}, {@code maxBatchSize = 50}. SES's {@code SendBulkEmail} takes
 * up to 50 {@code BulkEmailEntry} destinations in one call and returns a <em>per-destination</em>
 * status — so a batch is not all-or-nothing, and the adapter contract has to carry partial success.
 * That is the interesting difference from the other two channels, and it is why the platform's
 * batching logic cannot be "loop and hope".
 *
 * <p>Note what the 50 is: a hard API limit, not a tuning knob. Sending 51 is an
 * {@code AccountSendingPausedException}-class error, not a slow request, which is why
 * {@link ProviderCapabilities#maxBatchSize()} is consulted rather than a configured chunk size.
 *
 * <h2>Status query: true, but for a specific reason</h2>
 *
 * <p>Unlike Twilio, SES returns a {@code MessageId} that is echoed on every event-destination
 * notification (Delivery, Bounce, Complaint) and appears in the {@code Message-ID} header of the
 * mail itself. That gives a durable correlation key we chose, effectively, before the send — which
 * is enough for the reconciler to resolve an {@code UNKNOWN} attempt. It is a weaker guarantee than
 * a true idempotency key ({@code supportsIdempotencyKey} stays false — a retried
 * {@code SendEmail} is a second email), but it is a real one.
 *
 * <h2>Error mapping</h2>
 *
 * <table>
 *   <caption>Injected profile to SES error</caption>
 *   <tr><th>SES error</th><th>Classification</th></tr>
 *   <tr><td>{@code Throttling} (429, {@code Maximum sending rate exceeded})</td><td>RATE_LIMITED</td></tr>
 *   <tr><td>{@code MessageRejected} (content or reputation)</td><td>CONTENT_REJECTED</td></tr>
 *   <tr><td>{@code AccountSendingPausedException}</td><td>QUOTA_EXCEEDED</td></tr>
 *   <tr><td>{@code MailFromDomainNotVerifiedException}</td><td>AUTH_FAILURE</td></tr>
 *   <tr><td>{@code InvalidParameterValue} (malformed address)</td><td>INVALID_RECIPIENT</td></tr>
 *   <tr><td>Permanent bounce, {@code General} sub-type</td><td>INVALID_RECIPIENT, suppress</td></tr>
 *   <tr><td>{@code ServiceUnavailable} / 5xx</td><td>PROVIDER_5XX</td></tr>
 * </table>
 *
 * <p>The permanent bounce is the one with teeth: it must reach the suppression list, because SES
 * meters your bounce rate and pauses the account above 5%. A retry loop over a dead mailbox is how
 * a sending domain gets shut off.
 */
public final class MockEmailProvider extends AbstractMockProvider {

    /** SES {@code SendBulkEmail} accepts at most 50 destinations per call. Hard API limit. */
    public static final int SES_BULK_DESTINATION_LIMIT = 50;

    /** SES throttling is per-second; a one-second floor matches the quota window. */
    private static final Duration THROTTLE_RETRY_AFTER = Duration.ofSeconds(1);

    private final long costMicros;

    public MockEmailProvider(ProviderCode code, ChaosState chaos, FailureInjector injector, Sleeper sleeper) {
        this(code, chaos, injector, sleeper, 100L); // $0.10 per 1,000 = 100 micros each
    }

    public MockEmailProvider(ProviderCode code, ChaosState chaos, FailureInjector injector,
                             Sleeper sleeper, long costMicros) {
        super(code, chaos, injector, sleeper);
        this.costMicros = costMicros;
    }

    @Override
    public Channel channel() {
        return Channel.EMAIL;
    }

    @Override
    public ProviderCapabilities capabilities() {
        return new ProviderCapabilities(true, SES_BULK_DESTINATION_LIMIT, false, true, true);
    }

    @Override
    protected SendResult reject(SendCommand command, FailureProfile profile, Duration latency) {
        return switch (profile) {
            case RATE_LIMITED -> new SendResult.Rejected(FailureType.RATE_LIMITED, "Throttling",
                    "Maximum sending rate exceeded", latency, Optional.of(THROTTLE_RETRY_AFTER));
            case CONTENT_REJECTED -> SendResult.Rejected.of(FailureType.CONTENT_REJECTED, "MessageRejected",
                    "Email address is not verified or content was rejected", latency);
            case QUOTA_EXCEEDED -> SendResult.Rejected.of(FailureType.QUOTA_EXCEEDED,
                    "AccountSendingPausedException", "Sending is paused for this account", latency);
            case AUTH_FAILURE -> SendResult.Rejected.of(FailureType.AUTH_FAILURE,
                    "MailFromDomainNotVerifiedException", "MAIL FROM domain is not verified", latency);
            case INVALID_RECIPIENT, DEVICE_UNREGISTERED -> SendResult.Rejected.of(
                    FailureType.INVALID_RECIPIENT, "Bounce.Permanent.General",
                    "550 5.1.1 recipient address does not exist", latency);
            case UNSUBSCRIBED -> SendResult.Rejected.of(FailureType.UNSUBSCRIBED, "AccountSuppressionList",
                    "Recipient is on the account-level suppression list", latency);
            case TRANSIENT_NETWORK -> SendResult.Rejected.of(FailureType.TRANSIENT_NETWORK, "RequestTimeout",
                    "Connection reset before the request completed", latency);
            case PROVIDER_5XX -> SendResult.Rejected.of(FailureType.PROVIDER_5XX, "ServiceUnavailable",
                    "HTTP 503 from the SES v2 API", latency);
            case PAYLOAD_TOO_LARGE -> SendResult.Rejected.of(FailureType.PAYLOAD_TOO_LARGE,
                    "MessageTooLarge", "Message exceeds the 40 MB SES limit", latency);
            case SILENT_SUCCESS -> new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT,
                    "no response from SendEmail", latency);
            case NONE -> throw new IllegalArgumentException("NONE is not a rejection");
        };
    }

    /** SES message ids look like a UUID with a regional suffix; the shape is what matters here. */
    @Override
    protected String providerMessageId(SendCommand command) {
        var hex = deterministicId("", 32, command.idempotencyToken());
        return "%s-%s-%s-%s-%s-000000".formatted(
                hex.substring(0, 8), hex.substring(8, 12), hex.substring(12, 16),
                hex.substring(16, 20), hex.substring(20, 32));
    }

    @Override
    protected long costMicros() {
        return costMicros;
    }
}
