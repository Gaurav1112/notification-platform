package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.spi.ProviderCapabilities;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * FCM v1 + APNs-shaped push adapter.
 *
 * <h2>FCM has no batch endpoint, and the mock refuses to pretend otherwise</h2>
 *
 * <p>{@code supportsBatching = false}, {@code maxBatchSize = 1}. The {@code /batch} endpoint was
 * deprecated on <strong>2023-06-21</strong> and <strong>stopped working on 2024-06-21</strong>.
 * What replaced it, {@code sendEachForMulticast}, is a client-side convenience: it fans out to
 * <em>individual HTTP/2 requests</em>, one per token. The 500-token argument limit is a chunking
 * guard in the SDK, not a server-side batch.
 *
 * <p>Modelling this honestly is the difference between a load test that means something and one
 * that does not. A fake batch call makes a 500-recipient push look like one request; in production
 * it is 500 concurrent HTTP/2 streams, and the thing that breaks is the connection pool, the
 * per-host stream limit and the timeout pool — none of which a fake batch would ever touch.
 *
 * <h2>Idempotency: {@code apns-id} is correlation, not deduplication</h2>
 *
 * <p>{@code supportsIdempotencyKey = false}. You may supply an {@code apns-id} on an APNs request
 * and it is echoed back, which is genuinely useful for correlating a log line — but APNs does not
 * deduplicate on it. Send the same {@code apns-id} twice and the device gets two notifications.
 * {@code apns-collapse-id} is a different thing again: it replaces an <em>undelivered</em>
 * notification on the device, which is display behaviour, not delivery semantics.
 *
 * <h2>Error mapping</h2>
 *
 * <table>
 *   <caption>Injected profile to FCM/APNs wire representation</caption>
 *   <tr><th>Status</th><th>Vendor code</th><th>Classification</th></tr>
 *   <tr><td>404 / 410</td><td>{@code UNREGISTERED} / {@code BadDeviceToken}</td><td>DEVICE_UNREGISTERED — delete the token</td></tr>
 *   <tr><td>400</td><td>{@code INVALID_ARGUMENT}</td><td>INVALID_RECIPIENT</td></tr>
 *   <tr><td>429</td><td>{@code QUOTA_EXCEEDED}</td><td>RATE_LIMITED, Retry-After &ge; 60s</td></tr>
 *   <tr><td>401</td><td>{@code THIRD_PARTY_AUTH_ERROR}</td><td>AUTH_FAILURE</td></tr>
 *   <tr><td>503</td><td>{@code UNAVAILABLE}</td><td>PROVIDER_5XX, Retry-After &ge; 60s</td></tr>
 *   <tr><td>413</td><td>{@code PayloadTooLarge}</td><td>PAYLOAD_TOO_LARGE (APNs 4 KB)</td></tr>
 * </table>
 *
 * <p>The APNs 410 carries a {@code timestamp} — the moment the token stopped being valid. It is
 * load-bearing: if the device re-registered <em>after</em> that instant, the new token is fine and
 * deleting it would silently unsubscribe a live user. The mock puts the timestamp in the rejection
 * message so the token-deactivation path has something to parse.
 *
 * <p>FCM's documented backoff guidance is treated as a hard requirement, not advice: honour
 * {@code Retry-After}, default to 60 seconds when it is absent, and never retry sooner than 10
 * seconds. Hence {@link #MINIMUM_RETRY_AFTER}.
 */
public final class MockPushProvider extends AbstractMockProvider {

    /** FCM's documented floor for a 429/503 retry. Below this you are simply making it worse. */
    public static final Duration MINIMUM_RETRY_AFTER = Duration.ofSeconds(60);

    private final long costMicros;
    private final Clock clock;

    public MockPushProvider(ProviderCode code, ChaosState chaos, FailureInjector injector, Sleeper sleeper) {
        // FCM and APNs are free; the cost of push is our own fan-out, not the vendor's invoice.
        this(code, chaos, injector, sleeper, 0L, Clock.systemUTC());
    }

    public MockPushProvider(ProviderCode code, ChaosState chaos, FailureInjector injector,
                            Sleeper sleeper, long costMicros, Clock clock) {
        super(code, chaos, injector, sleeper);
        this.costMicros = costMicros;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public Channel channel() {
        return Channel.PUSH;
    }

    @Override
    public ProviderCapabilities capabilities() {
        // maxBatchSize is 1 because /batch is gone — see the class Javadoc. Webhook true: FCM
        // delivery-receipt export and APNs feedback both arrive asynchronously. Status query false:
        // there is no "did you deliver message X" API for either.
        return new ProviderCapabilities(false, 1, false, true, false);
    }

    @Override
    protected SendResult reject(SendCommand command, FailureProfile profile, Duration latency) {
        return switch (profile) {
            case DEVICE_UNREGISTERED, UNSUBSCRIBED -> SendResult.Rejected.of(
                    FailureType.DEVICE_UNREGISTERED, "UNREGISTERED",
                    "410 BadDeviceToken; token invalid since " + invalidSince(latency), latency);
            case INVALID_RECIPIENT -> SendResult.Rejected.of(FailureType.INVALID_RECIPIENT,
                    "INVALID_ARGUMENT", "400: the registration token is not a valid FCM token", latency);
            case RATE_LIMITED -> new SendResult.Rejected(FailureType.RATE_LIMITED, "QUOTA_EXCEEDED",
                    "429: sending limit exceeded for the message target", latency,
                    Optional.of(MINIMUM_RETRY_AFTER));
            case QUOTA_EXCEEDED -> new SendResult.Rejected(FailureType.QUOTA_EXCEEDED, "QUOTA_EXCEEDED",
                    "429: project-level quota exhausted", latency, Optional.of(MINIMUM_RETRY_AFTER));
            case AUTH_FAILURE -> SendResult.Rejected.of(FailureType.AUTH_FAILURE, "THIRD_PARTY_AUTH_ERROR",
                    "401: APNs key or FCM service-account credentials rejected", latency);
            case PROVIDER_5XX -> new SendResult.Rejected(FailureType.PROVIDER_5XX, "UNAVAILABLE",
                    "503: the server is overloaded", latency, Optional.of(MINIMUM_RETRY_AFTER));
            case TRANSIENT_NETWORK -> SendResult.Rejected.of(FailureType.TRANSIENT_NETWORK, "INTERNAL",
                    "HTTP/2 stream reset before headers", latency);
            case CONTENT_REJECTED -> SendResult.Rejected.of(FailureType.CONTENT_REJECTED, "INVALID_ARGUMENT",
                    "400: notification payload rejected", latency);
            case PAYLOAD_TOO_LARGE -> SendResult.Rejected.of(FailureType.PAYLOAD_TOO_LARGE, "PayloadTooLarge",
                    "413: payload exceeds the 4096-byte APNs limit", latency);
            case SILENT_SUCCESS -> new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT,
                    "HTTP/2 stream open with no response", latency);
            case NONE -> throw new IllegalArgumentException("NONE is not a rejection");
        };
    }

    /** FCM v1 names messages {@code projects/<id>/messages/<opaque>}. */
    @Override
    protected String providerMessageId(SendCommand command) {
        return "projects/mock-notification/messages/" + deterministicId("", 24, command.idempotencyToken());
    }

    @Override
    protected long costMicros() {
        return costMicros;
    }

    /**
     * The instant the token went invalid, as APNs reports on a 410.
     *
     * <p>Derived from the call latency rather than {@code Instant.now()} so it is slightly in the
     * past, which is what forces the deactivation path to compare it against the token's
     * registration time instead of blindly deleting.
     */
    private Instant invalidSince(Duration latency) {
        return clock.instant().minus(Duration.ofHours(1)).minus(latency);
    }
}
