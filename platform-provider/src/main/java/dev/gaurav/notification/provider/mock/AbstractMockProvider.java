package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Shared plumbing for the mock adapters: draw a fate, burn the latency, hand the outcome to the
 * subclass to dress in its vendor's clothes.
 *
 * <p>The split matters. Everything generic — determinism, latency shape, chaos mode, the
 * {@code SILENT_SUCCESS} overshoot — is identical across vendors and belongs in one place;
 * everything the platform has to be <em>correct</em> about — the error codes, the retry-after
 * floors, the message-id formats — is vendor-specific and belongs in the subclass, because that
 * mapping is the code a real integration would have to write too.
 *
 * <p>{@link #send(SendCommand)} is final: an adapter that overrode it could skip the injector, and
 * a mock that quietly always succeeds is worse than no mock at all.
 */
public abstract class AbstractMockProvider implements NotificationProvider {

    /** How far past the deadline {@code SILENT_SUCCESS} runs before it admits anything. */
    private static final Duration SILENT_SUCCESS_OVERSHOOT = Duration.ofMillis(250);

    private final ProviderCode code;
    private final ChaosState chaos;
    private final FailureInjector injector;
    private final Sleeper sleeper;

    protected AbstractMockProvider(ProviderCode code, ChaosState chaos,
                                   FailureInjector injector, Sleeper sleeper) {
        this.code = Objects.requireNonNull(code, "code");
        this.chaos = Objects.requireNonNull(chaos, "chaos");
        this.injector = Objects.requireNonNull(injector, "injector");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    @Override
    public final ProviderCode code() {
        return code;
    }

    @Override
    public abstract Channel channel();

    @Override
    public final SendResult send(SendCommand command) {
        var override = vendorOverride(command);
        if (override.isPresent()) {
            return override.get();
        }

        var draw = injector.draw(code, command, chaos.modeFor(code));

        if (draw.profile() == FailureProfile.SILENT_SUCCESS) {
            // Overshoot the caller's own deadline, so the TimeoutProvider above cuts us off and the
            // platform is left holding an Indeterminate it has to reconcile. In the full harness
            // the webhook still fires afterwards — that gap is the entire point of the profile.
            var overshoot = command.deadline().plus(SILENT_SUCCESS_OVERSHOOT);
            sleeper.sleep(overshoot);
            return new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT,
                    code + " acknowledged after the deadline; delivery state unknown", overshoot);
        }

        sleeper.sleep(draw.latency());

        return draw.isSuccess()
                ? new SendResult.Accepted(providerMessageId(command), draw.latency(), costMicros())
                : reject(command, draw.profile(), draw.latency());
    }

    @Override
    public boolean isHealthy() {
        // Deliberately a state read, never a vendor call: a health endpoint that pings the provider
        // multiplies every Kubernetes probe by the number of providers and pods.
        return chaos.modeFor(code) != ChaosState.Mode.HARD_DOWN;
    }

    /**
     * A vendor-specific short-circuit evaluated before the RNG, for addresses whose behaviour is
     * contractually fixed — Twilio's magic test numbers, for example. Default: no override.
     */
    protected Optional<SendResult> vendorOverride(SendCommand command) {
        return Optional.empty();
    }

    /** Dress an injected profile in this vendor's actual error code and message. */
    protected abstract SendResult reject(SendCommand command, FailureProfile profile, Duration latency);

    /** Shape of the vendor's accepted-message identifier, e.g. Twilio's {@code SM} + 32 hex. */
    protected abstract String providerMessageId(SendCommand command);

    /** Per-message price, so a failover to a pricier secondary shows up on the cost counter. */
    protected abstract long costMicros();

    protected ChaosState chaos() {
        return chaos;
    }

    /**
     * A stable pseudo-id derived from the idempotency token.
     *
     * <p>Random ids would make an end-to-end assertion — "the webhook for the id we were given
     * moves the row to DELIVERED" — impossible to write against a recorded fixture.
     */
    protected static String deterministicId(String prefix, int hexDigits, String token) {
        var hash = 0xCBF29CE484222325L;
        for (var i = 0; i < token.length(); i++) {
            hash ^= token.charAt(i);
            hash *= 0x100000001B3L;
        }
        var builder = new StringBuilder(prefix);
        var state = hash;
        while (builder.length() - prefix.length() < hexDigits) {
            state = state * 6364136223846793005L + 1442695040888963407L;
            builder.append(String.format("%016x", state));
        }
        return builder.substring(0, prefix.length() + hexDigits);
    }
}
