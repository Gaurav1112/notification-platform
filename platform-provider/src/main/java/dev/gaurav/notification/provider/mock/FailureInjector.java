package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendCommand;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

/**
 * Decides, deterministically, whether a given send fails and how long it takes.
 *
 * <h2>Why the RNG is re-seeded per call instead of shared</h2>
 *
 * <p>The obvious implementation — one {@code Random} field, {@code nextDouble()} per send — is
 * reproducible only in a single-threaded run that makes calls in a fixed order. The moment sixteen
 * worker threads pull from the same stream, draw <em>n</em> goes to whichever thread got there
 * first, and "exactly three messages reached the DLQ" becomes a flaky assertion that someone
 * eventually deletes.
 *
 * <p>So the seed is derived from {@code (base seed, provider, recipient, attempt)} and a fresh
 * {@link Random} is built for each draw. The outcome of a send is then a pure function of its
 * identity: independent of thread, of ordering, of how many other messages are in flight, and of
 * whether the test ran the batch forwards or backwards. That is what makes an exact DLQ count a
 * legitimate assertion.
 *
 * <p>The <em>attempt</em> is part of the key on purpose. Without it a retry would draw the same
 * outcome forever, so a TRANSIENT_NETWORK failure could never recover and the retry engine would
 * be untestable. With it, attempt 2 of a given message has its own fixed, reproducible fate.
 *
 * <h2>Why log-normal latency</h2>
 *
 * <p>Real provider latency is long-tailed: a median around 120 ms with a p99 an order of magnitude
 * higher. A uniform draw between two bounds has essentially no tail, so it never populates a p99
 * histogram bucket realistically and never trips a latency-based breaker — which makes every
 * latency test pass and every latency alert untested. The parameters are solved from the two
 * numbers an operator actually knows:
 *
 * <pre>
 *   mu    = ln(median)
 *   sigma = (ln(p99) - mu) / z(0.99),   z(0.99) = 2.3263478740408408
 *   ms    = exp(mu + sigma * gaussian)
 * </pre>
 */
public final class FailureInjector {

    /** The standard normal quantile at 0.99; the constant that ties sigma to the configured p99. */
    private static final double Z99 = 2.3263478740408408;

    /**
     * Set by the retry engine so a redelivery draws a different fate. Absent means attempt 1.
     */
    public static final String ATTEMPT_ATTRIBUTE = "attempt";

    /** A tail draw of 8 sigma is legal and would hang a test suite for an hour. */
    private static final double TAIL_CAP_MULTIPLE = 10.0;

    private static final Duration HARD_DOWN_LATENCY = Duration.ofMillis(5);
    private static final double DEGRADED_SUCCESS_MULTIPLIER = 0.5;
    private static final int DEGRADED_LATENCY_MULTIPLIER = 5;

    private final MockProviderProperties properties;

    public FailureInjector(MockProviderProperties properties) {
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    /**
     * The injected fate of one send.
     *
     * @param profile {@link FailureProfile#NONE} for a success
     * @param latency how long the adapter should appear to take; ignored for
     *                {@link FailureProfile#SILENT_SUCCESS}, which overshoots the deadline by
     *                definition
     */
    public record Draw(FailureProfile profile, Duration latency) {
        public Draw {
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(latency, "latency");
        }

        public boolean isSuccess() {
            return profile == FailureProfile.NONE;
        }
    }

    public Draw draw(ProviderCode provider, SendCommand command, ChaosState.Mode mode) {
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(command, "command");

        if (mode == ChaosState.Mode.HARD_DOWN) {
            // A hard outage needs no dice: every call fails, fast. Fast matters — the breaker has
            // to accumulate 20 calls in a 60s window, and it cannot do that behind slow timeouts.
            return new Draw(FailureProfile.PROVIDER_5XX, HARD_DOWN_LATENCY);
        }

        var settings = properties.forProvider(provider);
        var rng = new Random(seedFor(properties.seedFor(provider), provider,
                command.recipientId(), attemptOf(command)));

        var latency = logNormal(settings.latency().median(), settings.latency().p99(), rng.nextGaussian());
        var successRate = settings.successRate();
        if (mode == ChaosState.Mode.DEGRADED) {
            successRate *= DEGRADED_SUCCESS_MULTIPLIER;
            latency = latency.multipliedBy(DEGRADED_LATENCY_MULTIPLIER);
        }

        // Drawn unconditionally so the stream position does not depend on the success roll — a
        // provider switched from NORMAL to DEGRADED then keeps the same latency shape.
        var failureRoll = rng.nextDouble();

        if (rng.nextDouble() < successRate) {
            return new Draw(FailureProfile.NONE, latency);
        }
        return new Draw(pickProfile(settings.failures(), failureRoll), latency);
    }

    /** Which attempt this is; the retry engine sets it, and it makes retries recoverable. */
    public static int attemptOf(SendCommand command) {
        var raw = command.attributes().get(ATTEMPT_ATTRIBUTE);
        if (raw == null) return 1;
        try {
            return Math.max(1, Integer.parseInt(raw));
        } catch (NumberFormatException e) {
            return 1; // a malformed attribute must not change delivery behaviour
        }
    }

    /**
     * SplitMix64 finalisation over each component, so two providers or two adjacent recipient UUIDs
     * do not produce correlated streams. A plain {@code seed + hash} does exactly that, and the
     * symptom — every recipient in a sequentially-generated test fixture failing together — reads
     * like a real bug.
     */
    static long seedFor(long baseSeed, ProviderCode provider, UUID recipientId, int attempt) {
        var h = mix(baseSeed);
        h = mix(h ^ fnv1a(provider.value()));
        h = mix(h ^ recipientId.getMostSignificantBits());
        h = mix(h ^ recipientId.getLeastSignificantBits());
        return mix(h ^ attempt);
    }

    static Duration logNormal(Duration median, Duration p99, double gaussian) {
        var medianMs = Math.max(1.0, median.toMillis());
        var p99Ms = Math.max(medianMs + 1.0, p99.toMillis());
        var mu = Math.log(medianMs);
        var sigma = (Math.log(p99Ms) - mu) / Z99;
        var ms = Math.exp(mu + sigma * gaussian);
        return Duration.ofMillis(Math.round(Math.min(ms, p99Ms * TAIL_CAP_MULTIPLE)));
    }

    private static FailureProfile pickProfile(List<MockProviderProperties.WeightedProfile> failures, double roll) {
        var total = failures.stream().mapToInt(MockProviderProperties.WeightedProfile::weight).sum();
        if (total <= 0) {
            // All weights zeroed. Falling through to NONE would silently disable failure injection
            // and turn every chaos test green for the wrong reason.
            return FailureProfile.TRANSIENT_NETWORK;
        }
        var target = roll * total;
        var cumulative = 0.0;
        for (var candidate : failures) {
            cumulative += candidate.weight();
            if (target < cumulative) return candidate.profile();
        }
        return failures.get(failures.size() - 1).profile();
    }

    private static long mix(long value) {
        var z = value + 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** FNV-1a rather than {@code String.hashCode}, so the stream is stable if the JDK ever is not. */
    private static long fnv1a(String value) {
        var hash = 0xCBF29CE484222325L;
        for (var i = 0; i < value.length(); i++) {
            hash ^= value.charAt(i);
            hash *= 0x100000001B3L;
        }
        return hash;
    }
}
