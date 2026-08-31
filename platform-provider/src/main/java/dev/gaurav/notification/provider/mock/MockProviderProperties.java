package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.provider.spi.ProviderCode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-provider chaos configuration for the mock adapters.
 *
 * <pre>
 * notification.providers.mock:
 *   enabled: true
 *   seed: 20260831                     # move this and every failure in CI changes
 *   providers:
 *     mock-sms-primary:
 *       success-rate: 0.97
 *       latency: { median: 120ms, p99: 1500ms }
 *       failures:
 *         - { profile: RATE_LIMITED,   weight: 40 }
 *         - { profile: SILENT_SUCCESS, weight: 5 }
 * </pre>
 *
 * <p><strong>The seed is the important field.</strong> With it, a CI job can assert "exactly three
 * messages reached the DLQ" and have that assertion mean something on every machine, on every run,
 * at any level of parallelism — see {@link FailureInjector} for how that is achieved without a
 * shared RNG.
 *
 * @param enabled   registers the mock beans; the whole demo stack runs on these
 * @param seed      the base seed every provider's stream is derived from
 * @param providers keyed by {@link ProviderCode#value()}; absent keys get {@link ProviderSettings#defaults()}
 */
@ConfigurationProperties(prefix = "notification.providers.mock")
public record MockProviderProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("20260831") long seed,
        @DefaultValue Map<String, ProviderSettings> providers) {

    public MockProviderProperties {
        providers = providers == null ? Map.of() : Map.copyOf(providers);
    }

    public static MockProviderProperties defaults() {
        return new MockProviderProperties(true, 20260831L, Map.of());
    }

    /** Convenience for tests: one provider configured, everything else defaulted. */
    public MockProviderProperties with(ProviderCode code, ProviderSettings settings) {
        var merged = new LinkedHashMap<>(providers);
        merged.put(code.value(), settings);
        return new MockProviderProperties(enabled, seed, merged);
    }

    public ProviderSettings forProvider(ProviderCode code) {
        return providers.getOrDefault(code.value(), ProviderSettings.defaults());
    }

    /**
     * A per-provider seed override, falling back to the global one.
     *
     * <p>Overriding one provider lets a failover test make the primary reliably awful while the
     * secondary stays reliably fine, without perturbing anyone else's expected DLQ count.
     */
    public long seedFor(ProviderCode code) {
        var override = forProvider(code).seed();
        return override == null ? seed : override;
    }

    /**
     * @param successRate fraction of sends that succeed, before chaos mode is applied
     * @param latency     log-normal shape; see {@link FailureInjector}
     * @param seed        overrides {@link MockProviderProperties#seed()} for this provider only
     * @param failures    weighted draw used when the success roll fails; empty means
     *                    {@link #DEFAULT_FAILURE_MIX}
     */
    public record ProviderSettings(
            @DefaultValue("0.98") double successRate,
            @DefaultValue Latency latency,
            Long seed,
            @DefaultValue List<WeightedProfile> failures) {

        /**
         * Roughly what a healthy transactional SMS route actually looks like: mostly transient,
         * a real slice of throttling, a small permanent tail, and enough {@code SILENT_SUCCESS} to
         * keep the reconciler honest without drowning the run in {@code UNKNOWN}s.
         */
        public static final List<WeightedProfile> DEFAULT_FAILURE_MIX = List.of(
                new WeightedProfile(FailureProfile.TRANSIENT_NETWORK, 30),
                new WeightedProfile(FailureProfile.PROVIDER_5XX, 25),
                new WeightedProfile(FailureProfile.RATE_LIMITED, 20),
                new WeightedProfile(FailureProfile.INVALID_RECIPIENT, 10),
                new WeightedProfile(FailureProfile.SILENT_SUCCESS, 8),
                new WeightedProfile(FailureProfile.UNSUBSCRIBED, 4),
                new WeightedProfile(FailureProfile.QUOTA_EXCEEDED, 3));

        public ProviderSettings {
            if (successRate < 0.0 || successRate > 1.0) {
                throw new IllegalArgumentException("successRate must be in [0,1] but was " + successRate);
            }
            latency = latency == null ? Latency.defaults() : latency;
            failures = failures == null || failures.isEmpty() ? DEFAULT_FAILURE_MIX : List.copyOf(failures);
        }

        public static ProviderSettings defaults() {
            return new ProviderSettings(0.98, Latency.defaults(), null, DEFAULT_FAILURE_MIX);
        }

        public ProviderSettings withSuccessRate(double rate) {
            return new ProviderSettings(rate, latency, seed, failures);
        }

        public ProviderSettings withLatency(Latency value) {
            return new ProviderSettings(successRate, value, seed, failures);
        }

        public ProviderSettings withSeed(long value) {
            return new ProviderSettings(successRate, latency, value, failures);
        }

        public ProviderSettings withFailures(List<WeightedProfile> value) {
            return new ProviderSettings(successRate, latency, seed, value);
        }

        public int totalWeight() {
            return failures.stream().mapToInt(WeightedProfile::weight).sum();
        }
    }

    /**
     * Latency described by two points, because those are the two numbers a vendor SLA actually
     * quotes and the two an operator can reason about. {@link FailureInjector} solves the
     * log-normal parameters from them.
     */
    public record Latency(
            @DefaultValue("120ms") Duration median,
            @DefaultValue("1500ms") Duration p99) {

        public Latency {
            median = median == null ? Duration.ofMillis(120) : median;
            p99 = p99 == null ? Duration.ofMillis(1500) : p99;
            if (p99.compareTo(median) < 0) {
                throw new IllegalArgumentException("p99 (" + p99 + ") must not be below median (" + median + ")");
            }
        }

        public static Latency defaults() {
            return new Latency(Duration.ofMillis(120), Duration.ofMillis(1500));
        }

        public static Latency of(Duration median, Duration p99) {
            return new Latency(median, p99);
        }
    }

    /** One entry in the failure draw. Weights are relative, not percentages. */
    public record WeightedProfile(FailureProfile profile, @DefaultValue("1") int weight) {
        public WeightedProfile {
            if (profile == null) throw new IllegalArgumentException("profile is required");
            if (weight < 0) throw new IllegalArgumentException("weight must not be negative");
        }
    }
}
