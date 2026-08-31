package dev.gaurav.notification.provider.mock.admin;

import dev.gaurav.notification.provider.mock.admin.ProviderHealthResponse.ProviderHealth;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.CircuitState;
import dev.gaurav.notification.provider.decorator.MeteredProvider;
import dev.gaurav.notification.provider.registry.ProviderRegistry;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.resilience.circuitbreaker.ProviderCircuitBreakers;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.search.Search;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * What every registered provider looks like right now, assembled from the meters the send path
 * already writes.
 *
 * <p><strong>Why this reads Micrometer rather than a purpose-built health table.</strong> A
 * separate health store would be a second source of truth that can disagree with the one the router
 * actually scores on, and the disagreement would only ever surface during an incident. Reading the
 * same {@code notification.provider.send} timer the {@link MeteredProvider} decorator writes means
 * what an operator sees here is, by construction, what the selection strategy is deciding on.
 *
 * <p><strong>The numbers are this pod's.</strong> Circuit breaker state is per-process on purpose —
 * a shared breaker would need a consensus round on the hot path and would let one poisoned replica
 * open the circuit fleet-wide. Two replicas can therefore legitimately disagree, which is documented
 * on the response type rather than smoothed over here.
 *
 * <p><strong>Why this class sits in {@code platform-provider} and not in {@code app-api}.</strong>
 * Everything it reads -- the Resilience4j registry, the {@code notification.provider.send} timer,
 * {@link NotificationProvider#isHealthy()} -- is per-JVM state written by the send path. It was
 * originally hosted only by the API tier, which routes no traffic, so it reported the same
 * unchanging answer forever: every circuit CLOSED, every success rate 1.0, every p95 exactly 0.0.
 * That last number is the tell -- a provider that has genuinely never been slow still records a
 * latency once it has been called at all. Packaged with the providers, the endpoint is served by
 * whichever process owns the breakers it describes, so the worker's copy reports the worker's
 * sends. The API tier still exposes it, and still reports all-CLOSED, which is now the truth about
 * that process rather than an artefact of where the file lived.
 *
 * <p><strong>{@code successRate5m} is cumulative, not a true five-minute window.</strong> A rolling
 * window needs a step registry or a Prometheus range query; the honest short-term answer is the
 * lifetime ratio, and the honest place for the windowed one is the recording rule in
 * {@code docker/prometheus/rules/}. Naming the field after the SLO and computing something adjacent
 * would be worse than saying so.
 */
@RestController
@RequestMapping("/v1/providers")
@ConditionalOnWebApplication
@Tag(name = "Providers", description = "Provider health and circuit state, as this pod sees it")
public class ProviderHealthController {

    private final ProviderRegistry registry;
    private final MeterRegistry meters;
    private final ObjectProvider<ProviderCircuitBreakers> breakers;

    /**
     * {@code ProviderCircuitBreakers} is optional so the endpoint still answers in a deployment
     * where resilience autoconfiguration is off. A health endpoint that itself fails to start is
     * the least useful possible outcome during an incident.
     */
    public ProviderHealthController(ProviderRegistry registry, MeterRegistry meters,
                                    ObjectProvider<ProviderCircuitBreakers> breakers) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.meters = Objects.requireNonNull(meters, "meters");
        this.breakers = breakers;
    }

    @GetMapping("/health")
    @Operation(summary = "Provider health and circuit state")
    public ProviderHealthResponse health() {
        var providers = registry.all().stream()
                .map(this::snapshot)
                .sorted(Comparator.comparing(ProviderHealth::code))
                .toList();
        return new ProviderHealthResponse(providers);
    }

    private ProviderHealth snapshot(NotificationProvider provider) {
        var code = provider.code().value();
        var channel = provider.channel();
        return new ProviderHealth(
                code,
                channel,
                circuitStateOf(code, channel),
                provider.isHealthy(),
                successRate(code),
                p95LatencyMs(code),
                // Rate-limit utilisation is owned by the worker's token bucket, which the API tier
                // deliberately does not share -- see the note in the class Javadoc about not
                // inventing a second source of truth. Reported as 0.0 until the bucket exposes a
                // gauge, rather than guessed at from request counts.
                0.0,
                null);
    }

    private CircuitState circuitStateOf(String code, Channel channel) {
        var available = breakers.getIfAvailable();
        return available == null ? CircuitState.CLOSED : available.stateOf(code, channel);
    }

    /**
     * @return 1.0 when no traffic has been observed. "No data" must not read as "broken" — an idle
     * channel showing 0.0 would make every dashboard red on a quiet Sunday and train people to
     * ignore it
     */
    private double successRate(String providerCode) {
        var accepted = totalCount(Search.in(meters).name(MeteredProvider.SEND_TIMER)
                .tag("provider", providerCode).tag("outcome", "accepted"));
        var all = totalCount(Search.in(meters).name(MeteredProvider.SEND_TIMER).tag("provider", providerCode));
        return all == 0 ? 1.0 : accepted / all;
    }

    private double p95LatencyMs(String providerCode) {
        return Search.in(meters).name(MeteredProvider.SEND_TIMER).tag("provider", providerCode)
                .timers().stream()
                .map(timer -> timer.takeSnapshot().percentileValues())
                .flatMap(Arrays::stream)
                .filter(value -> Math.abs(value.percentile() - 0.95) < 1e-9)
                .mapToDouble(value -> value.value(TimeUnit.MILLISECONDS))
                .max()
                .orElse(0.0);
    }

    private static double totalCount(Search search) {
        return search.timers().stream().mapToDouble(Timer::count).sum();
    }
}
