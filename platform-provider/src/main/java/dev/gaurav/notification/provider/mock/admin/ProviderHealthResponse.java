package dev.gaurav.notification.provider.mock.admin;

import com.fasterxml.jackson.annotation.JsonInclude;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.CircuitState;
import dev.gaurav.notification.domain.enums.FailureType;

import java.time.Instant;
import java.util.List;

/**
 * What every provider looks like right now, from this pod's point of view.
 *
 * <p><strong>"From this pod's point of view" is not a caveat to hide.</strong> Circuit breaker
 * state is per-process by design — a shared, distributed breaker would need a consensus round on
 * the hot path and would let one poisoned pod open the circuit for the whole fleet. The consequence
 * is that two API replicas can legitimately disagree here, and a caller who does not know that will
 * file a bug. It is documented rather than smoothed over.
 *
 * <p>This endpoint exists because a provider that <em>degrades</em> never trips anything: a vendor
 * rejecting 4% of messages raises no exception and shows up in no error-rate panel. Success rate
 * and p95 latency per provider are the only signals that surface it, and they are also the inputs
 * the router scores on — so what a human reads here is what the selection strategy is deciding on.
 */
public record ProviderHealthResponse(List<ProviderHealth> providers) {

    /**
     * @param successRate5m        accepted ÷ total over the trailing window; 1.0 when no traffic
     *                             has been seen, because "no data" must not look like "broken" and
     *                             open a circuit on an idle channel
     * @param rateLimitUtilization fraction of the per-provider token bucket consumed; the number
     *                             that predicts a 429 before it happens
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ProviderHealth(
            String code,
            Channel channel,
            CircuitState circuitState,
            boolean healthy,
            double successRate5m,
            double p95LatencyMs,
            double rateLimitUtilization,
            LastFailure lastFailure
    ) {
    }

    /** The most recent classified failure, so a reader can tell a quota problem from an outage. */
    public record LastFailure(FailureType type, Instant at) {
    }
}
