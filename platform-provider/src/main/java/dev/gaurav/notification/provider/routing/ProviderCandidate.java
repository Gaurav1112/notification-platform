package dev.gaurav.notification.provider.routing;

import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCode;

import java.util.Objects;

/**
 * One provider offered to the router, with the live numbers the decision is made on.
 *
 * <p>The point of collapsing configuration ({@code priority}, {@code weight}, {@code costMicros},
 * {@code rateLimitRps}) and measurement ({@code successRate5m}, {@code p95LatencyMs}) into a single
 * value object is that scoring stays a <em>pure function</em>. The strategy never reaches into a
 * metrics registry, a Redis client or a database, so the whole selection policy — the part most
 * likely to be wrong, and the part hardest to reproduce in production — is unit-testable with
 * literals and has no clock in it.
 *
 * <p>The measured fields are a 5-minute window on purpose. Shorter and a burst of four failures
 * from one bad batch condemns a healthy provider; longer and a real outage takes minutes to move
 * the score, by which time the circuit breaker has already done the job and the score is just
 * lagging noise.
 *
 * @param provider      the (decorated) adapter that would be called
 * @param priority      operator preference, 1 = first choice; ties are broken by score
 * @param weight        static traffic share an operator has pinned, e.g. during a vendor migration
 * @param costMicros    price per message, so the router can be told to care about the bill
 * @param rateLimitRps  the contracted ceiling; 0 means unmetered
 * @param successRate5m fraction in [0,1] over the last five minutes
 * @param p95LatencyMs  p95 over the same window
 */
public record ProviderCandidate(
        NotificationProvider provider,
        int priority,
        int weight,
        long costMicros,
        int rateLimitRps,
        double successRate5m,
        double p95LatencyMs) {

    public ProviderCandidate {
        Objects.requireNonNull(provider, "provider");
        if (priority < 1) throw new IllegalArgumentException("priority starts at 1, was " + priority);
        if (weight < 0) throw new IllegalArgumentException("weight must not be negative");
        if (costMicros < 0) throw new IllegalArgumentException("costMicros must not be negative");
        if (rateLimitRps < 0) throw new IllegalArgumentException("rateLimitRps must not be negative");
        if (successRate5m < 0.0 || successRate5m > 1.0) {
            throw new IllegalArgumentException("successRate5m must be in [0,1], was " + successRate5m);
        }
        if (p95LatencyMs < 0.0) throw new IllegalArgumentException("p95LatencyMs must not be negative");
    }

    /**
     * A candidate with no history yet.
     *
     * <p>{@code successRate5m} starts at 1.0, not 0.0. A newly-registered provider with no traffic
     * would otherwise score last forever and never receive the traffic it needs to earn a score —
     * the cold-start deadlock that makes an operator disable the router and hard-code a vendor.
     */
    public static ProviderCandidate freshlyRegistered(NotificationProvider provider, int priority, long costMicros) {
        return new ProviderCandidate(provider, priority, 1, costMicros, 0, 1.0, 0.0);
    }

    public ProviderCode code() {
        return provider.code();
    }

    public ProviderCandidate withHealth(double successRate5m, double p95LatencyMs) {
        return new ProviderCandidate(provider, priority, weight, costMicros, rateLimitRps,
                successRate5m, p95LatencyMs);
    }
}
