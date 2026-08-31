package dev.gaurav.notification.provider.routing;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.provider.spi.NotificationProvider;

import java.util.List;
import java.util.Optional;

/**
 * Chooses which provider gets the next message.
 *
 * <p>An interface rather than a method on the router because the choice is genuinely pluggable and
 * genuinely contested: cheapest-first is right for a bulk campaign, strict-priority is what an
 * operator wants during a vendor migration, and health-weighted is right for everything else. The
 * seam also means the scoring policy can be swapped in a test without a Spring context.
 *
 * <p><strong>Callers filter first.</strong> The candidate list handed in is already restricted to
 * providers whose circuit is CLOSED or HALF_OPEN, whose rate-limit budget has room, and whose daily
 * cap is not spent. Putting those filters inside the strategy would make it depend on Redis and
 * stop being unit-testable, which is the whole reason the seam exists.
 *
 * @see HealthWeightedSelectionStrategy the default, implementing §8.5 of the design
 */
@FunctionalInterface
public interface ProviderSelectionStrategy {

    /**
     * @param tenantId   available for per-tenant pinning and cost policy; may be {@code null} for
     *                   platform-internal traffic
     * @param candidates already filtered for eligibility by the caller
     * @return empty when nothing is eligible — <strong>not</strong> an exception. Every provider
     *         for a channel being down is an operational state the caller has to handle (park on
     *         {@code retry.1m} and page), not a programming error
     */
    Optional<ProviderCandidate> select(Channel channel, String tenantId, List<ProviderCandidate> candidates);

    /** Convenience for callers that only want the adapter. */
    default Optional<NotificationProvider> selectProvider(Channel channel, String tenantId,
                                                          List<ProviderCandidate> candidates) {
        return select(channel, tenantId, candidates).map(ProviderCandidate::provider);
    }
}
