package dev.gaurav.notification.provider.registry;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The one place that knows which providers exist.
 *
 * <p>Spring collects every {@link NotificationProvider} bean into the constructor argument, so
 * adding a vendor is a new {@code @Bean} and nothing else — no registration call to forget, no
 * enum to extend, no switch to update. That property is what makes "adding a real provider is one
 * class plus two config rows" true rather than aspirational.
 *
 * <p><strong>Duplicate codes fail the context start.</strong> Two beans claiming
 * {@code mock-sms-primary} would leave the router and the chaos endpoint each resolving to
 * whichever one the map happened to keep — so a fault injected into "the" primary would land on a
 * provider nobody is routing to, and the failover demo would appear to do nothing. A startup
 * failure with both bean names in the message costs a minute; that bug costs an afternoon.
 *
 * <p>Immutable after construction and safe to share across every worker thread.
 */
@Component
public class ProviderRegistry {

    private static final Logger log = LoggerFactory.getLogger(ProviderRegistry.class);

    private final Map<Channel, List<NotificationProvider>> byChannel;
    private final Map<ProviderCode, NotificationProvider> byCode;
    private final List<NotificationProvider> all;

    public ProviderRegistry(List<NotificationProvider> providers) {
        Objects.requireNonNull(providers, "providers");

        var codeIndex = new HashMap<ProviderCode, NotificationProvider>();
        for (var provider : providers) {
            var existing = codeIndex.putIfAbsent(provider.code(), provider);
            if (existing != null) {
                throw new IllegalStateException(
                        "two providers registered under code '%s': %s and %s".formatted(
                                provider.code().value(),
                                existing.getClass().getName(),
                                provider.getClass().getName()));
            }
        }

        var channelIndex = new EnumMap<Channel, List<NotificationProvider>>(Channel.class);
        for (var provider : providers) {
            channelIndex.computeIfAbsent(provider.channel(), c -> new ArrayList<>()).add(provider);
        }
        channelIndex.replaceAll((channel, list) -> List.copyOf(list));

        this.byCode = Map.copyOf(codeIndex);
        this.byChannel = Map.copyOf(channelIndex);
        this.all = List.copyOf(providers);

        for (var channel : Channel.values()) {
            var count = this.byChannel.getOrDefault(channel, List.of()).size();
            if (count == 0) {
                // Not fatal: a deployment may legitimately run without push. Loud, though — the
                // silent version of this is every PUSH notification failing selection at 3am.
                log.warn("no provider registered for channel {}; sends on that channel will fail selection", channel);
            } else if (count == 1) {
                log.info("channel {} has a single provider ({}); there is nothing to fail over to",
                        channel, this.byChannel.get(channel).get(0).code());
            }
        }
    }

    /** Every provider for a channel, in bean-definition order. Never null; possibly empty. */
    public List<NotificationProvider> forChannel(Channel channel) {
        return byChannel.getOrDefault(Objects.requireNonNull(channel, "channel"), List.of());
    }

    /** Lookup by stable identifier — how a chaos command, a webhook and a stored attempt row resolve. */
    public Optional<NotificationProvider> byCode(ProviderCode code) {
        return Optional.ofNullable(byCode.get(Objects.requireNonNull(code, "code")));
    }

    public List<NotificationProvider> all() {
        return all;
    }

    /** Registered codes, for the health endpoint and for an error message worth reading. */
    public List<ProviderCode> codes() {
        return List.copyOf(byCode.keySet());
    }
}
