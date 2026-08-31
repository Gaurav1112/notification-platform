package dev.gaurav.notification.worker.support;

import dev.gaurav.notification.persistence.repository.ProviderRepository;
import dev.gaurav.notification.provider.spi.ProviderCode;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Translates the {@link ProviderCode} the code routes on into the {@code smallint provider_id}
 * the attempt row stores.
 *
 * <p>Two identifiers for one thing is not an accident of modelling. {@code delivery_attempt} is
 * the widest table in the platform — 100M rows a day — and a 2-byte foreign key instead of a
 * 24-byte string is roughly 2 GB a day of heap, WAL and index that never gets written. The string
 * code stays the identity everywhere a human, a webhook or a chaos command is involved.
 *
 * <p>Cached forever after first lookup. The {@code provider} table is operator-managed
 * configuration that changes at the rate of a vendor contract, and a database round trip per send
 * to resolve a constant would put PostgreSQL on the critical path of every provider call.
 *
 * <p>A missing row throws rather than defaulting. A provider bean with no {@code provider} row is
 * a deployment that skipped a config migration; sending anyway would write attempts against a
 * provider id that resolves to a different vendor, and the cost and health rollups would be wrong
 * in a way nobody would ever trace back.
 */
@Component
public class ProviderIds {

    private final ProviderRepository providers;
    private final Map<String, Short> cache = new ConcurrentHashMap<>();

    public ProviderIds(ProviderRepository providers) {
        this.providers = providers;
    }

    /** The {@code provider.id} for a code, resolved once and cached. */
    public short idOf(ProviderCode code) {
        return cache.computeIfAbsent(code.value(), value -> providers.findByCode(value)
                .orElseThrow(() -> new IllegalStateException(
                        "no notif.provider row for code '" + value + "'; the provider bean exists "
                                + "but the configuration migration that registers it has not run"))
                .getId());
    }
}
