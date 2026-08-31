package dev.gaurav.notification.worker.orchestrator;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.TrafficClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows everything, and says so loudly at startup.
 *
 * <p>TODO(phase-9): delete once the {@code PreferenceFilterChain} lands in
 * {@code platform-application}.
 *
 * <p>Fail-open is the right default <em>for a placeholder</em> and the wrong one for a
 * preference system, so the distinction is worth stating: a fail-closed stub would suppress
 * 100% of traffic and every integration test would pass for the wrong reason — no message
 * suppressed is no message duplicated either. Fail-open keeps the pipeline honest and makes the
 * gap visible as a warning rather than as a green test.
 *
 * <p>Registered by {@code WorkerDefaultsConfiguration} behind {@code @ConditionalOnMissingBean},
 * so the real chain replaces it by existing.
 */
public class PermissivePreferenceResolver implements PreferenceResolver {

    private static final Logger log = LoggerFactory.getLogger(PermissivePreferenceResolver.class);

    public PermissivePreferenceResolver() {
        log.warn("no PreferenceResolver on the classpath: opt-out, quiet hours and frequency caps "
                + "are NOT being enforced on the dispatch path");
    }

    @Override
    public PreferenceDecision resolve(long tenantId, String userRef, Channel channel,
                                      TrafficClass trafficClass) {
        return PreferenceDecision.allow();
    }
}
