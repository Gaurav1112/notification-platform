package dev.gaurav.notification.api.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * A single injected {@link Clock}, so nothing in the API layer calls {@code Instant.now()}.
 *
 * <p><strong>The failure this prevents is untestable time.</strong> Every rule in this module that
 * matters is a time rule — the webhook replay window, "sendAt is in the past", the one-year schedule
 * horizon, idempotency expiry. A static {@code Instant.now()} makes each of them testable only by
 * sleeping, so in practice they get tested with a fake clock somewhere else or not at all, and the
 * ±5-minute window quietly becomes ±5 minutes and whatever the CI machine's drift is.
 *
 * <p>{@link Clock#systemUTC()} rather than the default zone: this platform stores every instant in
 * UTC and derives local time from an explicit IANA zone at render time. A clock carrying the host's
 * zone would let a host-local value leak into a comparison and produce an off-by-hours bug that only
 * appears on machines outside UTC.
 */
@Configuration
public class ApiClockConfig {

    @Bean
    @ConditionalOnMissingBean
    public Clock apiClock() {
        return Clock.systemUTC();
    }
}
