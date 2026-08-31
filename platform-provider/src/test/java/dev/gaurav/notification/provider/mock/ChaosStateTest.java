package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.provider.spi.ProviderCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class ChaosStateTest {

    private static final ProviderCode PRIMARY = ProviderCode.of("mock-sms-primary");
    private static final ProviderCode SECONDARY = ProviderCode.of("mock-sms-secondary");

    @Test
    @DisplayName("an injected fault expires by itself, so a shared demo environment cannot stay broken")
    void faultsExpire() {
        // The version without an expiry is how an environment stays down until somebody remembers
        // what they typed on Tuesday.
        var clock = new MutableClock(Instant.parse("2026-08-31T09:00:00Z"));
        var chaos = new ChaosState(clock);

        chaos.setMode(PRIMARY, ChaosState.Mode.HARD_DOWN, Duration.ofSeconds(120));
        assertThat(chaos.modeFor(PRIMARY)).isEqualTo(ChaosState.Mode.HARD_DOWN);

        clock.advance(Duration.ofSeconds(121));

        assertThat(chaos.modeFor(PRIMARY)).isEqualTo(ChaosState.Mode.NORMAL);
        assertThat(chaos.active()).isEmpty();
    }

    @Test
    @DisplayName("a fat-fingered duration is clamped, so one bad request cannot wedge the environment for a fortnight")
    void durationIsClamped() {
        var clock = new MutableClock(Instant.parse("2026-08-31T09:00:00Z"));
        var chaos = new ChaosState(clock);

        var window = chaos.setMode(PRIMARY, ChaosState.Mode.HARD_DOWN, Duration.ofDays(14));

        assertThat(window.until()).isEqualTo(clock.instant().plus(ChaosState.MAX_DURATION));
    }

    @Test
    @DisplayName("a fault on the primary leaves the secondary alone, or the failover has nowhere to go")
    void faultsAreScopedToOneProvider() {
        var chaos = new ChaosState();

        chaos.setMode(PRIMARY, ChaosState.Mode.HARD_DOWN, Duration.ofMinutes(2));

        assertThat(chaos.modeFor(PRIMARY)).isEqualTo(ChaosState.Mode.HARD_DOWN);
        assertThat(chaos.modeFor(SECONDARY)).isEqualTo(ChaosState.Mode.NORMAL);
    }

    @Test
    @DisplayName("setting NORMAL clears the fault immediately rather than scheduling a 'be healthy' window")
    void normalClearsTheFault() {
        var chaos = new ChaosState();
        chaos.setMode(PRIMARY, ChaosState.Mode.DEGRADED, Duration.ofMinutes(30));

        chaos.setMode(PRIMARY, ChaosState.Mode.NORMAL, Duration.ofMinutes(30));

        assertThat(chaos.modeFor(PRIMARY)).isEqualTo(ChaosState.Mode.NORMAL);
        assertThat(chaos.active()).isEmpty();
    }

    @Test
    @DisplayName("a HARD_DOWN provider reports itself unhealthy, which is what takes it out of the routing pool")
    void hardDownPropagatesToTheAdapter() {
        var chaos = new ChaosState();
        var provider = new MockSmsProvider(PRIMARY, chaos,
                new FailureInjector(MockProviderProperties.defaults()), Sleeper.NONE);

        assertThat(provider.isHealthy()).isTrue();
        chaos.setMode(PRIMARY, ChaosState.Mode.HARD_DOWN, Duration.ofMinutes(2));

        assertThat(provider.isHealthy()).isFalse();
    }

    /** A clock a test can move forward. */
    private static final class MutableClock extends Clock {
        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
