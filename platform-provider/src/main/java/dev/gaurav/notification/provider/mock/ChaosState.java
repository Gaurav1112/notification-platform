package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.provider.spi.ProviderCode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runtime fault injection state, so the failover story can be demonstrated in thirty seconds
 * instead of described in a slide.
 *
 * <pre>
 * POST /admin/v1/mock-providers/mock-sms-primary/chaos {"mode":"HARD_DOWN","durationSeconds":120}
 *   → breaker OPEN at t+2s → failover to mock-sms-secondary → half-open probe at t+150s → CLOSED
 * </pre>
 *
 * <p>The state holder lives here rather than in the web layer because it is provider behaviour,
 * not an HTTP concern; the REST endpoint in {@code app-api} is a thin adapter over
 * {@link #setMode(ProviderCode, Mode, Duration)}.
 *
 * <p><strong>Every window has an expiry, and that is not a convenience.</strong> A fault injected
 * with no deadline is how a shared demo environment stays broken until somebody remembers what
 * they did on Tuesday. {@link #MAX_DURATION} additionally caps a fat-fingered
 * {@code "durationSeconds": 1200000}.
 *
 * <p>Thread-safe: workers read this on every send from many threads while an operator writes to it.
 */
public final class ChaosState {

    /** No injected fault outlives this, whatever the request asked for. */
    public static final Duration MAX_DURATION = Duration.ofHours(1);

    /** What kind of broken to be. */
    public enum Mode {
        /** Configured behaviour. */
        NORMAL,
        /** Half the usual success rate and five times the latency — the shape that trips p99 SLOs. */
        DEGRADED,
        /** Every call fails fast and {@code isHealthy()} goes false. A clean, total outage. */
        HARD_DOWN
    }

    /** An injected fault and the instant it stops applying. */
    public record Window(Mode mode, Instant until) {
        public Window {
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(until, "until");
        }
    }

    private final ConcurrentHashMap<String, Window> windows = new ConcurrentHashMap<>();
    private final Clock clock;

    public ChaosState() {
        this(Clock.systemUTC());
    }

    public ChaosState(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * @param duration clamped to {@link #MAX_DURATION}; {@link Mode#NORMAL} clears the window
     *                 outright rather than scheduling a "be healthy" fault
     */
    public Window setMode(ProviderCode provider, Mode mode, Duration duration) {
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(mode, "mode");
        if (mode == Mode.NORMAL) {
            windows.remove(provider.value());
            return new Window(Mode.NORMAL, clock.instant());
        }
        var effective = duration == null || duration.isNegative() || duration.isZero()
                ? Duration.ofMinutes(2)
                : min(duration, MAX_DURATION);
        var window = new Window(mode, clock.instant().plus(effective));
        windows.put(provider.value(), window);
        return window;
    }

    /** The mode in force right now. Expired windows are removed on read, so recovery is automatic. */
    public Mode modeFor(ProviderCode provider) {
        return windowFor(provider).map(Window::mode).orElse(Mode.NORMAL);
    }

    public Optional<Window> windowFor(ProviderCode provider) {
        var window = windows.get(provider.value());
        if (window == null) return Optional.empty();
        if (!window.until().isAfter(clock.instant())) {
            windows.remove(provider.value(), window);
            return Optional.empty();
        }
        return Optional.of(window);
    }

    public void clear(ProviderCode provider) {
        windows.remove(provider.value());
    }

    public void clearAll() {
        windows.clear();
    }

    /** Snapshot of currently-active faults, for the admin endpoint and for a test to assert on. */
    public Map<String, Window> active() {
        var now = clock.instant();
        windows.entrySet().removeIf(e -> !e.getValue().until().isAfter(now));
        return Map.copyOf(windows);
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }
}
