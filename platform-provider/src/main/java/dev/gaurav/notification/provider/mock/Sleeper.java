package dev.gaurav.notification.provider.mock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

/**
 * How a mock burns its simulated latency.
 *
 * <p>Injected rather than calling {@link Thread#sleep} inline for one reason: a contract test that
 * exercises 10,000 seeded sends against a mock configured with a 1,500 ms p99 would otherwise take
 * most of an hour. Unit tests use {@link #NONE} or {@link Recording}; the failover demo and the
 * timeout tests use {@link #REAL}, because there the wall clock <em>is</em> the thing under test.
 */
@FunctionalInterface
public interface Sleeper {

    void sleep(Duration duration);

    /** Real time. Restores the interrupt flag, which is how {@code Future.cancel(true)} gets through. */
    Sleeper REAL = duration -> {
        if (duration == null || duration.isZero() || duration.isNegative()) return;
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            // Swallowing this would make TimeoutProvider.cancel(true) a no-op and leak the thread
            // for the rest of the simulated latency.
            Thread.currentThread().interrupt();
        }
    };

    /** No time passes. The default for unit tests. */
    Sleeper NONE = duration -> { };

    /** Records what it was asked to sleep, so a test can assert on latency without spending it. */
    final class Recording implements Sleeper {
        private final List<Duration> slept = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void sleep(Duration duration) {
            slept.add(duration);
        }

        public List<Duration> slept() {
            return List.copyOf(slept);
        }

        public Duration total() {
            return slept().stream().reduce(Duration.ZERO, Duration::plus);
        }
    }
}
