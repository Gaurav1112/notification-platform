package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCapabilities;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The timeout decorator is the only thing standing between one hung vendor connection and a
 * cluster-wide Kafka rebalance, so its edge cases get explicit tests.
 */
class TimeoutProviderTest {

    private static final ProviderCode CODE = ProviderCode.of("test-sms");

    @Test
    @DisplayName("a provider that never answers yields Indeterminate rather than parking the consumer thread")
    void aHangingProviderIsCutOff() {
        var neverReturns = new StubProvider(command -> {
            try {
                Thread.sleep(30_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return accepted();
        });

        try (var bounded = TimeoutProvider.withDedicatedPool(neverReturns, 2, 4, Duration.ofMillis(60))) {
            var startedAt = System.nanoTime();
            var result = bounded.send(command(Duration.ofMillis(60)));
            var elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

            assertThat(result).isInstanceOf(SendResult.Indeterminate.class);
            assertThat(((SendResult.Indeterminate) result).type()).isEqualTo(FailureType.PROVIDER_TIMEOUT);
            assertThat(elapsed).isLessThan(Duration.ofSeconds(5));
        }
    }

    @Test
    @DisplayName("a timeout is Indeterminate, not Rejected — treating it as failure is how a user gets three OTPs")
    void aTimeoutIsNeverReportedAsAPlainFailure() {
        var slow = new StubProvider(command -> {
            try {
                Thread.sleep(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return accepted();
        });

        try (var bounded = TimeoutProvider.withDedicatedPool(slow, 1, 1, Duration.ofMillis(50))) {
            var result = bounded.send(command(Duration.ofMillis(50)));

            assertThat(result).isNotInstanceOf(SendResult.Rejected.class);
            assertThat(((SendResult.Indeterminate) result).type().isOutcomeIndeterminate())
                    .as("the reconciler only picks up attempts whose FailureType says the outcome is unknown")
                    .isTrue();
        }
    }

    @Test
    @DisplayName("the call runs on a named per-provider pool, never on ForkJoinPool.commonPool")
    void neverUsesTheCommonPool() {
        // On a 2-vCPU container the common pool is one thread wide. Blocking I/O parked there
        // starves every parallel stream in the JVM, and the stall is attributed to nobody.
        var observedThread = new AtomicReference<String>();
        var recorder = new StubProvider(command -> {
            observedThread.set(Thread.currentThread().getName());
            return accepted();
        });

        try (var bounded = TimeoutProvider.withDedicatedPool(recorder, 2, 4, Duration.ofSeconds(1))) {
            bounded.send(command(Duration.ofSeconds(1)));
        }

        assertThat(observedThread.get()).startsWith("provider-test-sms-");
        assertThat(observedThread.get()).doesNotContain("ForkJoinPool.commonPool");
    }

    @Test
    @DisplayName("the tighter of the command deadline and the configured ceiling wins")
    void theShorterBudgetApplies() {
        var slow = new StubProvider(command -> {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return accepted();
        });

        try (var bounded = TimeoutProvider.withDedicatedPool(slow, 1, 1, Duration.ofSeconds(30))) {
            var startedAt = System.nanoTime();
            bounded.send(command(Duration.ofMillis(50)));

            assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                    .as("a 30s ceiling must not override a CRITICAL message's 50ms deadline")
                    .isLessThan(Duration.ofMillis(900));
        }
    }

    @Test
    @DisplayName("a timed-out call is interrupted, so its thread comes back instead of leaking for 30 seconds")
    void theAbandonedCallIsInterrupted() throws Exception {
        var interrupted = new CountDownLatch(1);
        var blocker = new StubProvider(command -> {
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
            return accepted();
        });

        try (var bounded = TimeoutProvider.withDedicatedPool(blocker, 1, 1, Duration.ofMillis(50))) {
            bounded.send(command(Duration.ofMillis(50)));

            assertThat(interrupted.await(2, TimeUnit.SECONDS))
                    .as("without cancel(true) the pool bleeds a thread per timeout until it deadlocks")
                    .isTrue();
        }
    }

    @Test
    @DisplayName("a saturated pool sheds to the next provider instead of queueing behind a dying one")
    void poolSaturationShedsRatherThanQueues() throws Exception {
        var release = new CountDownLatch(1);
        var started = new CountDownLatch(1);
        var blocker = new StubProvider(command -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return accepted();
        });

        // One thread, no queue: the second concurrent send has nowhere to go.
        try (var bounded = TimeoutProvider.withDedicatedPool(blocker, 1, 1, Duration.ofSeconds(5))) {
            var background = new Thread(() -> bounded.send(command(Duration.ofSeconds(5))));
            background.setDaemon(true);
            background.start();
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();

            // Fill the single queue slot too, then the next submit must be rejected.
            var queued = new Thread(() -> bounded.send(command(Duration.ofSeconds(5))));
            queued.setDaemon(true);
            queued.start();
            Thread.sleep(100);

            var result = bounded.send(command(Duration.ofSeconds(5)));
            release.countDown();

            assertThat(result).isInstanceOf(SendResult.Rejected.class);
            var rejected = (SendResult.Rejected) result;
            assertThat(rejected.code()).isEqualTo("LOCAL_POOL_SATURATED");
            assertThat(rejected.type().shouldFailoverImmediately())
                    .as("our own saturation should move the message to the secondary, not burn an attempt")
                    .isTrue();
        }
    }

    @Test
    @DisplayName("an adapter that throws is surfaced loudly instead of being laundered into a retryable failure")
    void anAdapterBugIsNotDisguisedAsAProviderFailure() {
        // The SPI forbids throwing for a business failure. Converting an NPE into
        // Rejected(TRANSIENT_NETWORK) would retry a bug five times and then hide it forever.
        var broken = new StubProvider(command -> {
            throw new IllegalStateException("mapping gap");
        });

        try (var bounded = TimeoutProvider.withDedicatedPool(broken, 1, 1, Duration.ofSeconds(1))) {
            assertThatThrownBy(() -> bounded.send(command(Duration.ofSeconds(1))))
                    .isInstanceOf(TimeoutProvider.ProviderAdapterException.class)
                    .hasMessageContaining("test-sms")
                    .hasRootCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    @DisplayName("closing the chain shuts down only a pool it created, never one the caller owns")
    void closingDoesNotStealTheCallersExecutor() {
        var calls = new AtomicInteger();
        var shared = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            var fast = new StubProvider(command -> {
                calls.incrementAndGet();
                return accepted();
            });
            var bounded = new TimeoutProvider(fast, shared, Duration.ofSeconds(1));
            bounded.send(command(Duration.ofSeconds(1)));
            bounded.close();

            assertThat(shared.isShutdown())
                    .as("shutting down a shared executor on close would silently break every other provider using it")
                    .isFalse();
            assertThat(calls).hasValue(1);
        } finally {
            shared.shutdownNow();
        }
    }

    // ---- fixtures -------------------------------------------------------------------------------

    private static SendCommand command(Duration deadline) {
        return new SendCommand(UUID.randomUUID(), Channel.SMS, TrafficClass.CRITICAL, "+14155550123",
                null, "body", "tok-" + UUID.randomUUID(), Map.of(), deadline);
    }

    private static SendResult accepted() {
        return new SendResult.Accepted("SM" + "0".repeat(32), Duration.ofMillis(1), 7_900L);
    }

    private record StubProvider(Function<SendCommand, SendResult> behaviour) implements NotificationProvider {
        @Override
        public Channel channel() {
            return Channel.SMS;
        }

        @Override
        public ProviderCode code() {
            return CODE;
        }

        @Override
        public ProviderCapabilities capabilities() {
            return ProviderCapabilities.singleSend(true);
        }

        @Override
        public SendResult send(SendCommand command) {
            return behaviour.apply(command);
        }

        @Override
        public boolean isHealthy() {
            return true;
        }
    }
}
