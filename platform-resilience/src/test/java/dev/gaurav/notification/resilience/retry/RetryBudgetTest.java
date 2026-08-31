package dev.gaurav.notification.resilience.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/** Proves the budget caps retry amplification at the configured ratio and does so safely under load. */
class RetryBudgetTest {

    @Test
    @DisplayName("a total provider outage cannot amplify load, because deposits stop when successes do")
    void retriesStopWhenSuccessesStop() {
        var budget = new RetryBudget(0.10, 10);

        // The provider dies: everything fails, nothing succeeds, so nothing refills the bucket.
        int granted = 0;
        for (int i = 0; i < 1_000; i++) {
            if (budget.tryAcquire()) {
                granted++;
            }
        }

        assertThat(granted)
                .as("1000 failures must not produce 1000 retries — that is the 243x amplification")
                .isEqualTo(10);
        assertThat(budget.throttledCount()).isEqualTo(990);
    }

    @Test
    @DisplayName("once the startup allowance is spent, retries are capped at 10% of successful calls")
    void steadyStateHoldsTheConfiguredRatio() {
        var budget = new RetryBudget(0.10, 10);
        drain(budget);

        for (int i = 0; i < 100; i++) {
            budget.recordSuccess();
        }

        // 100 successes fund exactly 10 retries and not an eleventh.
        int granted = 0;
        while (budget.tryAcquire()) {
            granted++;
        }
        assertThat(granted).isEqualTo(10);
    }

    @Test
    @DisplayName("a quiet week cannot bank a stampede for the next outage")
    void depositsAreCappedAtCapacity() {
        var budget = new RetryBudget(0.10, 10);
        drain(budget);

        for (int i = 0; i < 1_000_000; i++) {
            budget.recordSuccess();
        }

        assertThat(budget.availableRetries())
                .as("an uncapped bucket would fund 100,000 simultaneous retries the moment a provider blips")
                .isEqualTo(10);
    }

    @Test
    @DisplayName("a JVM that has just rolled can still retry before it has recorded any success")
    void startsFullSoADeployIsNotItsOwnOutage() {
        assertThat(new RetryBudget(0.10, 5).tryAcquire()).isTrue();
    }

    @Test
    @DisplayName("eight worker threads racing on the last token cannot all spend it")
    void concurrentAcquisitionNeverOverspends() throws Exception {
        int capacity = 500;
        var budget = new RetryBudget(0.0, capacity);
        var granted = new AtomicInteger();
        var start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(8);

        try {
            for (int t = 0; t < 8; t++) {
                pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 1_000; i++) {
                        if (budget.tryAcquire()) {
                            granted.incrementAndGet();
                        }
                    }
                    return null;
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(granted.get())
                .as("a read-then-write budget lets two threads both spend the last token")
                .isEqualTo(capacity);
    }

    @Test
    @DisplayName("a ratio above 1 would licence more retries than calls and is refused at construction")
    void rejectsANonsensicalRatio() {
        assertThatIllegalArgumentException().isThrownBy(() -> new RetryBudget(1.5, 10));
        assertThatIllegalArgumentException().isThrownBy(() -> new RetryBudget(0.1, 0));
    }

    private static void drain(RetryBudget budget) {
        while (budget.tryAcquire()) {
            // spend the startup allowance
        }
    }
}
