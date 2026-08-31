package dev.gaurav.notification.messaging.producer;

import dev.gaurav.notification.messaging.event.NotificationEvent;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * The wait that stands between an unacknowledged produce and a committed offset.
 *
 * <p>Each test here is the difference between "the campaign went out" and "the campaign is in a
 * heap object that was garbage collected when the pod restarted".
 */
class PublishBatchTest {

    private static final Duration BUDGET = Duration.ofMillis(200);

    @Test
    @DisplayName("a send that the broker never acknowledged throws instead of returning, so the "
            + "caller cannot commit an offset for an event that is still only in this JVM")
    void aFailedSendIsReportedToTheCallerInsteadOfBeingSwallowed() {
        var broker = new CompletableFuture<SendResult<String, NotificationEvent>>();
        broker.completeExceptionally(new TimeoutException("Expiring 1 record(s): 30000 ms has passed"));

        var batch = new PublishBatch().add(completed()).add(broker);

        assertThatThrownBy(() -> batch.awaitAll(BUDGET))
                .isInstanceOf(PublishNotConfirmedException.class)
                .hasMessageContaining("1 of 2 sends")
                .hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    @DisplayName("a send still in flight when the budget runs out is a failure, not a maybe — "
            + "treating a timeout as success is the same silent loss with a longer fuse")
    void aSendThatHasNotCompletedInTimeIsTreatedAsUnconfirmed() {
        var stuck = new CompletableFuture<SendResult<String, NotificationEvent>>();

        assertThatThrownBy(() -> new PublishBatch().add(stuck).awaitAll(BUDGET))
                .isInstanceOf(PublishNotConfirmedException.class)
                .hasMessageContaining("200ms")
                .hasMessageContaining("refusing to commit the offset");
    }

    @Test
    @DisplayName("500 stuck sends share ONE deadline: a fan-out waits the budget once, not 500 "
            + "times, because 500 x 10 s inside one poll is 83 minutes and a rebalance storm")
    void theDeadlineIsSharedByTheWholeBatchRatherThanAppliedPerSend() {
        var batch = new PublishBatch(500);
        for (int i = 0; i < 500; i++) {
            batch.add(new CompletableFuture<>());
        }

        long startedAt = System.nanoTime();
        assertThatThrownBy(() -> batch.awaitAll(BUDGET))
                .isInstanceOf(PublishNotConfirmedException.class);
        Duration waited = Duration.ofNanos(System.nanoTime() - startedAt);

        // Per-send timeouts would be 500 x 200 ms = 100 s. A generous ceiling here still fails
        // that implementation by two orders of magnitude, and does not flake on a loaded CI box.
        assertThat(waited).isLessThan(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("sends the broker already acknowledged are not failed by an exhausted budget — "
            + "otherwise a long batch of fast sends would starve the one still in flight")
    void alreadyAcknowledgedSendsCostNothingFromTheBudget() {
        var batch = new PublishBatch().add(completed()).add(completed());

        batch.awaitAll(Duration.ZERO);

        assertThat(batch.size())
                .as("confirmed futures are released so a 10M-recipient fan-out does not retain them")
                .isZero();
    }

    @Test
    @DisplayName("an interrupt during shutdown does not confirm the publish, and leaves the "
            + "interrupt flag set so the container stops instead of looping")
    void anInterruptedWaitIsNotAConfirmation() {
        var stuck = new CompletableFuture<SendResult<String, NotificationEvent>>();
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> new PublishBatch().add(stuck).awaitAll(Duration.ofSeconds(30)))
                    .isInstanceOf(PublishNotConfirmedException.class)
                    .hasMessageContaining("interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();   // clear it, so the flag does not leak into the next test
        }
    }

    @Test
    @DisplayName("a listener that published nothing waits for nothing")
    void anEmptyBatchIsNotAWait() {
        new PublishBatch().awaitAll(Duration.ZERO);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<SendResult<String, NotificationEvent>> completed() {
        return CompletableFuture.completedFuture(mock(SendResult.class));
    }
}
