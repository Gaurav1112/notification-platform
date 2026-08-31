package dev.gaurav.notification.persistence.repository;

import static org.assertj.core.api.Assertions.assertThat;

import dev.gaurav.notification.persistence.AbstractPostgresTest;
import dev.gaurav.notification.persistence.entity.OutboxMessage;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Proves the outbox relay can be scaled horizontally.
 *
 * <p>The interesting test is the concurrent one. Everything else about the outbox pattern is
 * bookkeeping; the thing that decides whether it works with more than one relay instance is
 * whether two claimers can take disjoint batches without blocking each other.
 */
class OutboxRepositoryTest extends AbstractPostgresTest {

    @Autowired
    private OutboxRepository outbox;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Test
    @DisplayName("two concurrent relays claim disjoint batches instead of publishing the same rows twice")
    void skipLockedGivesConcurrentClaimersDisjointRows() throws Exception {
        seed(10);

        // Both claimers hold their locks until the other has finished claiming. Without the
        // barrier the first transaction could commit before the second even starts, and the test
        // would pass on plain FOR UPDATE — proving nothing.
        var bothClaimed = new CyclicBarrier(2);

        var pool = Executors.newFixedThreadPool(2);
        try {
            Future<List<Long>> first = pool.submit(() -> claimHolding(5, bothClaimed));
            Future<List<Long>> second = pool.submit(() -> claimHolding(5, bothClaimed));

            var firstBatch = first.get(30, TimeUnit.SECONDS);
            var secondBatch = second.get(30, TimeUnit.SECONDS);

            assertThat(firstBatch).hasSize(5);
            assertThat(secondBatch).hasSize(5);
            assertThat(firstBatch)
                    .as("SKIP LOCKED must step over the other claimer's rows, not wait for them")
                    .doesNotContainAnyElementsOf(secondBatch);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("a relay that asks for more than exists gets what there is, not an empty batch")
    void claimReturnsWhateverIsAvailable() {
        seed(3);

        var claimed = transactionTemplate.execute(status -> outbox.claimUnpublished(50));

        assertThat(claimed).hasSize(3);
    }

    @Test
    @DisplayName("published rows are deleted, so the unpublished backlog is the alertable number")
    void deletingClaimedRowsDrainsTheBacklog() {
        seed(4);

        transactionTemplate.executeWithoutResult(status -> {
            var claimed = outbox.claimUnpublished(4);
            var ids = claimed.stream().map(OutboxMessage::getId).toList();
            assertThat(outbox.deleteByIdIn(ids)).isEqualTo(4);
        });

        assertThat(outbox.countUnpublished())
                .as("stamping published_at instead of deleting would leave the table growing forever")
                .isZero();
    }

    @Test
    @DisplayName("claiming outside a transaction fails loudly rather than handing out useless locks")
    void claimWithoutATransactionIsRejected() {
        seed(1);

        // A lock taken outside a transaction is released immediately, so a caller who forgot the
        // transaction would 'succeed' and then let a second relay claim the same row. MANDATORY
        // propagation turns that into an exception at the first call instead.
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> outbox.claimUnpublished(1))
                .isInstanceOf(org.springframework.transaction.IllegalTransactionStateException.class);
    }

    private List<Long> claimHolding(int batchSize, CyclicBarrier bothClaimed) {
        return transactionTemplate.execute(status -> {
            var ids = outbox.claimUnpublished(batchSize).stream().map(OutboxMessage::getId).toList();
            try {
                bothClaimed.await(20, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("the other claimer never got its batch", e);
            }
            return ids;
        });
    }

    private void seed(int count) {
        for (var i = 0; i < count; i++) {
            outbox.save(new OutboxMessage(
                    "notification",
                    "notification-" + i,
                    "notification.queued",
                    "notification.dispatch.sms.tx",
                    "tenant-1:user-" + i,
                    "{\"seq\":%d}".formatted(i)));
        }
    }
}
