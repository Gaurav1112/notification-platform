package dev.gaurav.notification.domain.enums;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The monotonic state machine is the mechanism that makes at-least-once delivery safe, so these
 * are the highest-value tests in the domain module. Each one encodes a real production failure.
 */
class DeliveryStatusTest {

    @Nested
    @DisplayName("out-of-order provider webhooks")
    class OutOfOrder {

        @Test
        @DisplayName("a late SENT does not overwrite DELIVERED")
        void lateSentIsRejected() {
            // The provider's DELIVERED callback beat our own SENT write. Without the guard, a
            // naive setStatus() would move the record backwards, corrupting delivery metrics and
            // — if retries are driven off status — re-sending to a user who already received it.
            assertThat(DeliveryStatus.DELIVERED.transitionTo(DeliveryStatus.SENT)).isEmpty();
        }

        @Test
        @DisplayName("DELIVERED still applies when it arrives first")
        void deliveredAppliesOverSent() {
            assertThat(DeliveryStatus.SENT.transitionTo(DeliveryStatus.DELIVERED))
                    .contains(DeliveryStatus.DELIVERED);
        }

        @Test
        @DisplayName("a duplicate webhook is a no-op, not an error")
        void duplicateIsRejected() {
            // Providers retry their callbacks. Same rank means no forward movement.
            assertThat(DeliveryStatus.DELIVERED.transitionTo(DeliveryStatus.DELIVERED)).isEmpty();
        }
    }

    @Nested
    @DisplayName("terminal states are absorbing")
    class Terminal {

        @Test
        @DisplayName("nothing escapes a terminal state, even a higher rank")
        void terminalBlocksHigherRank() {
            // BOUNCED (85) outranks nothing that follows it; FAILED (58) is terminal even though
            // SENT (60) and DELIVERED (80) rank higher. A provider callback arriving after we
            // gave up must not resurrect the notification.
            assertThat(DeliveryStatus.FAILED.transitionTo(DeliveryStatus.DELIVERED)).isEmpty();
            assertThat(DeliveryStatus.BOUNCED.transitionTo(DeliveryStatus.UNKNOWN)).isEmpty();
        }

        @Test
        @DisplayName("DELIVERED is deliberately NOT terminal, because a hard bounce can follow")
        void deliveredIsNotTerminal() {
            // An SMTP 250 means "accepted", not "landed in the inbox". A hard bounce legitimately
            // follows. State machines that treat delivered as final silently drop bounce events,
            // which means the address never reaches the suppression list.
            assertThat(DeliveryStatus.DELIVERED.isTerminal()).isFalse();
            assertThat(DeliveryStatus.DELIVERED.transitionTo(DeliveryStatus.BOUNCED))
                    .contains(DeliveryStatus.BOUNCED);
        }
    }

    @Nested
    @DisplayName("rank ordering encodes business rules")
    class RankSemantics {

        @Test
        @DisplayName("you cannot cancel something already queued — enforced by rank, not an if")
        void cannotCancelAfterQueueing() {
            // CANCELLED is rank 28, QUEUED is 30. The guard itself forbids the transition, so
            // there is no `if (status == QUEUED) throw` for a future engineer to forget.
            assertThat(DeliveryStatus.CANCELLED.rank()).isLessThan(DeliveryStatus.QUEUED.rank());
            assertThat(DeliveryStatus.QUEUED.transitionTo(DeliveryStatus.CANCELLED)).isEmpty();
        }

        @Test
        @DisplayName("the reverse race is also correct: CANCELLED first blocks QUEUED")
        void cancelBeforeQueueingWins() {
            // Both orderings must be safe. If CANCELLED commits first it is terminal, so the
            // racing QUEUED is rejected by the terminal check rather than by rank.
            assertThat(DeliveryStatus.CANCELLED.isTerminal()).isTrue();
            assertThat(DeliveryStatus.CANCELLED.transitionTo(DeliveryStatus.QUEUED)).isEmpty();
        }

        @Test
        @DisplayName("EXPIRED ranks below QUEUED because a TTL can only elapse before sending")
        void expiredPrecedesSending() {
            assertThat(DeliveryStatus.EXPIRED.rank()).isLessThan(DeliveryStatus.QUEUED.rank());
        }

        @Test
        @DisplayName("every rank is unique, or the ordering is ambiguous")
        void ranksAreUnique() {
            var ranks = Arrays.stream(DeliveryStatus.values())
                    .map(DeliveryStatus::rank)
                    .collect(Collectors.toSet());
            assertThat(ranks).hasSize(DeliveryStatus.values().length);
        }
    }

    @Nested
    @DisplayName("the UNKNOWN state")
    class Unknown {

        @Test
        @DisplayName("UNKNOWN outranks everything so nothing accidentally overwrites it")
        void unknownIsHighestRank() {
            var maxOther = Arrays.stream(DeliveryStatus.values())
                    .filter(s -> s != DeliveryStatus.UNKNOWN)
                    .mapToInt(DeliveryStatus::rank)
                    .max()
                    .orElseThrow();
            assertThat(DeliveryStatus.UNKNOWN.rank()).isGreaterThan(maxOther);
        }

        @Test
        @DisplayName("UNKNOWN is not terminal, so reconciliation can still resolve it")
        void unknownRemainsResolvable() {
            // The provider timed out after possibly delivering. We must be able to move to a real
            // answer once the webhook arrives or reconciliation runs.
            assertThat(DeliveryStatus.UNKNOWN.isTerminal()).isFalse();
        }
    }
}
