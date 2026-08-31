package dev.gaurav.notification.worker.channel;

import dev.gaurav.notification.domain.enums.AttemptState;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.spi.SendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the three-case handling of {@link SendResult} — in particular that
 * {@code Indeterminate} has its own outcome and is not folded into failure.
 */
class SendOutcomeTest {

    private static final Duration LATENCY = Duration.ofMillis(250);

    @Test
    @DisplayName("an accepted send is SENT, not DELIVERED — a 250 means queued, not in the inbox")
    void acceptedBecomesSent() {
        var outcome = SendOutcome.from(new SendResult.Accepted("SM123", LATENCY, 750L));

        assertThat(outcome.attemptState()).isEqualTo(AttemptState.SUCCEEDED);
        assertThat(outcome.status()).isEqualTo(DeliveryStatus.SENT);
        assertThat(outcome.providerMessageId()).isEqualTo("SM123");
        assertThat(outcome.costMicros()).isEqualTo(750L);
        assertThat(outcome.succeeded()).isTrue();
    }

    @Test
    @DisplayName("a rejection keeps its classification, so the retry decision is never a guess")
    void rejectedCarriesTheClassification() {
        var rejected = new SendResult.Rejected(FailureType.RATE_LIMITED, "20429", "too many requests",
                LATENCY, Optional.of(Duration.ofSeconds(30)));

        var outcome = SendOutcome.from(rejected);

        assertThat(outcome.attemptState()).isEqualTo(AttemptState.FAILED);
        assertThat(outcome.status()).isEqualTo(DeliveryStatus.SEND_FAILED);
        assertThat(outcome.failureType()).isEqualTo(FailureType.RATE_LIMITED);
        assertThat(outcome.retryAfter()).contains(Duration.ofSeconds(30));
        assertThat(outcome.indeterminate()).isFalse();
    }

    @Test
    @DisplayName("a timeout becomes UNKNOWN rather than FAILED, because it may in fact have been sent")
    void indeterminateBecomesUnknownNotFailed() {
        var outcome = SendOutcome.from(
                new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT, "no response", LATENCY));

        assertThat(outcome.attemptState()).isEqualTo(AttemptState.UNKNOWN);
        assertThat(outcome.status()).isEqualTo(DeliveryStatus.UNKNOWN);
        assertThat(outcome.indeterminate()).isTrue();
        assertThat(outcome.succeeded()).isFalse();
    }

    @Test
    @DisplayName("UNKNOWN is not terminal, so a late webhook can still resolve the send")
    void unknownRemainsOpenForReconciliation() {
        var outcome = SendOutcome.from(
                new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT, "no response", LATENCY));

        assertThat(outcome.status().isTerminal())
                .as("a terminal UNKNOWN would make the reconciler's verdict unappliable forever")
                .isFalse();
    }

    @Test
    @DisplayName("the three cases produce three distinct attempt states — none collapses into another")
    void allThreeCasesAreDistinct() {
        var accepted = SendOutcome.from(new SendResult.Accepted("id", LATENCY, 0L));
        var rejected = SendOutcome.from(SendResult.Rejected.of(
                FailureType.INVALID_RECIPIENT, "21614", "not a mobile", LATENCY));
        var unknown = SendOutcome.from(
                new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT, "timeout", LATENCY));

        assertThat(Arrays.asList(accepted.attemptState(), rejected.attemptState(), unknown.attemptState()))
                .containsExactly(AttemptState.SUCCEEDED, AttemptState.FAILED, AttemptState.UNKNOWN)
                .doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("SendResult still permits exactly three cases, so the handler covers all of them")
    void theSealedHierarchyStillHasThreeCases() {
        // A fourth case fails here rather than at 3 a.m. inside dispatch().
        assertThat(SendResult.class.isSealed()).isTrue();
        assertThat(SendResult.class.getPermittedSubclasses())
                .containsExactlyInAnyOrder(SendResult.Accepted.class, SendResult.Rejected.class,
                        SendResult.Indeterminate.class);
    }

    @Test
    @DisplayName("the handler declares one method per SendResult case, so none can be forgotten")
    void theHandlerCoversEveryCase() {
        long abstractMethods = Arrays.stream(SendResultHandler.class.getDeclaredMethods())
                .filter(method -> Modifier.isAbstract(method.getModifiers()))
                .map(Method::getName)
                .distinct()
                .count();

        assertThat(abstractMethods)
                .as("Java 17 has no pattern switch; this count is what makes the handling exhaustive")
                .isEqualTo(SendResult.class.getPermittedSubclasses().length);
    }
}
