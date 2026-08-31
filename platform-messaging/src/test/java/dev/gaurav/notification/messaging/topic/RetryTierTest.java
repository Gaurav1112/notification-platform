package dev.gaurav.notification.messaging.topic;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RetryTierTest {

    @Test
    @DisplayName("the tiers sum to the published 1h12m35s budget — a tier whose delay grows "
            + "silently extends how long a dead message keeps costing provider calls")
    void theTotalRetryBudgetIsBounded() {
        long total = 0;
        for (RetryTier tier : RetryTier.values()) {
            total += tier.delay().toSeconds();
        }
        assertThat(total).isEqualTo(RetryTier.TOTAL_BUDGET_SECONDS);
        assertThat(Duration.ofSeconds(total)).isEqualTo(Duration.ofMinutes(72).plusSeconds(35));
    }

    @Test
    @DisplayName("delays are strictly increasing — a tier that is not shorter than the next one "
            + "would park long waits at the head of a short-delay partition and stall it")
    void delaysIncreaseStrictly() {
        Duration previous = Duration.ZERO;
        for (RetryTier tier : RetryTier.values()) {
            assertThat(tier.delay()).isGreaterThan(previous);
            previous = tier.delay();
        }
    }

    @Test
    @DisplayName("the last tier has no next tier, so a message cannot loop the retry ladder forever")
    void theLastTierTerminates() {
        assertThat(RetryTier.T1H.next()).isEmpty();
        assertThat(RetryTier.T5S.next()).contains(RetryTier.T30S);
        assertThat(RetryTier.forAttempt(RetryTier.values().length + 1)).isEmpty();
        assertThat(RetryTier.forAttempt(0)).isEmpty();
        assertThat(RetryTier.forAttempt(1)).contains(RetryTier.first());
    }

    @Test
    @DisplayName("every tier maps to a topic we declared, so the tier consumer cannot subscribe "
            + "to a topic the broker refuses to create")
    void everyTierTopicIsDeclared() {
        for (RetryTier tier : RetryTier.values()) {
            assertThat(Topics.isKnown(tier.topic())).isTrue();
            assertThat(RetryTier.forTopic(tier.topic())).contains(tier);
        }
        assertThat(RetryTier.forTopic(Topics.DLQ)).isEmpty();
        assertThat(RetryTier.forTopic("notification.retry.5m")).isEqualTo(Optional.empty());
    }
}
