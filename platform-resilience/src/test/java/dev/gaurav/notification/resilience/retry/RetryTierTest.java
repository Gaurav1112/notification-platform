package dev.gaurav.notification.resilience.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RetryTierTest {

    @Test
    @DisplayName("a 17 s backoff is not rounded down to the 5 s lane, which would discard the backoff")
    void roundsUpNotToTheArithmeticallyNearest() {
        // 17 s is closer to 5 s than to 30 s. Choosing 5 s would fire the retry 12 s early and hand
        // the failing provider back the load the backoff was calculated to withhold.
        assertThat(RetryTier.nearestFor(Duration.ofSeconds(17))).isEqualTo(RetryTier.T30S);
    }

    @Test
    @DisplayName("an exact tier delay stays in its own lane rather than being promoted")
    void exactBoundaryStaysPut() {
        assertThat(RetryTier.nearestFor(Duration.ofSeconds(30))).isEqualTo(RetryTier.T30S);
        assertThat(RetryTier.nearestFor(Duration.ofSeconds(5))).isEqualTo(RetryTier.T5S);
    }

    @Test
    @DisplayName("a backoff longer than the last lane clamps instead of falling through to nothing")
    void beyondTheLadderClampsToTheLastLane() {
        assertThat(RetryTier.nearestFor(Duration.ofHours(9))).isEqualTo(RetryTier.T1H);
    }

    @Test
    @DisplayName("a zero delay still routes through a topic, never straight back to the provider")
    void zeroDelayStillGoesThroughATier() {
        assertThat(RetryTier.nearestFor(Duration.ZERO)).isEqualTo(RetryTier.T5S);
    }

    @Test
    @DisplayName("tier topic names match the broker's provisioned topics exactly")
    void topicNamesAreStable() {
        // These strings are provisioned as Kafka topics; a typo here is an auto-created topic with
        // one partition and default retention, which silently swallows the retry lane.
        assertThat(RetryTier.T5S.topic()).isEqualTo("notification.retry.5s");
        assertThat(RetryTier.T30S.topic()).isEqualTo("notification.retry.30s");
        assertThat(RetryTier.T2M.topic()).isEqualTo("notification.retry.2m");
        assertThat(RetryTier.T10M.topic()).isEqualTo("notification.retry.10m");
        assertThat(RetryTier.T1H.topic()).isEqualTo("notification.retry.1h");
    }

    @Test
    @DisplayName("the ladder sums to the 72-minute retry budget the policy deadline is set from")
    void ladderMatchesTheDeadline() {
        Duration total = Duration.ZERO;
        for (RetryTier tier : RetryTier.values()) {
            total = total.plus(tier.delay());
        }

        assertThat(total).isEqualTo(Duration.ofMinutes(72).plusSeconds(35));
        assertThat(total).isLessThanOrEqualTo(DefaultRetryPolicy.platformDefault(new java.util.Random())
                .totalDeadline().plusMinutes(1));
    }
}
