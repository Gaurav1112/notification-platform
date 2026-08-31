package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.StubProvider;
import dev.gaurav.notification.provider.spi.SendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka is at-least-once, so a redelivered dispatch record is not an edge case — it is Tuesday.
 * These tests pin the behaviour that keeps a redelivery from becoming a second text message.
 */
class IdempotentProviderTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-08-31T09:00:00Z"), ZoneOffset.UTC);

    /**
     * The log and the decorator must share a clock. They do not by default, and the symptom is
     * subtle: every record looks expired the instant it is written, so deduplication silently
     * stops happening and nothing fails until a user gets two OTPs.
     */
    private static InMemorySentTokenLog tokenLog() {
        return new InMemorySentTokenLog(CLOCK, Duration.ofHours(1), 1_000);
    }

    @Test
    @DisplayName("a redelivered command is not sent twice to a provider that has no idempotency key")
    void aProvenSendIsNotRepeated() {
        var stub = StubProvider.alwaysAccepts("mock-sms-primary", Channel.SMS);
        var idempotent = new IdempotentProvider(stub, tokenLog(), CLOCK);
        var command = StubProvider.command(Channel.SMS, "tok-otp-1");

        var first = idempotent.send(command);
        var second = idempotent.send(command);

        assertThat(stub.callCount())
                .as("Twilio cannot deduplicate for us; if we call twice the user gets two OTPs")
                .isEqualTo(1);
        assertThat(second).isEqualTo(first);
    }

    @Test
    @DisplayName("the token is recorded before the call, because a crash after the call is the case it exists for")
    void theTokenIsRecordedBeforeTheProviderIsCalled() {
        var log = new InMemorySentTokenLog(CLOCK, Duration.ofHours(1), 1_000);
        var observed = new boolean[1];
        var stub = new StubProvider("mock-sms-primary", Channel.SMS, command -> {
            // Recording after the delegate returns would leave nothing behind for a worker killed
            // between the HTTP request and the attempt write — the exact window in question.
            observed[0] = log.startedAt(dev.gaurav.notification.provider.spi.ProviderCode.of("mock-sms-primary"),
                    command.idempotencyToken()).isPresent();
            return new SendResult.Accepted("SM1", Duration.ofMillis(5), 7_900L);
        });

        new IdempotentProvider(stub, log, CLOCK).send(StubProvider.command(Channel.SMS, "tok-otp-2"));

        assertThat(observed[0]).isTrue();
    }

    @Test
    @DisplayName("a previous Indeterminate does not short-circuit, because 'we do not know' is not 'already sent'")
    void anUnknownOutcomeIsRetried() {
        // Treating Indeterminate as proof of delivery silently drops real messages. The decision
        // belongs to the retry engine, which has the TTL and the attempt count; not here.
        var stub = new StubProvider("mock-sms-primary", Channel.SMS,
                command -> new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT, "no response",
                        Duration.ofSeconds(5)));
        var idempotent = new IdempotentProvider(stub, tokenLog(), CLOCK);
        var command = StubProvider.command(Channel.SMS, "tok-otp-3");

        idempotent.send(command);
        idempotent.send(command);

        assertThat(stub.callCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("a previous transient rejection does not short-circuit, or a retryable failure could never be retried")
    void aRejectedSendIsRetried() {
        var stub = new StubProvider("mock-sms-primary", Channel.SMS,
                command -> SendResult.Rejected.of(FailureType.TRANSIENT_NETWORK, "30003",
                        "Unreachable destination handset", Duration.ofMillis(20)));
        var idempotent = new IdempotentProvider(stub, tokenLog(), CLOCK);
        var command = StubProvider.command(Channel.SMS, "tok-otp-4");

        idempotent.send(command);
        idempotent.send(command);

        assertThat(stub.callCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("two providers do not share a token namespace, so a failover is not mistaken for a duplicate")
    void tokensAreScopedToTheProvider() {
        // The same message really is sent twice during a failover — once per provider. Keying the
        // log on the token alone would make the secondary refuse to send after the primary failed.
        var log = new InMemorySentTokenLog(CLOCK, Duration.ofHours(1), 1_000);
        var primary = StubProvider.alwaysAccepts("mock-sms-primary", Channel.SMS);
        var secondary = StubProvider.alwaysAccepts("mock-sms-secondary", Channel.SMS);
        var command = StubProvider.command(Channel.SMS, "tok-otp-5");

        new IdempotentProvider(primary, log, CLOCK).send(command);
        new IdempotentProvider(secondary, log, CLOCK).send(command);

        assertThat(primary.callCount()).isEqualTo(1);
        assertThat(secondary.callCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("the token log is bounded, because an unbounded map in a 24/7 worker is a scheduled OOM")
    void theLogEvictsRatherThanGrowingForever() {
        var log = new InMemorySentTokenLog(CLOCK, Duration.ofHours(1), 32);
        var code = dev.gaurav.notification.provider.spi.ProviderCode.of("mock-sms-primary");

        for (var i = 0; i < 500; i++) {
            log.recordAttempt(code, "tok-" + i, CLOCK.instant());
        }

        assertThat(log.size()).isLessThanOrEqualTo(32);
        assertThat(log.evictions()).isPositive();
    }

    @Test
    @DisplayName("an expired record stops answering, so a replay is not resolved with last Tuesday's outcome")
    void recordsExpire() {
        var start = Instant.parse("2026-08-31T09:00:00Z");
        var clock = new MutableClock(start);
        var log = new InMemorySentTokenLog(clock, Duration.ofMinutes(30), 1_000);
        var code = dev.gaurav.notification.provider.spi.ProviderCode.of("mock-sms-primary");

        log.recordAttempt(code, "tok-old", start);
        log.recordOutcome(code, "tok-old", new SendResult.Accepted("SM1", Duration.ofMillis(5), 1L));
        assertThat(log.previousOutcome(code, "tok-old")).isPresent();

        clock.advance(Duration.ofHours(2));

        assertThat(log.previousOutcome(code, "tok-old")).isEmpty();
    }

    /** A clock a test can move forward; {@code Clock.offset} would need re-wiring on every step. */
    private static final class MutableClock extends Clock {
        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
