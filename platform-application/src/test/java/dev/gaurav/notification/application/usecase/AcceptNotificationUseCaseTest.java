package dev.gaurav.notification.application.usecase;

import dev.gaurav.notification.application.exception.IdempotencyConflictException;
import dev.gaurav.notification.application.exception.QuotaExceededException;
import dev.gaurav.notification.application.exception.RequestInProgressException;
import dev.gaurav.notification.application.fake.FakeEventPublisher;
import dev.gaurav.notification.application.fake.FakeIdempotencyStore;
import dev.gaurav.notification.application.fake.FakeQuotaGuard;
import dev.gaurav.notification.application.fake.FakeResponseSerializer;
import dev.gaurav.notification.application.fake.RecordingAcceptanceWriter;
import dev.gaurav.notification.application.fake.TestCommands;
import dev.gaurav.notification.application.result.AcceptResult;
import dev.gaurav.notification.domain.enums.Channel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every test here is a production incident that this class exists to prevent.
 *
 * <p>The scenarios are all variants of one situation: a client whose connection dropped and who
 * therefore does not know whether we accepted their request. Everything they can reasonably do next
 * — retry the same request, retry with a corrected body, retry while the first one is still running
 * — has to produce the right answer, and only one of those answers is "another notification".
 */
class AcceptNotificationUseCaseTest {

    private static final Instant NOW = Instant.parse("2026-08-31T09:14:22.481Z");

    private FakeIdempotencyStore idempotency;
    private FakeQuotaGuard quota;
    private RecordingAcceptanceWriter writer;
    private FakeEventPublisher publisher;
    private AcceptNotificationUseCase useCase;

    @BeforeEach
    void setUp() {
        idempotency = new FakeIdempotencyStore();
        quota = FakeQuotaGuard.allowing();
        writer = new RecordingAcceptanceWriter();
        publisher = new FakeEventPublisher();
        useCase = build();
    }

    private AcceptNotificationUseCase build() {
        return new AcceptNotificationUseCase(
                idempotency, quota, writer, new FakeResponseSerializer(), publisher,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("a retried request with the same key and the same body replays the original "
            + "response and does not create a second notification")
    void sameKeySameBodyReplaysAndCreatesNothing() {
        var first = useCase.accept(TestCommands.send("order-4821", "{\"orderId\":\"A-4821\"}"));
        var second = useCase.accept(TestCommands.send("order-4821", "{\"orderId\":\"A-4821\"}"));

        assertThat(first.hasReplay()).isFalse();
        assertThat(second.hasReplay()).isTrue();
        // The bytes the first caller got, handed back verbatim — including the original ids.
        assertThat(second.replayed().orElseThrow().body())
                .isEqualTo(new FakeResponseSerializer().serialize(first));
        assertThat(second.replayed().orElseThrow().status()).isEqualTo(202);

        // The assertion that matters: the accept transaction ran exactly once.
        assertThat(writer.persistCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("the same key with a different body is a 409, not a silent replay of an "
            + "unrelated response")
    void sameKeyDifferentFingerprintConflicts() {
        useCase.accept(TestCommands.send("order-4821", "{\"orderId\":\"A-4821\"}"));

        assertThatThrownBy(() ->
                useCase.accept(TestCommands.send("order-4821", "{\"orderId\":\"A-9999\"}")))
                .isInstanceOf(IdempotencyConflictException.class)
                .hasMessageContaining("order-4821");

        // A replay here would tell the caller their A-9999 order shipped. It never would have.
        assertThat(writer.persistCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("a concurrent request holding the key gets request-in-progress rather than a "
            + "second accept")
    void concurrentRequestOnTheSameKeyIsRejected() {
        // Claim the key without completing it — the state another pod is in mid-transaction.
        idempotency.claim(TestCommands.TENANT, "order-4821", TestCommands.fingerprintOf("body"));

        assertThatThrownBy(() -> useCase.accept(TestCommands.send("order-4821", "body")))
                .isInstanceOf(RequestInProgressException.class);

        assertThat(writer.persistCount()).isZero();
    }

    @Test
    @DisplayName("an unreachable quota limiter admits the request — a Valkey outage must not "
            + "read as every tenant being over quota")
    void quotaFailureFailsOpen() {
        quota = FakeQuotaGuard.allowing().unavailable();
        useCase = build();

        var result = useCase.accept(TestCommands.send("order-4821", "body"));

        assertThat(result.hasReplay()).isFalse();
        assertThat(writer.persistCount()).isEqualTo(1);
        assertThat(quota.calls()).isEqualTo(1);
    }

    @Test
    @DisplayName("a tenant that is genuinely over quota is rejected before anything is written")
    void quotaDenialRejectsBeforePersisting() {
        quota = FakeQuotaGuard.allowing().denying();
        useCase = build();

        assertThatThrownBy(() -> useCase.accept(TestCommands.send("order-4821", "body")))
                .isInstanceOf(QuotaExceededException.class);

        assertThat(writer.persistCount()).isZero();
    }

    @Test
    @DisplayName("quota is charged per recipient, so one call cannot bypass the limit by "
            + "carrying the whole audience")
    void quotaIsChargedPerRecipient() {
        useCase.accept(TestCommands.send("order-4821", "body"));

        assertThat(quota.lastPermits()).isEqualTo(2);
    }

    @Test
    @DisplayName("a broker outage after commit does not turn a committed accept into a 500")
    void publishFailureDoesNotFailTheAccept() {
        publisher = new FakeEventPublisher().broken();
        useCase = build();

        var result = useCase.accept(TestCommands.send("order-4821", "body"));

        assertThat(result.requestId()).isNotNull();
        assertThat(writer.persistCount()).isEqualTo(1);
        // The row and its outbox entry are committed; the sweeper is the guarantee, not the publish.
        assertThat(idempotency.find(TestCommands.TENANT, "order-4821")).isPresent();
    }

    @Test
    @DisplayName("one notification is accepted per requested channel")
    void oneNotificationPerChannel() {
        var result = useCase.accept(
                TestCommands.send("order-4821", "body", Set.of(Channel.SMS, Channel.EMAIL, Channel.PUSH)));

        assertThat(result.notifications())
                .hasSize(3)
                .extracting(AcceptResult.AcceptedNotification::channel)
                .containsExactlyInAnyOrder(Channel.SMS, Channel.EMAIL, Channel.PUSH);
    }

    @Test
    @DisplayName("the fast-path notice is published only after the accept transaction, and "
            + "carries the partition key the expander needs")
    void publishedNoticeCarriesThePartitionKey() {
        var result = useCase.accept(TestCommands.send("order-4821", "body"));

        assertThat(publisher.requested()).hasSize(1);
        var notice = publisher.requested().get(0);
        assertThat(notice.requestId()).isEqualTo(result.requestId());
        // Without requestCreatedAt the expander scans every daily partition instead of one.
        assertThat(notice.requestCreatedAt()).isEqualTo(NOW);
        assertThat(notice.idempotencyKey()).isEqualTo("order-4821");
    }
}
