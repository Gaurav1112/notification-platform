package dev.gaurav.notification.provider;

import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.provider.decorator.TimeoutProvider;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The behavioural contract every provider adapter must satisfy — mock or real.
 *
 * <p>This exists because the expensive provider bugs are not vendor-specific. They are an adapter
 * that throws {@code IOException} on a connection reset instead of classifying it, an adapter that
 * reports {@code supportsBatching} with a {@code maxBatchSize} of 1, an adapter that returns an
 * {@code Accepted} with no message id so the webhook can never be correlated. Each of those breaks
 * the platform identically no matter whose API is behind it, so the assertions belong to the SPI,
 * not to any one adapter.
 *
 * <p>Extending this is the entire cost of adding a real provider's test suite. A subclass supplies
 * an adapter and a slow variant; everything below runs for free, and a new adapter cannot merge
 * while violating any of it.
 */
public abstract class ProviderContractTest {

    /** UUIDs from a fixed seed: the contract run must be identical on every machine. */
    private static final long FIXTURE_SEED = 424242L;

    /** The adapter under test. Should be configured to fail often, so the failure paths are exercised. */
    protected abstract NotificationProvider provider();

    /** An address this adapter considers ordinary — no magic-number behaviour. */
    protected abstract String validAddress();

    /** The same adapter configured to take longer than any reasonable deadline, using real time. */
    protected abstract NotificationProvider slowVariant();

    /** Force the adapter into a total outage; used to prove {@code isHealthy()} tracks reality. */
    protected abstract void forceHardDown();

    @Test
    @DisplayName("a business failure is returned, never thrown — 2,000 seeded sends produce zero exceptions")
    void businessFailuresAreReturnedNotThrown() {
        // An adapter that throws takes down the consumer thread rather than producing a classified
        // failure, so the retry engine never sees it and the message dies without a DLQ record.
        var provider = provider();
        var results = new ArrayList<SendResult>();

        assertThatCode(() -> {
            for (var recipientId : recipients(2_000)) {
                results.add(provider.send(command(recipientId)));
            }
        }).doesNotThrowAnyException();

        // Guard against a vacuous pass: if the fixture never failed, this test proved nothing.
        assertThat(results).anyMatch(r -> r instanceof SendResult.Rejected);
        assertThat(results).anyMatch(SendResult::isSuccess);
    }

    @Test
    @DisplayName("every result carries the fields the platform records on the attempt row")
    void everyResultIsUsable() {
        // A null message, a null failure type or a negative latency does not fail here — it fails
        // three layers away as a NOT NULL violation on delivery_attempt, or as a negative timer.
        for (var recipientId : recipients(500)) {
            var result = provider().send(command(recipientId));

            if (result instanceof SendResult.Accepted accepted) {
                assertThat(accepted.providerMessageId()).isNotBlank();
                assertThat(accepted.latency().isNegative()).isFalse();
                assertThat(accepted.costMicros()).isNotNegative();
            } else if (result instanceof SendResult.Rejected rejected) {
                assertThat(rejected.type()).isNotNull();
                assertThat(rejected.code()).isNotBlank();
                assertThat(rejected.message()).isNotBlank();
                assertThat(rejected.latency().isNegative()).isFalse();
                assertThat(rejected.retryAfter()).isNotNull();
                rejected.retryAfter().ifPresent(d -> assertThat(d.toMillis()).isPositive());
            } else if (result instanceof SendResult.Indeterminate indeterminate) {
                assertThat(indeterminate.type().isOutcomeIndeterminate())
                        .as("an Indeterminate must carry a FailureType the reconciler will pick up")
                        .isTrue();
                assertThat(indeterminate.message()).isNotBlank();
            } else {
                throw new AssertionError("unhandled SendResult case: " + result);
            }
        }
    }

    @Test
    @DisplayName("an accepted send returns a correlation id, or its delivery webhook can never be matched")
    void acceptedCarriesACorrelationId() {
        var accepted = firstAccepted();
        assertThat(accepted.providerMessageId()).isNotBlank();

        // Same idempotency token, same id: a redelivered command must not mint a second identity
        // that the status pipeline would treat as a different message.
        var fixedRecipient = UUID.fromString("11111111-2222-3333-4444-555555555555");
        var again = provider().send(command(fixedRecipient));
        var repeat = provider().send(command(fixedRecipient));
        assertThat(again).isEqualTo(repeat);
    }

    @Test
    @DisplayName("capabilities are self-consistent — maxBatchSize is 1 exactly when batching is off")
    void capabilitiesAreSelfConsistent() {
        var capabilities = provider().capabilities();

        assertThat(capabilities.maxBatchSize())
                .as("a batch of zero is not a thing")
                .isGreaterThanOrEqualTo(1);

        if (!capabilities.supportsBatching()) {
            assertThat(capabilities.maxBatchSize())
                    .as("supportsBatching=false with maxBatchSize>1 makes the fan-out code send a batch "
                            + "to an endpoint that cannot take one")
                    .isEqualTo(1);
        } else {
            assertThat(capabilities.maxBatchSize())
                    .as("supportsBatching=true with maxBatchSize=1 costs a batching code path that never batches")
                    .isGreaterThan(1);
        }
    }

    @Test
    @DisplayName("an adapter with no idempotency key must offer a webhook or a status query, or every timeout stays UNKNOWN forever")
    void indeterminateOutcomesAreResolvable() {
        var capabilities = provider().capabilities();
        if (capabilities.supportsIdempotencyKey()) {
            return; // the vendor deduplicates for us; nothing to resolve
        }
        assertThat(capabilities.supportsWebhook() || capabilities.supportsStatusQuery())
                .as("no idempotency key, no webhook and no status query means an attempt that times out "
                        + "can never be resolved and the reconciler has nothing to do")
                .isTrue();
    }

    @Test
    @DisplayName("a send that outlives its deadline surfaces as Indeterminate, not as a failure")
    void aTimedOutSendIsIndeterminate() {
        // Collapsing "we did not hear back" into "it failed" is what produces three OTPs for one
        // login: the retry engine re-sends a message the provider had already accepted.
        try (var bounded = TimeoutProvider.withDedicatedPool(slowVariant(), 2, 4, Duration.ofMillis(60))) {
            var result = bounded.send(command(UUID.randomUUID(), Duration.ofMillis(60)));

            assertThat(result).isInstanceOf(SendResult.Indeterminate.class);
            assertThat(((SendResult.Indeterminate) result).type()).isEqualTo(FailureType.PROVIDER_TIMEOUT);
            assertThat(result.isSuccess()).isFalse();
        }
    }

    @Test
    @DisplayName("identity survives every call — the registry, the router and the webhook all key on it")
    void identityIsStable() {
        var provider = provider();
        var channel = provider.channel();
        var code = provider.code();

        provider.send(command(UUID.randomUUID()));

        assertThat(provider.channel()).isEqualTo(channel);
        assertThat(provider.code()).isEqualTo(code);
        assertThat(provider.capabilities()).isEqualTo(provider.capabilities());
    }

    @Test
    @DisplayName("a hard outage flips isHealthy() so the router stops choosing a provider that cannot send")
    void hardDownIsVisibleToTheRouter() {
        var provider = provider();
        assertThat(provider.isHealthy()).isTrue();

        forceHardDown();

        assertThat(provider.isHealthy())
                .as("isHealthy() that ignores a known outage lets the router keep picking a dead provider "
                        + "until the breaker has burned 20 calls discovering it again")
                .isFalse();
        assertThat(provider.send(command(UUID.randomUUID())))
                .isInstanceOf(SendResult.Rejected.class);
    }

    // ---- fixtures -------------------------------------------------------------------------------

    protected SendCommand command(UUID recipientId) {
        return command(recipientId, Duration.ofSeconds(5));
    }

    protected SendCommand command(UUID recipientId, Duration deadline) {
        return new SendCommand(
                recipientId,
                provider().channel(),
                TrafficClass.TRANSACTIONAL,
                validAddress(),
                "Your verification code",
                "Your code is 314159. It expires in 60 seconds.",
                "tok-" + recipientId,
                Map.of(),
                deadline);
    }

    protected static List<UUID> recipients(int count) {
        var random = new Random(FIXTURE_SEED);
        var out = new ArrayList<UUID>(count);
        for (var i = 0; i < count; i++) {
            out.add(new UUID(random.nextLong(), random.nextLong()));
        }
        return out;
    }

    private SendResult.Accepted firstAccepted() {
        for (var recipientId : recipients(500)) {
            var result = provider().send(command(recipientId));
            if (result instanceof SendResult.Accepted accepted) return accepted;
        }
        throw new AssertionError("the fixture never produced an accepted send; check the configured success rate");
    }
}
