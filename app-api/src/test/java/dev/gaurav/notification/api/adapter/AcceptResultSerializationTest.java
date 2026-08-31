package dev.gaurav.notification.api.adapter;

import dev.gaurav.notification.application.result.AcceptResult;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.messaging.config.KafkaProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A metamorphic oracle for the idempotency replay path: {@code read(write(x))} must equal {@code x}.
 *
 * <p>This test exists because a real defect shipped past a green suite of 371 tests.
 * {@link AcceptResult} had both a {@code replay} record component and an {@code isReplay()}
 * accessor. Jackson maps a no-arg {@code isXxx()} to a boolean property named {@code xxx}, so the
 * serialiser wrote {@code "replay": false} over the {@link AcceptResult.Replay} object. Reading the
 * stored body back then failed with
 * {@code MismatchedInputException: cannot construct Replay from boolean value (false)}.
 *
 * <p>The user-visible effect: <strong>every idempotent replay returned HTTP 500</strong> — the one
 * behaviour the {@code Idempotency-Key} header exists to provide. Nine unit tests covered
 * {@code AcceptResult} and none caught it, because every one of them asserted on the in-memory
 * object. This record is written to {@code idempotency_record.response_body} and read back on the
 * replay path, so surviving JSON is not an implementation detail — it is the feature.
 *
 * <p>Found by POSTing the same idempotency key twice against a running server. That is the whole
 * argument for end-to-end verification in one sentence.
 */
class AcceptResultSerializationTest {

    /** The same mapper the production replay path uses, so the test cannot pass by using a kinder one. */
    private final JsonMapper mapper = new KafkaProducerConfig("localhost:9092").notificationEventJsonMapper();

    private static AcceptResult.AcceptedNotification notification(Channel channel) {
        return new AcceptResult.AcceptedNotification(UUID.randomUUID(), channel, AcceptResult.STATUS_ACCEPTED);
    }

    @Test
    @DisplayName("an acceptance survives a JSON round trip with its notification ids intact")
    void acceptanceRoundTrips() {
        var original = AcceptResult.accepted(
                UUID.randomUUID(),
                Instant.parse("2026-08-31T15:11:20.891967Z"),
                1,
                List.of(notification(Channel.EMAIL)));

        var restored = mapper.readValue(mapper.writeValueAsString(original), AcceptResult.class);

        assertThat(restored.requestId()).isEqualTo(original.requestId());
        assertThat(restored.acceptedAt()).isEqualTo(original.acceptedAt());
        assertThat(restored.recipientCount()).isEqualTo(original.recipientCount());
        assertThat(restored.notifications())
                .extracting(AcceptResult.AcceptedNotification::id)
                .isEqualTo(original.notifications().stream().map(AcceptResult.AcceptedNotification::id).toList());
    }

    @Test
    @DisplayName("the replay accessor must not shadow the replay component and serialise as a boolean")
    void replayComponentIsNotShadowedByItsAccessor() {
        var accepted = AcceptResult.accepted(
                UUID.randomUUID(), Instant.now(), 1, List.of(notification(Channel.SMS)));

        var json = mapper.writeValueAsString(accepted);

        assertThat(json)
                .as("a boolean 'replay' field means an isXxx() accessor has shadowed the record "
                        + "component; renaming hasReplay() back to isReplay() reintroduces the 500")
                .doesNotContain("\"replay\":false")
                .doesNotContain("\"replay\": false");
    }

    @Test
    @DisplayName("a body stored on first accept is readable on the replay, which is what 202 promises")
    void storedBodyIsReadableBack() {
        var original = AcceptResult.accepted(
                UUID.randomUUID(), Instant.now(), 2,
                List.of(notification(Channel.EMAIL), notification(Channel.PUSH)));

        // exactly the two operations the adapter performs across the two requests
        var storedBody = mapper.writeValueAsString(original);
        var replayed = mapper.readValue(storedBody, AcceptResult.class);

        assertThat(replayed.notifications())
                .extracting(AcceptResult.AcceptedNotification::id)
                .containsExactlyElementsOf(
                        original.notifications().stream().map(AcceptResult.AcceptedNotification::id).toList());
        assertThat(replayed.hasReplay()).isFalse();
    }
}
