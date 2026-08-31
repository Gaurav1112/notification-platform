package dev.gaurav.notification.messaging.event;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.TrafficClass;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wire format is a contract with future deploys of other services. These tests are what stop
 * a refactor from silently changing it.
 */
class NotificationEventSerializationTest {

    private final JsonMapper mapper = JsonMapper.builder()
            .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    @Test
    @DisplayName("every event type survives a round trip with its subtype intact — losing the "
            + "discriminator turns a dispatch into an unroutable blob on the consumer side")
    void everyEventTypeRoundTrips() {
        assertRoundTrips(TestEvents.requested());
        assertRoundTrips(TestEvents.dispatch());
        assertRoundTrips(TestEvents.status(DeliveryStatus.SENT, 60));
        assertRoundTrips(TestEvents.retry());
        assertRoundTrips(TestEvents.deadLetter());
    }

    @Test
    @DisplayName("the eventId survives serialisation — it is the only thing the idempotent "
            + "receiver has to recognise a redelivery, and a lost one means a duplicate SMS")
    void theEventIdSurvivesTheWire() {
        NotificationDispatchEvent original = TestEvents.dispatch();

        String json = mapper.writeValueAsString(original);
        var parsed = mapper.readValue(json, NotificationEvent.class);

        assertThat(json).contains(original.eventId().toString());
        assertThat(parsed.eventId()).isEqualTo(original.eventId());
    }

    @Test
    @DisplayName("the discriminator is a stable wire name, not a Java class name — an FQCN on the "
            + "wire breaks every consumer the day the class is moved to another package")
    void theDiscriminatorIsNotAJavaClassName() {
        String json = mapper.writeValueAsString(TestEvents.dispatch());

        assertThat(json)
                .contains("\"eventType\":\"" + NotificationEvent.TYPE_DISPATCH + "\"")
                .doesNotContain("dev.gaurav.notification");
    }

    @Test
    @DisplayName("a payload with a field this consumer has not been redeployed to know about is "
            + "still readable — otherwise adding a field becomes a coordinated release")
    void anUnknownFieldFromANewerProducerDoesNotBreakAnOlderConsumer() {
        String json = mapper.writeValueAsString(TestEvents.status(DeliveryStatus.DELIVERED, 80));
        String withNewField = json.replaceFirst("\\{",
                "{\"carrierNetworkCode\":\"23415\",");

        var parsed = (DeliveryStatusEvent) mapper.readValue(withNewField, NotificationEvent.class);

        assertThat(parsed.status()).isEqualTo(DeliveryStatus.DELIVERED);
        assertThat(parsed.version()).isEqualTo(80);
    }

    @Test
    @DisplayName("instants are ISO-8601, not epoch numbers — a DLQ dump has to be readable by a "
            + "human at 3am, and numeric timestamp precision has shifted between Jackson versions")
    void instantsAreWrittenAsIso8601() {
        String json = mapper.writeValueAsString(TestEvents.dispatch());

        assertThat(json).contains("2026-08-31T09:15:00Z");
    }

    @Test
    @DisplayName("a retry carries the whole dispatch, so the tier consumer never has to touch "
            + "PostgreSQL — the property that makes sharing tiers across channels safe")
    void aRetryCarriesTheWholeDispatchInline() {
        String json = mapper.writeValueAsString(TestEvents.retry());

        var parsed = (RetryScheduledEvent) mapper.readValue(json, NotificationEvent.class);

        assertThat(parsed.dispatch().addressCipher()).isNotBlank();
        assertThat(parsed.dispatch().idempotencyToken()).isEqualTo("tok-018f3c2a-1");
        assertThat(parsed.tier().topic()).isEqualTo("notification.retry.5s");
    }

    @Test
    @DisplayName("no plaintext address ever reaches the wire — Kafka retains dispatch records for "
            + "3 days and broker-level encryption protects the disk, not the topic reader")
    void theWireCarriesCiphertextNotAPhoneNumber() {
        String json = mapper.writeValueAsString(
                TestEvents.dispatch(TrafficClass.CRITICAL, Channel.SMS));

        assertThat(json)
                .contains("Y2lwaGVydGV4dA==")
                .contains("+4477***4210")
                .doesNotContain("+447712344210");
    }

    private void assertRoundTrips(NotificationEvent original) {
        String json = mapper.writeValueAsString(original);
        NotificationEvent parsed = mapper.readValue(json, NotificationEvent.class);

        assertThat(parsed).isEqualTo(original);
        assertThat(parsed.eventType()).isEqualTo(original.eventType());
    }
}
