package dev.gaurav.notification.persistence.entity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.domain.enums.ScheduleType;
import dev.gaurav.notification.domain.enums.SuppressionReason;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.persistence.AbstractPostgresTest;
import dev.gaurav.notification.persistence.repository.DeadLetterMessageRepository;
import dev.gaurav.notification.persistence.repository.NotificationRecipientRepository;
import dev.gaurav.notification.persistence.repository.NotificationRequestRepository;
import dev.gaurav.notification.persistence.repository.ProviderConfigurationRepository;
import dev.gaurav.notification.persistence.repository.ProviderRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;

/**
 * Round-trips the column types that Hibernate's schema validation cannot vouch for.
 *
 * <p>{@code ddl-auto: validate} already proves every entity's columns exist with the right SQL
 * types — that runs on context startup for all thirteen. What it cannot prove is that a value can
 * actually be <em>bound</em>: {@code jsonb}, {@code varchar[]} and {@code bytea} all have JDBC
 * bindings that compile, pass validation, and then throw on the first INSERT. Those three, plus
 * the CHECK constraints the schema relies on for safety, are what this class pins down.
 */
class EntityMappingIntegrationTest extends AbstractPostgresTest {

    @Autowired
    private NotificationRequestRepository requests;

    @Autowired
    private NotificationRecipientRepository recipients;

    @Autowired
    private ProviderRepository providers;

    @Autowired
    private ProviderConfigurationRepository configurations;

    @Autowired
    private DeadLetterMessageRepository deadLetters;

    @Test
    @DisplayName("a request round-trips its varchar[] channels and jsonb payload without a type error")
    void requestRoundTripsArrayAndJsonColumns() {
        var id = UUID.randomUUID();
        var createdAt = Instant.now();
        var request = new NotificationRequest(id, 1L, TrafficClass.CRITICAL,
                createdAt.plus(Duration.ofMinutes(1)));
        request.setCreatedAt(createdAt);
        request.setScheduleType(ScheduleType.IMMEDIATE);
        request.setRecipientSource(NotificationRequest.RecipientSource.INLINE);
        // PgJDBC will not bind a plain String to either of these columns — it sends varchar and
        // PostgreSQL refuses. If this insert succeeds the array and JSON mappings are real.
        request.setChannels(new String[] {Channel.SMS.name(), Channel.EMAIL.name()});
        request.setPayload("{\"code\":\"123456\"}");
        requests.save(request);

        var from = createdAt.truncatedTo(ChronoUnit.DAYS);
        var reloaded = requests.findInWindow(id, 1L, from, from.plus(Duration.ofDays(1))).orElseThrow();

        assertThat(reloaded.getChannels()).containsExactly("SMS", "EMAIL");
        assertThat(reloaded.getPayload()).contains("123456");
        assertThat(reloaded.getTrafficClass()).isEqualTo(TrafficClass.CRITICAL);
    }

    @Test
    @DisplayName("an encrypted address survives the bytea round trip byte for byte")
    void recipientRoundTripsByteaAndNullableEnums() {
        var id = UUID.randomUUID();
        var createdAt = Instant.now();
        var cipher = new byte[] {0, -1, 42, 127, -128};
        var hash = "hmac".getBytes(StandardCharsets.UTF_8);

        var recipient = new NotificationRecipient(id, UUID.randomUUID(), createdAt, 1L,
                Channel.EMAIL, cipher, hash);
        recipient.setCreatedAt(createdAt);
        recipient.setStatus(DeliveryStatus.SUPPRESSED);
        recipient.setSuppressionReason(SuppressionReason.QUIET_HOURS);
        recipient.setFailureType(FailureType.INVALID_RECIPIENT);
        recipients.save(recipient);

        var from = createdAt.truncatedTo(ChronoUnit.DAYS);
        var reloaded = recipients.findInWindow(id, 1L, from, from.plus(Duration.ofDays(1))).orElseThrow();

        // A lossy round trip here would corrupt ciphertext, and AES-GCM fails closed: the address
        // becomes permanently undecryptable and the recipient can never be contacted again.
        assertThat(reloaded.getAddressCipher()).isEqualTo(cipher);
        assertThat(reloaded.getAddressHash()).isEqualTo(hash);
        assertThat(reloaded.getSuppressionReason()).isEqualTo(SuppressionReason.QUIET_HOURS);
        assertThat(reloaded.getStatusRank()).isEqualTo((short) DeliveryStatus.SUPPRESSED.rank());
    }

    @Test
    @DisplayName("the database refuses to store a raw API key in credentials_ref")
    void credentialsRefMustBeASecretManagerPointer() {
        var provider = providers.save(new Provider("mock-sms", "Mock SMS", Channel.SMS, "mock"));
        var leaked = new ProviderConfiguration(provider.getId(), (short) 1,
                "SK1234567890abcdefTHISISAKEY");

        // Code review can miss a pasted key; a CHECK constraint cannot. This is the last line of
        // defence, and it has to be exercised or nobody will notice when someone drops it.
        assertThatThrownBy(() -> configurations.saveAndFlush(leaked))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("a valid provider configuration stores its jsonb settings")
    void providerConfigurationRoundTripsJsonSettings() {
        var provider = providers.save(new Provider("mock-email", "Mock Email", Channel.EMAIL, "mock"));
        var configuration = new ProviderConfiguration(provider.getId(), (short) 1, "mock:email-primary");
        configuration.setSettings("{\"region\":\"ap-northeast-1\"}");

        var saved = configurations.saveAndFlush(configuration);

        var reloaded = configurations.findById(saved.getId()).orElseThrow();
        assertThat(reloaded.getSettings()).contains("ap-northeast-1");
        assertThat(reloaded.getRowVersion()).isZero();
    }

    @Test
    @DisplayName("a poison-pill storm collapses into one dead-letter row with a count, not a million rows")
    void repeatedFailuresAreCountedNotDuplicated() {
        var digest = "stack-digest".getBytes(StandardCharsets.UTF_8);

        for (var i = 0; i < 5; i++) {
            deadLetters.record(Instant.now(), 1L, "notification.dispatch.sms.tx", 3, 100L + i,
                    "tenant-1:user-7", "{\"broken\":true}", "SerializationException",
                    "unknown field 'kind'", digest);
        }

        var rows = deadLetters.findAll();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getOccurrenceCount())
                .as("without the upsert this table becomes an unreadable four-million-row queue")
                .isEqualTo(5);
    }
}
