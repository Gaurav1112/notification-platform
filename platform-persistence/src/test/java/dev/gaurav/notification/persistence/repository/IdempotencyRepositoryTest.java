package dev.gaurav.notification.persistence.repository;

import static org.assertj.core.api.Assertions.assertThat;

import dev.gaurav.notification.persistence.AbstractPostgresTest;
import dev.gaurav.notification.persistence.entity.IdempotencyRecord;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Proves that the idempotency claim is a single serialisation point.
 *
 * <p>The failure being prevented is concrete and expensive: two retries of the same
 * {@code POST /notifications} both deciding they are the first, and the customer receiving — and
 * being billed for — two one-time passcodes.
 */
class IdempotencyRepositoryTest extends AbstractPostgresTest {

    private static final long TENANT = 1L;
    private static final long OTHER_TENANT = 2L;

    @Autowired
    private IdempotencyRepository idempotency;

    private Instant createdAt;
    private Instant windowFrom;
    private Instant windowTo;

    @BeforeEach
    void computeHourWindow() {
        createdAt = Instant.now();
        windowFrom = createdAt.truncatedTo(ChronoUnit.HOURS);
        windowTo = windowFrom.plus(Duration.ofHours(1));
    }

    @Test
    @DisplayName("a retried request with the same key claims nothing the second time")
    void secondClaimWithTheSameKeyAffectsNoRows() {
        assertThat(claim("order-4711", fingerprint("body-a"))).isEqualTo(1);

        var rowsAffected = claim("order-4711", fingerprint("body-a"));

        assertThat(rowsAffected)
                .as("ON CONFLICT DO NOTHING makes the unique index the serialisation point")
                .isZero();
    }

    @Test
    @DisplayName("the same key with a different body still loses the claim, so the caller can detect a 409")
    void reusedKeyWithDifferentBodyDoesNotOverwriteTheOriginal() {
        claim("order-4711", fingerprint("body-a"));

        var rowsAffected = claim("order-4711", fingerprint("body-completely-different"));

        assertThat(rowsAffected).isZero();
        // The stored fingerprint must remain the first caller's. If the second claim had won, a
        // replay would hand one caller another caller's response — a data leak, not a retry.
        assertThat(stored("order-4711").getRequestFingerprint())
                .isEqualTo(fingerprint("body-a"));
    }

    @Test
    @DisplayName("two tenants can use the same idempotency key without colliding")
    void keysAreScopedPerTenant() {
        assertThat(claim(TENANT, "checkout", fingerprint("body-a"))).isEqualTo(1);

        var rowsAffected = claim(OTHER_TENANT, "checkout", fingerprint("body-a"));

        assertThat(rowsAffected)
                .as("callers pick their own keys; 'checkout' is not ours to make globally unique")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("the losing claimer can read the winner's recorded response")
    void completionIsVisibleToTheLoser() {
        var requestId = UUID.randomUUID();
        claim("order-4711", fingerprint("body-a"));

        var completed = idempotency.complete(TENANT, "order-4711", windowFrom, windowTo,
                (short) 202, "{\"status\":\"ACCEPTED\"}", requestId);

        assertThat(completed).isEqualTo(1);
        var record = stored("order-4711");
        assertThat(record.getState()).isEqualTo(IdempotencyRecord.State.COMPLETED);
        assertThat(record.getResponseStatus()).isEqualTo((short) 202);
        assertThat(record.getRequestId()).isEqualTo(requestId);
    }

    @Test
    @DisplayName("completing twice is a no-op, so a late writer cannot overwrite a fenced record")
    void completeOnlyAppliesWhileInProgress() {
        claim("order-4711", fingerprint("body-a"));
        idempotency.complete(TENANT, "order-4711", windowFrom, windowTo,
                (short) 202, "{\"status\":\"ACCEPTED\"}", UUID.randomUUID());

        var second = idempotency.complete(TENANT, "order-4711", windowFrom, windowTo,
                (short) 500, "{\"status\":\"FAILED\"}", UUID.randomUUID());

        assertThat(second).isZero();
        assertThat(stored("order-4711").getResponseStatus()).isEqualTo((short) 202);
    }

    private int claim(String key, byte[] fingerprint) {
        return claim(TENANT, key, fingerprint);
    }

    private int claim(long tenantId, String key, byte[] fingerprint) {
        return idempotency.claim(tenantId, key, createdAt, fingerprint, UUID.randomUUID(),
                createdAt.plus(Duration.ofSeconds(30)), createdAt.plus(Duration.ofHours(24)));
    }

    private IdempotencyRecord stored(String key) {
        return idempotency.findInWindow(TENANT, key, windowFrom, windowTo).orElseThrow();
    }

    private static byte[] fingerprint(String body) {
        // Stands in for the SHA-256 of the canonical request body; only its stability matters here.
        return body.getBytes(StandardCharsets.UTF_8);
    }
}
