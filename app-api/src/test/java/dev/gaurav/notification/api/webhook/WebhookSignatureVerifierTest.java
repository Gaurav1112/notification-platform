package dev.gaurav.notification.api.webhook;

import dev.gaurav.notification.api.config.WebhookProperties;
import dev.gaurav.notification.api.error.ProblemType;
import dev.gaurav.notification.api.error.WebhookVerificationException;
import dev.gaurav.notification.api.error.WebhookVerificationException.Reason;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The four gates an inbound webhook must pass. Every test here is a way a forged or replayed
 * delivery receipt could otherwise mark a failed notification as delivered and suppress its retry.
 */
class WebhookSignatureVerifierTest {

    private static final String PROVIDER = "mock-sms-primary";
    private static final String SECRET = "local-development-hmac-secret-not-a-real-key";
    private static final Instant NOW = Instant.parse("2026-08-31T09:14:22Z");
    private static final byte[] BODY = """
            {"messageId":"SM9f3a","status":"delivered"}""".getBytes(StandardCharsets.UTF_8);

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    @Test
    @DisplayName("a correctly signed payload inside the window is accepted")
    void validSignatureIsAccepted() {
        var verifier = verifier(Set.of());
        var timestamp = Long.toString(NOW.getEpochSecond());

        var result = verifier.verify(PROVIDER, BODY, "sha256=" + sign(SECRET, timestamp, BODY),
                timestamp, "10.0.0.1");

        assertThat(result).isEqualTo(NOW);
    }

    @Test
    @DisplayName("a single flipped byte in the body invalidates the signature — the payload is signed, not just the id")
    void tamperedBodyIsRejected() {
        var verifier = verifier(Set.of());
        var timestamp = Long.toString(NOW.getEpochSecond());
        var signature = "sha256=" + sign(SECRET, timestamp, BODY);
        var tampered = """
                {"messageId":"SM9f3a","status":"DELIVERED"}""".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> verifier.verify(PROVIDER, tampered, signature, timestamp, "10.0.0.1"))
                .isInstanceOf(WebhookVerificationException.class)
                .extracting(e -> ((WebhookVerificationException) e).reason())
                .isEqualTo(Reason.SIGNATURE_MISMATCH);
    }

    @Test
    @DisplayName("moving the timestamp to dodge the replay window breaks the signature, because the timestamp is signed")
    void replayWithAdjustedTimestampIsRejected() {
        var verifier = verifier(Set.of());
        var captured = Long.toString(NOW.minus(Duration.ofHours(2)).getEpochSecond());
        var capturedSignature = "sha256=" + sign(SECRET, captured, BODY);
        var freshTimestamp = Long.toString(NOW.getEpochSecond());

        // Keeping the original timestamp fails gate two; changing it fails gate one. Both closed
        // is what makes a captured payload worthless five minutes later.
        assertThatThrownBy(() -> verifier.verify(PROVIDER, BODY, capturedSignature, captured, "10.0.0.1"))
                .isInstanceOf(WebhookVerificationException.class)
                .extracting(e -> ((WebhookVerificationException) e).reason())
                .isEqualTo(Reason.TIMESTAMP_OUTSIDE_WINDOW);

        assertThatThrownBy(() -> verifier.verify(PROVIDER, BODY, capturedSignature, freshTimestamp, "10.0.0.1"))
                .isInstanceOf(WebhookVerificationException.class)
                .extracting(e -> ((WebhookVerificationException) e).reason())
                .isEqualTo(Reason.SIGNATURE_MISMATCH);
    }

    @Test
    @DisplayName("a timestamp in the future is rejected too, so the replay window cannot be widened forwards")
    void futureTimestampIsRejected() {
        var verifier = verifier(Set.of());
        var future = Long.toString(NOW.plus(Duration.ofMinutes(30)).getEpochSecond());

        assertThatThrownBy(() -> verifier.verify(PROVIDER, BODY, "sha256=" + sign(SECRET, future, BODY),
                future, "10.0.0.1"))
                .isInstanceOf(WebhookVerificationException.class)
                .extracting(e -> ((WebhookVerificationException) e).reason())
                .isEqualTo(Reason.TIMESTAMP_OUTSIDE_WINDOW);
    }

    @Test
    @DisplayName("a payload signed with another provider's secret does not verify")
    void wrongSecretIsRejected() {
        var verifier = verifier(Set.of());
        var timestamp = Long.toString(NOW.getEpochSecond());

        assertThatThrownBy(() -> verifier.verify(PROVIDER, BODY,
                "sha256=" + sign("some-other-providers-secret", timestamp, BODY), timestamp, "10.0.0.1"))
                .isInstanceOf(WebhookVerificationException.class);
    }

    @Test
    @DisplayName("a provider with no configured secret cannot post at all, rather than falling through to no check")
    void unknownProviderIsRejected() {
        var verifier = verifier(Set.of());
        var timestamp = Long.toString(NOW.getEpochSecond());

        assertThatThrownBy(() -> verifier.verify("not-configured", BODY,
                "sha256=" + sign(SECRET, timestamp, BODY), timestamp, "10.0.0.1"))
                .isInstanceOf(WebhookVerificationException.class)
                .extracting(e -> ((WebhookVerificationException) e).reason())
                .isEqualTo(Reason.UNKNOWN_PROVIDER);
    }

    @Test
    @DisplayName("a valid signature from an address outside the allowlist is still rejected")
    void sourceIpOutsideAllowlistIsRejected() {
        var verifier = verifier(Set.of("203.0.113.7"));
        var timestamp = Long.toString(NOW.getEpochSecond());

        assertThatThrownBy(() -> verifier.verify(PROVIDER, BODY, "sha256=" + sign(SECRET, timestamp, BODY),
                timestamp, "198.51.100.9"))
                .isInstanceOf(WebhookVerificationException.class)
                .extracting(e -> ((WebhookVerificationException) e).reason())
                .isEqualTo(Reason.SOURCE_IP_NOT_ALLOWED);
    }

    @Test
    @DisplayName("garbage in the signature header is a rejection, not a 500")
    void malformedSignatureHeaderIsRejectedNotThrown() {
        var verifier = verifier(Set.of());
        var timestamp = Long.toString(NOW.getEpochSecond());

        assertThatThrownBy(() -> verifier.verify(PROVIDER, BODY, "sha256=zzzz", timestamp, "10.0.0.1"))
                .isInstanceOf(WebhookVerificationException.class)
                .extracting(e -> ((WebhookVerificationException) e).reason())
                .isEqualTo(Reason.SIGNATURE_MISMATCH);
    }

    @Test
    @DisplayName("the rejection never tells the caller which gate failed, so the endpoint is not a probing oracle")
    void rejectionReasonIsNeverDisclosed() {
        var verifier = verifier(Set.of());
        var timestamp = Long.toString(NOW.getEpochSecond());

        var mismatch = catchVerification(() ->
                verifier.verify(PROVIDER, BODY, "sha256=" + sign("wrong", timestamp, BODY), timestamp, "10.0.0.1"));
        var unknown = catchVerification(() ->
                verifier.verify("nope", BODY, "sha256=" + sign(SECRET, timestamp, BODY), timestamp, "10.0.0.1"));

        assertThat(mismatch.type()).isEqualTo(ProblemType.UNAUTHENTICATED);
        assertThat(mismatch.getMessage()).isEqualTo(unknown.getMessage());
        assertThat(mismatch.getMessage()).doesNotContainIgnoringCase("signature mismatch");
    }

    @Test
    @DisplayName("every rejection increments webhook_signature_invalid_total, tagged by gate")
    void rejectionIsCounted() {
        var verifier = verifier(Set.of());
        var timestamp = Long.toString(NOW.getEpochSecond());

        catchVerification(() -> verifier.verify(PROVIDER, BODY, "sha256=deadbeef", timestamp, "10.0.0.1"));

        var counter = meters.find(WebhookSignatureVerifier.INVALID_COUNTER)
                .tag("provider", PROVIDER)
                .tag("reason", Reason.SIGNATURE_MISMATCH.name())
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    private WebhookSignatureVerifier verifier(Set<String> allowedIps) {
        var properties = new WebhookProperties(null, null, Duration.ofMinutes(5),
                Map.of(PROVIDER, new WebhookProperties.Provider(SECRET, allowedIps)));
        return new WebhookSignatureVerifier(properties, meters, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static WebhookVerificationException catchVerification(Runnable action) {
        try {
            action.run();
            throw new AssertionError("expected verification to fail");
        } catch (WebhookVerificationException e) {
            return e;
        }
    }

    /** Signs exactly the way a provider would: HMAC-SHA256 over {@code timestamp + "." + body}. */
    private static String sign(String secret, String timestamp, byte[] body) {
        try {
            var mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            mac.update(timestamp.getBytes(StandardCharsets.UTF_8));
            mac.update((byte) '.');
            mac.update(body);
            return HexFormat.of().formatHex(mac.doFinal());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
