package dev.gaurav.notification.api.rest;

import dev.gaurav.notification.api.config.WebhookProperties;
import dev.gaurav.notification.api.error.ApiExceptionHandler;
import dev.gaurav.notification.api.port.WebhookIngestPort;
import dev.gaurav.notification.api.webhook.WebhookSignatureVerifier;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

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
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The inbound webhook contract, where the status code the provider receives is itself a control.
 *
 * <p>Each test pins one of the four properties that keep this endpoint from becoming either an
 * unauthenticated write path into the delivery state machine, or an infinite retry loop.
 */
class WebhookControllerTest {

    private static final String PROVIDER = "mock-sms-primary";
    private static final String SECRET = "local-development-hmac-secret-not-a-real-key";
    private static final Instant NOW = Instant.parse("2026-08-31T09:14:22Z");
    private static final String BODY = """
            {"messageId":"SM9f3a","status":"delivered"}""";

    private final WebhookIngestPort ingest = mock(WebhookIngestPort.class);

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        var properties = new WebhookProperties(null, null, Duration.ofMinutes(5),
                Map.of(PROVIDER, new WebhookProperties.Provider(SECRET, Set.of())));
        var verifier = new WebhookSignatureVerifier(properties, new SimpleMeterRegistry(),
                Clock.fixed(NOW, ZoneOffset.UTC));

        mvc = MockMvcBuilders
                .standaloneSetup(new WebhookController(verifier, ingest, properties,
                        Clock.fixed(NOW, ZoneOffset.UTC)))
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
    }

    @Test
    @DisplayName("the raw payload is committed before anything schedules interpretation of it")
    void rawPayloadIsPersistedBeforeProcessing() throws Exception {
        var stored = UUID.randomUUID();
        when(ingest.persistRaw(any(), any(), any(), any(), any()))
                .thenReturn(new WebhookIngestPort.RawReceipt(stored, false));

        mvc.perform(signed())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("accepted"))
                .andExpect(jsonPath("$.duplicate").value(false));

        // If interpretation were scheduled first, a parser that throws on an unfamiliar payload
        // would leave nothing on disk to replay after the fix.
        var order = inOrder(ingest);
        order.verify(ingest).persistRaw(any(), any(), any(), any(), any());
        order.verify(ingest).processLater(eq(stored));
    }

    @Test
    @DisplayName("a redelivered payload answers 200, because a 409 would make the provider retry forever")
    void duplicateAnswers200AndIsNotProcessedTwice() throws Exception {
        when(ingest.persistRaw(any(), any(), any(), any(), any()))
                .thenReturn(new WebhookIngestPort.RawReceipt(UUID.randomUUID(), true));

        mvc.perform(signed())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true));

        verify(ingest, never()).processLater(any());
    }

    @Test
    @DisplayName("an unsigned payload is a 401 and is never persisted — verification precedes storage")
    void unsignedPayloadIsRejectedBeforePersistence() throws Exception {
        mvc.perform(post("/v1/webhooks/{code}", PROVIDER)
                        .header("X-Timestamp", Long.toString(NOW.getEpochSecond()))
                        .contentType("application/json")
                        .content(BODY))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.type").value("https://docs.notification-platform.dev/errors/unauthenticated"));

        verify(ingest, never()).persistRaw(any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("a forged DELIVERED with a wrong signature is a 401, so it cannot suppress a retry")
    void forgedSignatureIsRejected() throws Exception {
        mvc.perform(post("/v1/webhooks/{code}", PROVIDER)
                        .header("X-Signature", "sha256=" + "0".repeat(64))
                        .header("X-Timestamp", Long.toString(NOW.getEpochSecond()))
                        .contentType("application/json")
                        .content(BODY))
                .andExpect(status().isUnauthorized());

        verify(ingest, never()).persistRaw(any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("the 401 body says nothing about which gate failed")
    void rejectionBodyDisclosesNothing() throws Exception {
        mvc.perform(post("/v1/webhooks/{code}", "unregistered-provider")
                        .header("X-Signature", "sha256=" + "0".repeat(64))
                        .header("X-Timestamp", Long.toString(NOW.getEpochSecond()))
                        .contentType("application/json")
                        .content(BODY))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.detail").value("Webhook signature verification failed."));
    }

    private org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder signed() {
        var timestamp = Long.toString(NOW.getEpochSecond());
        return post("/v1/webhooks/{code}", PROVIDER)
                .header("X-Signature", "sha256=" + sign(timestamp, BODY.getBytes(StandardCharsets.UTF_8)))
                .header("X-Timestamp", timestamp)
                .contentType("application/json")
                .content(BODY);
    }

    private static String sign(String timestamp, byte[] body) {
        try {
            var mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            mac.update(timestamp.getBytes(StandardCharsets.UTF_8));
            mac.update((byte) '.');
            mac.update(body);
            return HexFormat.of().formatHex(mac.doFinal());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
