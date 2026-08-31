package dev.gaurav.notification.api.rest;

import dev.gaurav.notification.api.config.WebhookProperties;
import dev.gaurav.notification.api.dto.WebhookAck;
import dev.gaurav.notification.api.port.WebhookIngestPort;
import dev.gaurav.notification.api.webhook.WebhookSignatureVerifier;
import dev.gaurav.notification.provider.spi.ProviderCode;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.util.Objects;

/**
 * Inbound delivery receipts from providers.
 *
 * <p>This is the only unauthenticated write path in the system, and the only one whose caller
 * cannot be told what went wrong. Four properties are non-negotiable, in this order:
 *
 * <ol>
 *   <li><strong>Verify before reading.</strong> HMAC, replay window and source IP all run in
 *       {@link WebhookSignatureVerifier} before a single field of the payload is interpreted.
 *       An unverified {@code DELIVERED} would mark a failed one-time passcode as sent and suppress
 *       the retry, so the payload is untrusted input until the MAC says otherwise.</li>
 *   <li><strong>Persist the raw bytes before interpreting them.</strong> If the parser throws on a
 *       shape the vendor introduced last night, the evidence has to survive. Committing the body
 *       first turns "we lost a webhook and cannot reproduce it" into a replay after a one-line
 *       fix.</li>
 *   <li><strong>Answer 200 immediately.</strong> Every provider treats slow as failed. Doing the
 *       state transition inline would put a database write and a status-machine evaluation inside
 *       the provider's timeout budget, and the provider's response to exceeding it is to retry —
 *       amplifying load precisely when the system is already slow.</li>
 *   <li><strong>A duplicate is also a 200.</strong> Redelivery is normal at-least-once traffic; a
 *       409 would make the provider retry forever and eventually disable the endpoint.</li>
 * </ol>
 *
 * <p>The body is bound as {@code byte[]}, not as a parsed object. Re-serialising a parsed payload to
 * verify a signature changes key order, whitespace and number formatting, and the MAC then never
 * matches — a bug that looks like a wrong secret and is not.
 */
@RestController
@RequestMapping("/v1/webhooks")
@Tag(name = "Webhooks", description = "Inbound provider delivery receipts (HMAC authenticated)")
public class WebhookController {

    private final WebhookSignatureVerifier verifier;
    private final WebhookIngestPort ingest;
    private final WebhookProperties properties;
    private final Clock clock;

    public WebhookController(WebhookSignatureVerifier verifier, WebhookIngestPort ingest,
                             WebhookProperties properties, Clock clock) {
        this.verifier = Objects.requireNonNull(verifier, "verifier");
        this.ingest = Objects.requireNonNull(ingest, "ingest");
        this.properties = Objects.requireNonNull(properties, "properties");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Accepts any content type: providers post JSON, form-encoded bodies and, in the case of SNS,
     * JSON labelled {@code text/plain}. Rejecting on {@code Content-Type} would fail verification
     * for a payload whose signature is perfectly valid.
     *
     * <p>The two header <em>names</em> are read from {@link WebhookProperties} rather than declared
     * as {@code @RequestHeader} parameters. Vendors disagree about them —
     * {@code X-Twilio-Signature}, {@code X-Hub-Signature-256}, {@code X-Notification-Signature} —
     * and a name baked into the method signature is resolved once at startup, so it cannot be one
     * value per provider. Pulling them off the request keeps the configured name authoritative.
     */
    @PostMapping(path = "/{providerCode}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Provider delivery receipt",
            description = "HMAC-SHA256 over timestamp + '.' + rawBody. 401 on any verification failure, "
                    + "200 on a duplicate.")
    public WebhookAck receive(@PathVariable String providerCode,
                              @RequestBody(required = false) byte[] rawBody,
                              HttpServletRequest request) {
        var signature = request.getHeader(properties.signatureHeader());
        var timestamp = request.getHeader(properties.timestampHeader());
        // An empty body is still signed material; treating null as an empty array keeps the MAC
        // computation identical to the provider's rather than short-circuiting to a 400.
        var body = rawBody == null ? new byte[0] : rawBody;

        var providerTimestamp = verifier.verify(providerCode, body, signature, timestamp, request.getRemoteAddr());

        var receipt = ingest.persistRaw(ProviderCode.of(providerCode), body, signature,
                providerTimestamp, clock.instant());

        if (receipt.duplicate()) {
            // Nothing to schedule: the first copy is already being processed or has been.
            return WebhookAck.alreadySeen();
        }
        ingest.processLater(receipt.id());
        return WebhookAck.accepted();
    }
}
