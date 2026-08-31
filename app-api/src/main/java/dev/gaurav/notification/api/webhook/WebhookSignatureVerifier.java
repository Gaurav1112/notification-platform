package dev.gaurav.notification.api.webhook;

import dev.gaurav.notification.api.config.WebhookProperties;
import dev.gaurav.notification.api.error.WebhookVerificationException.Reason;
import dev.gaurav.notification.api.error.WebhookVerificationException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * The four gates an inbound provider webhook passes before anything reads its contents.
 *
 * <ol>
 *   <li>HMAC-SHA256 over {@code timestamp + "." + rawBody}, compared in constant time</li>
 *   <li>Timestamp inside the replay window, both directions</li>
 *   <li>Source IP on the provider's allowlist</li>
 *   <li>{@code dedup_hash} UNIQUE insert — enforced downstream by the database, not here</li>
 * </ol>
 *
 * <p><strong>Why {@link MessageDigest#isEqual} and never {@code String.equals}.</strong>
 * {@code equals} on a {@code String} returns as soon as two characters differ. The time it takes is
 * therefore a function of how many leading characters were correct, and that difference is
 * measurable across a network given enough samples. An attacker who can post to this endpoint
 * repeatedly can recover a valid signature one hex character at a time — sixty-four rounds of a few
 * thousand requests each, no secret required. {@code MessageDigest.isEqual} compares every byte
 * regardless of where the first difference is, so the response time carries no information about
 * how close the guess was.
 *
 * <p>The comparison is on decoded <em>bytes</em> rather than hex strings for the same reason: a
 * length check on the string would short-circuit and leak the expected length, and case handling on
 * hex would introduce a branch.
 *
 * <p><strong>Why the timestamp is part of the signed material.</strong> Signing the body alone
 * makes every captured payload valid forever. Binding the timestamp into the MAC means an attacker
 * cannot replay yesterday's {@code DELIVERED} by adjusting the header — changing the timestamp
 * invalidates the signature, and keeping the original timestamp fails gate two.
 */
@Component
public class WebhookSignatureVerifier {

    private static final Logger log = LoggerFactory.getLogger(WebhookSignatureVerifier.class);

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final String SIGNATURE_PREFIX = "sha256=";

    /** Alert on this. A non-zero rate means either a misconfigured provider or somebody probing. */
    public static final String INVALID_COUNTER = "webhook_signature_invalid_total";

    private final WebhookProperties properties;
    private final MeterRegistry meters;
    private final Clock clock;

    public WebhookSignatureVerifier(WebhookProperties properties, MeterRegistry meters, Clock clock) {
        this.properties = Objects.requireNonNull(properties, "properties");
        this.meters = Objects.requireNonNull(meters, "meters");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * @param rawBody   the exact bytes received. Re-serialising a parsed object here would change
     *                  key order and whitespace and break every signature
     * @return the provider timestamp, once verified, so the caller does not parse it twice
     * @throws WebhookVerificationException on any gate failure, always rendered as an identical 401
     */
    public Instant verify(String providerCode, byte[] rawBody, String signatureHeader,
                          String timestampHeader, String sourceIp) {
        var provider = properties.forCode(providerCode)
                .orElseThrow(() -> reject(providerCode, Reason.UNKNOWN_PROVIDER));

        if (signatureHeader == null || signatureHeader.isBlank()) {
            throw reject(providerCode, Reason.MISSING_SIGNATURE);
        }
        if (timestampHeader == null || timestampHeader.isBlank()) {
            throw reject(providerCode, Reason.MISSING_TIMESTAMP);
        }

        var timestamp = parseEpochSeconds(providerCode, timestampHeader);

        // Signature first, timestamp window second. Both are checked, so order does not change the
        // outcome -- but verifying the MAC before trusting anything else means an unsigned payload
        // never influences a decision, not even the cheap one.
        var expected = hmac(provider.secret(), timestampHeader, rawBody);
        var presented = decodeHex(stripPrefix(signatureHeader));
        if (presented == null || !MessageDigest.isEqual(expected, presented)) {
            throw reject(providerCode, Reason.SIGNATURE_MISMATCH);
        }

        var skew = Duration.between(timestamp, clock.instant()).abs();
        if (skew.compareTo(properties.timestampTolerance()) > 0) {
            // Both directions. A timestamp in the future is not "harmless clock skew" -- it is how
            // a captured payload is made valid for longer than the window allows.
            throw reject(providerCode, Reason.TIMESTAMP_OUTSIDE_WINDOW);
        }

        if (!provider.permits(sourceIp)) {
            throw reject(providerCode, Reason.SOURCE_IP_NOT_ALLOWED);
        }

        return timestamp;
    }

    private Instant parseEpochSeconds(String providerCode, String header) {
        try {
            return Instant.ofEpochSecond(Long.parseLong(header.trim()));
        } catch (NumberFormatException | DateTimeException e) {
            throw reject(providerCode, Reason.MALFORMED_TIMESTAMP);
        }
    }

    private static byte[] hmac(String secret, String timestamp, byte[] rawBody) {
        try {
            var mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM));
            mac.update(timestamp.getBytes(StandardCharsets.UTF_8));
            // The separator is not decoration. Without it, (ts="1", body="23") and (ts="12",
            // body="3") produce the same MAC input, and a provider that controls part of the body
            // could forge a signature for a different timestamp.
            mac.update((byte) '.');
            mac.update(rawBody);
            return mac.doFinal();
        } catch (GeneralSecurityException e) {
            // HmacSHA256 is mandatory in every JRE; reaching here means the JVM is broken, not the
            // request. Surfacing it as a 500 is correct -- a 401 would blame the caller.
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }

    private static String stripPrefix(String header) {
        var trimmed = header.trim();
        return trimmed.regionMatches(true, 0, SIGNATURE_PREFIX, 0, SIGNATURE_PREFIX.length())
                ? trimmed.substring(SIGNATURE_PREFIX.length())
                : trimmed;
    }

    /** @return null for anything that is not even hex; the caller treats that as a mismatch */
    private static byte[] decodeHex(String hex) {
        if (hex.length() % 2 != 0) {
            return null;
        }
        var out = new byte[hex.length() / 2];
        for (var i = 0; i < out.length; i++) {
            var high = Character.digit(hex.charAt(i * 2), 16);
            var low = Character.digit(hex.charAt(i * 2 + 1), 16);
            if (high < 0 || low < 0) {
                return null;
            }
            out[i] = (byte) ((high << 4) | low);
        }
        return out;
    }

    private WebhookVerificationException reject(String providerCode, Reason reason) {
        Counter.builder(INVALID_COUNTER)
                .tag("provider", providerCode)
                .tag("reason", reason.name())
                .description("inbound webhooks rejected before interpretation, by gate")
                .register(meters)
                .increment();
        // The reason is logged here and nowhere else. It never reaches the response body.
        log.warn("rejected webhook for provider={} reason={}", providerCode, reason);
        return new WebhookVerificationException(reason);
    }
}
