package dev.gaurav.notification.worker.support;

import dev.gaurav.notification.domain.enums.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

/**
 * A stand-in vault that does <strong>no cryptography at all</strong>, registered only when
 * {@code platform-security} has not contributed a real one.
 *
 * <p>TODO(phase-13): replace with the KMS-backed implementation. Until then this exists so the
 * dispatch pipeline can be run and tested end to end, and it is written to be obviously
 * unacceptable in production rather than plausibly acceptable: the "ciphertext" is Base64 of the
 * UTF-8 plaintext, the "HMAC" is an unkeyed hash of the same, and the DEK reference is the literal
 * string {@value #DEK_REF}. Anything that reads {@code dekRef} can therefore tell at a glance that
 * the row is not protected — which is the property that matters. A stub that produced
 * real-looking key references would let unprotected rows survive a review.
 *
 * <p>It also logs a warning on every startup. A silent stub is how a development shortcut reaches
 * production; a noisy one is how it gets replaced.
 *
 * <p>The address itself is synthesised from the user reference because there is no profile service
 * yet either. That keeps the mock providers exercised — they only care about the shape of an
 * address — without inventing a fake directory of real-looking phone numbers.
 *
 * <p>Registered by {@code WorkerDefaultsConfiguration} behind {@code @ConditionalOnMissingBean},
 * not by {@code @Component}: a conditional on a scanned class is evaluated in scan order, so the
 * real vault would win or lose depending on package names.
 */
public class DevelopmentAddressVault implements RecipientAddressVault {

    /** Deliberately not a plausible ARN. See the class javadoc. */
    public static final String DEK_REF = "dev:NOT-ENCRYPTED";

    private static final Logger log = LoggerFactory.getLogger(DevelopmentAddressVault.class);

    public DevelopmentAddressVault() {
        log.warn("recipient addresses are NOT encrypted: {} is active because platform-security "
                + "has not contributed a RecipientAddressVault", getClass().getSimpleName());
    }

    @Override
    public Optional<SealedAddress> sealFor(long tenantId, String userRef, Channel channel) {
        if (userRef == null || userRef.isBlank()) {
            return Optional.empty();
        }
        String address = synthesiseAddress(userRef, channel);
        byte[] cipher = address.getBytes(StandardCharsets.UTF_8);
        // Not an HMAC. Tenant-mixed only so the two-tenants-same-number correlation test has
        // something to assert against once the real vault lands.
        byte[] hash = (tenantId + ":" + address).getBytes(StandardCharsets.UTF_8);
        return Optional.of(new SealedAddress(
                cipher,
                Base64.getEncoder().encodeToString(cipher),
                hash,
                redact(address),
                DEK_REF));
    }

    @Override
    public String open(String addressCipherBase64, String dekRef) {
        return new String(Base64.getDecoder().decode(addressCipherBase64), StandardCharsets.UTF_8);
    }

    private static String synthesiseAddress(String userRef, Channel channel) {
        int digits = Math.abs(userRef.hashCode() % 100_000_000);
        return switch (channel) {
            case SMS -> "+1555%08d".formatted(digits);
            case EMAIL -> userRef.replaceAll("[^A-Za-z0-9._-]", "-") + "@example.invalid";
            case PUSH -> "dev-token-" + userRef;
        };
    }

    /** {@code g***@example.com} / {@code +1555****89}. The only form safe to log. */
    private static String redact(String address) {
        int at = address.indexOf('@');
        if (at > 0) {
            return address.charAt(0) + "***" + address.substring(at);
        }
        return address.length() <= 4
                ? "****"
                : address.substring(0, 2) + "****" + address.substring(address.length() - 2);
    }
}
