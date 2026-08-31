package dev.gaurav.notification.provider.spi;

/**
 * What a provider can actually do. The platform adapts its own behaviour from this rather than
 * branching on provider identity, so there is no {@code if (provider == TWILIO)} anywhere.
 *
 * @param supportsBatching        true if multiple recipients go in one call
 * @param maxBatchSize            1 when batching is unsupported
 * @param supportsIdempotencyKey  false for every real provider we would plausibly integrate;
 *                                drives the UNKNOWN reconciliation path
 * @param supportsWebhook         true if delivery receipts arrive asynchronously
 * @param supportsStatusQuery     true if we can ask "did you send my message X" — false for Twilio,
 *                                which has no client-reference field on Messages
 */
public record ProviderCapabilities(
        boolean supportsBatching,
        int maxBatchSize,
        boolean supportsIdempotencyKey,
        boolean supportsWebhook,
        boolean supportsStatusQuery) {

    public static ProviderCapabilities singleSend(boolean webhook) {
        return new ProviderCapabilities(false, 1, false, webhook, false);
    }
}
