package dev.gaurav.notification.provider.mock;

import dev.gaurav.notification.domain.enums.FailureType;

/**
 * The shapes of failure a mock provider can be told to produce.
 *
 * <p>This is deliberately <em>not</em> {@link FailureType}. A failure profile is a scenario we
 * inject; a failure type is the platform's classification of what came back. Keeping them separate
 * is what lets each adapter map the same profile onto its own vendor's wire representation — the
 * same {@code UNSUBSCRIBED} scenario is Twilio {@code 21610}, an SES suppression-list rejection and
 * an FCM {@code UNREGISTERED}, and the mapping is exactly the code we need to test.
 */
public enum FailureProfile {

    /** No failure. Present as an enum constant so the injector has a single return type. */
    NONE(null),

    TRANSIENT_NETWORK(FailureType.TRANSIENT_NETWORK),
    PROVIDER_5XX(FailureType.PROVIDER_5XX),
    RATE_LIMITED(FailureType.RATE_LIMITED),
    AUTH_FAILURE(FailureType.AUTH_FAILURE),
    QUOTA_EXCEEDED(FailureType.QUOTA_EXCEEDED),
    INVALID_RECIPIENT(FailureType.INVALID_RECIPIENT),
    DEVICE_UNREGISTERED(FailureType.DEVICE_UNREGISTERED),
    UNSUBSCRIBED(FailureType.UNSUBSCRIBED),
    CONTENT_REJECTED(FailureType.CONTENT_REJECTED),
    PAYLOAD_TOO_LARGE(FailureType.PAYLOAD_TOO_LARGE),

    /**
     * The provider takes longer than our deadline and then acknowledges anyway — and, in the full
     * harness, fires its webhook afterwards.
     *
     * <p>This is the only profile that produces a genuine {@code UNKNOWN}, and it is the reason the
     * profile list exists at all. Every duplicate-OTP incident in this class of system lives in the
     * gap between "we timed out" and "it was not delivered". A mock that cannot produce that gap
     * cannot test the code that closes it.
     */
    SILENT_SUCCESS(FailureType.PROVIDER_TIMEOUT);

    private final FailureType failureType;

    FailureProfile(FailureType failureType) {
        this.failureType = failureType;
    }

    public boolean isFailure() {
        return failureType != null;
    }

    /** @throws IllegalStateException for {@link #NONE}, which has no classification by definition */
    public FailureType failureType() {
        if (failureType == null) {
            throw new IllegalStateException(name() + " is not a failure");
        }
        return failureType;
    }
}
