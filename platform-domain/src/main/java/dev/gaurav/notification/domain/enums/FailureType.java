package dev.gaurav.notification.domain.enums;

/**
 * The classification of a failed provider call, and the single input to the
 * retry-vs-fail-vs-failover decision.
 *
 * <p>A single {@code catch (Exception e) { retry(); }} branch is the most common bug in this class
 * of system. Retrying an invalid phone number five times burns budget, adds latency and helps
 * nobody; <em>not</em> retrying a transient socket reset loses a message that would have succeeded.
 * The two cases are indistinguishable unless you classify first.
 *
 * <p>Each constant carries its own policy, so the decision lives in one place rather than being
 * re-derived by every caller.
 */
public enum FailureType {

    // ---- transient: the call might succeed if we try again -------------------------------------

    /** Connection reset, DNS blip, socket timeout on connect. */
    TRANSIENT_NETWORK(true, 2, false, false),

    /** The provider accepted the connection then took too long. May have delivered — see UNKNOWN. */
    PROVIDER_TIMEOUT(true, 2, false, true),

    /** HTTP 5xx. The provider is having a bad time. */
    PROVIDER_5XX(true, 2, false, false),

    /** HTTP 429. Honour {@code Retry-After} if present, and fail over immediately. */
    RATE_LIMITED(true, 0, false, false),

    // ---- provider-level, not message-level: switch provider, don't retry this one --------------

    /** Our credentials are wrong or revoked. Retrying cannot help. Page someone. */
    AUTH_FAILURE(false, 0, false, false),

    /** The provider account is out of budget or quota. Switch provider, page someone. */
    QUOTA_EXCEEDED(false, 0, false, false),

    // ---- permanent: the message will never be deliverable to this address ----------------------

    /** Malformed or non-existent number/address. Deactivate it. */
    INVALID_RECIPIENT(false, -1, true, false),

    /** The push token is no longer registered on the device. Delete it. */
    DEVICE_UNREGISTERED(false, -1, true, false),

    /** The recipient replied STOP or unsubscribed. Add to the suppression list. */
    UNSUBSCRIBED(false, -1, true, false),

    /** Carrier or provider filtered the content as spam. Retrying identical content will not work. */
    CONTENT_REJECTED(false, -1, true, false),

    // ---- our bug: never blame the provider -----------------------------------------------------

    /** Template failed to render, or a required variable was missing. Goes to the DLQ. */
    TEMPLATE_ERROR(false, -1, false, false),

    /** Payload exceeded the provider's limit (APNs 4 KB, FCM 4096 bytes). Goes to the DLQ. */
    PAYLOAD_TOO_LARGE(false, -1, false, false),

    /** Classified as permanent but not otherwise recognised. Conservative default. */
    PERMANENT_UNKNOWN(false, -1, false, false);

    private final boolean retryable;
    private final int failoverAfterAttempts;
    private final boolean suppressAddress;
    private final boolean outcomeIndeterminate;

    FailureType(boolean retryable, int failoverAfterAttempts,
                boolean suppressAddress, boolean outcomeIndeterminate) {
        this.retryable = retryable;
        this.failoverAfterAttempts = failoverAfterAttempts;
        this.suppressAddress = suppressAddress;
        this.outcomeIndeterminate = outcomeIndeterminate;
    }

    /** Whether retrying the <em>same</em> provider could plausibly succeed. */
    public boolean isRetryable() {
        return retryable;
    }

    /**
     * How many attempts on this provider before trying the next candidate.
     * {@code 0} means switch immediately; {@code -1} means never fail over, the message is dead.
     */
    public int failoverAfterAttempts() {
        return failoverAfterAttempts;
    }

    /** Whether the recipient address should be deactivated or suppression-listed. */
    public boolean shouldSuppressAddress() {
        return suppressAddress;
    }

    /**
     * Whether we genuinely cannot tell if the message was delivered.
     *
     * <p>Only {@link #PROVIDER_TIMEOUT} sets this. It is the difference between "it failed" and
     * "it may have worked and we did not hear back" — and blind-retrying the second case is how
     * users receive three one-time passcodes.
     */
    public boolean isOutcomeIndeterminate() {
        return outcomeIndeterminate;
    }

    /** True when nothing further can be done with this message on any provider. */
    public boolean isPermanent() {
        return !retryable && failoverAfterAttempts < 0;
    }

    /** True when the message should move to another provider rather than being retried or dropped. */
    public boolean shouldFailoverImmediately() {
        return failoverAfterAttempts == 0;
    }
}
