package dev.gaurav.notification.domain.enums;

/**
 * Why we chose not to send. A suppression is always recorded with a reason — never a silent drop,
 * because "we sent nothing and nobody knows why" is indistinguishable from a bug.
 */
public enum SuppressionReason {
    USER_OPTED_OUT,
    QUIET_HOURS,
    FREQUENCY_CAP,
    DUPLICATE,
    GLOBAL_UNSUBSCRIBE,
    HARD_BOUNCE,
    SPAM_COMPLAINT,
    INVALID_ADDRESS,
    TENANT_QUOTA
}
