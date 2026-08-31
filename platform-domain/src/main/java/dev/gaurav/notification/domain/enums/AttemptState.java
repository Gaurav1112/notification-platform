package dev.gaurav.notification.domain.enums;

/**
 * The lifecycle of one provider call.
 *
 * <p>{@link #PENDING} is committed <em>before</em> the network call, so a crash mid-send leaves a
 * visible row instead of an invisible gap. {@link #UNKNOWN} is the state most designs omit: the
 * provider timed out after possibly delivering, and both retrying and giving up are wrong.
 */
public enum AttemptState {
    PENDING,
    SUCCEEDED,
    FAILED,
    UNKNOWN
}
