package dev.gaurav.notification.application.command;

/**
 * How the caller told us who to send to.
 *
 * <p>The distinction is a size decision before it is a semantic one. {@link #INLINE} and
 * {@link #USER_IDS} travel inside the request and inside the Kafka record; {@link #S3_MANIFEST} and
 * {@link #AUDIENCE_REF} are Claim Checks — a pointer travels, the list does not. A 10M-recipient
 * blast inlined into one Kafka record is ~600 MB against a 1 MB broker default, and raising that
 * default globally degrades latency on every other topic to accommodate one workload.
 */
public enum RecipientKind {

    /** Raw addresses supplied by the caller. No preference lookup is possible — there is no user. */
    INLINE,

    /** Tenant user references. Preferences, quiet hours and frequency caps all apply. */
    USER_IDS,

    /** {@code s3://…/audience.ndjson}. Expanded asynchronously after the 202. */
    S3_MANIFEST,

    /** A saved audience the platform resolves at fan-out time, so the list is fresh, not frozen. */
    AUDIENCE_REF
}
