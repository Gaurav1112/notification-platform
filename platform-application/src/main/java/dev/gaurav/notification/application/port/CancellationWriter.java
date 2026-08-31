package dev.gaurav.notification.application.port;

import java.time.Instant;
import java.util.UUID;

/**
 * Attempts the {@code CANCELLED} transition, and reports which of the three real outcomes happened.
 *
 * <p>The cancel is one conditional {@code UPDATE}, not a read-then-write. A read-then-write races
 * with the worker that is claiming the very same row — the interesting case is exactly the one
 * where a cancel and a dispatch arrive at the same instant, and a {@code SELECT} followed by an
 * {@code UPDATE} loses that race silently.
 *
 * <p>Correctness comes from rank ordering rather than an {@code if}: {@code CANCELLED} is 28 and
 * {@code QUEUED} is 30, so the monotonic guard refuses a cancel on anything already queued, and if
 * the cancel commits first it is terminal and the racing {@code QUEUED} is refused instead. Both
 * orderings are safe, which is the property that makes the answer trustworthy.
 */
public interface CancellationWriter {

    /** @param now the cancellation timestamp, supplied so the outcome is reproducible in a test */
    CancelOutcome cancel(String tenantId, UUID notificationId, Instant now);

    /** What the conditional update did. */
    enum CancelOutcome {

        /** The row was {@code PENDING} or {@code SCHEDULED} and is now {@code CANCELLED}. */
        CANCELLED,

        /** Zero rows: the rank guard refused because the notification is already in flight. */
        ALREADY_DISPATCHED,

        /** No such notification for this tenant. */
        NOT_FOUND
    }
}
