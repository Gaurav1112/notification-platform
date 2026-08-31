package dev.gaurav.notification.scheduler.config;

import java.time.Instant;
import java.util.Objects;

/**
 * Proof that this JVM held a named lease at a point in time, and the fencing token that came with
 * it.
 *
 * <p><strong>The token is the part that matters.</strong> A lease alone does not make a single
 * writer: a pod can hold the lease, enter a 40-second stop-the-world pause, have the lease expire
 * and be taken by a second pod, then wake up and continue writing as if nothing happened. Neither
 * pod can detect this locally — the first one's clock says it is still leader, and the second one
 * is genuinely leader. The only defence is at the <em>resource</em>: every write carries the
 * token, the resource remembers the highest token it has seen, and a write carrying a lower one is
 * refused. That is what {@link dev.gaurav.notification.scheduler.due.DueIndex#hydrate} does.
 *
 * <p>Tokens are issued by a Redis {@code INCR} in the same atomic step as the lock acquisition, so
 * they are strictly increasing across acquisitions and constant across renewals of one
 * acquisition. Renewing must <em>not</em> mint a new token: the resource guard compares with
 * {@code <}, and a token that moved during a hydration pass would fence out the leader's own
 * in-flight writes.
 *
 * @param leaseName    the logical lock, e.g. {@code notification-hydrator}
 * @param owner        {@link NodeIdentity#instanceId()} of the holder
 * @param fencingToken strictly increasing per acquisition; never reused, never decreases
 * @param expiresAt    when Redis will drop the key unless renewed. Local view only — Redis is the
 *                     authority, and this field exists for logging and for the metric, not for a
 *                     correctness decision
 */
public record Leadership(String leaseName, String owner, long fencingToken, Instant expiresAt) {

    public Leadership {
        Objects.requireNonNull(leaseName, "leaseName");
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(expiresAt, "expiresAt");
        // Token 0 is what the Lua script returns when the lock was NOT acquired. Letting it into
        // a Leadership would hand a caller a token that can never fence anything out.
        if (fencingToken <= 0) {
            throw new IllegalArgumentException("fencing token must be positive, got " + fencingToken);
        }
    }

    /** Same acquisition, later expiry. Deliberately keeps {@link #fencingToken()} unchanged. */
    public Leadership renewedUntil(Instant newExpiry) {
        return new Leadership(leaseName, owner, fencingToken, newExpiry);
    }
}
