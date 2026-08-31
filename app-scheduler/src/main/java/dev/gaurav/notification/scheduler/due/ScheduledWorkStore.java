package dev.gaurav.notification.scheduler.due;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * The durable ledger of scheduled work, behind an interface.
 *
 * <p>Two reasons this is not a Spring Data repository. First, the two stages need <em>opposite</em>
 * concurrency strategies — the scan must be a plain single-session read and the claim must be
 * {@code FOR UPDATE SKIP LOCKED} — and expressing that as derived query methods hides the one
 * detail a reader most needs to see. Second, and more practically, the reclaim and poison paths are
 * the highest-consequence logic in this module and they have to be unit-testable without a
 * database; an interface makes {@link LeaseReaper} testable in milliseconds.
 *
 * <p>Every method here is short and commits. Nothing in this interface may be called with a
 * provider HTTP call inside the transaction: a 30-second provider timeout inside a claim
 * transaction holds row locks and pins {@code xmin}, so autovacuum cannot reclaim the twenty
 * million dead tuples a day that status updates generate. That is the failure that takes the
 * cluster down at 3 a.m., and it starts as a convenience.
 */
public interface ScheduledWorkStore {

    /**
     * Reads READY rows due before {@code horizonEnd}, without locking anything.
     *
     * <p><strong>A pure {@code SELECT}, deliberately.</strong> The hydrator does not mutate
     * PostgreSQL at all — it only pushes into Redis — so the scan produces zero dead tuples no
     * matter how often it runs. That is the property that makes leader-electing it worthwhile:
     * {@code SKIP LOCKED} would fix correctness here but not bloat, and wasted index visits scale
     * as {@code B·W²/2} in the number of concurrent scanners.
     *
     * <p>Rows stay READY until a claimer takes them, so a hydrator that dies mid-pass loses at most
     * one scan interval of <em>scheduling</em> and no data.
     */
    List<ScheduledWork> scanReady(Instant horizonEnd, int limit);

    /**
     * Takes a lease on the rows that are still READY, skipping any another session holds.
     *
     * <p>{@code SKIP LOCKED} is here and nowhere else in this interface. Claimers are shard-affine,
     * so in steady state two of them never reach for the same row and there is nothing to skip;
     * this exists purely for the rebalance window, when a shard has briefly moved between pods.
     * That narrow role is the measured difference between 453 tps and 746 tps.
     *
     * <p>Does <strong>not</strong> increment {@code claim_count} — see {@link #reclaim}.
     *
     * @param leaseExpiresAt the correctness boundary. A pod killed after this call and before
     *                       {@link #markDispatched} releases the work at this instant, which is
     *                       what makes the platform at-least-once rather than at-most-once
     * @return the rows actually claimed, which may be fewer than requested
     */
    List<ScheduledWork> claim(int shard, Collection<UUID> ids, String owner,
                              Instant now, Instant leaseExpiresAt);

    /**
     * Marks work as handed to Kafka and drops the lease.
     *
     * <p>Guarded on {@code claimed_by = owner} so a pod that lost its lease during a pause cannot
     * mark someone else's in-flight work as done.
     *
     * @return rows updated; a shortfall means at least one lease was lost mid-publish
     */
    int markDispatched(Collection<UUID> ids, String owner);

    /**
     * CLAIMED rows whose lease has passed, oldest first. Served by the partial index
     * {@code sn_lease_expiry_ix … WHERE state = 'CLAIMED'} — measured at 1 buffer, 0.008 ms.
     */
    List<ScheduledWork> findExpiredLeases(Instant now, int limit);

    /**
     * Returns expired-lease rows to READY and increments {@code claim_count}.
     *
     * <p>The increment lives here rather than in {@link #claim} on purpose. A row that was claimed
     * and dispatched successfully is healthy and must not accumulate a count; a row whose claim
     * ended with the lease expiring is one that <em>killed the pod holding it</em>. Counting only
     * the second kind is what turns the counter into a poison-pill signal instead of a retry
     * tally that fires on every unrelated rolling deploy.
     *
     * @return rows reset; a shortfall means another reaper got there first, which is harmless
     */
    int reclaim(Collection<UUID> ids, Instant now);

    /**
     * Moves rows to FAILED without rescheduling them.
     *
     * <p>Only called after the dead-letter record has been acknowledged. Marking FAILED first and
     * failing to dead-letter would make the row disappear from every queue and every metric — the
     * exact invisibility the poison-pill detector exists to end.
     */
    int abandon(Collection<UUID> ids);
}
