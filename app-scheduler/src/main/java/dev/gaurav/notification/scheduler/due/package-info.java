/**
 * The three-layer due pipeline: a durable PostgreSQL ledger, a leader-elected hydrator, and
 * shard-affine claimers reading a Redis near-horizon.
 *
 * <p>The two stages use deliberately opposite concurrency strategies, and the reason is the single
 * most important idea in this package. {@code FOR UPDATE SKIP LOCKED} fixes correctness and
 * serialisation but <em>not</em> bloat: wasted index visits scale as {@code B·W²/2}, and a
 * documented case hit a hard wall at 128 concurrent claimers. So the <strong>scan</strong> is
 * single-writer and leader-elected, producing no dead tuples at all, while {@code SKIP LOCKED} is
 * reserved for the <strong>claim</strong>, where shard affinity has already made contention rare
 * and it only has to cover a rebalance window.
 *
 * <p>Measured, 16 concurrent claimers over 3M READY rows: naive {@code FOR UPDATE} 159 tps,
 * {@code SKIP LOCKED} 453 tps, shard-affine 746 tps.
 */
package dev.gaurav.notification.scheduler.due;
