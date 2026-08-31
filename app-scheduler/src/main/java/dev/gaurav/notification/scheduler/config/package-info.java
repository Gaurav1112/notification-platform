/**
 * Scheduler-wide wiring: typed configuration, node identity, and Redis-backed leader election with
 * fencing tokens.
 *
 * <p>The leader election here guards the <em>due scan</em> only. Claiming stays concurrent and
 * shard-affine — see {@link dev.gaurav.notification.scheduler.due} for why the two stages need
 * opposite concurrency strategies.
 */
package dev.gaurav.notification.scheduler.config;
