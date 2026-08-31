/**
 * Small shared pieces the orchestrator, the channel workers and the retry tiers all need.
 *
 * <p>{@link dev.gaurav.notification.worker.support.PartitionWindow} exists because every table the
 * worker touches is RANGE-partitioned by day, and a query without a bound on the partition column
 * is not an error — it is a scan of all 335 partitions that gets slower every day the platform
 * stays up.
 */
package dev.gaurav.notification.worker.support;
