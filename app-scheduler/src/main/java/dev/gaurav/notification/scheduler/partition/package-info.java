/**
 * Partition premake and the missing-partition canary.
 *
 * <p>Production creates partitions with {@code pg_partman} + {@code pg_cron}. The canary here
 * exists regardless of who creates them, because the failure mode of a missing partition is
 * silence: every insert lands in DEFAULT, nothing errors, and the first visible symptom is that
 * creating the real partition now fails under an {@code ACCESS EXCLUSIVE} lock.
 */
package dev.gaurav.notification.scheduler.partition;
