/**
 * What happens to a send that did not succeed.
 *
 * <p>{@link dev.gaurav.notification.worker.retry.RetryRouter} is a pure function from a classified
 * failure to one of six decisions, and it is the only place in the platform that makes that call.
 * {@link dev.gaurav.notification.worker.retry.RetryTierListener} carries out the waiting, by
 * pausing a Kafka partition rather than sleeping a thread — a sleep past
 * {@code max.poll.interval.ms} evicts the consumer, and under a provider outage it evicts all of
 * them at once.
 */
package dev.gaurav.notification.worker.retry;
