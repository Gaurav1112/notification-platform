/**
 * The dispatch tier: the code that actually talks to a third party.
 *
 * <p>{@link dev.gaurav.notification.worker.channel.AbstractChannelWorker} fixes an eight-step
 * sequence and the three channel subclasses supply only what genuinely differs. The step that
 * carries the most weight is the fifth — the {@code delivery_attempt} row is committed
 * <em>before</em> the network call, which is what turns a worker crash mid-send from an invisible
 * failure into a recoverable one.
 *
 * <p>The channels are not variations on a theme. SMS is at-most-once on an unknown outcome because
 * Twilio offers neither a client idempotency key nor reconciliation by our reference; email is
 * at-least-once because SES echoes our tags; push has no failover target at all. Those differences
 * are encoded in the retry router, not duplicated in the subclasses.
 */
package dev.gaurav.notification.worker.channel;
