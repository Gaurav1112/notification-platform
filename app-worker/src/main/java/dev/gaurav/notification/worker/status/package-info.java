/**
 * Projecting observations — ours, the provider's and the reconciler's — onto the rows.
 *
 * <p>The governing rule is that a rejected transition is <strong>not an error</strong>. Status
 * signals arrive out of order and more than once as a matter of routine, so the monotonic guard
 * returning zero rows is the expected outcome for a duplicated webhook. It is recorded with
 * {@code applied = false} and counted, and that single choice is what lets every consumer in the
 * platform be at-least-once.
 */
package dev.gaurav.notification.worker.status;
