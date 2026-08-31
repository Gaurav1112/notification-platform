/**
 * Backoff, retry policy, tiered delay topics and the retry budget.
 *
 * <p>Three independent controls, because each fixes a different failure: jitter fixes <em>when</em>
 * retries land, the tier topics fix <em>where</em> they wait, and the budget fixes <em>how many</em>
 * there are. Any two without the third still produce an outage.
 */
package dev.gaurav.notification.resilience.retry;
