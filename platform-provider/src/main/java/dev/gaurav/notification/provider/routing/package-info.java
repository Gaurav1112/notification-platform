/**
 * Provider selection: which of several eligible vendors gets the next message.
 *
 * <p>Kept separate from the registry because the two answer different questions. The registry says
 * <em>what exists</em>; this package says <em>what to use right now</em>, and only the second one
 * depends on live health, cost and latency. Scoring is a pure function of
 * {@link dev.gaurav.notification.provider.routing.ProviderCandidate} values, so the policy most
 * likely to be argued about is also the one easiest to test.
 */
package dev.gaurav.notification.provider.routing;
