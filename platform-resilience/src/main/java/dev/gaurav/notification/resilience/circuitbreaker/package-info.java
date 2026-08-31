/**
 * Per-{@code (provider, channel)} circuit breakers and the failure classification that feeds them.
 *
 * <p>A breaker is only as good as what it counts: recording our own 4xx bugs as provider failures
 * takes a healthy provider offline, which is a worse outcome than having no breaker at all.
 */
package dev.gaurav.notification.resilience.circuitbreaker;
