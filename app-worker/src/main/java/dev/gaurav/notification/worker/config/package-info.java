/**
 * Wiring, bulkheads, and the placeholders for modules that have not shipped.
 *
 * <p>The two pieces here that are design rather than plumbing:
 * {@link dev.gaurav.notification.worker.config.ChannelExecutors}, one bounded pool per channel so a
 * single slow vendor cannot consume every thread in the JVM, and
 * {@link dev.gaurav.notification.worker.config.ProviderHealthGate}, which pauses a channel's lanes
 * while every provider on it is circuit-open — because lag during a provider outage is an outage
 * signal, and treating it as a scaling signal points more consumers at a recovering vendor.
 */
package dev.gaurav.notification.worker.config;
