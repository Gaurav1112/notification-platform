/**
 * The expander: accepted requests in, one dispatch event per (recipient, channel) out.
 *
 * <p>Everything here runs before a provider is ever contacted, and everything here is about
 * deciding <em>whether</em> to send: preferences re-evaluated at dispatch rather than at accept,
 * templates rendered once per channel rather than once per recipient, recipient rows committed
 * before any Kafka record refers to them.
 *
 * <p>Three ports in this package — {@link dev.gaurav.notification.worker.orchestrator.PreferenceResolver},
 * {@link dev.gaurav.notification.worker.orchestrator.TemplateRenderer} and
 * {@link dev.gaurav.notification.worker.orchestrator.RecipientManifestReader} — stand in for
 * {@code platform-application} work that has not landed. Each has a placeholder implementation
 * that warns at startup, so the gap is visible rather than assumed.
 */
package dev.gaurav.notification.worker.orchestrator;
