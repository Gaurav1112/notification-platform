/**
 * The driven adapters that satisfy the application's Valkey-backed outbound ports.
 *
 * <p>Both ports here guard something rather than store it: a quota the fleet shares, and a
 * "do not send this" marker workers read immediately before a provider call. Neither is a system of
 * record — every one of them has a PostgreSQL fallback or a documented fail-open — which is what
 * makes it correct for this whole package to degrade rather than fail when Valkey is unreachable.
 *
 * <p>Placed under {@code dev.gaurav.notification.adapter.cache} for the same reason as the other
 * adapter packages: the platform modules' own test contexts scan their own root package and must
 * not pull cross-module collaborators in. The three deployables scan
 * {@code dev.gaurav.notification}.
 */
package dev.gaurav.notification.adapter.cache;
