/**
 * The driving ports the HTTP layer calls, and nothing else.
 *
 * <p><strong>Why these live in {@code app-api} rather than in {@code platform-application}.</strong>
 * In a ports-and-adapters layout the <em>driving</em> adapter owns the interface it needs; the
 * application module owns its use cases. Putting the port here means the REST layer compiles,
 * ships and is testable against a mock without waiting on — or being reshaped by — the internal
 * signature of a use case. When {@code platform-application} lands, a thin adapter in this module
 * implements each port by delegating to the corresponding use case and translating application
 * exceptions into {@link dev.gaurav.notification.api.error.ApiException} subtypes. That translation
 * step is the anti-corruption layer, and it is the reason a change to a use-case signature cannot
 * silently change the public API contract.
 *
 * <p>Every port method takes an explicit {@link dev.gaurav.notification.api.port.ApiCaller}. Tenant
 * identity is never read from a thread-local inside these interfaces: an implicit tenant is how a
 * background thread, an async webhook processor or a test ends up executing against whichever
 * tenant ran last, and cross-tenant leakage is the one bug in this system with no acceptable blast
 * radius.
 */
package dev.gaurav.notification.api.port;
