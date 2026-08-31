/**
 * Hand-written fakes for the outbound ports.
 *
 * <p>Fakes rather than mocks on purpose. The behaviour that matters on the accept path is
 * <em>stateful</em> — the second call with the same idempotency key must see what the first one
 * left behind — and a mock that is told what to return cannot demonstrate that. A stubbed
 * "return REPLAY" proves the branch compiles; {@link dev.gaurav.notification.application.fake.FakeIdempotencyStore}
 * proves the two calls actually interact.
 *
 * <p>They also keep this module's test scope to JUnit and AssertJ, so the fast unit suite has no
 * bytecode-manipulating dependency in it.
 */
package dev.gaurav.notification.application.fake;
