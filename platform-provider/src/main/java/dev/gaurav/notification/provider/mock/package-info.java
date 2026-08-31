/**
 * Mock provider adapters that emulate real vendor <em>semantics</em>, not generic success.
 *
 * <p>Only the leaf adapter is mocked (ADR-005). The SPI, the whole decorator stack, the registry,
 * selection scoring, failure classification, retry tiers and the state machine are the real
 * implementations, exercised by these mocks exactly as they would be by Twilio, SES or FCM.
 *
 * <p>A mock that always returns success proves nothing. These reproduce the specific error codes,
 * batching limits and idempotency gaps of the real vendors, because those are what the platform
 * has to be correct about:
 *
 * <ul>
 *   <li>{@link dev.gaurav.notification.provider.mock.MockSmsProvider} — Twilio: no client
 *       idempotency key, which forces the {@code UNKNOWN} reconciliation path into existence.</li>
 *   <li>{@link dev.gaurav.notification.provider.mock.MockEmailProvider} — SES: 50-destination
 *       bulk, per-destination results, {@code Throttling}, bounce and complaint feedback.</li>
 *   <li>{@link dev.gaurav.notification.provider.mock.MockPushProvider} — FCM v1 + APNs:
 *       <strong>no batch endpoint</strong>, {@code UNREGISTERED} token deactivation, 429 with a
 *       one-minute floor.</li>
 * </ul>
 *
 * <p>Failure injection is seeded per (provider, recipient, attempt), so a CI run can assert
 * "exactly three messages reached the DLQ" and mean it.
 */
package dev.gaurav.notification.provider.mock;
