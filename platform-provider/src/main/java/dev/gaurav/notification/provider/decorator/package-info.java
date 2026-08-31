/**
 * The decorator stack that wraps every provider adapter.
 *
 * <p>A leaf adapter contains vendor logic and nothing else. Everything that must be true of
 * <em>every</em> provider — a span, a timer, a breaker, a rate limit, a hard deadline and an
 * idempotency record — lives here, applied identically to a mock and to a real vendor. That is
 * what makes the chaos tests meaningful: the mock is exercised through the same resilience code
 * that production traffic will use.
 *
 * @see dev.gaurav.notification.provider.decorator.ProviderDecoratorChain for the order and why it
 *      is fixed rather than caller-chosen
 */
package dev.gaurav.notification.provider.decorator;
