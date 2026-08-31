/**
 * Read models returned by the use cases.
 *
 * <p>Separate from both the persistence entities and the API DTOs. Returning an entity would let a
 * lazy association load outside its transaction and would make a column rename a breaking API
 * change; returning an API DTO would put the wire contract inside the use case. These sit in the
 * middle and are the only shape the API layer has to map.
 *
 * <p>Nothing here carries a plaintext address. Only {@code addressHint} — {@code g***@example.com}
 * — because a status endpoint that echoes phone numbers is an address-harvesting endpoint for
 * anyone who obtains one token.
 */
package dev.gaurav.notification.application.result;
