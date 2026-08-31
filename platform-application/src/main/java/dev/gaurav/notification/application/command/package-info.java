/**
 * Immutable command records — the validated, transport-free form of an inbound request.
 *
 * <p>A command is constructed once, at the edge, and is already legal by the time a use case sees
 * it: the compact constructors reject anything a use case would otherwise have to re-check. That is
 * why the use cases below contain no defensive validation — an illegal command cannot be built.
 *
 * <p>Nothing here mentions HTTP. The API module's request DTOs carry the wire shape and the Jackson
 * annotations; these carry the meaning. Keeping them separate is what lets the JSON contract evolve
 * without touching the accept logic.
 */
package dev.gaurav.notification.application.command;
