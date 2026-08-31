/**
 * The complete set of business failures the application layer can report, as a sealed hierarchy.
 *
 * <p>Sealed on purpose: the exception-to-HTTP mapping in {@code app-api} is a
 * {@code switch} over {@link dev.gaurav.notification.application.exception.ApplicationException},
 * and sealing makes that switch exhaustive. Add a subtype without handling it and the build fails,
 * instead of the new failure quietly becoming a {@code 500} with a leaked stack trace.
 *
 * <p>Each type carries its own RFC 9457 {@code type} slug and status. Clients branch on a stable
 * URI rather than string-matching English messages — which is what they will do if you do not give
 * them a choice, and it is why changing an error message becomes a breaking change.
 */
package dev.gaurav.notification.application.exception;
