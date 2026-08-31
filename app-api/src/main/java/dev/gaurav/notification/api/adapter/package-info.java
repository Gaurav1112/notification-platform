/**
 * The driving adapters: one per inbound port, each delegating to a use case in
 * {@code platform-application}.
 *
 * <p>This package is the anti-corruption layer the {@code api.port} package javadoc promised. Two
 * translations happen here and nowhere else:
 *
 * <ul>
 *   <li><strong>Wire shape to command.</strong> {@code api.dto} carries the JSON contract and its
 *       Jackson and bean-validation annotations; {@code application.command} carries the meaning.
 *       Keeping them apart is what lets the JSON contract evolve without touching accept logic, and
 *       it is the reason a renamed request field cannot silently change what gets persisted.</li>
 *   <li><strong>Application failure to HTTP problem.</strong>
 *       {@link dev.gaurav.notification.api.adapter.ApplicationProblems} switches over the sealed
 *       {@code ApplicationException} hierarchy, so a new business failure is a compile error here
 *       rather than a 500 with a leaked stack trace in production.</li>
 * </ul>
 *
 * <p>Nothing in this package makes a decision. A branch that changes what is persisted, published
 * or charged belongs in a use case; if one appears here, the seam has been drawn in the wrong
 * place.
 */
package dev.gaurav.notification.api.adapter;
