/**
 * The transactional outbox relay.
 *
 * <p>One class. It is the safety net behind the API's fast-path publish, and the reason an
 * accepted notification cannot be silently lost between the database commit and the broker.
 */
package dev.gaurav.notification.scheduler.outbox;
