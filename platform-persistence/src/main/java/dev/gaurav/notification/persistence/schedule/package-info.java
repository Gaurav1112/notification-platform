/**
 * The write side of {@code notif.scheduled_notification}.
 *
 * <p><strong>Why this lives in {@code platform-persistence} and not in {@code app-scheduler}
 * next to the reader.</strong> Two different deployables touch this table and they touch it from
 * opposite ends: {@code app-worker} writes rows when fan-out defers a send, and
 * {@code app-scheduler} scans, claims and releases them. Neither app module may depend on the
 * other, so the {@code INSERT} has to live somewhere both can see. The alternative — a second
 * copy of the column list in the worker — is the failure this package exists to prevent: the two
 * copies drift, the writer stops supplying {@code due_bucket} the way the scan predicate expects,
 * and the row becomes invisible to the due scan with nothing logged anywhere.
 *
 * <p>JDBC rather than JPA, matching {@code JdbcScheduledWorkStore} on the read side. Hibernate
 * would need an entity whose {@code @Id} cannot express the composite
 * {@code (due_bucket, id)} primary key without a second class, and it would dirty-check that
 * entity graph on a path whose whole point is that it is a single statement.
 */
package dev.gaurav.notification.persistence.schedule;
