/**
 * JPA entities mapped onto the {@code notif} schema created by {@code V1__baseline.sql}.
 *
 * <p>Three mapping decisions apply across this whole package, and each of them prevents a
 * specific production failure rather than expressing a preference:
 *
 * <ol>
 *   <li><strong>Partitioned tables get a single-column {@code @Id}.</strong> Their real primary
 *       key is composite and leads with the partition column, because PostgreSQL requires every
 *       unique constraint to contain the partition key. Modelling that with {@code @IdClass} or
 *       {@code @EmbeddedId} would push a composite key type into every repository signature,
 *       every {@code findById} and every service that holds a reference — for a key whose second
 *       column exists only to satisfy the storage engine. So the composite PK stays a DDL-level
 *       fact and the entity declares the {@code uuid}/{@code bigint} column alone. The price is
 *       that Hibernate cannot prune partitions on its own: <strong>every query against these
 *       entities must carry an explicit bound on the partition column</strong>, which is why the
 *       repositories in the sibling package take a {@code createdAtFrom}/{@code createdAtTo}
 *       window instead of exposing the derived finders Spring Data would happily generate.
 *       See {@code docs/DATABASE.md} section 5, gotcha 8.</li>
 *   <li><strong>Enums are always {@link jakarta.persistence.EnumType#STRING}.</strong> The columns
 *       are {@code varchar} with {@code CHECK} constraints, and ordinal mapping would silently
 *       re-point every stored row the first time somebody inserts a constant in the middle of an
 *       enum.</li>
 *   <li><strong>Timestamps are {@link java.time.Instant}, never {@code java.util.Date}.</strong>
 *       The columns are {@code timestamptz}; {@code Date} has no zone, silently uses the JVM
 *       default and is mutable, and this platform reasons about quiet hours across timezones.</li>
 * </ol>
 *
 * <p>Foreign-key columns on the high-volume tables are mapped as plain scalars rather than
 * {@code @ManyToOne} associations. The schema deliberately has no foreign keys there (an FK turns
 * an O(1) {@code DETACH PARTITION} into a validation scan, and retention depends on that staying
 * cheap), so an association would be a JPA-level fiction that also invites N+1 lazy loads on the
 * send path.
 */
package dev.gaurav.notification.persistence.entity;
