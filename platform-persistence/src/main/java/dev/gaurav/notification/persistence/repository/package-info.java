/**
 * Spring Data JPA repositories over the {@code notif} schema.
 *
 * <p><strong>Every finder that touches a partitioned table takes an explicit time window.</strong>
 * That is not a style choice and it is not defensive verbosity — it is the only thing that makes
 * these queries stay fast. Hibernate cannot infer a partition bound from an entity whose
 * {@code @Id} is the uuid alone (see the entity package javadoc for why it is mapped that way),
 * so a derived finder such as {@code findById} compiles happily and then asks PostgreSQL to probe
 * every partition. With ninety days of retention that is ninety index scans instead of one, and
 * the regression grows with the age of the deployment rather than with the size of the result —
 * which is precisely the kind of slowdown that never reproduces in staging.
 *
 * <p>So the derived-finder shortcut is deliberately not used on those entities. Each method here
 * spells the predicate out, and the window parameters are named {@code createdAtFrom} /
 * {@code createdAtTo} (half-open: {@code >= from}, {@code < to}) so that a caller reading the
 * signature cannot mistake them for a business filter.
 *
 * <p>Repositories over the small, non-partitioned configuration tables are ordinary Spring Data
 * interfaces, because there nothing is being hidden.
 */
package dev.gaurav.notification.persistence.repository;
