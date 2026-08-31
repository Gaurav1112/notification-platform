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
 *
 * <h2>Every query over a tenant-owned table carries a tenant predicate</h2>
 *
 * <p><strong>The second non-negotiable rule, and the one with teeth.</strong> Six tables in this
 * schema carry a {@code tenant_id}: {@code notification}, {@code notification_recipient},
 * {@code notification_request}, {@code notification_event}, {@code delivery_attempt} and
 * {@code idempotency_record}. A query against any of them that does not name a tenant is a
 * cross-tenant read, and on the two monotonic guards it is a cross-tenant <em>write</em> — an
 * irreversible one, because a status transition that lands cannot be taken back and a forged
 * terminal status silently ends someone else's delivery.
 *
 * <p>The predicate goes in the {@code WHERE} clause, never in a check on the result. Filtering
 * afterwards makes "not yours" and "does not exist" distinguishable, which turns a status endpoint
 * into an oracle for whether a given id is live somewhere in the platform, and it puts the
 * enforcement in whichever caller remembered it rather than in the one place it can be audited.
 *
 * <p>There is no PostgreSQL row-level security behind this. RLS would need every connection to
 * carry a session variable, and the platform runs pooled connections shared across tenants and
 * across timer-driven sweeps that have no tenant at all — so a missed {@code SET} would fail open
 * on exactly the paths that matter. The predicate is enforced instead by {@code
 * TenantScopedQueryArchTest}, which reflects over every repository interface in this package and
 * fails the build on an {@code @Query} that touches a tenant-owned table without one.
 *
 * <p>Four kinds of method are genuinely exempt, and each is named in an explicit allowlist inside
 * that test rather than inferred: the lease/in-flight sweepers, the retry and TTL reapers, the
 * outbox drain, and aggregate operational counts that return no tenant-owned column. They run on
 * timers with no request and therefore no tenant, and a per-tenant variant of any of them would
 * silently skip the tenants nobody enumerated.
 */
package dev.gaurav.notification.persistence.repository;
