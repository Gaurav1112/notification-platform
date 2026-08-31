/**
 * The driven adapters that satisfy {@code platform-application}'s outbound ports with PostgreSQL.
 *
 * <p>Every class here is deliberately thin: it translates a port's domain-shaped arguments into the
 * repository call that already exists in {@link dev.gaurav.notification.persistence.repository} and
 * translates the row back. No SQL is invented here that a repository does not already own, and no
 * business rule is re-implemented — a transaction boundary or a retry loop appearing in this
 * package means the logic belongs in a use case instead.
 *
 * <p><strong>Why {@code dev.gaurav.notification.adapter.persistence} and not
 * {@code dev.gaurav.notification.persistence.adapter}.</strong> The module's own Spring test
 * context ({@code PersistenceTestApplication}) component-scans
 * {@code dev.gaurav.notification.persistence}, and it exists to verify entity mappings and
 * repositories against a bare PostgreSQL — nothing else. Adapters placed inside that package would
 * be scanned there too and would drag Kafka and Valkey collaborators into a test that has no
 * business needing them. The three deployables scan {@code dev.gaurav.notification}, so they pick
 * this package up; the persistence module's own test does not.
 *
 * <h2>What a tenant reference is</h2>
 *
 * <p>The application layer speaks of a tenant as an opaque {@code String}. In this platform that
 * string is the tenant's <em>public</em> identifier — the UUID that appears in tokens and URLs, or
 * the slug — never the {@code bigint} surrogate key. {@link
 * dev.gaurav.notification.adapter.persistence.TenantDirectory} is the single place the translation
 * happens, because a surrogate key that leaked into the application layer would leak into the API
 * next, and an enumerable tenant id in a URL is the thing {@code tenant.public_id} exists to
 * prevent.
 *
 * <h2>Every read carries a partition window</h2>
 *
 * <p>The hot tables are RANGE-partitioned by day. A query without a bound on the partition column
 * is correct and gets linearly slower with retention, which is the regression that never reproduces
 * in staging. {@link dev.gaurav.notification.adapter.persistence.PartitionWindows} holds the two
 * windows these adapters use and the reasoning for their widths.
 */
package dev.gaurav.notification.adapter.persistence;
