package dev.gaurav.notification.persistence.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

/**
 * Fails the build when a query over a tenant-owned table has no tenant predicate.
 *
 * <p><strong>Why a reflective test and not review.</strong> Tenant isolation in this schema is not
 * a property of any one class; it is a property of every {@code WHERE} clause, and there are
 * dozens. It is invisible in a diff — an unscoped finder and a scoped one differ by nine
 * characters, both compile, both pass every functional test, and both return rows in a
 * single-tenant test fixture. It is also invisible in the tests that look like they cover it: a
 * fake repository keyed on {@code tenant|id} isolates because the fake's author wrote a map that
 * way, not because the SQL does. The only artefact that cannot lie about this is the query text
 * itself, so that is what this test reads.
 *
 * <p>The audit that produced this class found {@code NotificationRecipientRepository} and
 * {@code DeliveryAttemptRepository} with no tenant predicate on a single method, and both monotonic
 * guards addressable by id alone — a cross-tenant <em>write</em>, and an irreversible one. Fixing
 * those six queries is a commit. Stopping the seventh is this file.
 *
 * <p><strong>There is no PostgreSQL row-level security behind this.</strong> RLS would need every
 * connection to carry a session variable, and this platform runs a shared pool across tenants and
 * across timer-driven sweeps that legitimately have no tenant; a missed {@code SET} fails open on
 * exactly the paths that matter most. The predicate in the query is the enforcement, so the
 * predicate is what gets tested.
 *
 * @see #INFRASTRUCTURE_SWEEPS the deliberate cross-tenant exceptions, each with its reason
 */
class TenantScopedQueryArchTest {

    /** The package that must hold every repository; a move without a test update fails below. */
    private static final String REPOSITORY_PACKAGE = "dev.gaurav.notification.persistence.repository";

    /**
     * The tables whose every row belongs to exactly one tenant.
     *
     * <p>Matched with a word boundary so {@code notif.notification} does not also match
     * {@code notif.notification_recipient} — they are different tables with different partition
     * keys and a substring match would let a recipient query inherit a notification query's verdict.
     *
     * <p>Deliberately absent, and each for a reason:
     *
     * <ul>
     *   <li>{@code notif.outbox_message} — no tenant column at all. The relay drains it as one
     *       ordered stream and {@code FOR UPDATE SKIP LOCKED} is what lets two relays share it; a
     *       tenant predicate would partition that stream and reintroduce head-of-line blocking.</li>
     *   <li>{@code notif.dead_letter_message} — {@code tenant_id} is nullable because a poisoned
     *       message is often one we could not deserialise far enough to know whose it was. It is an
     *       operator triage queue, not a tenant-readable table.</li>
     *   <li>{@code notif.provider_configuration} — {@code tenant_id} nullable <em>is</em> the
     *       feature: null means the platform default that every tenant falls back to. Its one
     *       routing query already handles precedence explicitly.</li>
     *   <li>{@code notif.tenant}, {@code notif.provider}, {@code notif.retry_policy},
     *       {@code notif.delivery_status} — reference and configuration data owned by the
     *       platform.</li>
     * </ul>
     */
    private static final Set<String> TENANT_OWNED_TABLES = Set.of(
            "notif.notification",
            "notif.notification_recipient",
            "notif.notification_request",
            "notif.notification_event",
            "notif.delivery_attempt",
            "notif.idempotency_record");

    /** The JPQL entity names for the same six tables. A JPQL query names these, not the tables. */
    private static final Set<String> TENANT_OWNED_ENTITIES = Set.of(
            "NotificationEntity",
            "NotificationRecipient",
            "NotificationRequest",
            "NotificationEvent",
            "DeliveryAttempt",
            "IdempotencyRecord");

    /**
     * Queries that legitimately sweep across every tenant.
     *
     * <p>Every entry is infrastructure driven by a timer, not data access driven by a request.
     * There is no tenant in scope for any of them, and — this is the part worth stating — a
     * per-tenant variant would be <em>worse</em> than useless: it would have to iterate a tenant
     * list, and the tenants it failed to enumerate would be exactly the ones whose stuck rows
     * nobody ever notices. Adding to this set is a deliberate act and should be argued for in the
     * method's own javadoc as well as here.
     */
    private static final Set<String> INFRASTRUCTURE_SWEEPS = Set.of(

            // The retry backstop. Finds rows whose next_attempt_at passed without the Kafka tier
            // firing; a retry that was owed is owed regardless of who is calling, and nobody is.
            "NotificationRecipientRepository#findDueForRetry",

            // The lease reaper. These rows are leases held by workers that died between claiming
            // and reporting. They belong to a dead process, not to a tenant's request.
            "NotificationRecipientRepository#findStaleInFlight",

            // The TTL reaper. An expiry is our promise about a notification, made at accept time
            // and enforced on a timer.
            "NotificationRepository#findExpiredInWindow",

            // The expander backstop. A request stuck in EXPANDING means the fan-out died mid
            // campaign; scoping it per tenant would leave half-sent campaigns nobody enumerated.
            "NotificationRequestRepository#findByStatusInWindow",

            // Reconciliation input: attempts left PENDING past any plausible provider deadline.
            // Each is a send whose outcome is unknown to the platform, which is the only actor
            // that can resolve it.
            "DeliveryAttemptRepository#findAbandonedAttempts",

            // Provider health rollup. Answers "is this vendor working", which is a property of our
            // contract with the vendor. Returns [state, count] pairs and never a row, so no
            // tenant-owned column crosses the boundary through it.
            "DeliveryAttemptRepository#countByStateForProvider",

            // The dedup pre-check, and the subtle one. ne_dedup_uk is UNIQUE (dedup_hash,
            // occurred_at) with no tenant column, so the constraint this probe stays ahead of is
            // global. A tenant-scoped probe against a global index would disagree with it: probe
            // says absent, caller inserts, index rejects — turning a quiet duplicate into an
            // exception on the webhook path. It returns a boolean about a SHA-256 digest, never a
            // row. If that index ever gains tenant_id, this entry must go in the same migration.
            "NotificationEventRepository#existsByDedupHashInWindow");

    /**
     * Known debt, kept separate from {@link #INFRASTRUCTURE_SWEEPS} on purpose.
     *
     * <p>These are not decisions. They are queries whose only caller could pass a tenant today and
     * has not been changed yet, and keeping them in their own set means nobody reads them as
     * settled. This set should be empty; an entry that outlives the change it is waiting for is a
     * bug report sitting in a test file, which is the right place for it to nag from.
     */
    private static final Set<String> PENDING_CALLER_MIGRATION = Set.of(
            // Empty, and worth keeping that way. Its last entry was
            // NotificationRepository#findByRequest, whose caller — app-worker's RequestFanOut —
            // already held event.tenantId(); the query and the call site now pass it.
            );

    /** Native: {@code AND r.tenant_id = :tenantId}. JPQL: {@code AND r.tenantId = :tenantId}. */
    private static final Pattern TENANT_PREDICATE =
            Pattern.compile("tenant_?[iI]d\\s*=\\s*:tenantId");

    private static List<Class<?>> repositories;

    @BeforeAll
    static void importRepositories() {
        repositories = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages(REPOSITORY_PACKAGE)
                .stream()
                .filter(javaClass -> javaClass.isInterface())
                .map(javaClass -> javaClass.reflect())
                .filter(Repository.class::isAssignableFrom)
                .sorted(Comparator.comparing(Class::getSimpleName))
                .collect(Collectors.toList());
    }

    @Test
    @DisplayName("a query that reads or writes a tenant-owned table without a tenant predicate fails the build")
    void everyTenantOwnedQueryIsScopedToOneTenant() {
        var unscoped = new ArrayList<String>();

        for (Class<?> repository : repositories) {
            for (Method method : repository.getDeclaredMethods()) {
                String sql = queryTextOf(method);
                if (sql == null || !touchesTenantOwnedTable(sql)) {
                    continue;
                }
                String key = keyOf(repository, method);
                if (INFRASTRUCTURE_SWEEPS.contains(key) || PENDING_CALLER_MIGRATION.contains(key)) {
                    continue;
                }
                if (!isTenantScoped(sql)) {
                    unscoped.add(key + " — its @Query touches "
                            + tenantOwnedTablesIn(sql)
                            + " but has no tenant predicate. Add 'AND <alias>.tenantId = :tenantId' "
                            + "(JPQL) or 'AND <alias>.tenant_id = :tenantId' (native) and a "
                            + "@Param(\"tenantId\") parameter, or — if this really is a "
                            + "timer-driven sweep with no tenant in scope — name it in "
                            + "INFRASTRUCTURE_SWEEPS with the reason.");
                    continue;
                }
                if (!declaresTenantIdParameter(method)) {
                    unscoped.add(key + " — its @Query names :tenantId but the method declares no "
                            + "@Param(\"tenantId\"), so the binding will fail at first call.");
                }
            }
        }

        assertThat(unscoped)
                .as("every query over %s must name a tenant; these do not:%n%s",
                        TENANT_OWNED_TABLES, String.join(System.lineSeparator(), unscoped))
                .isEmpty();
    }

    @Test
    @DisplayName("an allowlist entry naming a method that no longer exists fails the build")
    void allowlistDoesNotRot() {
        var declared = new LinkedHashSet<String>();
        for (Class<?> repository : repositories) {
            for (Method method : repository.getDeclaredMethods()) {
                declared.add(keyOf(repository, method));
            }
        }

        var stale = new ArrayList<String>();
        allEntries().stream().filter(entry -> !declared.contains(entry)).forEach(stale::add);

        // A renamed or deleted method leaves its exemption behind, and the exemption then silently
        // covers the next method that happens to take the same name. That is how an allowlist stops
        // being a list of decisions and becomes a list of accidents.
        assertThat(stale)
                .as("these allowlist entries name methods that no longer exist: %s", stale)
                .isEmpty();
    }

    @Test
    @DisplayName("an allowlisted query that has since gained a tenant predicate must leave the allowlist")
    void allowlistDoesNotOutliveItsReason() {
        var redundant = new ArrayList<String>();

        for (Class<?> repository : repositories) {
            for (Method method : repository.getDeclaredMethods()) {
                String key = keyOf(repository, method);
                if (!allEntries().contains(key)) {
                    continue;
                }
                String sql = queryTextOf(method);
                if (sql != null && isTenantScoped(sql)) {
                    redundant.add(key);
                }
            }
        }

        // The other direction of rot. An exemption that no longer exempts anything reads as
        // 'this one is allowed to be unscoped' to the next person to touch the file, and invites
        // them to remove the predicate rather than the entry.
        assertThat(redundant)
                .as("these queries are tenant-scoped and no longer need an exemption: %s", redundant)
                .isEmpty();
    }

    @Test
    @DisplayName("a repository moved out of the scanned package would silently escape this test")
    void everyRepositoryInTheModuleIsScanned() {
        // The whole test is worth nothing if the importer finds nothing, and it finds nothing on a
        // package rename — quietly, and green. Pinning the count is not the point; proving the
        // scan reached the interfaces this file was written about is.
        assertThat(repositories)
                .extracting(Class::getSimpleName)
                .contains("NotificationRepository",
                        "NotificationRecipientRepository",
                        "DeliveryAttemptRepository",
                        "NotificationEventRepository",
                        "NotificationRequestRepository",
                        "IdempotencyRepository");
    }

    private static Set<String> allEntries() {
        var all = new LinkedHashSet<>(INFRASTRUCTURE_SWEEPS);
        all.addAll(PENDING_CALLER_MIGRATION);
        return all;
    }

    private static String keyOf(Class<?> repository, Method method) {
        return repository.getSimpleName() + "#" + method.getName();
    }

    private static String queryTextOf(Method method) {
        Query query = method.getAnnotation(Query.class);
        if (query == null || query.value().isBlank()) {
            return null;
        }
        return query.value();
    }

    private static boolean touchesTenantOwnedTable(String sql) {
        return !tenantOwnedTablesIn(sql).isEmpty();
    }

    private static List<String> tenantOwnedTablesIn(String sql) {
        var found = new ArrayList<String>();
        TENANT_OWNED_TABLES.stream().filter(table -> mentions(sql, table)).forEach(found::add);
        TENANT_OWNED_ENTITIES.stream().filter(entity -> mentions(sql, entity)).forEach(found::add);
        found.sort(Comparator.naturalOrder());
        return found;
    }

    /**
     * Word-boundary match, so {@code notif.notification} does not claim a query that only names
     * {@code notif.notification_recipient}. The dot is escaped for the same reason.
     */
    private static boolean mentions(String sql, String name) {
        return Pattern.compile("\\b" + Pattern.quote(name) + "\\b").matcher(sql).find();
    }

    /**
     * An {@code INSERT} has no {@code WHERE} to scope: it is scoped by the {@code tenant_id} value
     * it writes. Requiring a predicate there would be a rule nobody could satisfy, and answering
     * "it is an insert, so skip it" would exempt every write. So the rule for an insert is that it
     * must name the column and bind the parameter.
     */
    private static boolean isTenantScoped(String sql) {
        if (sql.stripLeading().toUpperCase(Locale.ROOT).startsWith("INSERT")) {
            return sql.contains("tenant_id") && sql.contains(":tenantId");
        }
        return TENANT_PREDICATE.matcher(sql).find();
    }

    /**
     * Spring Data will not bind {@code :tenantId} to a parameter it cannot name, and on a native
     * query the failure is at first invocation rather than at context start — so an annotation that
     * looks right can ship. Checked here rather than trusted.
     */
    private static boolean declaresTenantIdParameter(Method method) {
        return Arrays.stream(method.getParameters())
                .anyMatch(TenantScopedQueryArchTest::isTenantIdParameter);
    }

    private static boolean isTenantIdParameter(Parameter parameter) {
        Param param = parameter.getAnnotation(Param.class);
        return param != null && "tenantId".equals(param.value());
    }
}
