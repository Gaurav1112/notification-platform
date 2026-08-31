package dev.gaurav.notification.persistence;

import dev.gaurav.notification.persistence.config.PartitionMaintenanceService;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Base class for tests that need a real PostgreSQL with the real schema applied.
 *
 * <p>Everything interesting in this module is behaviour PostgreSQL provides and an in-memory
 * database does not: range partitioning, {@code FOR UPDATE SKIP LOCKED}, {@code ON CONFLICT DO
 * NOTHING}, and an {@code UPDATE} whose {@code WHERE} clause anti-joins a lookup table. H2 in
 * PostgreSQL-compatibility mode either rejects those outright or, worse, accepts them with
 * different semantics — which is how a concurrency bug ships with a green test suite. So the
 * tests run against the same 18.6 image the schema was verified on.
 *
 * <p>The container is a plain {@code static final} started in a static initialiser rather than a
 * JUnit {@code @Container}: that way one container serves every test class in the module, instead
 * of one per class. Ryuk removes it when the JVM exits.
 *
 * <p>Tagged {@code integration} and excluded from the default Surefire run. The static initialiser
 * above runs before any {@code @BeforeAll}, so on a machine without a reachable Docker daemon every
 * subclass dies with {@code NoClassDefFoundError} — a red suite that says nothing about the code.
 * Tag filtering happens during JUnit discovery, so an excluded class is never initialised and the
 * container is never contacted. Run them with {@code ./mvnw verify -Pintegration}.
 */
@Tag("integration")
@SpringBootTest(classes = PersistenceTestApplication.class)
public abstract class AbstractPostgresTest {

    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6");

    static {
        POSTGRES.start();
        migrate();
    }

    /**
     * Applies {@code V1__baseline.sql} to the freshly started container.
     *
     * <p>Done here rather than through {@code spring.flyway.*} because Spring Boot 4 split its
     * auto-configurations into per-technology modules and this module depends on
     * {@code flyway-core} without {@code org.springframework.boot:spring-boot-flyway} — so the
     * properties in {@code src/test/resources/application.yml} are currently inert. Running it
     * explicitly also removes any dependency on bean-initialisation order: the schema exists
     * before Spring has a {@code DataSource}, which is the same ordering production gets from
     * running the migration as a Kubernetes Job ahead of the deploy.
     */
    private static void migrate() {
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas("notif")
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected PartitionMaintenanceService partitions;

    /**
     * Empties the volatile tables between tests.
     *
     * <p>{@code TRUNCATE} on a partitioned parent cascades to its partitions, so this leaves the
     * partition set intact — a {@code DROP} would take the partitions with it and the next test
     * would silently write into the DEFAULT partition, which is exactly the failure mode the
     * production alarm exists for.
     *
     * <p>Reference data seeded by V1 ({@code delivery_status}, {@code retry_policy}) is left
     * alone: the monotonic guard joins {@code delivery_status}, so a test that truncated it would
     * pass for the wrong reason.
     */
    @BeforeEach
    void resetVolatileTables() {
        partitions.ensureWindow(1);
        jdbcTemplate.execute("""
                TRUNCATE notif.notification,
                         notif.notification_recipient,
                         notif.notification_request,
                         notif.notification_event,
                         notif.delivery_attempt,
                         notif.idempotency_record,
                         notif.outbox_message,
                         notif.dead_letter_message
                """);
        // Separate statement, and CASCADE, because these three are joined by real foreign keys —
        // unlike the hot tables above, which have none by design.
        jdbcTemplate.execute("""
                TRUNCATE notif.provider_configuration, notif.provider, notif.tenant
                RESTART IDENTITY CASCADE
                """);
    }
}
