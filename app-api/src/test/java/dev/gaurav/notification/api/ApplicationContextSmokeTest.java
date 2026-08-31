package dev.gaurav.notification.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.ActiveProfiles;

/**
 * Starts the whole {@code app-api} context.
 *
 * <p><strong>This is the test that was missing, and its absence is the entire reason this module
 * shipped unable to boot.</strong> 371 unit tests were green while
 * {@code NotificationController} could not be constructed: every one of its tests used
 * {@code standaloneSetup} with mocked ports, which is the right way to assert an HTTP contract and
 * says nothing at all about whether a bean exists to satisfy the constructor at runtime. A missing
 * bean is not a logic error, so no amount of logic testing finds it. Only a context load does.
 *
 * <p>Runs against an in-memory database in the {@code test} profile rather than Testcontainers, on
 * purpose. The value here is "does every constructor in the graph have something to inject", and
 * that question must be answerable on a laptop with no Docker daemon and in a CI job with no
 * privileged runner — otherwise the check gets tagged {@code integration}, excluded from the
 * default build, and stops being run at exactly the moment it would have caught something. The
 * queries these adapters issue are PostgreSQL-specific and are verified separately against a real
 * 18.6 container in {@code platform-persistence}.
 *
 * @see dev.gaurav.notification.api.adapter.NotificationCommandAdapter
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class ApplicationContextSmokeTest {

    @Test
    @DisplayName("the Spring context loads — 371 green unit tests did not catch a missing bean")
    void contextLoads() {
    }
}
