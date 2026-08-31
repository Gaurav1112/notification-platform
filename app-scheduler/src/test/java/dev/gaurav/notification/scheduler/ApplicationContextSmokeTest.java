package dev.gaurav.notification.scheduler;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.ActiveProfiles;

/**
 * Starts the whole {@code app-scheduler} context.
 *
 * <p>This module was believed to boot and did not. {@code LeaderElection} declares two public
 * constructors and neither carried {@code @Autowired}, so the container could not choose, fell back
 * to looking for a no-argument constructor, and failed with "No default constructor found" — an
 * error naming a constructor nobody wrote. A unit test that calls the four-argument constructor
 * directly, which is exactly what {@code LeaderElectionTest} does, passes either way.
 *
 * <p>The scheduled jobs are pushed out to a long interval in the {@code test} profile so this test
 * measures the bean graph and not what a due-scan does against an empty in-memory database.
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class ApplicationContextSmokeTest {

    @Test
    @DisplayName("the Spring context loads — 371 green unit tests did not catch a missing bean")
    void contextLoads() {
    }
}
