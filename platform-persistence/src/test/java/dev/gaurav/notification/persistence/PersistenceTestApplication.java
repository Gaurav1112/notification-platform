package dev.gaurav.notification.persistence;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Boot entry point that exists only so {@code @SpringBootTest} has a context to build.
 *
 * <p>{@code platform-persistence} is a library and has no deployable of its own, but the
 * repositories and the entity mappings are exactly the things that cannot be verified without a
 * real Hibernate {@code SessionFactory} and a real PostgreSQL. Declaring this in
 * {@code src/test/java} under the module's root package makes Boot's auto-configuration package
 * {@code dev.gaurav.notification.persistence}, so entities and repositories are discovered the
 * same way they will be inside {@code app-api} and {@code app-worker} — the test verifies the
 * discovery, not a hand-wired substitute for it.
 */
@SpringBootApplication
public class PersistenceTestApplication {
}
