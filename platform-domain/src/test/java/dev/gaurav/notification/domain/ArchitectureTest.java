package dev.gaurav.notification.domain;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architecture that isn't enforced by a test is a wish.
 *
 * <p>The domain module is the core of the hexagon: it holds entities, value objects, enums and
 * invariants, and it must not know that Spring, JPA or Kafka exist. Without a test, framework
 * imports leak in gradually — one {@code @Component} at a time — and by the time anyone notices,
 * the domain can no longer be unit-tested without a Spring context, and swapping the persistence
 * or messaging layer means editing business logic.
 */
class ArchitectureTest {

    private static JavaClasses domainClasses;

    @BeforeAll
    static void importDomain() {
        domainClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("dev.gaurav.notification.domain");
    }

    @Test
    @DisplayName("the domain does not depend on Spring")
    void domainIsFrameworkFree() {
        noClasses()
                .should().dependOnClassesThat().resideInAnyPackage("org.springframework..")
                .because("the domain must be unit-testable without a Spring context, and "
                        + "swapping the framework must not mean editing business rules")
                .check(domainClasses);
    }

    @Test
    @DisplayName("the domain does not depend on JPA or Jakarta persistence")
    void domainIsPersistenceFree() {
        noClasses()
                .should().dependOnClassesThat().resideInAnyPackage("jakarta.persistence..", "javax.persistence..")
                .because("entities are a persistence concern; the domain model is not an ORM mapping")
                .check(domainClasses);
    }

    @Test
    @DisplayName("the domain does not depend on Kafka")
    void domainIsMessagingFree() {
        noClasses()
                .should().dependOnClassesThat().resideInAnyPackage("org.apache.kafka..")
                .because("Kafka is transport; the domain must not know how it is delivered")
                .check(domainClasses);
    }

    @Test
    @DisplayName("the domain does not depend on Jackson")
    void domainIsSerialisationFree() {
        noClasses()
                .should().dependOnClassesThat().resideInAnyPackage("com.fasterxml.jackson..", "tools.jackson..")
                .because("wire format is an adapter concern; annotating the domain with @JsonProperty "
                        + "couples the public API shape to the internal model")
                .check(domainClasses);
    }

    @Test
    @DisplayName("the domain does not use java.util.Date or Calendar")
    void domainUsesModernTime() {
        noClasses()
                .should().dependOnClassesThat().haveFullyQualifiedName("java.util.Date")
                .orShould().dependOnClassesThat().haveFullyQualifiedName("java.util.Calendar")
                .because("this platform is timezone-critical; java.time is the only safe choice")
                .check(domainClasses);
    }
}
