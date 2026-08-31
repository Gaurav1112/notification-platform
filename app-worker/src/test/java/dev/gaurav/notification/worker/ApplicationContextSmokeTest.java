package dev.gaurav.notification.worker;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.ActiveProfiles;

/**
 * Starts the whole {@code app-worker} context.
 *
 * <p>This module was believed to boot. It did not, and for two reasons neither of which a unit test
 * could see: {@code @EnableKafka} was nowhere in the application, so
 * {@code KafkaListenerEndpointRegistry} did not exist and {@code ProviderHealthGate} could not be
 * constructed; and {@code resilience4j.circuitbreaker.configs.provider.randomized-wait-factor} was
 * {@code 1.0}, which the library validates into {@code [0, 1)} and rejects. Both are startup
 * failures with no code path a test could exercise short of starting the context.
 *
 * <p>The Kafka listener containers are held down in the {@code test} profile
 * ({@code spring.kafka.listener.auto-startup: false}). That is the difference between asserting
 * "the bean graph is complete" and asserting "a broker is reachable" — the second is a valuable
 * test and a different one, and mixing them makes this check fail for reasons that are not about
 * this module.
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class ApplicationContextSmokeTest {

    @Test
    @DisplayName("the Spring context loads — 371 green unit tests did not catch a missing bean")
    void contextLoads() {
    }
}
