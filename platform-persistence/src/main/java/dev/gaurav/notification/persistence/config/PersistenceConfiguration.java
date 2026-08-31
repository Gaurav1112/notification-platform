package dev.gaurav.notification.persistence.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Wiring for the persistence module.
 *
 * <p>Intentionally thin. Entities and repositories are discovered by Spring Boot's own scanning
 * from whichever application module imports this one, so there is no {@code @EntityScan} or
 * {@code @EnableJpaRepositories} here — declaring them would pin the packages and quietly break
 * the moment an app wanted to add its own entity.
 */
@Configuration(proxyBeanMethods = false)
public class PersistenceConfiguration {

    /**
     * Local and test partition provisioning. Production uses {@code pg_partman} + {@code pg_cron};
     * see {@link PartitionMaintenanceService} for why a scheduled job in this JVM is the wrong
     * answer there.
     */
    @Bean
    public PartitionMaintenanceService partitionMaintenanceService(JdbcTemplate jdbcTemplate) {
        return new PartitionMaintenanceService(jdbcTemplate);
    }
}
