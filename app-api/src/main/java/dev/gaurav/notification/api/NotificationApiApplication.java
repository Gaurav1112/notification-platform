package dev.gaurav.notification.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/** Ingest and query tier. Stateless, scales 3 to 60 on request rate. Owns the accept transaction, idempotency, quota and inbound provider webhooks. */
@SpringBootApplication(scanBasePackages = "dev.gaurav.notification")
@EntityScan(basePackages = "dev.gaurav.notification.persistence.entity")
@EnableJpaRepositories(basePackages = "dev.gaurav.notification.persistence.repository")
public class NotificationApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationApiApplication.class, args);
    }
}
