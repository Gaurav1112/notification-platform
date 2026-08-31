package dev.gaurav.notification.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/** Dispatch tier. Scales on Kafka consumer lag, gated on provider health. Owns orchestration, channel workers, provider routing and status projection. */
@SpringBootApplication(scanBasePackages = "dev.gaurav.notification")
@EntityScan(basePackages = "dev.gaurav.notification.persistence.entity")
@EnableJpaRepositories(basePackages = "dev.gaurav.notification.persistence.repository")
public class NotificationWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationWorkerApplication.class, args);
    }
}
