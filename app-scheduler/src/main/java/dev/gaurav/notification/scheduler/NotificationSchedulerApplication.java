package dev.gaurav.notification.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/** Scheduling tier. Fixed replica count, on-demand nodes only. Owns the leader-elected due scan, campaign fan-out, the outbox sweeper and retry promotion. */
@SpringBootApplication(scanBasePackages = "dev.gaurav.notification")
@EntityScan(basePackages = "dev.gaurav.notification.persistence.entity")
@EnableJpaRepositories(basePackages = "dev.gaurav.notification.persistence.repository")
public class NotificationSchedulerApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationSchedulerApplication.class, args);
    }
}
