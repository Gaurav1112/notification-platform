package dev.gaurav.notification.scheduler.config;

import java.time.Clock;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Turns on {@code @Scheduled} and sizes the pool that runs it.
 *
 * <p><strong>The pool size is the point of this class.</strong> Spring's default
 * {@code @Scheduled} executor is a <em>single</em> thread. Every job in this application would
 * then share it, which means the daily partition job — a few hundred DDL statements — blocks the
 * 100 ms outbox sweep and the 100 ms claim loop for as long as it runs, and the 30 s lease reaper
 * behind that. Nothing errors. The only symptom is scheduler lag, on a graph, at 00:05 UTC every
 * day, and it is close to unattributable after the fact.
 *
 * <p>Six threads: outbox sweeper, hydrator, claimer, lease reaper, retry promoter, partition job —
 * one each, so a slow job delays only itself.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(SchedulerProperties.class)
public class SchedulerConfiguration {

    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(6);
        scheduler.setThreadNamePrefix("np-sched-");
        // Let in-flight passes finish on shutdown: a claim pass killed between the claim commit
        // and the Kafka publish leaves rows leased for 60 s that nobody is working on.
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(30);
        // Virtual threads are on for the servlet container, but scheduled jobs are long-lived
        // loops that mostly block on JDBC — a platform thread each is cheaper than the scaffolding.
        scheduler.setVirtualThreads(false);
        return scheduler;
    }

    /**
     * Injected rather than called statically, so a test can advance time instead of sleeping.
     * A scheduler test that waits out a 60-second lease in real time is a test nobody runs.
     */
    @Bean
    public Clock schedulerClock() {
        return Clock.systemUTC();
    }
}
