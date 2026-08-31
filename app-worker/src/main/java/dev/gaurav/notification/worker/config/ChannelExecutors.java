package dev.gaurav.notification.worker.config;

import dev.gaurav.notification.domain.enums.Channel;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * One bounded thread pool per channel. The bulkhead.
 *
 * <p><strong>Why not {@code ForkJoinPool.commonPool()}</strong> — which is what
 * {@code CompletableFuture.supplyAsync(task)} uses when nobody passes an executor, and therefore
 * what this code would run on by accident:
 *
 * <ol>
 *   <li><strong>It is sized for CPU work, not for waiting.</strong> The common pool has
 *       {@code availableProcessors() - 1} threads. On a 4-vCPU pod that is three. Three concurrent
 *       provider calls at 250 ms is twelve sends per second, against a design point of 3,900.</li>
 *   <li><strong>It is shared with everything else in the JVM.</strong> Parallel streams, other
 *       libraries, and all three channels. A vendor that starts taking eight seconds per call
 *       occupies every thread, and SMS, email and push all stop — plus any parallel stream
 *       anywhere in the process. One provider incident becomes a total outage. That is precisely
 *       the coupling a bulkhead exists to break.</li>
 *   <li><strong>Its queue is unbounded.</strong> Backpressure disappears: work piles up in memory
 *       until the pod OOMs, instead of being refused while the system is still healthy enough to
 *       refuse it.</li>
 *   <li><strong>You cannot shut it down.</strong> Graceful shutdown cannot drain it, so a rolling
 *       deploy abandons in-flight sends and manufactures the {@code UNKNOWN} attempts the
 *       reconciler then has to chase.</li>
 * </ol>
 *
 * <p>The queue here is bounded and the rejection policy is
 * {@link ThreadPoolExecutor.AbortPolicy} — an explicit {@link RejectedExecutionException} rather
 * than {@code CallerRunsPolicy}. Caller-runs would look like graceful degradation and would in fact
 * run the provider call <em>on the Kafka consumer thread</em>, which is the one thread that must
 * keep polling. A rejection is a classified, retryable failure; a blocked consumer thread is a
 * rebalance.
 *
 * <p>Sizing is per channel because the channels are not alike: push has one provider and the
 * widest topic, SMS has the tightest latency objective. A single shared pool would have to be
 * sized for the worst of them and would let the bulk lane consume it.
 */
public class ChannelExecutors implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ChannelExecutors.class);

    private final Map<Channel, ThreadPoolTaskExecutor> pools = new EnumMap<>(Channel.class);

    public ChannelExecutors(WorkerProperties properties, MeterRegistry meters) {
        for (Channel channel : Channel.values()) {
            var executor = new ThreadPoolTaskExecutor();
            executor.setThreadNamePrefix("np-" + channel.name().toLowerCase() + "-");
            executor.setCorePoolSize(properties.channelPoolSize());
            executor.setMaxPoolSize(properties.channelPoolSize());
            executor.setQueueCapacity(properties.channelQueueCapacity());
            // Refuse loudly instead of degrading invisibly. See the class javadoc.
            executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
            // A send that is in flight when the pod is told to stop must be allowed to finish and
            // record its outcome; killing it produces an UNKNOWN attempt for the reconciler.
            executor.setWaitForTasksToCompleteOnShutdown(true);
            executor.setAwaitTerminationSeconds(30);
            executor.initialize();
            ExecutorServiceMetrics.monitor(meters, executor.getThreadPoolExecutor(),
                    "notification-dispatch", List.of(
                            io.micrometer.core.instrument.Tag.of("channel", channel.name())));
            pools.put(channel, executor);
        }
        log.info("channel bulkheads initialised: {} threads and a {}-deep queue per channel",
                properties.channelPoolSize(), properties.channelQueueCapacity());
    }

    /** The bulkhead a channel's provider calls run on. */
    public ExecutorService forChannel(Channel channel) {
        return pools.get(channel).getThreadPoolExecutor();
    }

    /** Queued-but-not-started sends. A non-zero value means the channel is saturated. */
    public int queueDepth(Channel channel) {
        return pools.get(channel).getThreadPoolExecutor().getQueue().size();
    }

    @Override
    public void close() {
        pools.values().forEach(ThreadPoolTaskExecutor::shutdown);
    }
}
