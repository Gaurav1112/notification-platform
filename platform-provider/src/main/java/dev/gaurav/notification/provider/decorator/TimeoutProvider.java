package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Puts a hard ceiling on a provider call.
 *
 * <p><strong>The failure this prevents:</strong> a vendor that accepts the TCP connection and then
 * never responds. An HTTP client's socket timeout does not cover connection-pool acquisition, DNS
 * or TLS, so "5 second read timeout" routinely becomes a 90-second call. On a Kafka consumer that
 * blows through {@code max.poll.interval.ms}, the group rebalances, the partition is reassigned,
 * the in-flight batch is redelivered — and one slow provider turns into a cluster-wide stall plus
 * duplicate sends.
 *
 * <p><strong>Why a dedicated pool and never {@link java.util.concurrent.ForkJoinPool#commonPool}:</strong>
 * the common pool is shared with parallel streams and {@code CompletableFuture} defaults across the
 * whole JVM. Its width is {@code availableProcessors() - 1}, which on a 2-vCPU container is
 * <em>one thread</em>. Blocking I/O parked there starves every other user of it, and the
 * resulting stall is invisible in a thread dump taken from the wrong angle. A named, bounded,
 * per-provider pool makes saturation attributable to the provider that caused it.
 *
 * <p><strong>Why {@link SendResult.Indeterminate} and not a failure:</strong> the request was on
 * the wire. The provider may well have delivered it. Reporting this as a plain failure is what
 * produces three one-time passcodes for one login.
 */
public final class TimeoutProvider extends AbstractProviderDecorator implements AutoCloseable {

    /** Below this, the deadline is unsatisfiable and we would only be measuring our own overhead. */
    private static final Duration MIN_BUDGET = Duration.ofMillis(20);

    private final ExecutorService executor;
    private final Duration ceiling;
    private final boolean ownsExecutor;

    public TimeoutProvider(NotificationProvider delegate, ExecutorService executor, Duration ceiling) {
        this(delegate, executor, ceiling, false);
    }

    private TimeoutProvider(NotificationProvider delegate, ExecutorService executor,
                            Duration ceiling, boolean ownsExecutor) {
        super(delegate);
        this.executor = Objects.requireNonNull(executor, "executor");
        this.ceiling = Objects.requireNonNull(ceiling, "ceiling");
        if (ceiling.compareTo(MIN_BUDGET) < 0) {
            throw new IllegalArgumentException("ceiling must be at least " + MIN_BUDGET);
        }
        this.ownsExecutor = ownsExecutor;
    }

    /**
     * A pool sized and named for one provider, so a thread dump names the culprit and saturation
     * of one vendor cannot starve another.
     *
     * @param queueDepth deliberately small — see {@link #send(SendCommand)} for why queuing is
     *                   worse than shedding here
     */
    public static TimeoutProvider withDedicatedPool(NotificationProvider delegate, int threads,
                                                    int queueDepth, Duration ceiling) {
        var counter = new AtomicInteger();
        var name = "provider-" + delegate.code().value() + "-";
        ThreadFactory factory = runnable -> {
            var thread = new Thread(runnable, name + counter.incrementAndGet());
            thread.setDaemon(true); // must not keep the JVM alive during a shutdown drain
            return thread;
        };
        var pool = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueDepth), factory, new ThreadPoolExecutor.AbortPolicy());
        return new TimeoutProvider(delegate, pool, ceiling, true);
    }

    @Override
    public SendResult send(SendCommand command) {
        var budget = min(command.deadline(), ceiling);
        var startedAt = System.nanoTime();

        Future<SendResult> inFlight;
        try {
            inFlight = executor.submit(() -> delegate.send(command));
        } catch (RejectedExecutionException e) {
            // The pool is full. Parking the calling thread until a slot frees would hand the
            // provider's backlog straight to our Kafka consumer. Shedding to the next candidate is
            // strictly better: RATE_LIMITED fails over immediately without burning an attempt.
            return SendResult.Rejected.of(FailureType.RATE_LIMITED, "LOCAL_POOL_SATURATED",
                    "no send slot available for " + code(), elapsedSince(startedAt));
        }

        try {
            return inFlight.get(budget.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            inFlight.cancel(true); // interrupt, so a sleeping adapter releases its thread
            return new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT,
                    "no response from " + code() + " within " + budget.toMillis() + "ms",
                    elapsedSince(startedAt));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // never swallow: shutdown depends on this flag
            inFlight.cancel(true);
            return new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT,
                    "interrupted while awaiting " + code(), elapsedSince(startedAt));
        } catch (ExecutionException e) {
            // The SPI forbids throwing for a business failure, so this is an adapter bug — an NPE
            // or a mapping gap. Converting it to a retryable result would hide it forever and
            // retry a bug 5 times. It is rethrown so the worker DLQs the message loudly.
            throw new ProviderAdapterException(code(), e.getCause());
        }
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static Duration elapsedSince(long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos);
    }

    /** The effective ceiling, for tests and diagnostics. */
    public Duration ceiling() {
        return ceiling;
    }

    /** Only shuts down a pool this instance created; a shared executor stays the caller's problem. */
    @Override
    public void close() {
        if (ownsExecutor) {
            executor.shutdownNow();
        }
    }

    /** Signals a broken adapter, never a provider-side failure. */
    public static final class ProviderAdapterException extends RuntimeException {
        private final transient Optional<String> providerCode;

        ProviderAdapterException(dev.gaurav.notification.provider.spi.ProviderCode code, Throwable cause) {
            super("adapter " + code + " threw instead of returning a SendResult", cause);
            this.providerCode = Optional.of(code.value());
        }

        public Optional<String> providerCode() {
            return providerCode;
        }
    }
}
