package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.provider.spi.NotificationProvider;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Assembles the decorator stack in <strong>one fixed order</strong>, whatever order the builder
 * methods are called in.
 *
 * <p>That is the point of this class. Decorator order is a design decision with observable
 * consequences, and a caller who wires the chain by hand gets it subtly wrong once and then nobody
 * notices for six months. Outermost first:
 *
 * <pre>
 *   Traced  →  Metered  →  CircuitBreaker  →  RateLimited  →  Timeout  →  Idempotent  →  adapter
 * </pre>
 *
 * <p>Each boundary earns its position:
 * <ol>
 *   <li><strong>Traced outermost</strong> — the span must cover time spent waiting on our own
 *       limiter and pool, or the trace blames the vendor for latency we created.</li>
 *   <li><strong>Metered above the breaker</strong> — a short-circuited call is an outcome the
 *       caller experienced. Measured below the breaker, an open circuit looks like zero traffic
 *       and 100% health, which is exactly backwards.</li>
 *   <li><strong>CircuitBreaker above RateLimited</strong> — a rate-limit rejection is our own
 *       back-pressure, not evidence the provider is ill. If the limiter sat outside, its
 *       rejections would never reach the breaker; with it inside, the breaker must be configured
 *       not to count them. Either way this ordering keeps the breaker's window populated by real
 *       vendor responses.</li>
 *   <li><strong>RateLimited above Timeout</strong> — the deadline exists to bound the
 *       <em>provider</em>. Time queued for one of our own tokens should not silently consume the
 *       vendor's budget and make a healthy provider look slow.</li>
 *   <li><strong>Timeout above Idempotent</strong> — the token must be written before the clock can
 *       cut the call off. A timed-out call is precisely the case where that record is the only
 *       evidence a send ever happened.</li>
 * </ol>
 *
 * <p>Stages are optional: leaving one out yields a shorter chain, not a different order.
 */
public final class ProviderDecoratorChain {

    private ProviderDecoratorChain() {
    }

    public static Builder around(NotificationProvider adapter) {
        return new Builder(adapter);
    }

    /** Mutable assembly helper; not thread-safe, and only used at startup. */
    public static final class Builder {

        private final NotificationProvider adapter;

        private SentTokenLog tokenLog;
        private Clock clock = Clock.systemUTC();
        private ExecutorService timeoutExecutor;
        private Duration timeoutCeiling;
        private int dedicatedThreads = -1;
        private int dedicatedQueueDepth = 0;
        private boolean rateLimited;
        private boolean circuitBroken;
        private MeterRegistry meterRegistry;
        private boolean traced;

        private Builder(NotificationProvider adapter) {
            this.adapter = Objects.requireNonNull(adapter, "adapter");
        }

        public Builder idempotent(SentTokenLog tokenLog) {
            this.tokenLog = Objects.requireNonNull(tokenLog, "tokenLog");
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /** Bound the call using a pool the caller owns and shuts down. */
        public Builder timeout(ExecutorService executor, Duration ceiling) {
            this.timeoutExecutor = Objects.requireNonNull(executor, "executor");
            this.timeoutCeiling = Objects.requireNonNull(ceiling, "ceiling");
            this.dedicatedThreads = -1;
            return this;
        }

        /** Bound the call using a pool named after this provider; the chain owns it. */
        public Builder timeoutWithDedicatedPool(int threads, int queueDepth, Duration ceiling) {
            this.dedicatedThreads = threads;
            this.dedicatedQueueDepth = queueDepth;
            this.timeoutCeiling = Objects.requireNonNull(ceiling, "ceiling");
            this.timeoutExecutor = null;
            return this;
        }

        public Builder rateLimited() {
            this.rateLimited = true;
            return this;
        }

        public Builder circuitBroken() {
            this.circuitBroken = true;
            return this;
        }

        public Builder metered(MeterRegistry registry) {
            this.meterRegistry = Objects.requireNonNull(registry, "registry");
            return this;
        }

        public Builder traced() {
            this.traced = true;
            return this;
        }

        /** Everything the platform expects of a production provider, in the canonical order. */
        public Builder full(MeterRegistry registry, SentTokenLog tokenLog, Duration ceiling) {
            return traced()
                    .metered(registry)
                    .circuitBroken()
                    .rateLimited()
                    .timeoutWithDedicatedPool(16, 32, ceiling)
                    .idempotent(tokenLog);
        }

        public NotificationProvider build() {
            NotificationProvider chain = adapter;
            if (tokenLog != null) {
                chain = new IdempotentProvider(chain, tokenLog, clock);
            }
            if (timeoutCeiling != null) {
                chain = dedicatedThreads > 0
                        ? TimeoutProvider.withDedicatedPool(chain, dedicatedThreads, dedicatedQueueDepth, timeoutCeiling)
                        : new TimeoutProvider(chain, timeoutExecutor, timeoutCeiling);
            }
            if (rateLimited) {
                chain = new RateLimitedProvider(chain);
            }
            if (circuitBroken) {
                chain = new CircuitBreakerProvider(chain);
            }
            if (meterRegistry != null) {
                chain = new MeteredProvider(chain, meterRegistry);
            }
            if (traced) {
                chain = new TracedProvider(chain);
            }
            return chain;
        }
    }
}
