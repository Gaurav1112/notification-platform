package dev.gaurav.notification.scheduler.config;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Redis-backed leader election with fencing tokens: the component that turns three scheduler
 * replicas into one due-scan writer.
 *
 * <p><strong>Why this exists at all.</strong> {@code FOR UPDATE SKIP LOCKED} makes concurrent
 * claimers correct, and it is often assumed to make them <em>fast</em>. It does not fix bloat. A
 * documented pgsql-general case hit a hard wall at 128 concurrent claimers on 80 cores, because
 * every session has to step over every dead or non-matching tuple left behind by all the others,
 * and wasted index visits scale as {@code B·W²/2}. Running the due scan on all three replicas
 * therefore triples the wasted work without moving a single row one millisecond sooner. River and
 * Oban both reached the same conclusion: leader-elect the scan, reserve {@code SKIP LOCKED} for
 * the claim step where contention is actually useful.
 *
 * <p><strong>Renewal never mints a new token.</strong> The token identifies an <em>acquisition</em>
 * — the interval during which this JVM has been continuously leader. Bumping it on renewal would
 * make the leader's own resource guard reject its own in-flight writes, and would silently destroy
 * the ordering property the guard depends on.
 *
 * <p><strong>Losing a renewal does not trigger an immediate re-acquire.</strong> If the CAS fails,
 * somebody else may already be scanning; the campaign is retried on the next tick, one lease
 * duration later at the earliest. Retrying in a tight loop is how a flapping network turns into
 * two hydrators alternating on every tick.
 *
 * <p>Callers use it as {@code campaign(name, ttl).ifPresent(this::doLeaderWork)}. There is
 * deliberately no {@code isLeader()} returning a bare boolean: a boolean cannot be passed to the
 * resource as a fencing token, and a caller holding only a boolean has no way to prove which
 * acquisition it belongs to.
 */
@Component
public class LeaderElection implements SmartLifecycle {

    /**
     * The braces are a Redis Cluster hash tag. Without them {@code leader:hydrator} and
     * {@code leader:hydrator:fence} hash to different slots and the acquire script — which touches
     * both — is rejected with CROSSSLOT the day the cache is moved to cluster mode.
     */
    private static final String LOCK_KEY_FORMAT = "leader:{%s}";
    private static final String FENCE_KEY_FORMAT = "leader:{%s}:fence";

    private static final Logger log = LoggerFactory.getLogger(LeaderElection.class);

    private final LeaderLockStore store;
    private final NodeIdentity node;
    private final MeterRegistry meters;
    private final Clock clock;

    /** Leases this JVM believes it holds, by lease name. */
    private final Map<String, Leadership> held = new ConcurrentHashMap<>();

    private volatile boolean running;

    /**
     * Convenience for a test that does not care about time. Deliberately not the one Spring uses —
     * see the {@code @Autowired} below.
     */
    public LeaderElection(LeaderLockStore store, NodeIdentity node, MeterRegistry meters) {
        this(store, node, meters, Clock.systemUTC());
    }

    /**
     * {@code @Autowired} because this class has two public constructors and Spring will not guess.
     *
     * <p>With neither annotated, the container finds no unambiguous candidate, falls back to
     * looking for a no-arg constructor and fails the context with "No default constructor found" —
     * a message that points at a constructor nobody wrote rather than at the ambiguity. Marking the
     * four-argument one also makes the injected {@link Clock} the one the application configured,
     * so every lease deadline in this class is comparable with every other deadline in the
     * scheduler and is movable in a test.
     */
    @Autowired
    public LeaderElection(LeaderLockStore store, NodeIdentity node, MeterRegistry meters, Clock clock) {
        this.store = store;
        this.node = node;
        this.meters = meters;
        this.clock = clock;
    }

    /**
     * Renews the lease if this JVM holds it, otherwise tries to take it.
     *
     * <p>Call this on every tick of the work it guards, not from a separate renewal thread. A
     * renewal thread that keeps ticking while the work thread is wedged advertises a healthy
     * leader that is doing nothing, and the lease then never expires for a replica that could
     * actually make progress. Tying renewal to the work loop makes a stuck leader lose the lease.
     *
     * @return the leadership and its fencing token, or empty when another replica holds the lease
     */
    public Optional<Leadership> campaign(String leaseName, Duration leaseDuration) {
        var lockKey = LOCK_KEY_FORMAT.formatted(leaseName);
        var current = held.get(leaseName);

        if (current != null) {
            if (store.renew(lockKey, node.instanceId(), leaseDuration)) {
                var renewed = current.renewedUntil(clock.instant().plus(leaseDuration));
                held.put(leaseName, renewed);
                return Optional.of(renewed);
            }
            // Lost it: expired under a pause, or an operator deleted the key. The token we hold is
            // dead, and any write still in flight under it will be fenced out at the resource.
            held.remove(leaseName);
            meters.counter("scheduler.leader.lost", "lease", leaseName).increment();
            log.warn("lost leadership of {} (token {}); standing down until the next tick",
                    leaseName, current.fencingToken());
            return Optional.empty();
        }

        long token = store.acquire(lockKey, FENCE_KEY_FORMAT.formatted(leaseName),
                node.instanceId(), leaseDuration);
        if (token == LeaderLockStore.NOT_ACQUIRED) {
            return Optional.empty();
        }
        var acquired = new Leadership(leaseName, node.instanceId(), token,
                clock.instant().plus(leaseDuration));
        held.put(leaseName, acquired);
        meters.counter("scheduler.leader.acquired", "lease", leaseName).increment();
        log.info("acquired leadership of {} as {} with fencing token {}",
                leaseName, node.instanceId(), token);
        return Optional.of(acquired);
    }

    /** True if this JVM currently believes it holds the lease. For {@code /actuator/info} only. */
    public boolean holds(String leaseName) {
        return held.containsKey(leaseName);
    }

    /**
     * Hands the lease back.
     *
     * <p>Run on shutdown so a rolling deploy transfers the hydrator in milliseconds. Without it
     * every deploy stops scheduling for a full lease duration, which at a 5-minute horizon is
     * invisible in staging and a paged alert during a campaign.
     */
    public void resign(String leaseName) {
        var current = held.remove(leaseName);
        if (current != null) {
            store.release(LOCK_KEY_FORMAT.formatted(leaseName), node.instanceId());
            log.info("resigned leadership of {} (token {})", leaseName, current.fencingToken());
        }
    }

    @Override
    public void start() {
        running = true;
    }

    /** Resigns every held lease, after the scheduled jobs have stopped. See {@link #getPhase()}. */
    @Override
    public void stop() {
        held.keySet().forEach(this::resign);
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * The lowest phase, so this is the <em>last</em> lifecycle bean to stop.
     *
     * <p>Spring stops the highest phase first, and {@code ThreadPoolTaskScheduler} sits at
     * {@code Integer.MAX_VALUE / 2}. Resigning before it has drained would let a hydration pass
     * that is still running finish its writes under a lease it no longer owns — the exact
     * zombie-writer case fencing tokens exist for, manufactured by our own shutdown ordering.
     */
    @Override
    public int getPhase() {
        return Integer.MIN_VALUE;
    }
}
