package dev.gaurav.notification.scheduler.config;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exclusivity is the only property leader election sells, so it is asserted against a store that
 * genuinely honours set-if-absent under concurrency rather than a mock that returns {@code true}.
 * {@link InMemoryLeaderLockStore} is backed by {@code ConcurrentHashMap.putIfAbsent}, which gives
 * the same atomicity guarantee as Redis {@code SET NX} for the purposes of this test.
 */
class LeaderElectionTest {

    private static final String LEASE = "notification-hydrator";
    private static final Duration TTL = Duration.ofSeconds(30);

    private final InMemoryLeaderLockStore store = new InMemoryLeaderLockStore();

    @Test
    @DisplayName("eight pods campaigning at the same instant produce exactly one hydrator")
    void onlyOneCampaignerWins() throws Exception {
        int pods = 8;
        var elections = IntStream.range(0, pods)
                .mapToObj(i -> election("notification-scheduler-" + i))
                .toList();

        var startLine = new CountDownLatch(1);
        var results = new ConcurrentHashMap<Integer, Optional<Leadership>>();
        var pool = Executors.newFixedThreadPool(pods);
        try {
            for (int i = 0; i < pods; i++) {
                int index = i;
                pool.submit(() -> {
                    startLine.await();
                    results.put(index, elections.get(index).campaign(LEASE, TTL));
                    return null;
                });
            }
            startLine.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        var winners = results.values().stream().flatMap(Optional::stream).toList();
        assertThat(winners)
                .as("two hydrators means the due scan runs twice, and the whole reason it is "
                        + "leader-elected is that wasted index visits scale as B*W^2/2")
                .hasSize(1);
    }

    @Test
    @DisplayName("a renewal keeps the same fencing token, so the leader cannot fence out itself")
    void renewalDoesNotMintANewToken() {
        var election = election("notification-scheduler-0");

        long first = election.campaign(LEASE, TTL).orElseThrow().fencingToken();
        long second = election.campaign(LEASE, TTL).orElseThrow().fencingToken();
        long third = election.campaign(LEASE, TTL).orElseThrow().fencingToken();

        assertThat(List.of(second, third))
                .as("the DueIndex guard rejects a token lower than the one it has seen; if renewal "
                        + "bumped the token, the leader's own in-flight writes would be refused")
                .containsExactly(first, first);
    }

    @Test
    @DisplayName("the pod that takes over gets a strictly higher token than the pod that died")
    void tokensIncreaseAcrossAcquisitions() {
        var first = election("notification-scheduler-0");
        var second = election("notification-scheduler-1");

        long firstToken = first.campaign(LEASE, TTL).orElseThrow().fencingToken();
        assertThat(second.campaign(LEASE, TTL)).isEmpty();

        store.expire(LEASE);

        long secondToken = second.campaign(LEASE, TTL).orElseThrow().fencingToken();
        assertThat(secondToken)
                .as("the resource keeps the highest token it has seen, so a zombie waking from a "
                        + "GC pause with the old token has its writes refused by Redis")
                .isGreaterThan(firstToken);
    }

    @Test
    @DisplayName("a pod that lost the lease under a pause stands down instead of renewing it")
    void aLostLeaseIsNotSilentlyReacquired() {
        var leader = election("notification-scheduler-0");
        var usurper = election("notification-scheduler-1");

        leader.campaign(LEASE, TTL).orElseThrow();
        store.expire(LEASE);
        long usurperToken = usurper.campaign(LEASE, TTL).orElseThrow().fencingToken();

        assertThat(leader.campaign(LEASE, TTL))
                .as("the CAS renewal must fail against another owner's value; renewing "
                        + "unconditionally would evict a leader that this pod does not know exists")
                .isEmpty();
        assertThat(leader.holds(LEASE)).isFalse();
        assertThat(usurper.campaign(LEASE, TTL).orElseThrow().fencingToken()).isEqualTo(usurperToken);
    }

    @Test
    @DisplayName("resigning on shutdown releases the lease instead of stalling a rolling deploy")
    void resignHandsTheLeaseOverImmediately() {
        var leaving = election("notification-scheduler-0");
        var arriving = election("notification-scheduler-1");

        leaving.campaign(LEASE, TTL).orElseThrow();
        assertThat(arriving.campaign(LEASE, TTL)).isEmpty();

        leaving.resign(LEASE);

        assertThat(arriving.campaign(LEASE, TTL))
                .as("without this, every deploy stops scheduling for a full lease duration")
                .isPresent();
    }

    @Test
    @DisplayName("an unreachable lock store makes a pod a follower, never a leader")
    void anUnavailableStoreFailsClosed() {
        var unavailable = new LeaderElection(new LeaderLockStore() {
            @Override
            public long acquire(String lockKey, String fenceKey, String owner, Duration ttl) {
                return NOT_ACQUIRED;
            }

            @Override
            public boolean renew(String lockKey, String owner, Duration ttl) {
                return false;
            }

            @Override
            public void release(String lockKey, String owner) {
            }
        }, new NodeIdentity("notification-scheduler-0"), new SimpleMeterRegistry());

        assertThat(unavailable.campaign(LEASE, TTL))
                .as("fail open on a rate limit, never on leadership: every replica believing it is "
                        + "the single writer is the exact situation this prevents")
                .isEmpty();
    }

    private LeaderElection election(String podName) {
        return new LeaderElection(store, new NodeIdentity(podName), new SimpleMeterRegistry());
    }

    /**
     * Redis {@code SET NX} / {@code INCR} semantics, in memory.
     *
     * <p>{@code putIfAbsent} is the only operation that matters: it is atomic, so N threads racing
     * on one key produce exactly one winner, which is precisely the guarantee the real store gets
     * from Redis being single-threaded. Expiry is manual — a test that waits out a real TTL is a
     * test nobody runs.
     */
    private static final class InMemoryLeaderLockStore implements LeaderLockStore {

        private final ConcurrentHashMap<String, String> locks = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicLong> fences = new ConcurrentHashMap<>();

        @Override
        public long acquire(String lockKey, String fenceKey, String owner, Duration ttl) {
            if (locks.putIfAbsent(lockKey, owner) != null) {
                return NOT_ACQUIRED;
            }
            return fences.computeIfAbsent(fenceKey, key -> new AtomicLong()).incrementAndGet();
        }

        @Override
        public boolean renew(String lockKey, String owner, Duration ttl) {
            return owner.equals(locks.get(lockKey));
        }

        @Override
        public void release(String lockKey, String owner) {
            locks.remove(lockKey, owner);
        }

        /** Simulates the TTL firing while the holder is paused and unaware. */
        void expire(String leaseName) {
            locks.keySet().removeIf(key -> key.contains(leaseName));
        }
    }
}
