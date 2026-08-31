# ADR-009: Retry via five tiered delay topics and partition pause

**Status:** Accepted
**Date:** 2026-08-31

## Context

A failed send needs to be retried after a backoff. The backoff ladder is 5 s → 30 s → 2 m → 10 m →
1 h, capped by a 72-minute total deadline.

Kafka has **no native per-message delay**. Something has to hold the message for its backoff, and the
obvious implementation — sleep in the consumer and try again — is wrong.

## Decision

**Five delay topics, one per tier, consumed by a listener that pauses the partition rather than
sleeping.**

```java
T5S (Duration.ofSeconds(5),  "notification.retry.5s"),
T30S(Duration.ofSeconds(30), "notification.retry.30s"),
T2M (Duration.ofMinutes(2),  "notification.retry.2m"),
T10M(Duration.ofMinutes(10), "notification.retry.10m"),
T1H (Duration.ofHours(1),    "notification.retry.1h");
```

The tier consumer does **no external I/O at all**: it seeks the record back, pauses the partition,
re-polls until `ready_at`, re-checks eligibility (cancelled? expired?) and republishes to the
dispatch lane.

## Why `Thread.sleep()` in the consumer is wrong

Two independent reasons, and each on its own is disqualifying.

### 1. It holds the partition

The consumer owns that partition for the whole sleep. Every healthy message queued behind one
recipient stalled on a 30-second backoff waits too. At a 12.8% steady retry rate, a meaningful
fraction of every partition is sleeping.

### 2. It breaks `max.poll.interval.ms`

Kafka measures consumer liveness by the gap between `poll()` calls — 300 s in this configuration. The
10-minute and 1-hour tiers obviously exceed it.

Sleep past it and the broker declares the consumer dead. The group rebalances, in-flight partitions
are revoked and reassigned, and the in-flight messages are redelivered elsewhere. **Under a provider
outage *every* consumer sleeps at once**, so the whole group thrashes at precisely the moment it must
stay stable.

And the reason this is debugged as a broker problem for hours: **KIP-62 moved heartbeats to a
background thread.** The consumer keeps heartbeating and looks perfectly alive the entire time it is
blowing through the poll interval. Every liveness signal says healthy while throughput is zero.

**Raising `max.poll.interval.ms` is not the fix.** It costs failover time, and worst-case rebalance
detection then takes up to twice the interval.

## Why pausing works

A paused partition **still gets polled** — the poll simply returns nothing for it. So:

- The poll interval is never breached.
- The consumer stays in the group. No rebalance.
- The record was `seek`ed back and its offset was never committed, so a pod that dies while paused
  loses nothing.

## Why messages hop tiers rather than waiting in one

A partition is FIFO. A message waiting an hour at the head of the 5-second lane stalls everything
behind it.

Hopping means head-of-line blocking within a tier is **bounded by that tier's own delay**, which is
also what makes it safe to share five tiers across all three channels: the tier consumer never calls a
provider, so nothing in it can be slow for a channel-specific reason.

Five shared tiers instead of five per channel: 5 topics rather than 15.

## Rounding is up, never nearest

```java
public static RetryTier nearestFor(Duration delay) {
    for (RetryTier tier : values()) {
        if (tier.delay.compareTo(delay) >= 0) return tier;
    }
    return T1H;
}
```

A 17-second backoff is arithmetically closer to 5 s than to 30 s. Rounding to "closest" would fire the
retry 12 seconds early and **hand the failing provider back the load the backoff was calculated to
withhold**. Rounding up costs a little latency; rounding down discards the backoff.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **`Thread.sleep()` in the consumer** | Holds the partition; breaks the poll interval; rebalance storm under exactly the conditions retries happen. See above |
| **A database poll for `next_attempt_at`** | Puts retry load on the datastore that is already second on the bottleneck ladder, and re-introduces the `SKIP LOCKED` contention that [ADR-013](ADR-013-shard-affine-due-scan.md) works to avoid. Kept as a **backstop**, not the mechanism — `RetryPromoter` sweeps only rows past a grace period, covering retries the tiers genuinely lost |
| **A separate scheduler service holding retries in memory** | Loses everything in flight on a restart, exactly when restarts are most likely |
| **Kafka `DefaultErrorHandler` with a `BackOff`** | Spring's built-in retry does sleep in the container thread. Same problem, one layer down |
| **A dead-letter-and-replay loop with no tiers** | Every retry goes through the DLQ, which destroys the DLQ's value as a signal — a busy DLQ should be an incident, not the normal path |
| **One delay topic per distinct delay** | Unbounded topic count, and the delays come from a jittered continuous distribution anyway |
| **Pulsar's native per-message delay** | Would remove this entire mechanism. Rejected with the broker choice — [ADR-006](ADR-006-kafka-as-broker.md) |

## Consequences

### Positive

- **The wait is durable.** A pod restart does not lose a parked retry; the record is in Kafka.
- **No rebalance risk**, which is the whole point.
- The tiers are a visible shock absorber: `notification.retry.5s` has 24 partitions, sized for a
  **total provider outage** at 9,861/s, not for the 12.8% steady rate. Sizing them for an average day
  means they are useless on the day they are needed.
- The tier consumer's re-check of eligibility means a cancelled or expired message dies in the tier
  rather than being sent 40 minutes late — *a 40-minute-old OTP is worse than no OTP.*
- Retry traffic is visible per tier: a growing 1h tier is a very different signal from a growing 5s
  tier.

### Negative

- **Five extra topics** and one more consumer group, on top of the six dispatch topics.
- **The delay is quantised.** A computed backoff of 17 s waits 30 s. Full jitter already makes
  individual latency noisy, so this compounds — the platform optimises fleet recovery over
  single-message latency, and this is part of that trade.
- **A message can hop four times**, so its total in-Kafka path is longer and harder to trace. The
  `traceparent` header carries through, but a trace with five republish hops is not easy to read.
- **Duplicate republishes are possible and deliberately not deduplicated in the tier.** A redelivery
  produces a dispatch record with the same `eventId` and attempt number, which the channel worker's
  attempt-scoped idempotent receiver drops. Adding a Redis round trip to the tier consumer to re-solve
  a problem already solved downstream would cost the one property that makes the tiers safe — that
  they do almost no I/O.
- Pausing and seeking is more code than `Thread.sleep(30_000)`, and it looks like premature
  cleverness until you have seen a rebalance storm.

## Related

- [ADR-006](ADR-006-kafka-as-broker.md) — Kafka's lack of native delay is the constraint here
- [ADR-008](ADR-008-topic-split-by-class.md) — tiers are shared across channels for the opposite
  reason that dispatch topics are split
