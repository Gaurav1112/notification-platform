# Failure Modes

Every dependency that can fail, what the platform does when it does, and how it recovers.

Source: [§9 of the design spec](design/DESIGN-SPEC.md#9-failure-model).
Where the behaviour described here is **designed but not built**, it says so inline. Operational
procedures live in [RUNBOOK.md](RUNBOOK.md); this page is the model behind them.

---

## The one-page table

| Dependency down | Degraded behaviour | Recovery |
|---|---|---|
| **Kafka** | Accept transaction still commits (notification + outbox). Fast path fails silently; sweeper retries. **No loss, delivery delays.** Outbox > 500k → shed `BULK` at the API with `503`; `CRITICAL` keeps flowing | Sweeper drains ~10k/s; alert on outbox age > 60 s |
| **Postgres primary** | Multi-AZ failover 60–120 s. `503 + Retry-After` for writes; reads from replica. Optional emergency mode: `CRITICAL` straight to Kafka with `degraded=true`, reconciled later (off by default) | Automatic |
| **Redis / Valkey** | **Fail-open on quota** — never reject paying customers over a cache. Idempotency falls back to Postgres. Circuit breakers fall back to per-pod local state. Scheduler locks → Postgres advisory locks | Automatic; `redis_fallback_active` gauge |
| **One provider** | Circuit opens, traffic shifts to the secondary. All open for a channel → park on a retry tier + page | Half-open probes every 30 s (jittered per pod) |
| **Network timeout** | Attempt → `UNKNOWN`, reconcile. **Never blind-retry** | Webhook or reconciler |
| **Worker crash** | Uncommitted offsets redeliver; `PENDING` attempts reconciled | < 30 s |
| **Consumer rebalance** | KIP-848 broker-side coordinator (or `CooperativeStickyAssignor` on the classic protocol). Offsets committed after the DB commit | Seconds |
| **Poison message** | 3 in-place attempts → DLQ with context → **offset committed**. Schema-invalid → separate invalid-message channel | Operator replay API |
| **One AZ** | Multi-AZ everywhere; `min.insync.replicas=2` survives it | Automatic |
| **Region** | Warm standby, replica promotion, MSK Replicator, IaC EKS. **RTO 30 min, RPO < 5 min** | Runbook + quarterly game day |

---

## 1. Kafka unavailable

### What happens

The accept transaction is unaffected. `AcceptNotificationUseCase` commits the `notification_request`
row, the per-channel `notification` rows and the `outbox_message` row in **one database transaction**,
and only then attempts to publish:

```java
try {
    eventPublisher.publishRequested(notice);
} catch (RuntimeException e) {
    // The row and its outbox entry are already committed.
    log.warn("fast-path publish failed for request={}, outbox sweeper will recover", …);
}
```

The client still gets a `202`, and that `202` is still honest — the request is durable and will be
delivered. What is lost is **latency, not messages.**

Producer-side, `delivery.timeout.ms=120000` and `retries=MAX_VALUE` mean the publish itself keeps
trying for two minutes before the exception is raised, so a broker blip shorter than that never
reaches the catch block at all.

### Back-pressure

The outbox is the pressure gauge. As it grows:

| Outbox depth | Action |
|---|---|
| Normal | Sweeper finds an empty table almost every tick |
| Rising | `outbox_oldest_age_seconds` climbs; ticket at 60 s, page at 120 s |
| **> 500k** | Shed `BULK` at the API with `503 + Retry-After`. `TRANSACTIONAL` degrades. `CRITICAL` keeps flowing |

### Recovery

`OutboxSweeper` drains at roughly 10k/s. Published rows are **DELETEd**, not stamped, so the partial
index `WHERE published_at IS NULL` shrinks back to a few pages and the claim query stays
constant-time throughout the drain.

**Duplicates during recovery are expected and harmless.** The fast path and the sweeper will
sometimes both publish the same record. Every consumer is an idempotent receiver keyed on `eventId`,
and every status write goes through the monotonic guard.

### Built?

Yes — `OutboxSweeper`, `OutboxRepository`, the partial index, `OutboxAgeHigh` and
`OutboxDepthShedding` alerts. **Not built:** the `SheddingLevel` controller that would actually
return the `503`. `ProblemType.SERVICE_DEGRADED` exists; nothing sets a shedding level.

---

## 2. PostgreSQL primary unavailable

### What happens

RDS Multi-AZ failover takes 60–120 seconds. During that window:

- **Writes** → `503` with `Retry-After`. The accept path has no meaningful degraded mode: the accept
  transaction *is* the contract, so a `202` we cannot durably record would be a lie.
- **Reads** → served from the replica. Status and attempts queries stay up.
- **Workers** → their `delivery_attempt` INSERT fails before the provider call, so nothing is sent
  that cannot be recorded. Offsets are not committed, so the records redeliver.

### The emergency mode we deliberately default to off

`CRITICAL` traffic straight to Kafka with `degraded=true`, reconciled into PostgreSQL afterwards.

It is off by default because it breaks the one guarantee that makes everything else work: that a
`202` corresponds to a committed row. Turning it on trades the delivery contract for availability,
and that is a decision an operator should make consciously during an incident, not one the system
makes silently.

### Why this is survivable at all

Because there is **no in-between state**. The accept transaction either committed or it did not.
There is no "written to Kafka but not the database" case to reconcile, which is the entire reason
the outbox exists rather than a dual write.

### Built?

Partially. The transaction boundary and the outbox are real. Multi-AZ, the replica read path and the
emergency mode are design only — the local stack is a single PostgreSQL container.

---

## 3. Redis / Valkey unavailable

**The principle: Redis loss degrades quality, never correctness.**

Every use of Redis in this platform has a defined fallback, and every one of them fails *open*.

| Use | Fallback | Cost of the fallback |
|---|---|---|
| **Quota** | Admit the request | A tenant can briefly exceed quota |
| **Idempotency (layer 1)** | PostgreSQL `idempotency_record` | Slower; hour-partitioned so expiry is still a `DROP TABLE` |
| **Consumer dedup (layer 2)** | Process anyway | More duplicate work; layers 4 and 5 still hold |
| **Circuit breaker state** | Per-pod local state | Each pod rediscovers an outage independently |
| **Scheduler leader lock** | PostgreSQL advisory lock | Slower election |
| **Provider rate limiting** | Per-pod approximation | Global budget becomes approximate |

The quota fail-open is explicit in the code:

```java
try {
    admitted = quotaGuard.tryConsume(tenantId, permits);
} catch (RuntimeException e) {
    // Fail-open: an unreachable limiter must not read as "everyone is over quota".
    log.warn("quota check unavailable, admitting tenant={} permits={}", tenantId, permits, e);
    return;
}
```

So is the dedup fail-open, and it names the layers still protecting it:

> *Failing closed would convert a cache outage into a total delivery outage, which is strictly worse
> than a small window of duplicates — and we are not undefended in that window. Layer 4 is a
> `UNIQUE (recipient_id, attempt_number)` constraint written before the provider call, and layer 5
> is the provider idempotency token.*

`IdempotentConsumer.isFullyProtected()` reports the degraded mode so it is **visible rather than
assumed**, and the `redis_fallback_active` gauge drives a ticket-level alert.

### Built?

The fail-open behaviour is built for quota and consumer dedup. The PostgreSQL fallbacks for
idempotency and scheduler locks are design only — `InMemoryDeduplicationStore` and
`InMemorySentTokenLog` are the current single-JVM implementations, and both document the gap.

---

## 4. One provider fails

This is the failure the platform is most thoroughly built for, and the one the `make demo` target
exercises end to end.

### Sequence

```
t+0s    mock-sms-primary → HARD_DOWN
t+2s    ≥20 calls, >50% failing in the 60s window → circuit OPEN
t+2s    ChannelProviderRouter drops it from the candidate list
        → HealthWeightedSelectionStrategy selects mock-sms-secondary
t+30s   automaticTransitionFromOpenToHalfOpen fires, jittered per pod to 30–60s
t+…     3 probes per JVM; success → CLOSED, traffic returns to the cheaper primary
```

**The interesting part is not that the circuit opened.** It is that the accept path never returned a
single error while it happened, because accept and dispatch are decoupled by the outbox and Kafka.

### The four things that make this work

1. **An open circuit removes a candidate; it does not fail the send.** With two providers on a
   channel, an open primary means traffic on the secondary, not an error.
2. **A 4xx never trips the breaker.** `FailureClassifier.shouldRecordAsCircuitFailure` excludes
   every message-scoped failure. One bad campaign with malformed numbers must not take a healthy
   provider offline for every other tenant.
3. **`AUTH_FAILURE` fails over even though it is not retryable.** `RetryRouter` checks failover
   *above* the retryable test, because revoked credentials are a property of our account with one
   vendor, not of the message.
4. **The backup is kept warm.** The router sends the runner-up a capped minority share (25%) so its
   connection pool, DNS, TLS sessions and — crucially — its `successRate5m` statistic are all live
   at the moment failover happens.

### All providers open for a channel

`ChannelProviderRouter` returns empty, which is a **normal return**, not an exception. The message is
parked on a retry tier — the spec calls this lane `retry.1m`; the implemented ladder is
`5s / 30s / 2m / 10m / 1h`, so it lands on `notification.retry.2m` — and `AllProvidersOpenForChannel`
pages.

Push genuinely has one route to a device, so "nothing eligible" is a Tuesday, not a bug.

### Built?

The breaker, the classifier, the router, the strategy, the registry and the mock chaos endpoint are
all built and tested. **The `CircuitBreakerProvider` decorator is a pass-through** — failover works
via the router's candidate filter, not via the decorator. See [STATUS.md](STATUS.md).

---

## 5. Network timeout — the `UNKNOWN` case

The most important failure in the system, and the one most designs get wrong.

```java
} catch (TimeoutException e) {
    inFlight.cancel(true);
    return new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT, …);
}
```

`Indeterminate` is a distinct case in the sealed `SendResult`, not a flavour of failure. It means the
request was on the wire and the provider may well have delivered it.

### Why blind retry is wrong, per channel

The delivery semantic **differs per channel by cost asymmetry**, and the design states which one it
has chosen rather than claiming a uniform guarantee:

| Channel | Duplicate cost | Semantic on `UNKNOWN` |
|---|---|---|
| **SMS** | Real money, user trust, carrier spam flags — and **no way to reconcile on Twilio** | **At-most-once.** Do not resend. Accept a small, measured loss: a lost OTP is recoverable by the user retrying; a duplicate OTP is not recoverable at all |
| **Email** | Negligible; exact reconciliation via `custom_args` / `EmailTags` | At-least-once + reconcile |
| **Push** | ~zero | At-least-once, with `apns-collapse-id` / `collapse_key` set to our dedup key so a duplicate **replaces** rather than stacks |

`RetryRouter` checks `Indeterminate` **first**, before anything else, because an unknown outcome is
not a failure and must not be fed into a failure ladder.

### What makes it recoverable

Committing the attempt row as `PENDING` **before** the network call:

```
t0  INSERT delivery_attempt(state=PENDING, idempotency_token=T)  ← COMMITTED
t1  provider.send(payload, T)
t2  pod OOM-killed
t3  redelivery → attempt found PENDING, response_at NULL, age > timeout
    → UNKNOWN, do NOT blind-retry → reconcile
```

Write the row afterwards and the crash is indistinguishable from never having sent, which forces a
guess: retry and duplicate, or drop and lose.

### Built?

The `Indeterminate` case, the `TimeoutProvider`, the `PENDING`-before-call ordering, the
`SILENT_SUCCESS` mock profile that produces the gap, and `SentTokenLog.startedAt()` (which exists so
a reconciler can bound its status query) are all built. **The reconciler itself is not built.**

---

## 6. Worker crash

Uncommitted offsets redeliver. That is the whole recovery mechanism, and it works because of two
deliberate choices:

**`enable.auto.commit=false`.** Auto-commit advances the offset on a timer that knows nothing about
whether the database write succeeded. A pod evicted at the wrong moment is a notification Kafka
believes was handled and PostgreSQL never saw — a loss with no error anywhere.

**`AckMode.MANUAL_IMMEDIATE`, not `MANUAL`.** A batched acknowledgement lost to a pod eviction
redelivers the whole poll — 100 duplicate provider calls instead of one.

Redelivery is absorbed by the idempotent receiver (layer 2), the `UNIQUE (recipient_id,
attempt_number)` constraint (layer 4), and the monotonic status guard.

`PENDING` attempts left behind are the reconciler's input — see §5.

Recovery time: **< 30 s**, bounded by `session.timeout.ms` and the rebalance.

---

## 7. Consumer rebalance

A rebalance is not a failure — it happens on every deploy — but a *rebalance storm* is, and the
platform is shaped to avoid causing one.

### The storm mechanism

```
provider degrades to 600 ms per call
→ 500 records × 600 ms = 5 minutes for one poll
→ max.poll.interval.ms (300 s) exceeded
→ consumer evicted, group rebalances
→ backlog grew during the rebalance
→ every consumer pulls a full batch against a larger backlog
→ the hanging record is reassigned
→ repeat
```

Throughput reaches zero while the Kafka cluster reports perfect health. **KIP-62 moved heartbeats to
a background thread, so the consumer keeps heartbeating and looks alive the entire time**, which is
why this gets debugged as a broker problem for hours.

### The four defences

| Defence | Where |
|---|---|
| `max.poll.records=100`, not 500 | `KafkaConsumerConfig` |
| A hard ceiling on the provider call | `TimeoutProvider` |
| Delay **topics**, never `Thread.sleep()` | `RetryTier`, `RetryTierListener` |
| Partition **pause** in the tier listener, not sleep | `RetryTierListener` |

**Raising `max.poll.interval.ms` is not the fix.** It costs failover time, and worst-case rebalance
detection then takes up to twice the interval.

### Rebalance protocol

KIP-848 (`group.protocol=consumer`, GA in Kafka 4.0) by default: assignment moves to the broker-side
coordinator, which removes the group-wide synchronisation barrier — the thing that makes a rebalance
stop *every* consumer rather than just the ones losing partitions.

Set `notification.kafka.consumer.group-protocol=classic` to fall back, and the config installs
`CooperativeStickyAssignor` instead. The two are mutually exclusive: the new protocol rejects
`partition.assignment.strategy` outright.

`KafkaRebalanceStorm` is a dedicated alert, separate from lag.

---

## 8. Poison message

Three in-place attempts, then the DLQ **with context**, then the offset is committed. Committing the
offset is the important half — a poison message that keeps its offset uncommitted stops the partition
forever.

Two distinct classes, handled differently:

| Class | Handling |
|---|---|
| **Business-poison** (deterministically fails processing) | 3 attempts → `notification.dlq` with the failure context |
| **Schema-invalid** (unparseable) | `ErrorHandlingDeserializer` turns it into a record the error handler can dead-letter, rather than an exception inside the poll loop |

Without `ErrorHandlingDeserializer`, a single unparseable record throws inside `poll()`, the container
seeks back to it, and **the partition stops forever with no consumer error rate to alarm on.**

### Replay targets the retry tier, never the source topic

```java
/** Replay targets {@link #RETRY_5S}, never the source topic: naive bulk replay of a
 *  deterministic poison message recreates the identical storm that produced it. */
public static final String DLQ = "notification.dlq";
```

### Detection

`KafkaPoisonPillSuspected` is a **conjunction**: lag rising **and** commit rate flat. Lag alone is
equally consistent with a poison pill, a stuck poll, a provider outage and a healthy catch-up after a
deploy — four different responses, so a single `lag > N` alert trains the on-call to ignore it.

### Built?

The DLQ topic, the routing to it, `ErrorHandlingDeserializer`, and the alerts are built. **The
operator replay API is not.**

---

## 9. One availability zone

Multi-AZ everywhere. `min.insync.replicas=2` with `acks=all` survives the loss of one broker without
losing an acknowledged record.

`notification.dispatch.sms.tx` runs `minISR=3` — the only topic that does. An OTP is worth more than
the availability that third replica costs, and the cost is explicit: with minISR=3 and RF=3, losing
one broker makes that topic unavailable for writes rather than merely less durable.

That is a deliberate trade and it should be said out loud, because it is the opposite of the default
advice.

---

## 10. Region loss

Warm standby: replica promotion, MSK Replicator, EKS from IaC.

**RTO 30 min, RPO < 5 min.**

Active/active was considered and rejected — [ADR-016](adr/ADR-016-single-region.md). The blocker is
cross-region deduplication: two regions accepting the same idempotency key cannot agree without a
synchronous round trip, and a synchronous cross-region round trip on the accept path costs more than
the availability it buys.

Design only. There is no DR tooling in this repository.

---

## The load-shedding ladder

A **published design decision, not emergent behaviour**:

```
1. BULK ingest         → 503 + Retry-After
2. BULK dispatch       → consumers paused, lag builds
3. Non-critical status → buffered in Kafka, PG writes deferred
4. TRANSACTIONAL       → degraded latency, never dropped
5. CRITICAL            → protected to the last drop of capacity
```

The ordering is already encoded in the domain as `TrafficClass.shedOrder()` — BULK is 2, CRITICAL is
0 — so the ladder is a rule expressed as data rather than a runbook step someone has to remember.

Driven by a Redis-backed `SheddingLevel` that every component reads, set by a controller watching
consumer lag, outbox depth and DB connection saturation.

**Built?** `TrafficClass.shedOrder()` and `ProblemType.SERVICE_DEGRADED` exist. The `SheddingLevel`
controller does not.

---

## Accepted limits, stated rather than hidden

These are the things this platform does **not** guarantee. Naming them is part of the design.

- **Duplicate delivery < 0.01%**, concentrated in the provider-timeout case. Measured and published,
  not eliminated. Exactly-once delivery to a third party over a network is not achievable — the
  provider call cannot enlist in a transaction ([ADR-007](adr/ADR-007-at-least-once.md)).
- **A region loss costs up to 5 minutes** of accepted-but-unreplicated notifications.
- **Redis loss degrades quality, never correctness** — more duplicate work, weaker frequency capping.
- **A single tenant can still saturate one provider *account*.** Mitigated by per-tenant credentials
  for tier-1 tenants, which is design only.
- **Ordering is per `(recipient, channel)` only.** Global ordering is not offered and cannot be,
  given partitioned topics.

---

## The failure mode this document is itself about

**Metastable failure**: the system's own recovery mechanism sustains the outage. Three instances of
it are designed against explicitly, and each has its own defence:

| Metastable loop | Defence |
|---|---|
| Synchronised retry storm | `BackoffStrategy.FULL` — `random(0, backoff)`, not `backoff ± noise` |
| Retry amplification (3⁵ = 243) | `RetryBudget` — retries capped at 10% of successes |
| Half-open probe stampede | Per-JVM jittered `waitDurationInOpenState`; 40 pods × 3 probes would otherwise be 120 synchronised probes |

There is a `MetastableFailureSuspected` alert in the rule file for the general case: sustained high
retry rate combined with a falling success rate is the signature.
