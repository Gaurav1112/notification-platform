# Architecture Summary

The two-page version. Full detail in [ARCHITECTURE.md](ARCHITECTURE.md) and the
[design spec](superpowers/specs/2026-08-31-notification-platform-design.md).

---

## In one paragraph

A stateless API accepts requests and commits the notification **and** an outbox row in a single
transaction — so "accepted" is one atomic fact and nothing can be silently lost. Kafka carries
the work; PostgreSQL remains the system of record. Traffic is split into physically separate
topics by channel and priority lane so a 10M-recipient marketing blast can never queue ahead of
a login OTP. Workers resolve preferences, render templates, then dispatch through a decorator
stack — tracing, metrics, circuit breaker, rate limiter, timeout, idempotency — to whichever
provider currently scores best on health, cost and rate budget. Failures are classified before
they are retried: transient ones get exponential backoff with full jitter via tiered delay
topics, permanent ones fail fast, and exhausted ones land in a DLQ with replay tooling. Delivery
receipts arrive asynchronously by webhook and are applied through a monotonic state machine, so
duplicated, delayed and out-of-order events are all safe. Everything scales horizontally on
consumer lag — and the first real ceiling is not ours, it is the providers'.

---

## Requirement traceability

| Requirement | Solution | The non-obvious part |
|---|---|---|
| API receives requests | Validate → idempotency → quota → **single accept transaction** → `202` | Preferences, templating and provider selection are deliberately *excluded* from the sync path — none should be able to fail a send request |
| Immediate or scheduled | Outbox fast-path; or `scheduled_notification` claimed by shard-affine scanners | Deterministic jitter `due_at += hash(id) % 300s` defuses the top-of-hour cliff: 20,833/s → 4,167/s |
| Multiple providers | `NotificationProvider` SPI + registry + scoring strategy | Adding one = **one class + two config rows**. Zero router/retry/worker changes |
| Providers fail | Circuit breaker per `(provider, channel)`, Redis-shared; ordered failover; bulkheads | A **4xx never trips the breaker** — a bad payload is our bug, not the provider's |
| Retry | `FailureType` taxonomy → full jitter → 5 tiered delay topics → DLQ | Never `Thread.sleep()` in a consumer; it holds the partition and breaks `max.poll.interval.ms` |
| Millions/day | Modelled at 5M and 100M/day; 288 partitions derived from consumer rates | Sized to **Burst A = 14,583/s**, not the 1,157/s average. Averages don't size systems |
| Delivery status | 12-state lifecycle; HMAC webhooks; monotonic rank guard | `CANCELLED=28 < QUEUED=30` makes "can't cancel something queued" a *database* invariant |
| Horizontal scale | 3 stateless deployables, KEDA on consumer lag | The documented ladder names what breaks first at each step — and it's provider quotas |

---

## The five decisions that define the system

**1. PostgreSQL is the system of record; Kafka is transport.**
Replaying a topic must never be the only way to know what happened. Every state transition lands
in Postgres, so the DLQ, the audit trail and the status API all read one consistent story.

**2. The transactional outbox makes "accepted" atomic.**
`INSERT notification` + `INSERT outbox_message` + `COMMIT` is the single accept decision. A
best-effort publish after commit keeps latency low; the sweeper is the safety net. Republishing
something already sent is harmless because everything downstream is idempotent.

**3. Traffic classes are physically separate, end to end.**
Kafka partitions are strictly FIFO, so a priority *field* on a shared topic is a lie — the
consumer still reads past 10M bulk records to reach the OTP behind them. Separate topics,
separate consumer groups, separate scaling policies.

**4. Idempotency is five layers, not one.**
HTTP key → Kafka `eventId` → business dedup → attempt registration → **provider-side token**.
Only the last can prevent a genuine duplicate, and only where the vendor supports it. Where none
exists, we degrade to `UNKNOWN` + reconciliation and *measure* the duplicate rate rather than
claiming zero.

**5. The status machine only moves forward.**
One atomic `UPDATE … WHERE status_rank < $new AND NOT terminal`. Zero rows returned is not an
error — it means the event was stale, duplicated or illegal. That single choice makes
at-least-once consumers, freely-retried webhooks and DLQ replay all safe.

---

## Numbers that matter

| | Base (5M/day) | Stretch (100M/day) |
|---|---:|---:|
| Peak notifications/s | 173.6 | 3,472.2 |
| **Burst A** (10M campaign + peak) | — | **14,583/s** ← the design point |
| Kafka ingress (compressed) | 0.12 MB/s | 2.3 MB/s |
| Postgres row-ops/s at peak | 1,481 | 29,611 |
| Kafka partitions | 288 | 288 |
| Est. AWS/month | ~$4,600 | ~$19,700 |
| **Provider fees/month** | ~$123k | **~$2,460,000** |

**AWS is 1.2% of TCO.** A 20% SMS→push down-route saves $474k/month — 15× the entire AWS bill.

### Measured, not estimated

| Thing | Result |
|---|---|
| Due-scan: naive `FOR UPDATE` | 159 tps, 100.5 ms latency |
| Due-scan: `SKIP LOCKED` | 453 tps |
| Due-scan: **shard-affine** | **746 tps, 21.5 ms** |
| Provider success rate — raw scan | 481 MB, 290.7 ms (one day, one partition) |
| Provider success rate — rollup table | **2 buffers, 0.015 ms** (19,000× faster) |
| Monotonic status guard | 8 buffers, **0.041 ms** |
| Partial retry index | **3.6 B/row** vs 40+ for a full index |
| BRIN vs B-tree on append-only time column | **48 kB vs 120 MB** |

---

## Bottleneck ladder

1. **Provider quotas** — ~200–500 msg/s per account, hit at ~17M/day. *The true system cap.*
2. Postgres write path — ~30k ops/s, or any single 10M campaign
3. Dispatch consumer parallelism — 21,600/s
4. Redis single-shard CPU — ~100k cmd/s
5. Pod scale-from-zero — 60–120 s (mitigated by pre-warming on campaign schedule)
6. **MSK brokers** — ~300k msg/s

AWS infrastructure is *sixth*. The instinct to start by tuning Kafka is backwards.

---

## What is deliberately not built

- Multi-region active/active — needs cross-region dedup coordination; warm DR instead
- Exactly-once delivery — the provider HTTP call cannot enlist in a transaction
- Real vendor integrations — mocks that emulate real semantics, so CI is deterministic
- A UI — the API and Grafana dashboards are the interface
- ClickHouse — deferred behind a rollup table that delivers the query win without a 4th datastore
