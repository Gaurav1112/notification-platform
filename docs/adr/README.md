# Architecture Decision Records

Eighteen decisions, each with the alternatives that were rejected and the price of the one that was
chosen.

**Every record has a Consequences section with a *Negative* half, and that half is the point.** A
decision record that only lists benefits is a sales page. The interesting question about any of these
is not "why is it good" but "what did it cost, and when would you reverse it".

Source: [§20 of the design spec](../design/DESIGN-SPEC.md#20-architecture-decision-records).

---

## The index

| # | Decision | Chosen | Rejected | One-line rationale |
|---|---|---|---|---|
| [001](ADR-001-spring-boot-4.md) | Framework | Spring Boot 4.1.1 | 3.5.x, Quarkus, Micronaut | 3.5.x is OSS-EOL since 2026-06-30 |
| [002](ADR-002-outbox-plus-fast-path.md) | Accept semantics | Outbox + post-commit fast path | Dual write, Kafka-first, XA | Dual write has no atomicity; Kafka-first breaks read-your-writes |
| [003](ADR-003-fanout-in-scheduler.md) | Fan-out placement | Scheduler tier, own bulkhead | A 4th deployable | A bulkhead achieves the isolation without a service |
| [004](ADR-004-delivery-attempt-in-postgres.md) | `delivery_attempt` | Postgres, 30-day tail | ClickHouse day one | 48% of write ops at 100M/day → exit at ~50M/day |
| [005](ADR-005-mock-providers.md) | Providers | Mock only, seeded injection | Real SDKs, sandboxes, WireMock | Zero-credential clone-and-run; deterministic CI |
| [006](ADR-006-kafka-as-broker.md) | Broker | Kafka 4.3.1 (KRaft) | RabbitMQ, SQS/SNS, Pulsar, Redis Streams | Replay, keyed ordering, group rescale, compaction |
| [007](ADR-007-at-least-once.md) | Delivery semantics | At-least-once + idempotent dispatch | Exactly-once | The provider call cannot enlist in a transaction |
| [008](ADR-008-topic-split-by-class.md) | Topic split | channel × {tx, bulk} | Shared topic + priority field | FIFO partitions make a priority field a lie |
| [009](ADR-009-tiered-retry-topics.md) | Retry mechanism | 5 delay topics + partition pause | `Thread.sleep()`, DB polling | Sleeping holds the partition and breaks the poll interval |
| [010](ADR-010-varchar-enums.md) | Enum storage | `varchar` + CHECK, one lookup table | Native `ENUM`, `DOMAIN`, FK lookups | Enum values can never be removed; `DOMAIN` locks all dependents |
| [011](ADR-011-uuidv7-keys.md) | Keys | UUIDv7 public, bigint internal | UUIDv4, ULID, Snowflake | v7 gives locality *and* self-describing partition pruning |
| [012](ADR-012-analytics-deferred.md) | Analytics | Deferred; rollup table | ClickHouse day one | ~19,000× win without a 4th datastore |
| [013](ADR-013-shard-affine-due-scan.md) | Due-scan | Shard-affine + `SKIP LOCKED`, leader-elected hydrator | Naive `FOR UPDATE`, Quartz | **Measured 746 vs 159 tps** |
| [014](ADR-014-no-foreign-keys.md) | FKs on hot tables | None | Full referential integrity | An FK turns O(1) `DETACH` into a validation scan |
| [015](ADR-015-valkey-over-redis.md) | Cache | Valkey 9 (BSD-3) | Redis 8 (AGPL/RSAL/SSPL), KeyDB, Dragonfly | Cleaner licence; protocol-compatible; reversible |
| [016](ADR-016-single-region.md) | Region topology | Single + warm DR | Active/active | Cross-region dedup outweighs the availability benefit |
| [017](ADR-017-crypto-shredding.md) | GDPR erasure | Crypto-shred the per-user DEK | Row rewrite, tokenisation | 1.8B dead tuples avoided; one KMS call covers every store |
| [018](ADR-018-webhooks-over-polling.md) | Delivery callbacks | Push webhooks + reconciler | Polling status APIs | Polling 100M messages is untenable — and impossible on Twilio, FCM and APNs |

---

## If you only read four

| Read | Because |
|---|---|
| **[007](ADR-007-at-least-once.md)** — at-least-once, not exactly-once | It contains the verified per-provider idempotency table, and it is the decision the rest of the correctness design hangs from |
| **[008](ADR-008-topic-split-by-class.md)** — topic split by class | The clearest single argument in the set: FIFO partitions make a priority field meaningless, and the fix is physical, not logical |
| **[013](ADR-013-shard-affine-due-scan.md)** — shard-affine due-scan | The only decision here backed by a measurement, and the only one where the naive alternative fails **silently** |
| **[017](ADR-017-crypto-shredding.md)** — crypto-shredding | The `suppression_entry` caveat is the best example of a naive reading of a requirement producing the worse outcome |

---

## Decisions grouped by theme

### Correctness under failure
[002](ADR-002-outbox-plus-fast-path.md) outbox ·
[007](ADR-007-at-least-once.md) at-least-once ·
[009](ADR-009-tiered-retry-topics.md) retry tiers ·
[018](ADR-018-webhooks-over-polling.md) webhooks + monotonic guard

### Throughput and isolation
[006](ADR-006-kafka-as-broker.md) Kafka ·
[008](ADR-008-topic-split-by-class.md) topic split ·
[003](ADR-003-fanout-in-scheduler.md) fan-out placement ·
[013](ADR-013-shard-affine-due-scan.md) due-scan

### Data at scale
[004](ADR-004-delivery-attempt-in-postgres.md) attempt storage ·
[010](ADR-010-varchar-enums.md) enum storage ·
[011](ADR-011-uuidv7-keys.md) keys ·
[012](ADR-012-analytics-deferred.md) analytics ·
[014](ADR-014-no-foreign-keys.md) no FKs

### Platform and posture
[001](ADR-001-spring-boot-4.md) framework ·
[005](ADR-005-mock-providers.md) mock providers ·
[015](ADR-015-valkey-over-redis.md) Valkey ·
[016](ADR-016-single-region.md) single region ·
[017](ADR-017-crypto-shredding.md) crypto-shredding

---

## Decisions with an explicit exit trigger

Three records defer something rather than reject it, and each names the condition that reopens it.
A deferred decision that nobody schedules becomes a permanent one, so the triggers are written down.

| ADR | Deferred | Trigger |
|---|---|---|
| [004](ADR-004-delivery-attempt-in-postgres.md) | Columnar store for `delivery_attempt` | ~50M notifications/day, or the write path becomes the binding constraint |
| [012](ADR-012-analytics-deferred.md) | Analytics store | Same trigger, or ad-hoc analysis becomes a recurring need |
| [016](ADR-016-single-region.md) | Active/active | A contractual RTO under 30 minutes |

Both [004](ADR-004-delivery-attempt-in-postgres.md) and [012](ADR-012-analytics-deferred.md) also say
**do this before sharding the primary** — moving two append-only tables is cheaper, simpler, and buys
more than sharding, which is a one-way door.

---

## Format

Each record follows the same shape:

```
Title
Status: Accepted
Date: 2026-08-31

Context      — the forces, with numbers where they exist
Decision     — what was chosen, stated plainly
Alternatives — what was rejected, and specifically why
Consequences — Positive and Negative. The negative half is not optional
```

All eighteen are **Accepted** and dated 2026-08-31, the date of the design. None has been superseded.
When one is, the record stays and gains a `Superseded by ADR-0NN` line — an ADR set that deletes its
history is a changelog, not a decision record.

---

## A caveat on what these describe

The ADRs record the *design* decisions. Several of them describe behaviour that is designed and not
yet implemented — crypto-shredding, single-region DR, the reconciler, tenant scoping.

[STATUS.md](../STATUS.md) is the authoritative account of what exists in code. Where an ADR describes
something unbuilt, it says so in its Consequences section.
