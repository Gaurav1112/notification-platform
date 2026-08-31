# ADR-004: `delivery_attempt` stays in PostgreSQL with a 30-day tail

**Status:** Accepted
**Date:** 2026-08-31

## Context

`delivery_attempt` is the ledger: one row per provider call, written **before** the call as `PENDING`
and updated with the outcome. It is what makes a worker crash mid-send recoverable, it is the
evidence behind a billing dispute, and it is the input to the reconciler.

It is also expensive. At the stretch operating point:

```
Row-ops per notification = 8.528
  1 INSERT notification + 0.4 recipient + 1.128 attempt + 3 event + 3 status UPDATE

delivery_attempt is 1.128 of 8.528 write operations directly,
and roughly 48% of total write volume once the status updates it drives are counted.
```

At 100M notifications/day that is a lot of rows in a row-store that is also serving the hot query
path.

The obvious move is a columnar store — ClickHouse — where append-only, high-volume, rarely-updated
data belongs.

## Decision

**Keep `delivery_attempt` in PostgreSQL, RANGE-partitioned by day with a 30-day hot tail, archived to
S3 Parquet for 7 years. Plan to exit to a columnar store at roughly 50M notifications/day.**

The exit is planned, dated and has a trigger. It is not deferred indefinitely.

## Alternatives considered

### ClickHouse from day one

Correct for the data shape and wrong for the project shape.

| Cost | Detail |
|---|---|
| A fourth datastore | PostgreSQL, Kafka, Valkey, and now ClickHouse — each with its own failover, backup, upgrade and on-call story |
| The write is no longer transactional | The `PENDING` row must commit **in the same transaction** as nothing at all, but its *visibility* to a redelivered worker is what makes crash recovery work. A ClickHouse insert is asynchronous and eventually visible, which breaks the one property the row exists for |
| Local development | A clone-and-run project cannot reasonably require a ClickHouse container as well |
| Joins | The reconciler and the attempts API join attempts to notifications. Cross-store joins are application-level |

That second row is the decisive one. The whole point of `delivery_attempt` is the sequence:

```
t0  INSERT delivery_attempt(state=PENDING) ← COMMITTED, and visible to a redelivery
t1  provider.send(...)
t2  pod OOM-killed
t3  redelivery sees PENDING with a null response → UNKNOWN, reconcile, do not blind-retry
```

If step 0's visibility is eventual, step 3 can read nothing and blind-retry — which is the duplicate
OTP the design exists to prevent.

### Kafka-only, no attempt table

The `notification.delivery` topic already carries the outcomes. But a topic is not queryable by
`(recipient_id, attempt_number)`, and layer 4 of the idempotency stack is exactly that UNIQUE
constraint. Kafka cannot enforce it.

### Keep it in PostgreSQL forever

Fails at scale. 48% of write ops is the single biggest lever on the PostgreSQL write path, and the
bottleneck ladder puts the write path second only to provider quotas.

## Consequences

### Positive

- **One datastore for anything transactional.** The `PENDING`-before-call ordering, the
  `UNIQUE (recipient_id, attempt_number)` constraint and the attempts API all work with no
  cross-store coordination.
- Daily partitions mean retention is `DETACH PARTITION` + archive — O(1), no `DELETE`, no dead
  tuples.
- The exit path is cheap when it comes: the table is append-mostly, so a Debezium tap or a nightly
  Parquet export to a columnar store is straightforward. Nothing about the schema makes it hard.

### Negative

- **It is 48% of the write load, and that is known and accepted.** Every capacity number in
  [SCALABILITY.md](../SCALABILITY.md) carries it.
- 30 days of hot data on `db.r7g.8xlarge` storage is a real cost that a columnar store would make
  much cheaper.
- Any analytical query over attempts — success rate by provider by hour over 30 days — runs against
  the OLTP primary or a replica, and is slow. This is why the design has a **rollup table** rather
  than ad-hoc analytics ([ADR-012](ADR-012-analytics-deferred.md)).
- The exit is future work that has to actually happen. A deferred decision that nobody schedules
  becomes a permanent one.

## Exit trigger

Move `delivery_attempt` and `notification_event` to a columnar store when **any** of:

- Sustained notification volume exceeds ~50M/day
- The write path is the binding constraint on the bottleneck ladder
- Hot-window storage approaches 20 TB

**Do this before sharding the primary** — it is cheaper, simpler, and buys more.

## Related

- [ADR-012](ADR-012-analytics-deferred.md) — the rollup table that defers the analytics need
- [ADR-014](ADR-014-no-foreign-keys.md) — why partition maintenance stays O(1)
