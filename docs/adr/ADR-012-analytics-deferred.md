# ADR-012: Defer the analytics store; use a rollup table

**Status:** Accepted
**Date:** 2026-08-31

## Context

The platform generates analytical questions naturally: success rate by provider by hour, cost per
thousand by channel, delivery latency percentiles by tenant, bounce rate trends. The data lives in
`delivery_attempt` and `notification_event`, which together are roughly 48% of the write volume and,
at the stretch operating point, the largest tables in the database.

Those tables are append-only, high-volume, rarely updated and almost always queried in aggregate.
That is a textbook description of a columnar workload, and the textbook answer is ClickHouse.

## Decision

**No analytics store. A pre-aggregated rollup table in PostgreSQL, refreshed on a schedule.**

The rollup carries the dimensions the dashboards actually need — `(hour, tenant, channel, provider,
status)` with counts, cost sums and latency histogram buckets — and every executive and provider
dashboard reads from it rather than from the raw tables.

## The arithmetic

A dashboard panel asking "success rate by provider over the last 24 hours" against raw
`delivery_attempt`:

```
~1.128 attempts/notification × 100M/day = 113M rows scanned per panel refresh
```

Against the rollup:

```
24 hours × 5 providers × 3 channels × ~16 statuses ≈ 5,760 rows
```

**Roughly a 19,000× reduction**, on the same datastore, with no new component.

That number is the whole argument. A columnar store would make the raw scan fast; the rollup makes it
unnecessary.

## Alternatives considered

### ClickHouse from day one

The right long-term answer, rejected as premature.

| Cost | Detail |
|---|---|
| **A fourth datastore** | PostgreSQL, Kafka, Valkey, ClickHouse — each with backup, failover, upgrade and on-call |
| **A pipeline to keep it fed** | Debezium or a Kafka sink, with its own lag, its own schema evolution and its own failure mode |
| **Two sources of truth for the same number** | The rollup and ClickHouse will disagree during a lag window, and someone will ask which is right |
| **Local development** | A clone-and-run project cannot reasonably require a ClickHouse container as well |

For a **19,000× win available without any of that**, day-one ClickHouse is buying capability the
platform does not need yet at a fixed operational cost it pays every day.

### Materialised views

Closer to free than a rollup table, and PostgreSQL's `REFRESH MATERIALIZED VIEW CONCURRENTLY` still
rescans the base table. On a 113M-row daily window that is the expensive query, just moved to a
schedule.

An incrementally-maintained rollup — insert the last hour's aggregate rather than recomputing
everything — does the same job for a fraction of the work.

### Query the raw tables directly with good indexes

Tried in the design and rejected. The indexes needed to make arbitrary aggregate queries fast on
`delivery_attempt` are the indexes that make its **insert** path slow, and inserts are the hot path.
This is the same reasoning that keeps `notification.status` unindexed.

### Push everything to S3 + Athena

Cheap storage, good for the 7-year archive (and it *is* the archive strategy). Query latency in the
seconds-to-minutes range makes it unusable for a live dashboard.

### Prometheus for everything

Prometheus already holds the operational metrics and the SLI recording rules. It is the right tool for
"what is happening now" and the wrong one for "what did tenant 42 spend last month" — cardinality
explodes on tenant, and retention is short.

## Consequences

### Positive

- **Three datastores, not four.** Every operational cost of the fourth is avoided.
- One source of truth. The rollup is derived from the same tables the API reads, in the same
  transaction-consistent database.
- The rollup schema is small and explicit, so a new dashboard question is a visible schema change
  rather than an ad-hoc scan that quietly costs 113M rows.
- The exit path is unobstructed: both source tables are append-only, so a Debezium tap or a nightly
  Parquet export is straightforward whenever the trigger fires.

### Negative

- **The rollup's dimensions are fixed at design time.** A question the rollup does not answer either
  runs against the raw tables — slowly, on the OLTP primary — or waits for a schema change and a
  backfill. That is a real loss of flexibility, and it is the main thing a columnar store would give.
- **No ad-hoc exploration.** "Why did delivery to this carrier degrade on Tuesday afternoon" is
  exactly the question a columnar store is good at and this is not.
- **Aggregation is a scheduled job that can fail.** A missed refresh means a stale dashboard, and a
  stale dashboard that looks live is worse than a broken one.
- **Analytical load still lands on the OLTP database**, even reduced. During a campaign burst the
  rollup refresh competes with the write path that is already second on the bottleneck ladder.
- **A deferred decision that nobody schedules becomes a permanent one.** This ADR has a trigger for
  exactly that reason; the trigger needs an owner.

## Exit trigger

Introduce a columnar store when **any** of:

- Sustained volume exceeds ~50M notifications/day (the same trigger as
  [ADR-004](ADR-004-delivery-attempt-in-postgres.md))
- Ad-hoc analytical queries become a recurring operational need rather than an occasional one
- The rollup refresh measurably competes with the write path
- A tenant-facing analytics product is committed to

**Do this before sharding the primary.** Moving two append-only tables is cheaper, simpler and buys
more than sharding a relational primary, which is a one-way door.

## Related

- [ADR-004](ADR-004-delivery-attempt-in-postgres.md) — the same tables, the same exit trigger
- [SCALABILITY.md](../SCALABILITY.md) — where these tables sit on the bottleneck ladder
