# ADR-014: No foreign keys on the hot partitioned tables

**Status:** Accepted
**Date:** 2026-08-31

## Context

Five tables are RANGE-partitioned by day — `notification`, `notification_recipient`,
`delivery_attempt`, `notification_event`, `notification_request` — and `idempotency_record` by hour.
Together they hold the overwhelming majority of the platform's rows: at the stretch operating point,
197.5 GB/day and 1.8 billion rows in the 90-day hot window.

Retention on those tables is a partition **detach**, which is O(1) and holds a brief lock:

```sql
ALTER TABLE notif.notification DETACH PARTITION notif.notification_p2026_05_01;
```

The natural relational modelling would put foreign keys between them: a recipient references a
notification, an attempt references a recipient, an event references an attempt.

## Decision

**No foreign key constraints on any partitioned hot table.** Referential integrity comes from
same-transaction writes plus a nightly reconciliation job that emits a metric.

Foreign keys **are** used on small, static, non-partitioned reference tables where they cost nothing.

## Why

### An FK turns O(1) `DETACH` into a validation scan

This is the decisive reason.

When a partition is detached from a table that is the *target* of a foreign key, PostgreSQL must
verify that no referencing row is left pointing into it. That is a scan of the referencing table —
which is itself partitioned and enormous.

**Retention depends on `DETACH` being instant.** A design where dropping yesterday's data takes a scan
over a billion rows does not have a retention story; it has a nightly incident.

### An FK is a write-path cost on every insert

Each FK check is an index probe on the parent, inside the inserting transaction, taking a row-level
lock on the referenced row. At the stretch operating point:

```
8.528 row-ops per notification × 3,472 notifications/s at peak ≈ 29,611 row-ops/s
```

Add two or three FK checks per insert and the write path — already **second on the bottleneck ladder**
— gets measurably worse for a guarantee that same-transaction writes already provide.

### An FK to a partitioned parent has awkward semantics anyway

Foreign keys referencing a partitioned table have been supported since PostgreSQL 12, but the parent's
unique constraint must include the partition key. So the FK becomes a composite
`(created_at, id)` reference, and the child has to carry the parent's `created_at`. That is a
denormalisation the design does anyway (for partition pruning), but it means the FK is not the clean
single-column reference it appears to be.

## What replaces it

### 1. Same-transaction writes

The rows that must be consistent are written together:

- `notification_request` + one `notification` per channel + `outbox_message` — one transaction
  (`AcceptanceWriter.persist`)
- `notification` + `notification_recipient` rows — one transaction (`RequestFanOut`)
- `delivery_attempt` — written by the worker that owns the recipient row it references

An orphan requires a transaction to have committed half of itself, which PostgreSQL does not do.

### 2. A nightly reconciliation job that emits a metric

Counts orphans across the join edges and exports the count. A non-zero value is a ticket, and it is
also the honest measurement of what the missing constraint actually costs — rather than an assumption
that it costs nothing.

### 3. Application-level ordering

Writes always go parent-first. There is no code path that inserts a child before its parent.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Full referential integrity with FKs** | Breaks O(1) `DETACH`, which retention depends on. Costs an index probe and a row lock on every insert on the hottest write path in the system |
| **FKs with `NOT VALID`** | Skips the initial validation, but the constraint still applies to new rows, so the per-insert cost remains and `DETACH` still validates |
| **Deferred FKs (`INITIALLY DEFERRED`)** | Moves the check to commit time. Does not help `DETACH` at all, and it makes the failure surface at commit where it is harder to attribute |
| **Triggers instead of FKs** | Strictly worse: the same per-row cost, plus PL/pgSQL execution, plus a hand-written implementation of something the FK machinery does correctly |
| **Don't partition** | Then FKs are fine — and retention becomes `DELETE FROM … WHERE created_at < …`, which at 197.5 GB/day generates dead tuples faster than autovacuum reclaims them. This is the trade being made, and partitioning wins |

## Consequences

### Positive

- **`DETACH PARTITION` stays O(1)**, so retention and GDPR archival are instant catalogue operations.
- The write path carries no FK overhead on 29,611 row-ops/s at peak.
- Partition creation and attachment are similarly unencumbered — `PartitionMaintenanceJob` can create
  tomorrow's partitions without lock contention against the referencing tables.
- Bulk operations that partitioning enables — `COPY` for campaign expansion, in particular — are not
  slowed by per-row constraint checks.

### Negative

- **The database no longer prevents orphans.** A bug in a write path can create a `delivery_attempt`
  referencing a `notification_recipient` that does not exist, and nothing will stop it. The
  reconciliation job will *notice*, hours later.
- **This is a real guarantee downgrade**, and the honest framing is: integrity moved from "the database
  will not let this happen" to "the code does not do this, and a nightly job checks". Those are not
  the same strength.
- **Every new write path is a chance to get it wrong.** An engineer who does not know this rule will
  write a child insert outside its parent's transaction and it will work in testing.
- The reconciliation job is another scheduled job that can fail, and a failed integrity check that
  nobody notices is worse than no check.
- `ON DELETE CASCADE` is not available, so any manual cleanup has to delete in the right order by
  hand.

## Mitigation the design leans on

The absence of FKs makes the **monotonic status guard** more important, not less. It is a single
`UPDATE … WHERE status_rank < $2 AND NOT EXISTS (… is_terminal)` statement, so the invariant that
matters most — that status never goes backwards — is enforced by the database even though referential
integrity is not.

That is the general pattern here: pick the invariants that must be database-enforced, enforce those
absolutely, and be explicit about the ones that are not.

## Related

- [ADR-011](ADR-011-uuidv7-keys.md) — the other constraint partitioning imposes on the key design
- [ADR-004](ADR-004-delivery-attempt-in-postgres.md) — the largest table this applies to
- [ADR-010](ADR-010-varchar-enums.md) — why lookup tables with FKs were not an option for enums
