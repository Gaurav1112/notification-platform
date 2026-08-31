# ADR-011: UUIDv7 public identifiers, bigint internal keys

**Status:** Accepted
**Date:** 2026-08-31

## Context

Every notification, request, recipient, attempt and event needs an identifier. The identifier appears
in three places with different requirements:

| Place | Needs |
|---|---|
| API responses and URLs | Not guessable, not enumerable, stable across systems |
| Primary keys and indexes | Insert locality, small, fast to compare |
| Partitioned tables | To coexist with a partition key that must be in every unique constraint |

## Decision

**UUIDv7 for public identifiers. `bigint` identity for internal-only surrogate keys.**

On the partitioned tables the primary key is `(created_at, id)` with a separate `UNIQUE (id, created_at)`
to serve lookup-by-id.

## Why UUIDv7 rather than v4

UUIDv7 puts a **millisecond Unix timestamp in the high 48 bits**, followed by randomness.

### Insert locality

A v4 UUID is uniformly random, so consecutive inserts land on random leaf pages of the B-tree. At
scale this means:

- The index working set is the whole index, not the recent tail.
- Every insert dirties a different page, so WAL volume and checkpoint I/O go up.
- Page splits happen everywhere rather than at the right edge.

At 100M notifications/day into a 1.8-billion-row table, that difference is the gap between an index
that fits in the buffer cache's hot set and one that does not.

UUIDv7 inserts are monotonic-ish, so they append at the right edge like a sequence, while remaining
unguessable in the low bits.

### It is self-describing for pruning

This is the property that earns it over ULID.

The tables are RANGE-partitioned by day on `created_at`. A query with only an id has to scan every
partition — unless the id itself tells you roughly when the row was created. With v7, the timestamp
prefix bounds the partition set:

```java
// app-worker/.../support/PartitionWindow.java exists for exactly this
```

That turns a 90-partition scan into a 1–2 partition scan **from the id alone**, which is what makes
`GET /v1/notifications/{id}` cheap without the caller supplying a date.

### It is not enumerable

Unlike a `bigint` in a URL, a v7 id does not tell a caller how many notifications the platform has
sent, and it cannot be incremented to find someone else's. The timestamp prefix does leak *when* the
row was created, which is discussed under Consequences.

## Why `bigint` internally

Where an identifier never leaves the database — join keys on internal tables, the `delivery_status`
rank, sequence-backed surrogates — a `bigint` identity is 8 bytes against 16, compares in one
instruction, and has perfect insert locality.

The rule is simple: **if it appears in an API response, it is a UUIDv7. If it does not, it is a
bigint.**

## Alternatives considered

| Alternative | Why not |
|---|---|
| **UUIDv4 everywhere** | Random insert distribution destroys B-tree locality at this volume, and carries no timestamp, so partition pruning by id is impossible |
| **ULID** | Almost the same properties — 48-bit timestamp, monotonic, unguessable. Rejected because it is **not a UUID**: PostgreSQL has no native type, PgJDBC has no binding, and it becomes a `varchar(26)` or a `bytea`. UUIDv7 gets `uuid` storage, `uuid` indexes and native driver support for the same benefit. ULID's Crockford base32 is nicer to read; that is not worth a type mismatch |
| **`bigserial` in public URLs** | Enumerable — a competitor can read your daily volume off the id — and it makes ids collide across environments during a data migration |
| **Snowflake IDs** | 64-bit, time-ordered, and they need a coordinated worker-id allocation across every pod. That is a distributed-systems problem introduced to solve an identifier problem |
| **Composite natural keys** | `(tenant_id, idempotency_key)` is meaningful but variable-length, caller-controlled and unsuitable as a join key |

## The partitioning tax

Partitioning imposes a rule that shapes the key design: **every unique constraint must include the
partition key.** So:

```sql
PRIMARY KEY (created_at, id),
UNIQUE (id, created_at)          -- serves lookup-by-id
```

**Consequence worth stating out loud: the database cannot enforce global uniqueness of `id`.** Two rows
with the same `id` in different partitions are legal as far as PostgreSQL is concerned. What prevents
it is that UUIDv7 collision probability is negligible and ids are generated in exactly one place.

That is a real guarantee downgrade, from "the database will not let this happen" to "this will not
happen". It is the price of partitioning, and it applies to any key scheme, not just this one.

## Consequences

### Positive

- **Insert locality without a sequence.** Append-at-the-right-edge behaviour with no central
  allocator and no coordination between pods.
- **Partition pruning from the id alone** — the property that makes lookup-by-id affordable across 90
  daily partitions.
- Native `uuid` storage and indexing; no varchar keys, no application-side encoding.
- Ids are unique across environments, so a production id pasted into a staging query returns nothing
  rather than the wrong row.
- Not enumerable, so a public id leaks no volume information.

### Negative

- **16 bytes against 8.** On 1.8 billion rows, in a primary key that appears in every secondary index,
  that is a meaningful amount of storage and buffer cache.
- **The creation timestamp is public.** A UUIDv7 in an API response tells the holder, to the
  millisecond, when the row was created. For notifications this is benign — the caller knows when they
  sent it — but it is a genuine information leak and would not be acceptable for every entity type.
- **The database cannot enforce global uniqueness**, per the partitioning tax above.
- **Java 17 has no built-in UUIDv7 generator.** `UUID.randomUUID()` is v4. Generation is
  library-provided or hand-rolled, and a hand-rolled generator with a weak monotonic counter reduces to
  v4's insert behaviour without anyone noticing.
- Two id schemes means a reviewer has to know which is which, and "why is this one a bigint" is a
  recurring question.

## Related

- [ADR-014](ADR-014-no-foreign-keys.md) — the other constraint partitioning imposes
- [ADR-010](ADR-010-varchar-enums.md) — the same "what can the database actually enforce" theme
