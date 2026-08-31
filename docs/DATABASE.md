# Database

PostgreSQL 18.6. Every figure below was **measured** on a validation run with 9M synthetic rows,
not estimated. Full DDL lives in `platform-persistence/src/main/resources/db/migration/`.

---

## 1. What belongs where

The membership test is one question: **is there a transaction where this must be atomic with
something else?** Preferences must be atomic with their audit record. Idempotency must be atomic
with request creation. The outbox must be atomic with the notification. Everything that fails the
test is a candidate for eviction.

| Store | Holds | Rule |
|---|---|---|
| **PostgreSQL** | System of record | What a transaction must be atomic with |
| **Kafka** | Event transport | What happened |
| **Valkey** | Cache + coordination | Whatever is too hot to fetch twice. Loss degrades quality, never correctness |
| **S3** | Bodies, manifests, archive | 800 GB/day of rendered content at stretch — never in the heap |
| **ClickHouse** | Deferred (ADR-012) | What it means |

## 2. Enum storage — `varchar` + CHECK

Three native-enum failure modes, reproduced on 18.6:

```
ALTER TYPE chan_e ADD VALUE 'PUSH';   -- then use it in the same transaction
ERROR:  unsafe use of new value "PUSH" of enum type chan_e
HINT:   New enum values must be committed before they can be used.

-- PgJDBC binds String as varchar:
ERROR:  column "c" is of type chan_e but expression is of type character varying
```

And the one that actually decides it: **an enum value can never be removed.** Roll back a bad
deploy and the zombie value lives in `pg_enum` forever, invisible to the next engineer's
exhaustive Java `switch`.

`varchar` + CHECK evolves cleanly: `DROP CONSTRAINT` → `ADD CONSTRAINT … NOT VALID` (catalog-only,
instant) → later `VALIDATE CONSTRAINT` (`SHARE UPDATE EXCLUSIVE`, concurrent DML allowed).

**`DOMAIN` with a shared CHECK was considered and rejected.** `ALTER DOMAIN … ADD CONSTRAINT`
locks *every* dependent table in a single transaction — `ACCESS EXCLUSIVE` on `notification`
(90 partitions) and `delivery_attempt` (30) simultaneously. Per-table CHECK lets you go
table-by-table with `lock_timeout` and retry.

### The one lookup table

`delivery_status` is a table, not a CHECK, because it carries **data the SQL reads**:

```sql
CREATE TABLE delivery_status (
    code        varchar(24) PRIMARY KEY,
    rank        smallint    NOT NULL UNIQUE,
    is_terminal boolean     NOT NULL,
    is_failure  boolean     NOT NULL,
    is_billable boolean     NOT NULL DEFAULT false
);
```

| code | rank | terminal | Why this rank |
|---|---:|---|---|
| `PENDING` | 10 | no | |
| `SCHEDULED` | 20 | no | |
| `SUPPRESSED` | 25 | **yes** | |
| `EXPIRED` | 27 | **yes** | Before `QUEUED` — "TTL elapsed before send" can only *precede* sending |
| `CANCELLED` | 28 | **yes** | **Below `QUEUED`, so the guard itself forbids cancelling something queued** |
| `QUEUED` | 30 | no | |
| `SENDING` | 50 | no | |
| `SEND_FAILED` | 55 | no | Transient; retry scheduled |
| `FAILED` | 58 | **yes** | |
| `SENT` | 60 | no | |
| `DELIVERED` | 80 | **no** | Deliberately non-terminal — a hard bounce legitimately follows an SMTP 250 |
| `BOUNCED` | 85 | **yes** | |

Adding a status is an `INSERT` — zero DDL, zero lock, zero deploy coupling. That is the payoff.

## 3. Keys

| Table | Key | Why |
|---|---|---|
| `tenant`, `user_account` | `bigint` PK + `uuid` v7 `public_id` | PK denormalised onto billions of rows |
| `provider`, `retry_policy` | `smallint` | < 300 rows ever, copied onto 10B attempt rows |
| `notification` | **UUIDv7**, PK `(created_at, id)` | Public ID; known pre-INSERT for the outbox; index locality |
| `delivery_attempt` | `bigint`, PK `(id, attempted_at)` | Never exposed — saves ~160 GB/yr |

**Measured index cost:** bigint+ts **31.5 B/row** · uuid+ts **40.6 B/row** · `(ts, uuid)` PK
**52.5 B/row**. A uuid key costs ~9 B/row/index more; on `notification` with six uuid-bearing
indexes that is **~400 GB/yr of pure index inflation** — hence `bigint` for `user_id`.

**UUIDv7 carries its own partition-pruning predicate.** `uuid_extract_timestamp()` recovers the
creation time, so `WHERE id = $1` becomes `WHERE id = $1 AND created_at BETWEEN …`:

```
Q1  with a created_at bound:     Index Scan, 7 buffers, 0.476 ms
Q1b without a created_at bound:  Append across ALL 90 partitions
```

An ArchUnit test fails the build on any repository method against `Notification` that lacks a
`created_at` bound. There is no way to recover this later.

PostgreSQL 18 has native `uuidv7()`; Spring Boot 4.1.1 ships Hibernate 7 with
`@UuidGenerator(style = VERSION_7)`. **IDs are generated in Java, not by the column DEFAULT** —
the outbox row needs the ID inside the same transaction.

**ULID rejected:** no PG type. `char(26)` is 69% larger keys plus collation issues;
`bytea(16)` loses `uuid` type-checking and `uuid_extract_timestamp()`. If human-readable IDs are
wanted, render the UUIDv7 as Crockford base32 at the API edge — lossless, and free.

## 4. Indexes

| Index | Size | B/row | Serves |
|---|---:|---:|---|
| `n_retry_ix` partial, `WHERE status IN ('SEND_FAILED','QUEUED')` | 7 MB | **3.6** | Retry candidates |
| `n_inflight_ix` partial, `WHERE status IN ('CLAIMED','SENDING')` | **8 kB** | ~0 | Stuck-send detector |
| `n_user_hist_ix` covering, `INCLUDE (id, channel, status, status_at)` | 151 MB | 79 | User history — `Heap Fetches: 0` |
| `n_provider_msg_ix` partial | 89 MB | 47 | Webhook → notification |
| `da_attempted_brin` BRIN, `pages_per_range 64` | **48 kB** | 0.01 | Time-range scans |
| `sn_due_shard_ix` covering, `WHERE state='READY'` | 315 MB | 110 | Due scan — index-only |

Partial indexes are the whole game: `n_retry_ix` covers 5% of rows at **3.6 B/row** instead of
40+. `n_inflight_ix` is 8 kB on 2M rows because a healthy system has ~zero in-flight.

BRIN on an append-only time column is **48 kB vs 120 MB** — 2,500× smaller. Set
`pages_per_range` to 32–64, not the default 128: you want roughly a minute of data per range so a
5-minute dashboard query prunes properly.

### Deliberately not created

- **`notification(status)`** — the killer. `status` is updated 3–5× per row; a B-tree on it makes
  every transition a **non-HOT** update: new heap tuple plus a new tuple in *every* index, all
  WAL-logged. At 80M updates/day that's **~4 GB/day of extra WAL** for a column with 15 distinct
  values, ~90% in one of them. Set `fillfactor=85` on `notification` so HOT can actually apply;
  leave append-only partitions at 100.
- **`n_dedupe_uk`** (189 MB, 99 B/row) → **moved to Valkey** (`SET dedupe:{tenant}:{hash} NX EX 86400`),
  saving ~170 GB over the 90-day window and removing a unique-conflict check from the hot insert.
- **More than three indexes on `delivery_attempt`** — 300–1,000 inserts/sec, never updated;
  each extra B-tree is ~32 GB/month.
- **Low-cardinality columns** — 4–48 distinct values over 20M rows is never selective.

### Extended statistics are mandatory

`channel`, `status` and `tenant_id` are correlated — a `SEND_FAILED` row is disproportionately
SMS. Without `CREATE STATISTICS n_chan_status_stx (dependencies, ndistinct)` the planner
multiplies independent selectivities, underestimates ~10×, and flips a nested loop into a hash
join on a 20M-row partition.

## 5. Partitioning

| Table | Scheme | Granularity | Retention |
|---|---|---|---|
| `notification` | RANGE(`created_at`) | 1 day | 90 d → S3 Parquet |
| `delivery_attempt` | RANGE(`attempted_at`) | 1 day | 30 d |
| `notification_event` | RANGE(`occurred_at`) | 6 hours | **7 d tail only** |
| `scheduled_notification` | RANGE(`due_bucket`) — **immutable** | 1 day | rolling ±90 d |
| `idempotency_record` | RANGE(`created_at`) | **1 hour** | 24–48 h |
| `audit_record` | RANGE(`occurred_at`) | 1 month | 13 mo → S3 WORM |
| `notification_preference` | **HASH(`user_id`) × 32** | — | indefinite |

Hourly `idempotency_record` partitions are the clearest "partition purely for cheap deletion"
case: `DROP TABLE` is O(1) with zero dead tuples, where
`DELETE … WHERE expires_at < now()` would generate 20M dead tuples/day and pin autovacuum.

`notification_preference` uses HASH because there is **no time dimension** — a 2019 user's
preference row is as hot as today's, and access is always a point lookup. Modulus 32 is
effectively immutable (changing it is a full rewrite), chosen with headroom to 1B rows.

**Composite RANGE(day) → HASH(tenant) rejected:** 720 leaf tables. Measured **1,068 shared buffers
and 3.3 ms *planning* time with only 3 partitions**; at 720 the catalog lookups dominate a 0.5 ms
query. Revisit only if one tenant exceeds ~5M/day.

**Lifecycle: pg_partman + pg_cron**, `premake=14`, `retention_keep_table=false`, with a pre-drop
S3 export. A Spring `@Scheduled` job is rejected — it can't run during a deploy, and its failure
mode is "everything silently lands in DEFAULT". A Java **canary** alerts if tomorrow's partition
doesn't exist by 12:00 UTC. (`pg_partman_bgw` isn't usable on RDS; `pg_cron` needs a custom
parameter group and a reboot.)

### Eight gotchas, all reproduced

1. **`CREATE INDEX CONCURRENTLY` on a partitioned parent is still impossible in PG 18.**
   `CREATE INDEX ON ONLY parent` (invalid stub) → `CREATE INDEX CONCURRENTLY` per partition
   (**862 ms measured on 2M rows**) → `ALTER INDEX … ATTACH PARTITION`. Parent stays
   `indisvalid = false` until the last attach. ~4 min for 90 partitions, `SHARE UPDATE EXCLUSIVE`
   throughout. Afterwards, sweep for leftovers:
   `SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid OR NOT indisready;`
2. **Unique constraints must include every partition-key column.** Hence PK `(created_at, id)`
   *and* a second `UNIQUE (id, created_at)`. **Consequence: the DB cannot enforce global
   uniqueness of `id`.** UUIDv7 makes it a non-issue probabilistically — but say so out loud.
3. **Partial unique indexes on partitioned tables *are* allowed.** `ON CONFLICT` must repeat the
   `WHERE` clause or index inference won't match.
4. **Zero FKs on the six hot tables.** An FK turns O(1) `DETACH`/`DROP` into a validation scan —
   the single most important retention operation. Full FKs on every config table. A nightly
   anti-join reconciliation per partition emits a metric. **Document this loudly** or someone will
   "fix" the missing FKs.
5. **Partition-key UPDATE is a silent DELETE+INSERT** at ~3× the WAL cost — and with a partitioned
   table, a concurrent cross-partition row move raises **error 40001**, which `SKIP LOCKED` cannot
   silently ignore. Reschedules `DELETE` + `INSERT` explicitly.
6. **The DEFAULT partition is a trap.** Once a row lands there, creating the covering partition
   *fails* while holding `ACCESS EXCLUSIVE` during the scan. Keep it (a missing partition is
   otherwise an outage) but alarm on `count(*) > 0`.
7. **Storage parameters are illegal on a partitioned parent** — set per leaf via a pg_partman
   template table.
8. **JPA:** keep `@Id` as the single `uuid` column and map the composite PK at DDL level only.
   `@GeneratedValue(IDENTITY)` forces a `RETURNING` round-trip per row and **disables JDBC
   batching** — use a client-side allocator with `reWriteBatchedInserts=true`,
   `hibernate.jdbc.batch_size=50`, `order_inserts=true`.

## 6. Hot queries

| Q | Purpose | Index | Measured |
|---|---|---|---|
| Q1 | Status by id | `n_id_uk` | 7 buffers, **0.476 ms** |
| Q3 | Retry candidates | `n_retry_ix` | `Heap Fetches: 0`, 16.0 ms |
| Q4 | Per-user history | `n_user_hist_ix` | 7 buffers, **0.017 ms** |
| Q5 | Provider success rate — **raw** | none | **481 MB, 290.7 ms** (one day, one partition) |
| Q5b | Provider success rate — **rollup** | `provider_health_minute` PK | **2 buffers, 0.015 ms** |
| Q6 | Attempts for one notification | `da_notification_ix` | 6 buffers, 0.016 ms |
| Q9 | Expired-lease reclaim | `sn_lease_expiry_ix` | 1 buffer, 0.008 ms |
| Q10 | Outbox drain, `SKIP LOCKED` | partial on `published_at IS NULL` | constant |

Q5 → Q5b is a **19,000× improvement**. The rollup stores `latency_ms_sum` + `attempts`, **never
`avg`** — averages don't compose across buckets. It's a plain table incrementally UPSERTed by the
Kafka consumer, not a materialised view (`REFRESH … CONCURRENTLY` re-reads all 800M rows).

The real-time circuit breaker doesn't read Postgres at all — it uses a Valkey sliding window. The
rollup serves dashboards and billing.

**Q3 needs a lower bound.** Without `next_attempt_at > now() - interval '1 hour'` the index scan
has no start point and degrades linearly with backlog depth.

**Q8 (`count(*)` for a day) does not scale** — 270k buffers / 2.1 GB. Counts come from the rollup.

## 7. Concurrency

| Use case | Isolation | Mechanism |
|---|---|---|
| Send-path writes | READ COMMITTED | Constraints do the work |
| Claim batch | READ COMMITTED | `SKIP LOCKED` — REPEATABLE READ would throw `40001` on a path that must not fail |
| Status transition | READ COMMITTED | Monotonic guard (single atomic statement) |
| Idempotency claim | READ COMMITTED | The unique index *is* the serialisation point |
| Quota counter | — | **Valkey `INCRBY`**, not a hot Postgres row |
| Provider config change | REPEATABLE READ | Multi-row consistency, low frequency |
| Billing reconciliation | **SERIALIZABLE READ ONLY DEFERRABLE** | The one place a phantom read costs money |

`@Version` on config tables (human-edited, low contention, "someone else changed this" is the
right UX). **Deliberately absent from `notification`** — 20M rows/day updated by concurrent
workers *and* webhooks would produce an `OptimisticLockException` storm on a path with no human to
retry. The monotonic guard is strictly better: one atomic statement, idempotent, correct under
reordering.

### The monotonic state machine

```sql
UPDATE notification n
SET status = $new, status_rank = $new_rank,
    status_at = greatest(n.status_at, $event_time), ...
WHERE n.created_at = $created_at
  AND n.id = $id
  AND n.status_rank < $new_rank              -- rejects stale and duplicate
  AND NOT EXISTS (SELECT 1 FROM delivery_status d
                  WHERE d.code = n.status AND d.is_terminal)
RETURNING n.status, n.status_rank;
```

Measured: `Nested Loop Anti Join`, **8 buffers, 0.041 ms**.

**Zero rows returned is not an error.** It means the event was stale, duplicated or illegal. The
caller records `notification_event.applied = false` and moves on. Those `applied = false` rows are
the single most valuable debugging artefact in the system — they are the webhooks you correctly
ignored, and without them "why is this stuck in SENT" is unanswerable.

`status_rank` is denormalised onto `notification` so the guard stays one index probe; a CHECK plus
a nightly consistency query catches drift.

## 8. Migrations

Flyway, run as a **Kubernetes Job before the rolling deploy — never at app startup**. N pods
racing Flyway's advisory lock combined with `CREATE INDEX CONCURRENTLY` is the documented
indefinite hang. Where unavoidable, set **both** `executeInTransaction=false` **and**
`spring.flyway.postgresql.transactional-lock=false` — setting only the first is the most common
cause of the hang.

Every migration begins:

```sql
SET lock_timeout = '3s';
SET statement_timeout = '0';
```

so DDL that can't get its lock fails fast instead of queueing and blocking every writer behind it.

**CI gate:** run each migration against a Testcontainers PG 18.6 seeded with 10M rows; fail the
build if any statement holds `ACCESS EXCLUSIVE` for more than 1 s.

### Adding a NOT NULL column — the PG 18 trap

`ADD CONSTRAINT … NOT NULL … NOT VALID` only skips validating *pre-existing* rows. It **fully
enforces on every new INSERT**, immediately:

```
ALTER TABLE notification ADD CONSTRAINT n_campaign_nn NOT NULL campaign_id NOT VALID;
-- old pod, unaware of the column:
ERROR:  null value in column "campaign_id" violates not-null constraint
```

It avoids a full-table scan. It does **not** tolerate old code. Recipe:

| Release | Action |
|---|---|
| N | `ADD COLUMN` nullable, no default — catalog-only, **7 ms measured** |
| N | Deploy code that writes it and tolerates NULL on read |
| N+1 | Backfill in batches, one daily partition per transaction, `VACUUM` between |
| N+2 | `ADD CONSTRAINT … NOT VALID` — safe *because* every pod already writes it |
| N+2 | `VALIDATE CONSTRAINT` — `SHARE UPDATE EXCLUSIVE`, concurrent DML allowed |
| N+3 | Remove the NULL-tolerant reads |

`ADD COLUMN … NOT NULL DEFAULT <const>` collapses this to 7 ms with no rewrite — **only** for a
non-volatile default. `DEFAULT now()` or `DEFAULT uuidv7()` **does** rewrite the table.

### Changing an enum value

```sql
SET lock_timeout = '3s';
ALTER TABLE user_contact DROP CONSTRAINT user_contact_channel_ck;
ALTER TABLE user_contact ADD CONSTRAINT user_contact_channel_ck
  CHECK (channel IN (…, 'RCS')) NOT VALID;     -- release N: read it
-- release N+1: write it
-- release N+2: VALIDATE CONSTRAINT
```

Removal is the same in reverse. **With a native enum, the removal step is impossible.**

### Renaming a column takes five releases

Add → dual-write → backfill → read new → drop old. `RENAME` is atomic and instant, but there is no
window where both names exist, so it breaks old pods' prepared statements the moment it commits.
**This is why naming carefully in V0001 matters more than any of this.**

**Never write a `DROP` in the same migration as an `ADD`.** Expand steps are purely additive, so
rollback is always safe.

## 9. Retention and GDPR

| Table | Hot | Then |
|---|---|---|
| `notification` | 90 d | S3 Parquet 13 mo → Glacier 7 yr |
| `delivery_attempt` | 30 d | S3 Parquet 7 yr (billing evidence) |
| `notification_event` | 7 d | S3 7 yr |
| `audit_record` | 13 mo | **S3 Object Lock (WORM)** 7 yr |
| `idempotency_record` | 24–48 h | dropped |

Export via the native `aws_s3` extension (CSV/text only — Glue converts to Parquet).

### Crypto-shredding

Rewriting 90 partitions of a 1.8B-row table to null one user generates 1.8B dead tuples and a
multi-day vacuum. Instead: all PII is AES-GCM under a **per-user DEK** wrapped by a KMS CMK.
**Erasure = destroy the DEK. One KMS call.** Every ciphertext in Postgres, S3, Kafka and the
archive becomes permanently unreadable simultaneously, with zero rows rewritten. The 30-day SLA is
met in minutes; `erasure_request` records each timestamp as evidence.

Two honest caveats:

- **`address_hmac` is tenant-keyed, not user-keyed** (suppression must match across users), so it
  is *not* covered by the shred and must be nulled — a few thousand rows per user via
  `n_user_hist_ix`, batched at 1,000/txn. Seconds, not days.
- **`suppression_entry` must survive erasure.** Deleting someone's unsubscribe means you may
  lawfully mail them again — a worse compliance outcome than retaining a pseudonym. Rewrite with
  `reason='GDPR_ERASURE'`, drop the provider attribution, and get legal sign-off rather than
  deciding it in code review.

Kafka: PII-bearing topics get `retention.ms=7d` — that is the actual guarantee. Compaction
tombstones are best-effort with no SLA and must not be relied on.

## 10. Operational watch-list

| Metric | Why it pages |
|---|---|
| `age(datfrozenxid)` | ~80M write txns/day against `autovacuum_freeze_max_age` 200M means anti-wraparound roughly every 2.5 days. A stalled autovacuum is ~25 days from a wraparound shutdown |
| Oldest `xmin` holder | One long-running transaction anywhere stalls freezing **globally** |
| DEFAULT partition row count | Non-zero means partition creation is about to fail under `ACCESS EXCLUSIVE` |
| Replication slot `wal_status` | `reserved → extended → unreserved → **lost**`. `max_slot_wal_keep_size = -1` risks a disk-full outage; finite risks silent slot invalidation |
| Invalid indexes | Leftovers from failed `CIC` runs cost write amplification while being useless to the planner |

**Autovacuum defaults are wrong at this scale.** `autovacuum_vacuum_scale_factor = 0.2` on a
1-billion-row table waits for **200 million dead tuples**. Override per table:
`scale_factor=0.0, threshold=50000, cost_delay=0, freeze_max_age=100000000`.
