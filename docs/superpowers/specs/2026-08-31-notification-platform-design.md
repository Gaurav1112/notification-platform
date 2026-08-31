# Notification Platform — Design Specification

**Status:** Approved for implementation
**Date:** 2026-08-31
**Author:** Kumar Gaurav
**Repository:** `notification-platform` (personal project)

---

## 0. Problem

Design a large-scale notification platform capable of sending SMS, email and push
notifications to 50 million users.

- An API receives notification requests.
- Notifications can be immediate or scheduled.
- Multiple providers exist per channel.
- Providers can fail.
- The system must support retry.
- Users can receive millions of notifications per day.
- Delivery status must be tracked.
- The system must scale horizontally.

Every decision in this document answers four questions: **why?**, **what can fail?**,
**how does it scale?**, and **how do we observe and recover?**

---

## 1. Requirements

### 1.1 Assumptions

| # | Assumption | Value |
|---|---|---|
| A1 | Registered users | 50,000,000 |
| A2 | Base volume | 5,000,000 notifications/day |
| A3 | Stretch volume | 100,000,000 notifications/day |
| A4 | Channel mix | Email 60% / Push 30% / SMS 10% |
| A5 | Fan-out | 1 request → 1…10,000,000 recipients |
| A6 | Peak:average | 3.0 (derived in §2.4) |
| A7 | Multi-tenant | Yes; noisy-neighbour is an explicit threat |
| A8 | Region | Single primary + warm DR |

### 1.2 Functional requirements

| ID | Requirement |
|---|---|
| F1 | Accept REST send requests for SMS / EMAIL / PUSH, immediate or scheduled |
| F2 | Accept bulk/campaign requests without blocking the API |
| F3 | Cancel or reschedule a not-yet-dispatched notification |
| F4 | Render content from versioned, per-locale templates |
| F5 | Honour preferences: opt-in/out, per-channel, quiet hours, frequency caps, unsubscribe |
| F6 | Route across N providers per channel by health, priority, cost, rate limit, success rate |
| F7 | Retry transient failures with exponential backoff + jitter; fail fast on permanent |
| F8 | Fail over to the next provider; circuit-break the unhealthy one |
| F9 | Track the full delivery lifecycle, per notification and per attempt |
| F10 | Ingest asynchronous provider delivery receipts via authenticated webhooks |
| F11 | Route exhausted/poison messages to a DLQ with triage and replay |
| F12 | CRUD for templates, preferences, providers; read-only provider health |
| F13 | Emit an immutable audit trail of every state transition and admin action |

### 1.3 Traffic classes

Notifications are not one workload. Kafka partitions are strictly FIFO, so a priority
*field* on a shared topic is a lie — the consumer still reads past 10M bulk records to
reach the OTP behind them. Only physically separate topics give real isolation.

| Class | Example | Latency budget (accept → provider) | TTL | Shed order |
|---|---|---|---|---|
| `CRITICAL` | OTP, 2FA, fraud alert | **p99 < 5 s** | 60 s | never |
| `TRANSACTIONAL` | Receipt, password reset | p99 < 30 s | 24 h | fourth |
| `BULK` | Marketing, digest | p99 < 15 min | 72 h | first |

### 1.4 SLIs and SLOs (30-day window)

| SLI | Definition | SLO |
|---|---|---|
| `api_availability` | non-5xx ÷ total on `POST /v1/notifications` | **99.95%** (21.6 min/mo) |
| `accept_latency` | request received → 202 returned | p95 < 100 ms, **p99 < 250 ms** |
| `dispatch_latency` | accept → first provider call, per class | CRITICAL **p99 < 5 s** |
| `delivery_success_rate` | DELIVERED ÷ (DELIVERED + FAILED), excl. invalid recipient | per-channel, tracked |
| `duplicate_rate` | delivered ≥2× to same recipient ÷ total | **< 0.01%** |
| `scheduler_lag` | `now − scheduled_at` at dispatch | **p99 < 30 s** |
| `acceptance_durability` | accepted requests reaching a terminal state | **99.999%** |

### 1.5 The delivery contract

> **Guaranteed:** once we return `202 Accepted`, the request is durably recorded and will
> reach a terminal state (`DELIVERED`, `FAILED`, `EXPIRED`, `SUPPRESSED`) or sit visibly in
> the DLQ. It will never be silently dropped.
>
> **Not guaranteed:** exactly-once *delivery*. Transport is at-least-once; dispatch is
> idempotent. A provider that ACKs after our client timeout can still produce a genuine
> duplicate. We target < 0.01%, measure it, and publish it.
>
> **Ordering:** per `(recipient, channel)` only. Global ordering is not offered.

### 1.6 Non-goals

Cut deliberately, each with a reason:

- **Multi-region active/active** — needs conflict-free cross-region dedup; warm DR instead.
- **In-app / WhatsApp / Slack channels** — the SPI makes them a ~200-line addition; building them proves nothing new.
- **A UI** — the API and Grafana dashboards are the interface.
- **Exactly-once via Kafka transactions on the dispatch path** — the provider call cannot enlist in the transaction.
- **Audience segmentation engine** — we accept a recipient list or an opaque audience reference.
- **Real provider credentials** — mock providers only (ADR-005).

---

## 2. Capacity Planning

### 2.1 Traffic derivation

```
Base:    5,000,000 / 86,400 =    57.9 notifications/s average
Stretch: 100,000,000 / 86,400 = 1,157.4 notifications/s average
```

Fan-out: 60% transactional (1:1), 40% campaign (mean 5,000 recipients base / 10,000 stretch).

```
Base requests/day    = 3,000,000 + (2,000,000 / 5,000)   = 3,000,400
Stretch requests/day = 60,000,000 + (40,000,000 / 10,000) = 60,004,000
Blended fan-out      = 1.67 recipients per request
```

**Consequence:** the API tier scales with *requests*; the dispatch tier with *notifications*.
A campaign request is O(1) at the edge and O(10,000) behind it — expansion must never happen
in the request thread.

### 2.2 Peak-to-average ratio

Peak hour is 8.4% of daily volume; intra-hour 5-minute burst factor is 1.49.

```
PAR = (0.084 × 24) × 1.49 = 2.016 × 1.49 = 3.00
```

| Metric | Base | Stretch |
|---|---:|---:|
| Peak notifications/s | 173.6 | 3,472.2 |
| Peak API RPS | 104.2 | 2,083.5 |
| Peak provider attempts/s (×1.128 retry) | 195.8 | 3,916.7 |

### 2.3 The two bursts

**Burst A — 10M-recipient campaign in 15 minutes, on the 20:00 peak:**
```
10,000,000 / 900 s = 11,111 /s  +  diurnal peak 3,472 /s  =  14,583 notifications/s
```
That is 12.6× the stretch average and 252× the base average. **This is the design point.**

**Burst B — scheduled cliff:** 25M scheduled/day, ~5% landing in one minute = 20,833/s.
Mitigation costs nothing: `due_at += hash(notificationId) % 300s`. Deterministic (so it
survives restarts and stays idempotent), spreading a 60-second cliff over 5 minutes →
4,167/s. Now smaller than Burst A.

### 2.4 Kafka throughput

Message sizes include ~200 B headers and ~60 B record overhead. Large bodies are **not** in
Kafka — S3 pointer only (Claim Check).

| Event | Bytes |
|---|---:|
| `requested` | 1,500 |
| `dispatch` (blended) | 1,250 |
| `delivery` | 700 |
| `retry` | 1,400 |
| `status` | 550 |
| `dlq` | 2,050 |

```
Stretch: 604M records/day = 502 GB/day raw
         peak = 17.43 MB/s × RF3 = 52.3 MB/s cluster write
         after zstd 2.5:1 → 16.5 MB/s ingress
Burst A: 57,222 msg/s, 41.2 MB/s raw → 16.5 MB/s compressed
```

**Finding: bytes are never the constraint.** A single `express.m7g.large` broker is rated at
15 MB/s ingress. The constraints are *message rate*, *partition count* and *consumer
concurrency*.

### 2.5 PostgreSQL load

Row-ops per notification: 1 INSERT notification + 0.4 recipient + 1.128 attempt + 3 event +
3 status update = **8.528**.

| | Base | Stretch |
|---|---:|---:|
| Peak row-ops/s | 1,481 | **29,611** |
| Burst A + peak row-ops/s | — | **124,367** |
| Storage growth | 9.9 GB/day | **197.5 GB/day (72 TB/yr)** |

Read:write at peak = 1:14.8. **This is a write-dominated OLTP system**, which drives every
subsequent decision.

### 2.6 Redis load

| Use | Peak ops/s | Memory |
|---|---:|---:|
| Idempotency (24 h TTL) | 2,083 | 10.20 GB |
| Dedup set | 14,583 | 17.00 GB |
| Preferences (7 d TTL) | 14,583 | 5.00 GB |
| Templates, rate limits, circuit state, locks, timers | ~4,600 | 0.85 GB |
| **Naive total ×1.35 overhead** | **35,847** | **44.62 GB** |

With levers (idempotency TTL 24 h→6 h; dedup exact-set → cuckoo filter at 1.2 B/entry):
**11.50 GB**.

### 2.7 Infrastructure sizing

| | Base (5M/day) | Stretch (100M/day) |
|---|---|---|
| MSK | 3 × `kafka.m7g.large`, 200 GB | **6 × `kafka.m7g.xlarge`, 1 TB, 288 partitions** |
| RDS | `db.r7g.xlarge` Multi-AZ, 1 TB | **`db.r7g.8xlarge` Multi-AZ, 16 TiB, 30k IOPS, 2 replicas** |
| ElastiCache | 1 shard × `r7g.xlarge` + replica | **3 shards × `r7g.xlarge` + replicas** |
| EKS | ~6 × m7g.2xlarge | ~18 avg (KEDA elastic) |
| **Est. monthly AWS** | ~$4,600 | ~$30,500 naive / **~$19,700 levered** |

Broker CPU sizing (message rate, not bytes):
```
Burst A: 57,222 msg/s ingress × 3 (incl. replication fetch) = 171,666 msg/s handled
÷ 12,000 msg/s per vCPU at 60% target = 14.3 vCPU
6 × kafka.m7g.xlarge = 24 vCPU → 60% utilised. m7g.large (12 vCPU) = 119%, insufficient.
```

Storage:
```
967 GB retained single copy × RF3 = 2,901 GB ÷ 6 brokers = 483.5 GB
÷ 0.60 headroom = 806 GB → provision 1 TB/broker
```

### 2.8 The finding that reframes the project

```
Provider fees at stretch (monthly):
  SMS   300,000,000 × $0.0079 = $2,370,000
  EMAIL 900,000,000 × $0.0001 =    $90,000
  PUSH                          =         $0
                        TOTAL   = $2,460,000
AWS infrastructure              =    $30,500  (1.2% of TCO)
```

**A 20% SMS→push down-route saves $474,000/month — 15× the entire AWS bill.** The
highest-leverage engineering is the routing engine and the suppression pipeline, not broker
tuning.

*Cost caveat: MSK broker/storage rates and EKS control-plane price verified against AWS docs;
several RDS/ElastiCache instance rates are linear extrapolations and are labelled as
estimates.*

---

## 3. High-Level Architecture

### 3.1 Components

| Component | Responsibility |
|---|---|
| ALB + WAF | TLS, global rate limit, managed rules |
| `app-api` | AuthN/Z, validation, idempotency, quota, accept transaction, query, webhooks |
| `app-worker` | Orchestrator, channel workers, provider router, status processor |
| `app-scheduler` | Due scan, campaign fan-out, outbox sweep, retry promotion |
| Kafka (MSK) | Transport: requested, dispatch ×6, retry ×5, status, delivery, DLQ |
| PostgreSQL | **System of record** |
| Redis / Valkey | Idempotency cache, quotas, circuit state, dedup, template + preference cache, near-horizon timers |
| S3 | Rendered bodies, recipient manifests, archive |
| OTel → Prometheus / Tempo / Loki → Grafana | Observability |

### 3.2 Synchronous path (must complete in < 250 ms p99)

```
1. authenticate + authorise (JWT, tenant scope)
2. validate payload (schema, channel-specific recipient format, size caps)
3. idempotency  → Redis GET (hit: replay stored response)
                → miss: INSERT idempotency_record ON CONFLICT (durable claim)
4. tenant quota → Redis token bucket (fail-open on Redis down)
5. BEGIN TX
     INSERT notification_request
     INSERT notification (+ recipients, or S3 pointer if large)
     INSERT outbox_message
   COMMIT                        ← the single atomic accept decision
6. store response in idempotency_record, warm Redis
7. return 202
8. [after commit, best-effort] publish to Kafka   ← fast path; failure is fine
```

Deliberately **absent** from the sync path: preference lookup, template rendering, dedup,
provider selection. None of those should be able to make `POST /notifications` fail.

### 3.3 Asynchronous path

Orchestrator consumes → resolves preferences (may `SUPPRESS`) → renders template → dedup →
emits per-recipient dispatch → channel worker → router selects provider → adapter calls it →
status event → monotonic state machine → PostgreSQL. Retries and webhooks re-enter the same
spine.

### 3.4 The three defining decisions

1. **PostgreSQL is the system of record; Kafka is transport.** Replaying a topic must never
   be the only way to know what happened.
2. **The outbox makes "accepted" a single atomic fact.** No dual write. The fast path keeps
   p99 accept latency in budget; the sweeper is the safety net.
3. **Traffic classes are physically separate end to end** — separate topics, consumer groups
   and scaling policies. Structural isolation, not convention.

---

## 4. Design Patterns

### 4.1 Architectural

| Pattern | Where | Prevents |
|---|---|---|
| Hexagonal / Ports & Adapters | domain + application define ports; infra implements | Domain coupling to Spring/JPA/Kafka (ArchUnit-enforced) |
| DDD tactical | `Notification` aggregate owns recipients + attempts | Anemic model; rules leaking into services |
| CQRS (light) | Write = aggregates; read = flat projections + Redis | Read indexes on a table doing thousands of inserts/sec |
| Modular monolith → 3 deployables | 9 libs, 3 boot apps | Distributed-monolith tax |

### 4.2 Enterprise Integration Patterns

Transactional Outbox · Idempotent Receiver · Competing Consumers · Content-Based Router ·
Splitter (fan-out) · Aggregator (campaign rollup) · **Claim Check** (S3 for large recipient
lists) · Dead Letter Channel · Invalid Message Channel (separate from DLQ) · Message
Translator · Correlation Identifier · Wire Tap (audit) · **Delayed Message** (tiered retry
topics) · Guaranteed Delivery.

### 4.3 Resilience

Circuit Breaker (Redis-shared state) · Bulkhead (per channel *and* class) · Retry with
**full jitter** · Timeout (always < `max.poll.interval.ms`) · Rate Limiter · Fallback/Failover ·
Leader Election with fencing tokens · Cache-Aside · Health Endpoint Monitoring · Backpressure ·
Graceful Degradation.

### 4.4 GoF

Strategy (`ProviderSelectionStrategy`, `RetryPolicy`) · Adapter (provider adapters) ·
**Decorator** (the provider call stack) · Template Method (`AbstractChannelWorker`) · Chain of
Responsibility (`PreferenceFilterChain`) · State (`DeliveryStateMachine`) · Registry
(`ProviderRegistry`) · Builder · Observer · Specification · Null Object (`NoOpProvider`).

The decorator stack — each layer independently testable, and the mock adapter gets identical
resilience behaviour to a real one:

```
ChannelWorker → Traced → Metered → CircuitBreaker → RateLimited → Timeout → Idempotent → Adapter
```

### 4.5 Rejected patterns

| Rejected | Why |
|---|---|
| Dual write (DB then Kafka) | No atomicity |
| Full Event Sourcing | Replaying 500M notifications for a status lookup |
| Saga / 2PC | One aggregate, one DB — a saga would be ceremony |
| Shared-database integration | Each service owns its tables |
| Exactly-once on the dispatch path | The provider call cannot enlist |
| `Thread.sleep()` retry in-consumer | Holds the partition, breaks `max.poll.interval.ms` |
| Naive `SELECT … WHERE due_at < now()` polling | Measured 159 tps vs 746 (§8.2) |
| Anemic domain model | Invariants belong in the aggregate |

---

## 5. Domain Model

### 5.1 Entities

`tenant` · `user_account` · `user_contact` · `notification_template` ·
`notification_template_version` · `notification_preference` · `suppression_entry` ·
`notification_request` · `notification_recipient` · `notification` ·
`scheduled_notification` · `delivery_attempt` · `delivery_status` (lookup) · `provider` ·
`provider_configuration` · `retry_policy` · `idempotency_record` · `outbox_message` ·
`notification_event` · `dead_letter_message` · `audit_record` · `provider_health_minute` ·
`erasure_request`.

Full DDL: `docs/DATABASE.md`. ER diagrams: `docs/diagrams/` and the FigJam board.

### 5.2 Enums

```
Channel            SMS | EMAIL | PUSH
TrafficClass       CRITICAL | TRANSACTIONAL | BULK
Priority           P0_URGENT | P1_HIGH | P2_NORMAL | P3_LOW
ScheduleType       IMMEDIATE | SCHEDULED | RECURRING
NotificationStatus ACCEPTED | VALIDATING | SCHEDULED | EXPANDING | QUEUED | PROCESSING
                   | PARTIALLY_COMPLETED | COMPLETED | CANCELLED | FAILED | EXPIRED
DeliveryStatus     PENDING | SUPPRESSED | QUEUED | PROCESSING | SENT | DELIVERED
                   | BOUNCED | FAILED | RETRYING | EXPIRED | CANCELLED | UNKNOWN
FailureType        TRANSIENT_NETWORK | PROVIDER_TIMEOUT | PROVIDER_5XX | RATE_LIMITED
                   | AUTH_FAILURE | QUOTA_EXCEEDED | INVALID_RECIPIENT | UNSUBSCRIBED
                   | CONTENT_REJECTED | TEMPLATE_ERROR | PAYLOAD_TOO_LARGE
                   | DEVICE_UNREGISTERED | PERMANENT_UNKNOWN
RetryStatus        NOT_APPLICABLE | SCHEDULED | IN_BACKOFF | EXHAUSTED | ABANDONED
ProviderStatus     ACTIVE | DEGRADED | CIRCUIT_OPEN | DISABLED | MAINTENANCE
CircuitState       CLOSED | OPEN | HALF_OPEN | FORCED_OPEN
SuppressionReason  USER_OPTED_OUT | QUIET_HOURS | FREQUENCY_CAP | DUPLICATE
                   | GLOBAL_UNSUBSCRIBE | HARD_BOUNCE | SPAM_COMPLAINT | INVALID_ADDRESS
                   | TENANT_QUOTA
AttemptState       PENDING | SUCCEEDED | FAILED | UNKNOWN
ScheduleState      READY | CLAIMED | DISPATCHED | CANCELLED | FAILED
```

`FailureType` is the most important enum — it is the sole input to retry/fail/failover:

| FailureType | Retry | Failover | Deactivate address |
|---|---|---|---|
| `TRANSIENT_NETWORK`, `PROVIDER_5XX`, `PROVIDER_TIMEOUT` | yes, backoff | after 2 | no |
| `RATE_LIMITED` | yes, honour `Retry-After` | immediately | no |
| `AUTH_FAILURE`, `QUOTA_EXCEEDED` | no | immediately + page | no |
| `INVALID_RECIPIENT`, `DEVICE_UNREGISTERED` | no | no | yes |
| `UNSUBSCRIBED`, `CONTENT_REJECTED` | no | no | yes (suppression list) |
| `TEMPLATE_ERROR`, `PAYLOAD_TOO_LARGE` | no | no → DLQ | no |

### 5.3 Core interfaces

```java
public interface NotificationProvider {
    Channel channel();
    ProviderCode code();
    ProviderCapabilities capabilities();   // batching, idempotency key, webhooks, max batch
    SendResult send(SendCommand command);  // never throws for business failures
    HealthSnapshot health();
}

public sealed interface SendResult {
    record Accepted(String providerMessageId, Duration latency, long costMicros) implements SendResult {}
    record Rejected(FailureType type, String code, String message) implements SendResult {}
    record Indeterminate(FailureType type, String message) implements SendResult {}  // → UNKNOWN
}

public interface ProviderSelectionStrategy {
    Optional<ProviderCandidate> select(Channel c, TenantId t, List<ProviderCandidate> healthy);
}

public interface RetryPolicy {
    boolean isRetryable(FailureType type);
    Optional<Duration> nextDelay(int attempt, FailureType type, Optional<Duration> retryAfter);
    int maxAttempts();
}

public interface IdempotencyStore {
    ClaimResult claim(TenantId t, String key, byte[] fingerprint);  // CLAIMED|REPLAY|CONFLICT|IN_PROGRESS
    void complete(TenantId t, String key, int status, byte[] body);
}

public interface DeliveryStateMachine {
    /** Empty when the transition is illegal or stale — never throws on out-of-order input. */
    Optional<StatusTransition> apply(DeliveryStatus current, DeliveryStatus proposed, Instant occurredAt);
}
```

`SendResult` is sealed with **three** cases. `Indeterminate` forces every caller to handle the
`UNKNOWN` path at compile time.

---

## 6. Database Design

### 6.1 Why relational

Postgres holds whatever must be **atomic with something else**: preferences with their audit
record, idempotency with request creation, the outbox with the notification. That test decides
membership; everything failing it is a candidate for eviction.

Division of responsibility:

- **Postgres** — system of record; what a transaction must be atomic with.
- **Kafka** — what happened.
- **Redis** — a copy of whatever is too hot to fetch twice. Loss degrades quality, never correctness.
- **S3** — rendered bodies (800 GB/day at stretch), manifests, 7-year archive.
- **ClickHouse (deferred, ADR-012)** — what it means.

### 6.2 Enum storage

**Chosen: `varchar(n)` + table-local `CHECK`. Native PG enum nowhere. One lookup table.**

Reproduced on PostgreSQL 18.6:

```
ALTER TYPE chan_e ADD VALUE 'PUSH';  -- then use it in the same tx
ERROR:  unsafe use of new value "PUSH" of enum type chan_e

-- JDBC binds String as varchar:
ERROR:  column "c" is of type chan_e but expression is of type character varying
```

Plus: **an enum value can never be removed.** Roll back a bad deploy and the zombie value
lives in `pg_enum` forever, invisible to the next engineer's exhaustive Java `switch`.

`DOMAIN` with a shared CHECK was rejected: `ALTER DOMAIN … ADD CONSTRAINT` locks *every*
dependent table in one transaction — `ACCESS EXCLUSIVE` on `notification` (90 partitions) and
`delivery_attempt` (30) simultaneously. Per-table CHECK allows table-by-table with
`lock_timeout` and retry.

`delivery_status` **is** a lookup table because it carries `rank`, `is_terminal`, `is_failure`,
`is_billable` — data the SQL reads, not a label. Adding a status is an `INSERT`: zero DDL, zero
lock, zero deploy coupling.

### 6.3 Keys

| Table | Key | Why |
|---|---|---|
| `tenant`, `user_account` | `bigint` identity PK + `uuid` v7 `public_id` | PK is denormalised onto billions of rows; 8 B beats 16 B by ~60 GB/yr |
| `provider`, `retry_policy` | `smallint` identity | < 300 rows ever; copied onto 10B attempt rows |
| `notification`, `notification_request` | **UUIDv7**, PK `(created_at, id)` | Public resource ID; known pre-INSERT for the outbox; index locality |
| `scheduled_notification` | UUIDv7, PK `(due_at, id)` | Partition-compatible uniqueness |
| `delivery_attempt` | `bigint` identity, PK `(id, attempted_at)` | Never exposed; saves ~160 GB/yr |
| `notification_event`, `audit_record` | UUIDv7 | Client-side generation for pre-DB dedup |

**Measured index cost:** bigint+ts 31.5 B/row, uuid+ts 40.6 B/row, `(ts, uuid)` PK 52.5 B/row.

**UUIDv7 carries its own partition-pruning predicate** — `uuid_extract_timestamp()` recovers
creation time, so `WHERE id = $1` becomes `WHERE id = $1 AND created_at BETWEEN …`:

```
Q1  with created_at bound:     Index Scan, 7 buffers, 0.476 ms
Q1b without created_at bound:  Append across ALL partitions
```

PostgreSQL 18 has native `uuidv7()`. Spring Boot 4.1.1 ships Hibernate 7 with
`@UuidGenerator(style = VERSION_7)`. IDs are generated in Java, not by the column DEFAULT, so
the outbox row can be written in the same transaction.

ULID rejected: no PG type; `char(26)` is 69% larger keys, `bytea(16)` loses type-checking and
`uuid_extract_timestamp()`. Render UUIDv7 as Crockford base32 at the API edge if human-readable
IDs are wanted.

### 6.4 Indexes

| Index | Size | B/row | Note |
|---|---:|---:|---|
| `n_retry_ix` (partial, `status IN ('SEND_FAILED','QUEUED')`) | 7 MB | **3.6** | vs 40+ for a full index |
| `n_inflight_ix` (partial, `status IN ('CLAIMED','SENDING')`) | **8 kB** | ~0 | Healthy state has ~zero in-flight |
| `n_user_hist_ix` (covering, `INCLUDE (id, channel, status, status_at)`) | 151 MB | 79 | `Heap Fetches: 0`, 0.017 ms |
| `da_attempted_brin` (BRIN, `pages_per_range 64`) | **48 kB** | 0.01 | 2,500× smaller than B-tree |
| `sn_due_shard_ix` (covering, `WHERE state='READY'`) | 315 MB | 110 | Index-only, 0.275 ms / 500 rows |

**Deliberately NOT created:**

- **`notification(status)`** — the killer. `status` is updated 3–5× per row; a B-tree makes
  every transition a non-HOT update (new heap tuple + new tuple in *every* index + WAL).
  At 80M updates/day that is ~4 GB/day of extra WAL for a column with 15 distinct values,
  ~90% in one of them. Partial indexes bake the predicate in for free.
- **`n_dedupe_uk`** (189 MB, 99 B/row) → **moved to Redis** (`SET dedupe:{tenant}:{hash} NX EX 86400`),
  saving ~170 GB over the 90-day window plus a unique-conflict check on every insert.
- **More than 3 indexes on `delivery_attempt`** — 300–1,000 inserts/sec, never updated;
  each extra B-tree is ~32 GB/month.
- **Low-cardinality columns** (`channel`, `category`, `priority`) — 4–48 distinct values on 20M rows.

**Extended statistics** are required: `channel`, `status`, `tenant_id` are correlated
(a `SEND_FAILED` row is disproportionately SMS). Without
`CREATE STATISTICS n_chan_status_stx (dependencies, ndistinct)`, the planner multiplies
independent selectivities and underestimates ~10×, flipping a nested loop into a hash join.

### 6.5 Partitioning

> **Volume-model note.** The per-partition sizes in this section were *measured* on a
> PostgreSQL 18.6 validation run at **20M notifications/day** — a mid-point between the §1.1
> base (5M) and stretch (100M) cases, chosen so the synthetic dataset was large enough to
> produce honest query plans. They scale linearly: `notification` is 14.2 GB/day at 20M/day,
> 3.6 GB/day at base, and 71 GB/day at stretch. Retention windows are unchanged across all
> three; only the instance class and partition count change (§2.7, §10).

| Table | Scheme | Granularity | Retention |
|---|---|---|---|
| `notification` | RANGE(`created_at`) | 1 day (14.2 GB/day @ 20M/day) | 90 d → S3 Parquet |
| `delivery_attempt` | RANGE(`attempted_at`) | 1 day | 30 d |
| `notification_event` | RANGE(`occurred_at`) | 6 hours | **7 d tail only** |
| `scheduled_notification` | RANGE(`due_at`) | 1 day | rolling ±90 d |
| `notification_request` / `_recipient` | RANGE(`created_at`) | 1 day | 30 d |
| `idempotency_record` | RANGE(`created_at`) | **1 hour** | 24–48 h |
| `audit_record` | RANGE(`occurred_at`) | 1 month | 13 mo hot, 7 yr S3 WORM |
| `notification_preference` | **HASH(`user_id`) × 32** | — | indefinite |

Hourly `idempotency_record` partitions are the clearest "partition purely for cheap deletion"
case: `DROP TABLE` is O(1) and produces zero dead tuples, where `DELETE … WHERE expires_at < now()`
would generate 20M dead tuples/day and pin autovacuum.

`notification_preference` uses HASH because there is **no time dimension** — a 2019 user's
preference row is as hot as today's, and access is always a point lookup on `user_id`. Modulus
32 is effectively immutable (changing it requires a full rewrite), so it is chosen with headroom
to 1B rows.

**Composite RANGE(day) → HASH(tenant) rejected:** 720 leaf tables. Measured 1,068 shared buffers
and 3.3 ms *planning* time with only 3 partitions; at 720 the catalog lookups dominate a 0.5 ms
query. Revisit if one tenant exceeds ~5M/day.

Lifecycle: **pg_partman + pg_cron** (supported on RDS; `pg_partman_bgw` is not, and `pg_cron`
needs a custom parameter group + reboot), `premake=14`, `retention_keep_table=false`, with a
pre-drop S3 export. A Spring `@Scheduled` job is rejected — it can't run during a deploy and
its failure mode is "everything silently lands in DEFAULT". A Java **canary** alerts if
tomorrow's partition is missing by 12:00 UTC.

**Gotchas, all reproduced on 18.6:**

1. `CREATE INDEX CONCURRENTLY` on a partitioned parent is **still impossible in PG 18**.
   Workaround: `CREATE INDEX ON ONLY parent` (invalid stub) → `CREATE INDEX CONCURRENTLY` per
   partition (862 ms measured on 2M rows) → `ALTER INDEX … ATTACH PARTITION`. Parent stays
   `indisvalid = false` until the last attach. ~4 min for 90 partitions, `SHARE UPDATE EXCLUSIVE` only.
2. Unique constraints must include every partition-key column → PK `(created_at, id)` plus a
   second `UNIQUE (id, created_at)`. **The DB cannot enforce global uniqueness of `id`.** State it.
3. Partial unique indexes on partitioned tables *are* allowed; `ON CONFLICT` must repeat the
   `WHERE` clause for index inference to match.
4. **Zero FKs on the six hot tables.** An FK turns O(1) `DETACH`/`DROP` into a validation scan —
   the single most important retention operation. Full FKs on every config table. A nightly
   anti-join reconciliation per partition emits a metric. Document loudly, or someone will
   "fix" the missing FKs.
5. Partition-key UPDATE is a silent DELETE+INSERT (~3× WAL). Reschedules `DELETE` + `INSERT`
   explicitly so the cost is visible in code.
6. **The DEFAULT partition is a trap** — once a row lands there, creating the covering
   partition *fails* while holding `ACCESS EXCLUSIVE` during the scan. Keep it (a missing
   partition is otherwise an outage) but alarm on `count(*) > 0`.
7. Storage parameters are illegal on a partitioned parent; set per leaf via a pg_partman template.
8. **JPA:** keep the `@Id` as the single `uuid` column, map the composite PK at DDL level only.
   Enforce with an ArchUnit test failing any repository method on `Notification` lacking a
   `created_at` bound. `@GeneratedValue(IDENTITY)` disables JDBC batching — use a client-side
   allocator with `reWriteBatchedInserts=true`.

### 6.6 Hot queries

| Q | Query | Index | Measured |
|---|---|---|---|
| Q1 | Status by id (+ created_at window) | `n_id_uk` | Index Scan, 7 buffers, **0.476 ms** |
| Q3 | Retry candidates | `n_retry_ix` | Index Only Scan, `Heap Fetches: 0`, 16.0 ms |
| Q4 | Per-user history | `n_user_hist_ix` | Index Only Scan, 7 buffers, **0.017 ms** |
| Q5 | Provider success rate (raw) | none | **Parallel Seq Scan, 481 MB, 290.7 ms** — does not scale |
| Q5b | Provider success rate (rollup) | `provider_health_minute` PK | **2 buffers, 0.015 ms** — 19,000× faster |
| Q6 | Attempts for one notification | `da_notification_ix` | Index Scan, 6 buffers, 0.016 ms |
| Q9 | Expired-lease reclaim | `sn_lease_expiry_ix` | 1 buffer, 0.008 ms |
| Q10 | Outbox drain (`SKIP LOCKED`) | partial on `published_at IS NULL` | few pages, constant |

`provider_health_minute` stores `latency_ms_sum` + `attempts`, **never `avg`** — averages don't
compose across buckets. It is a plain table incrementally UPSERTed by the Kafka consumer, not a
materialised view (`REFRESH … CONCURRENTLY` re-reads all 800M rows).

The real-time circuit breaker doesn't read Postgres at all — it uses a Redis sliding window.
The rollup serves dashboards and billing.

### 6.7 Migration strategy

Flyway, versioned SQL, run as a **Kubernetes Job before the rolling deploy — never at app
startup** (N pods racing Flyway's advisory lock plus `CREATE INDEX CONCURRENTLY` is the
documented indefinite hang). Where unavoidable, set **both** `executeInTransaction=false` and
`spring.flyway.postgresql.transactional-lock=false`.

Every migration starts with `SET lock_timeout = '3s'; SET statement_timeout = '0';` so DDL that
can't get its lock fails fast instead of queueing and blocking every writer behind it.

CI gate: run each migration against a Testcontainers PG 18.6 seeded with 10M rows; fail the
build if any statement holds `ACCESS EXCLUSIVE` for > 1 s.

**Adding a NOT NULL column — the PG 18 trap.** `ADD CONSTRAINT … NOT NULL … NOT VALID` only
skips validating *pre-existing* rows; it **fully enforces on every new INSERT**. Reproduced:

```
ALTER TABLE notification ADD CONSTRAINT n_campaign_nn NOT NULL campaign_id NOT VALID;
-- old pod, unaware of the column:
ERROR:  null value in column "campaign_id" violates not-null constraint
```

It is a tool for avoiding a full-table scan, **not** for tolerating old code. Recipe:

| Release | Action |
|---|---|
| N | `ADD COLUMN` nullable, no default (catalog-only, 7 ms) |
| N | Deploy code that writes it and tolerates NULL on read |
| N+1 | Backfill in batches, one daily partition per transaction |
| N+2 | `ADD CONSTRAINT … NOT VALID` (safe *because* every pod already writes it) |
| N+2 | `VALIDATE CONSTRAINT` (`SHARE UPDATE EXCLUSIVE`, concurrent DML allowed) |
| N+3 | Remove NULL-tolerant reads |

`ADD COLUMN … NOT NULL DEFAULT <const>` collapses this to 7 ms with no rewrite — but only for a
non-volatile default. `DEFAULT now()` or `DEFAULT uuidv7()` **does** rewrite the table.

**Renaming a column takes five releases** (add → dual-write → backfill → read new → drop old).
There is no window where both names exist, so `RENAME` breaks old pods' prepared statements the
instant it commits. This is why naming carefully in V0001 matters more than any of this.

**Never write a `DROP` in the same migration as an `ADD`.** Expand steps are purely additive so
rollback is always safe.

### 6.8 Retention and GDPR

| Table | Hot | Archive |
|---|---|---|
| `notification` | 90 d (1.28 TB) | S3 Parquet 13 mo → Glacier 7 yr |
| `delivery_attempt` | 30 d | S3 Parquet 7 yr (billing evidence) |
| `notification_event` | 7 d | S3 7 yr |
| `audit_record` | 13 mo | **S3 Object Lock (WORM)** 7 yr |
| `idempotency_record` | 24–48 h | none |

**Crypto-shredding.** Rewriting 90 partitions of a 1.8B-row table to null one user generates
1.8B dead tuples and a multi-day vacuum. Instead: all PII is AES-GCM under a per-user DEK
wrapped by a KMS CMK. **Erasure = destroy the DEK — one KMS call.** Every ciphertext in
Postgres, S3, Kafka and the archive becomes permanently unreadable simultaneously, with zero
rows rewritten. The 30-day SLA is met in minutes.

Two honest caveats:

- `address_hmac` is **tenant-keyed, not user-keyed** (suppression must match across users), so
  it is not covered by the shred and must be nulled — a few thousand rows per user via
  `n_user_hist_ix`, batched. Seconds, not days.
- **`suppression_entry` must survive erasure.** Deleting someone's unsubscribe means you may
  lawfully mail them again — a worse compliance outcome than retaining a pseudonym. Rewrite with
  `reason='GDPR_ERASURE'`, drop the provider attribution, and get legal sign-off rather than
  deciding it in code review.

Kafka: PII-bearing topics get `retention.ms=7d` (the actual guarantee); compaction tombstones
are best-effort with no SLA.

---

## 7. Kafka Design

### 7.1 Partition sizing method

```
P = ceil( peakRate / consumerRatePerPartition × 1.30 × K )   rounded to a multiple of 3

  1.30 = operational headroom (broker restart, rolling upgrade, rebalance)
  K    = 1.5 for KEYED topics (cannot repartition without breaking per-key order)
       = 1.0 for unkeyed
```

`consumerRatePerPartition` is derived from **work per message**, not bytes:
SMS 80/s, Email 200/s, Push 300/s — set by provider round-trip latency.

### 7.2 Topology (288 partitions, 16 topics)

| Topic | P | Key | RF/minISR | Retention | Cleanup |
|---|---:|---|---|---|---|
| `notification.requested` | 12 | `tenantId\|idempotencyKey` | 3/2 | 7 d | delete |
| `notification.scheduled` | 6 | `tenantId\|requestId` | 3/2 | 7 d | delete |
| `notification.dispatch.push.tx` | 18 | `tenantId\|recipientId\|PUSH` | 3/2 | 3 d | delete |
| `notification.dispatch.push.bulk` | **54** | same | 3/2 | 3 d | delete |
| `notification.dispatch.email.tx` | 12 | `…\|EMAIL` | 3/2 | 3 d | delete |
| `notification.dispatch.email.bulk` | 36 | same | 3/2 | 3 d | delete |
| `notification.dispatch.sms.tx` | 12 | `…\|SMS` | **3/3** | 3 d | delete |
| `notification.dispatch.sms.bulk` | 6 | same | 3/2 | 3 d | delete |
| `notification.delivery` | 24 | `notificationId` | 3/2 | 7 d | delete |
| `notification.status` | 48 | `notificationId` | 3/2 | 7 d | **compact,delete** |
| `notification.retry.{5s,30s,2m,10m,1h}` | 24/12/6/6/6 | `…\|channel` | 3/2 | 2 d | delete |
| `notification.dlq` | 6 | `notificationId` | 3/2 | **30 d** | delete |

`sms.tx` carries OTPs and uses `minISR=3` — trading availability for zero loss. Everything else
uses `minISR=2` (AWS guidance: `minISR ≤ RF−1`, or you cannot produce during a rolling update).

`288 × RF3 = 864 replicas ÷ 6 brokers = 144/broker` — 14% of the recommended ceiling.
Deliberate, because partition count is the thing you cannot cheaply change later.

**Per-channel topics, not one shared topic:** sizing a shared topic to the slowest channel needs
182 partitions vs 138 split. Worse, one stalled SMS record (Twilio 429, 30 s backoff)
head-of-line-blocks every push and email behind it — one provider incident degrades all three
channels.

**`.tx` / `.bulk` lanes:** without them a 10M blast puts ~11,000 records ahead of a password
reset in every partition; at 300 msg/s that is a 37-second delay.

**Mapping traffic classes (§1.3) onto lanes:** `CRITICAL` and `TRANSACTIONAL` **share the `.tx`
lane**; `BULK` has its own. They are differentiated *within* the `.tx` lane by the `priority`
field, which is meaningful there because the lane is already low-latency and shallow — the
FIFO objection in §1.3 applies to mixing `BULK` with everything else, not to ordering two
classes that both drain in seconds. `CRITICAL` additionally gets a dedicated consumer group
with a higher replica floor and is the last thing shed (§9). If `CRITICAL` dispatch p99 is
ever observed degrading behind `TRANSACTIONAL`, the remedy is a third lane per channel
(`.critical`), which is a topic addition rather than a redesign.

**Five retry tiers, shared across channels** (5 s → 30 s → 2 m → 10 m → 1 h; total budget
1 h 12 m). Shared is safe because the retry consumer does no external I/O — it pauses and
republishes — so head-of-line blocking inside a tier is bounded by the tier delay itself.
`retry.5s` is sized for a **total provider outage** (9,861/s), not the 12.8% steady rate.

Producer config: `acks=all`, `enable.idempotence=true`, `max.in.flight=5`,
`retries=MAX_VALUE`, `delivery.timeout.ms=120000`, `compression.type=zstd`, `linger.ms=25`,
`batch.size=262144`, default murmur2 partitioner.

### 7.3 Ordering

- **Guaranteed:** FIFO per `(tenantId, recipientId, channel)`. That is the only contract published.
- Producer retries don't reorder — with `enable.idempotence=true` the Java client re-sequences.
- **Deliberately broken by retry tiers.** A message that fails and lands in `retry.30s` re-enters
  after a later message for the same key. 99%+ of notifications are mutually independent; where
  ordering matters, an opt-in `sequence_no` guard checks that `n−1` is terminal before dispatch.
- **Intra-partition parallelism must use the Confluent Parallel Consumer in `KEY` mode**, not
  `UNORDERED` — N-way concurrency while serialising per key.
- **`notification.status` compaction:** compaction keeps the highest *offset*, not the latest
  *state*. Carry a monotonic `version` and drop `version <= current`. Never rely on compaction
  for correctness.

### 7.4 Hot partitions

With `recipientId` in the key there are ~50M distinct keys over 54 partitions:

```
mean = 100,000,000 / 72 = 1,388,889 records/partition
sd   = sqrt(n·(1/p)·(1−1/p)) = 1,170
4σ / mean = 0.337%     ← statistically negligible
```

Every real hot partition is a bug or an adversary:

| Cause | Fix |
|---|---|
| Key = `tenantId` alone | Always include `recipientId` |
| Runaway recipient (bot, retry storm) | Per-recipient token bucket at the expander |
| Custom partitioner on sequential keys | Ban custom partitioners in code review |
| Time in the key | Ban |
| Campaign emitted in ID order | Shuffle each 10k chunk before produce (free) |
| Mega-tenant at 40% of volume | Kafka quotas + dedicated topics for tier-1 tenants |

Alarm on `max(partition_lag) / mean(partition_lag) > 3` sustained 5 min.

### 7.5 Repartitioning without downtime

**Rule 0: avoid it** — partitions can be increased but never decreased, and increasing re-maps
`hash(key) % N`, permanently breaking per-key ordering. Hence `K = 1.5` on every keyed topic.

1. **Scale inside the partition first** — Parallel Consumer `KEY` mode gives ~4× with zero topic
   changes and zero ordering loss. Exhaust this before touching partition counts.
2. **Unkeyed topics:** `--alter --partitions` is safe and online.
3. **Keyed topics: shadow-topic cutover.** Create `<topic>.v2`; deploy v2 consumers subscribed
   but feature-flagged off; flip the producer topic-name config atomically after `flush()`;
   **drain v1 to zero lag (measure, don't guess)**; then enable v2 dispatch. The
   ordering-hazard window is empty by construction. Expect 3–8 minutes of drain. Never during a
   campaign window.
4. **Adding brokers ≠ repartitioning** — use `kafka-reassign-partitions --throttle` (AWS: ≤10
   partitions per call) or Cruise Control; never above 70% broker CPU.

---

## 8. Correctness

### 8.1 Idempotency — five layers

| # | Boundary | Key | Store | TTL | Defends against |
|---|---|---|---|---|---|
| 1 | HTTP ingress | `Idempotency-Key` scoped `(tenantId, key)` | Postgres + Redis cache | 24 h | Client/ALB retries, double-submit |
| 2 | Kafka consume | `eventId` (UUIDv7 header) | Redis `SETNX` → Postgres fallback | 7 d | Redelivery, rebalance replay |
| 3 | Business dedup | `hash(tenant, user, channel, template, window)` | Redis cuckoo filter | 24 h | Same logical send via two paths |
| 4 | Attempt registration | `(recipient_id, attempt_number)` UNIQUE | Postgres, **before** the call | permanent | Worker crash mid-send |
| 5 | Provider dispatch | `idempotency_token` sent to the provider | Postgres UNIQUE | provider | **Provider ACKed and we never saw it** |

Only layer 5 can prevent a genuine duplicate, and only where the provider supports it
(SNS FIFO `MessageDeduplicationId`, APNs `apns-id`, FCM `collapse_key`; Twilio and SES have no
client key). Where none exists we degrade honestly to `UNKNOWN` + reconciliation, and **measure**
the duplicate rate rather than claiming zero.

The `request_fingerprint` (SHA-256 of the canonical body) is what most implementations skip:
same key + **different** body must be `409`, not a silent replay of an unrelated response.

**The dangerous sequence — worker crash mid-send:**

```
t0  INSERT delivery_attempt(state=PENDING, idempotency_token=T)  ← COMMITTED
t1  provider.send(payload, T)
t2  pod OOM-killed
t3  redelivery → attempt found PENDING, response_at NULL, age > timeout
    → UNKNOWN, do NOT blind-retry
    → reconcile via provider status API or the delivery webhook
    → resolve to DELIVERED or FAILED
```

Committing `PENDING` **before** the network call converts an invisible failure into a visible,
reconcilable one. This is the whole trick.

### 8.2 Concurrency

| Race | Mechanism |
|---|---|
| Two workers, one notification | Kafka partition ownership + `@Version` on the recipient row |
| Two schedulers, one due row | Shard affinity + `FOR UPDATE SKIP LOCKED` + lease + **fencing token** |
| Zombie scheduler (GC pause past lease) | Fencing token rejected at dispatch |
| Concurrent retries | `(recipient_id, attempt_number)` UNIQUE |
| Out-of-order webhooks | Monotonic rank guard + `last_status_at` |
| Duplicate webhooks | `provider_callback.dedup_hash` UNIQUE; ACK fast, process async |
| Rollup counter drift | **Never** `SET delivered_count = delivered_count + 1` (serialises on one row) — Redis increment, periodic flush, reconcile on read |
| Circuit-breaker split brain | Local Resilience4j + Redis-published aggregate at 1 Hz |
| Preference change mid-flight | Re-checked at dispatch, not only at accept — opt-out always wins |

**Isolation levels:** READ COMMITTED for the send path and claims (REPEATABLE READ would throw
`40001` on a path that must not fail); REPEATABLE READ + retry for config changes;
SERIALIZABLE `READ ONLY DEFERRABLE` for monthly billing reconciliation — the one place a phantom
read costs money.

**`@Version` deliberately absent from `notification`:** 20M rows/day updated by concurrent
workers *and* webhooks would produce an `OptimisticLockException` storm on a path with no human
to retry. The monotonic guard is strictly better — one atomic statement, idempotent, correct
under reordering.

### 8.3 The monotonic state machine

```sql
UPDATE notification n
SET status = $new, status_rank = $new_rank,
    status_at = greatest(n.status_at, $event_time), ...
WHERE n.created_at = $created_at            -- partition pruning
  AND n.id = $id
  AND n.status_rank < $new_rank             -- MONOTONIC: rejects stale & duplicate
  AND NOT EXISTS (SELECT 1 FROM delivery_status d
                  WHERE d.code = n.status AND d.is_terminal)
RETURNING n.status, n.status_rank;
```

Measured: `Nested Loop Anti Join`, **8 buffers, 0.041 ms**.

**Zero rows returned is not an error** — it means the event was stale, duplicated or illegal.
The caller records `notification_event.applied = false` and moves on. That one choice makes the
whole status pipeline idempotent and reorder-safe, which is what allows at-least-once consumers,
freely-retried webhooks, and harmless DLQ replay.

Rank values are load-bearing:

- `CANCELLED = 28 < QUEUED = 30` → the guard *itself* enforces "cannot cancel something queued".
  No `if` statement anywhere. Both orderings are correct: a `CANCELLED` that commits first is
  terminal, so the racing `QUEUED` is rejected by the `NOT EXISTS`.
- `BOUNCED = 85 > DELIVERED = 80`, and `DELIVERED` is **not** terminal — a hard bounce
  legitimately follows an SMTP 250. This breaks naive "delivered is final" machines.
- `EXPIRED = 27` (before `QUEUED`) — "TTL elapsed before send" can only precede sending.
- Engagement (`OPENED`, `CLICKED`) is **not** a status — it isn't monotone along the pipeline.

`status_rank` is denormalised onto `notification` so the guard stays a single index probe; a
`CHECK` plus a nightly consistency query catches drift.

### 8.4 Retry engine

```
delay = min(initial × multiplier^attempt, max)
jittered = random(0, delay)                 ← FULL jitter, not delay ± noise
if RATE_LIMITED and Retry-After present: jittered = max(jittered, retryAfter)
route to nearest tier topic
```

With 100k messages failing at the same instant, deterministic backoff means all 100k retry at
exactly `t+2s`, re-killing the provider the moment it recovers. Full jitter spreads them
uniformly.

The promoter **pauses the partition and re-polls** until `ready_at` — never `Thread.sleep()`.
Before republishing it re-checks eligibility (cancelled? expired?).

Max 5 attempts by default; DLQ on exhaustion with full context, and **the offset is committed** —
a poison message must never block a partition.

### 8.5 Provider selection and failover

```
candidates = enabled configs for channel, ordered by priority
  → filter: circuit CLOSED or HALF_OPEN
  → filter: rate-limit budget available (Redis sliding window)
  → filter: daily cap not reached
score = w1·successRate(5m) + w2·(1/normLatencyP95) + w3·(1/normCost)
      + w4·priorityBoost − w5·recentFailurePenalty
select: highest score, weighted-random across the top two (keeps the backup warm)
```

Circuit opens at >50% failure over ≥20 calls in a 60 s window; state published to Redis so 40
pods don't each independently discover the outage. Half-open after 30 s with 3 probes.

**A 4xx never trips the breaker** — a malformed payload is our bug, and opening the circuit on it
takes a healthy provider offline.

`AUTH_FAILURE` / `QUOTA_EXCEEDED` → no retry on that provider, immediate failover, **page on-call**.
All providers open for a channel → `retry.1m` + page.

### 8.6 Scheduling

**Measured, 16 concurrent claimers, 100 rows/txn, 3M READY rows:**

| Strategy | TPS | rows/s | avg latency |
|---|---:|---:|---:|
| `FOR UPDATE` (stampede) | 159 | 15,900 | **100.5 ms** |
| `FOR UPDATE SKIP LOCKED` | 453 | 45,300 | 35.3 ms |
| **Shard-affine + SKIP LOCKED** | **746** | **74,600** | **21.5 ms** |

The naive version throws **zero errors** — it just serialises, because all 16 pods walk the same
index in the same order. That is the trap: it looks like "the database is slow", not a design bug.

**Design:**

- **Layer 1 — Redis ZSET for the 0–5 minute horizon.** `ZRANGEBYSCORE due:{shard}` — sub-ms,
  zero Postgres load, 100 ms poll interval. A cache of the schedule, never the source of truth.
- **Layer 2 — Postgres is durable and owns everything beyond 5 minutes.** A hydrator moves rows
  into Redis 5 minutes ahead. Redis flush = delay, never loss.
- **Layer 3 — shard-affine scan.** 256 shards assigned to ≤32 pods; pods never contend.
  `SKIP LOCKED` remains purely as a rebalance-window safety net (that's the 746-vs-453 gap).
  Read half measured at **75 buffers / 0.275 ms for 500 rows**.

**Correctness:**

- The **lease** (`claim_expires_at = now() + 60s`), not the lock, is the correctness boundary.
  A pod OOM-killed mid-send releases its work in ≤60 s. This makes the system at-least-once;
  idempotency must therefore live at the provider (layer 5, §8.1).
- `CHECK ((state = 'CLAIMED') = (claimed_by IS NOT NULL))` — a biconditional making "CLAIMED with
  no owner" **unrepresentable**. Catches a buggy lease-reclaim job before it double-sends.
- `claim_count > 5` is a **poison-pill detector** — this row kills a worker every time it's
  claimed. Route to DLQ instead of re-claiming. Without it, one malformed row crash-loops a pod
  forever and never appears in metrics.
- **The claim transaction must never span the provider HTTP call.** Claim (short txn, commit) →
  send → record outcome (short txn). A 30-second timeout inside the claim transaction holds row
  locks and pins `xmin`, so autovacuum cannot clean the 20M dead tuples/day that status updates
  generate. This is the failure mode that takes the cluster down at 3 a.m.

**Timezones:** `scheduled_at` is stored `timestamptz`; the API requires an ISO-8601 offset (a
bare local time is a `400` — guessing is how you send OTPs at 3 a.m.). Quiet hours are stored as
`time` + IANA zone, evaluated in the *user's* zone at dispatch, not at accept.

**Kafka time-bucket topics rejected:** cancellation would require a tombstone plus a
consume-time filter; Postgres handles it with a `DELETE`.

### 8.7 User preferences

`PreferenceFilterChain` (Chain of Responsibility), evaluated at **dispatch**, not accept:

```
OptOut → GlobalUnsubscribe → Suppression → QuietHours → FrequencyCap → Dedup → Consent
```

Any link may return `SUPPRESSED` with a reason, which is a terminal state, recorded and
reported — not a silent drop. Quiet hours may defer rather than suppress for `TRANSACTIONAL`;
`CRITICAL` bypasses quiet hours entirely (an OTP at 2 a.m. was requested by the user).

Unsubscribe is a signed, unauthenticated one-click token (RFC 8058) writing a
`suppression_entry` — which **survives user erasure** (§6.8).

---

## 9. Failure Model

| Dependency down | Degraded behaviour | Recovery |
|---|---|---|
| **Kafka** | Accept transaction still commits (notification + outbox). Fast path fails silently; sweeper retries. **No loss, delivery delays.** Outbox > 500k → shed `BULK` at the API with `503`; `CRITICAL` keeps flowing | Sweeper drains ~10k/s; alert on outbox age > 60 s |
| **Postgres primary** | Multi-AZ failover 60–120 s. `503 + Retry-After` for writes; reads from replica. Optional emergency mode: `CRITICAL` straight to Kafka with `degraded=true`, reconciled later (off by default) | Automatic |
| **Redis** | **Fail-open on quota** (never reject paying customers over a cache). Idempotency falls back to Postgres. Circuit breakers fall back to per-pod local state. Scheduler locks → Postgres advisory locks | Automatic; `redis_fallback_active` gauge |
| **One provider** | Circuit opens, traffic shifts. All open for a channel → `retry.1m` + page | Half-open probes every 30 s |
| **Network timeout** | Attempt → `UNKNOWN`, reconcile. Never blind-retry | Webhook or reconciler |
| **Worker crash** | Uncommitted offsets redeliver; `PENDING` attempts reconciled | < 30 s |
| **Consumer rebalance** | `CooperativeStickyAssignor` (incremental). Offsets committed after DB commit | Seconds |
| **Poison message** | 3 in-place attempts → DLQ with context → **offset committed**. Schema-invalid → separate invalid-message channel | Operator replay API |
| **One AZ** | Multi-AZ everywhere; `min.insync.replicas=2` survives it | Automatic |
| **Region** | Warm standby, replica promotion, MSK Replicator, IaC EKS. **RTO 30 min, RPO < 5 min** | Runbook + quarterly game day |

**Load shedding ladder** — a published design decision, not emergent behaviour:

```
1. BULK ingest         → 503 + Retry-After
2. BULK dispatch       → consumers paused, lag builds
3. Non-critical status → buffered in Kafka, PG writes deferred
4. TRANSACTIONAL       → degraded latency, never dropped
5. CRITICAL            → protected to the last drop of capacity
```

Driven by a Redis-backed `SheddingLevel` that every component reads, set by a controller
watching consumer lag, outbox depth and DB connection saturation.

**Accepted limits, stated rather than hidden:** duplicate delivery < 0.01% concentrated in the
provider-timeout case; a region loss costs up to 5 minutes of accepted-but-unreplicated
notifications; Redis loss degrades quality (more duplicate calls, weaker frequency capping)
never correctness; a single tenant can still saturate one provider *account*, mitigated by
per-tenant credentials for tier-1 tenants.

---

## 10. Scalability

| Scale | Volume | Kafka | Postgres | Redis | First thing that breaks |
|---|---|---|---|---|---|
| 1M users | 1M/day | 3 × m7g.large | `r7g.large` | 1 × `r7g.large` | **Provider quotas.** Nothing we own is stressed |
| 10M users | 10M/day | 3 × m7g.large | `r7g.2xlarge` + replica | 1 shard `r7g.xlarge` | **`notification_event` write amplification** — move events off PG *here*, not at 100M |
| 50M users | **100M/day** | **6 × m7g.xlarge, 288 partitions** | **`r7g.8xlarge`, 16 TiB, 2 replicas** | **3 shards** | **PG primary during campaigns** (124,367 vs 179,200 ops/s max) |
| 50M users | 500M–1B/day | 12 × m7g.2xlarge + tiered storage | **Citus, 16 shards**, attempts+events in ClickHouse | 12 shards | Provider quotas again |

**Bottleneck ladder — what saturates first:**

1. **Provider quotas, ~200–500 msg/s per account** (hit at ~17M/day). Multi-provider routing,
   quota increases, SES dedicated IPs, short codes (~1,000/s) vs long codes (~1/s).
   **This is the true system cap; everything below is secondary.**
2. **Postgres write path**, ~30k ops/s or any single 10M campaign. Levers in order:
   events → Kafka/S3 (−35%), collapse 3 status UPDATEs → 1 (−24%), `COPY` for expansion,
   `fillfactor=85` for HOT updates, then shard by `tenant_id`.
3. **Dispatch consumer parallelism**, 21,600/s. Parallel Consumer `KEY` mode → 4× free.
4. **Redis single-shard CPU**, ~100k cmd/s. Lease-based rate limiting turns 18,500 ops/s into ~200.
5. **Pod scale-from-zero**, 60–120 s. **Pre-warm on campaign schedule** — the scheduler knows a
   10M blast is due at 20:00, so scale at 19:55.
6. **MSK brokers**, ~300k msg/s. Note AWS infrastructure is *sixth*.

**Single-primary ceiling:** `64 vCPU × 4,000 ops/s × 0.70 ÷ 8.528 ÷ PAR 3 × 86,400 ≈ 605M/day`
theoretical. Binding constraints bite earlier — Burst A capacity at ~100M/day, storage at ~400M/day.
After levers 1+2 (3.528 ops/notification): ~1.46B/day on CPU.

**Shard when any of:** hot-window storage > 20 TB (~400M/day) · sustained writes > 120k/s
including the worst concurrent campaign · a partition's autovacuum can't finish within retention ·
failover/restore RTO exceeds the SLO. **Shard key = `tenant_id`** — every read path is already
tenant-scoped and the HASH sub-partitioning is already on it.

**Move `delivery_attempt` + `notification_event` to a columnar store before sharding** — cheaper,
simpler, and buys more (ADR-012).

---

## 11. AWS Deployment

| Component | Service | Configuration |
|---|---|---|
| Ingress | ALB + WAF | TLS 1.3, managed rules, per-IP rate limit |
| Compute | EKS 1.31 + Karpenter | `api` (m7g.large, on-demand), `worker` (m7g.2xlarge, **70% spot**), `scheduler` (on-demand only) |
| Streaming | MSK | 6 × `kafka.m7g.xlarge`, 3 AZ, 1 TB gp3, IAM + TLS, KRaft |
| Database | RDS PostgreSQL 18.6 | `db.r7g.8xlarge` Multi-AZ, 16 TiB gp3, 30k IOPS, 2 replicas |
| Cache | ElastiCache (Valkey 9) | 3 shards × `r7g.xlarge` + replica, cluster mode on |
| Storage | S3 | Bodies, manifests, Parquet archive, Object Lock on audit |
| Secrets | Secrets Manager + KMS | Per-provider-per-tenant, 90-day rotation, per-user DEKs |
| Observability | OTel Collector → AMP / AMG / X-Ray | Prometheus + Grafana + Tempo locally |

Workers on 70% spot is safe **because** the design is crash-tolerant — spot eviction is just
another instance of the failure model already handled.

**Kubernetes:** `notification-api` HPA 3→60 on RPS + p95; six `worker-dispatch-*` Deployments
KEDA-scaled on consumer lag (`lagThreshold: 500`); `scheduler` fixed at 3 on-demand;
`flyway-migrate` as a pre-deploy Job. `terminationGracePeriodSeconds: 60`, `preStop` deregisters
from the consumer group, readiness gated on Kafka + DB, **liveness deliberately not** (a Postgres
blip must not cause a restart storm). `PodDisruptionBudget` and `topologySpreadConstraints` on all.

**DR:** cross-region read replica (RPO ~1 s), MSK Replicator with offset translation (~5 s),
S3 CRR, multi-region secrets, Terraform EKS scaled to zero. Failover: promote → scale → repoint
Route 53 → verify translated offsets → **replay `notification.requested` from the mirror since
the last known-good offset** (which is why its retention is 7 days).

---

## 12. Observability

**SLI recording rules** — an SLO you can't compute is a slogan. Each SLI in §1.4 maps to a
Prometheus expression; see `docs/OBSERVABILITY.md`.

**Metric catalog** (labels omitted for brevity): `notification_accept_duration_seconds`,
`notification_accepted_total`, `idempotency_outcome_total`, `kafka_consumergroup_lag`,
`outbox_pending_count`, `outbox_oldest_age_seconds`, `notification_dispatch_latency_seconds`,
`provider_call_duration_seconds`, `provider_success_rate`, `provider_circuit_state`,
`provider_failover_total`, `provider_cost_micros_total`, `notification_retry_total`,
`notification_dlq_total`, `dlq_backlog_size`, `notification_delivery_latency_seconds`,
`status_event_out_of_order_total`, `webhook_signature_invalid_total`,
`scheduler_dispatch_lag_seconds`, `scheduler_lease_expired_total`,
`scheduler_claim_count_exceeded_total`, `redis_fallback_active`, `shedding_level`.

**Alerting is burn-rate based, not threshold based:**

```yaml
- alert: NotificationAPIErrorBudgetFastBurn        # 2% of budget in 1h
  expr: (1 - api_availability_1h) > 14.4 * 0.0005
    and (1 - api_availability_5m) > 14.4 * 0.0005
  for: 2m
  severity: page
```

The two-window condition suppresses false pages: a 30-second blip trips the 1-hour window but
not the 5-minute one.

**Pages (symptoms):** CRITICAL dispatch p99 > 5 s · consumer lag > 100k or growing 10 min ·
DLQ > 100/min · all providers open for a channel · scheduler lag p99 > 60 s · outbox age > 120 s ·
duplicate rate > 0.1%.

The duplicate-rate page fires at **10× the SLO** (0.1% vs the 0.01% target in §1.4)
deliberately: the SLO is a 30-day budget tracked by the slow-burn rule, while the page is for
a systemic break — a provider replaying callbacks, or an idempotency layer failing open. A
page at exactly the SLO threshold would fire on statistical noise at low volume.

**Tickets (causes):** single provider degraded · retry rate > 20% · Redis fallback active ·
partition skew > 3× · **DEFAULT partition non-empty** · replica lag > 30 s.

**Tracing:** `traceparent` rides in Kafka headers so async hops join one trace, accept → webhook.
Sampling: 100% of errors and `CRITICAL`, 1% of `BULK`.

**Dashboards:** Executive (throughput, success rate, cost/1k, SLO burn) · Operational (lag,
dispatch latency by class, provider matrix, DLQ) · Provider (per-provider health, failover
events) · Data (partition sizes, replica lag, autovacuum, DEFAULT count, outbox depth).

---

## 13. Security

| Control | Design |
|---|---|
| **AuthN** | OAuth2 client-credentials (JWT RS256), JWKS cached with rotation; mTLS available; admin API on separate ingress with SSO + MFA |
| **AuthZ** | Tenant scoping at the **repository layer**, ArchUnit-enforced. Scopes: `notifications:send\|read`, `templates:write`, `providers:read`, `admin:*` |
| **In transit** | TLS 1.3 everywhere; `sslmode=verify-full` to RDS; IAM + TLS to MSK |
| **At rest** | KMS CMK on RDS/MSK/ElastiCache/S3/EBS; **field-level** AES-GCM on addresses and bodies under per-user DEKs |
| **Secrets** | Secrets Manager + External Secrets Operator. **Zero secrets in git, config or the DB** — the schema enforces `CHECK (credentials_ref ~ '^(arn:aws:secretsmanager:\|ssm:)')`, so pasting a key fails the insert |
| **Provider credential isolation** | One secret per `(provider, tenant)`; only the worker IRSA role can decrypt, scoped by tag. **The API tier cannot decrypt provider credentials at all** |
| **PII** | Addresses never logged plaintext — `address_hint` (`g***@example.com`) only. Log filter redacts by key. Bodies in S3, never the heap. Erasure by crypto-shred |
| **Rate limiting** | WAF per-IP → Redis token bucket per `(tenant, endpoint)` → per-`(tenant, provider)` budget. **Fails open** |
| **Audit** | Write-once, monthly partitions, 13 mo hot → S3 **Object Lock (WORM)** 7 yr |
| **Webhook verification** | HMAC-SHA256 **constant-time** (`MessageDigest.isEqual`) · timestamp ±5 min · IP allowlist · `dedup_hash` UNIQUE. Raw payload persisted **before** interpretation |
| **Supply chain** | Dependabot, OWASP Dependency-Check, CycloneDX SBOM, Trivy, distroless, `runAsNonRoot`, read-only root FS |

**Threat model highlights:**

| Threat | Mitigation |
|---|---|
| Stolen token → mass spam | Per-tenant quota + anomaly alert at >3× baseline + per-tenant kill switch |
| Forged webhook marks all delivered | HMAC + timestamp + IP allowlist; monotonic guard bounds blast radius |
| Template injection → phishing | Auto-escaping engine; `variables_schema` validated; no raw HTML from variables |
| Tenant A reads tenant B | Repository scoping + ArchUnit + integration test asserting cross-tenant **404** (not 403 — a 403 confirms existence) |
| Insider exfiltration | Field-level encryption; DB access alone yields ciphertext; KMS decrypt audited in CloudTrail |
| Replay of a captured request | Idempotency key + fingerprint → returns the original response, sends nothing |

---

## 14. API Design

Base `/v1`. OpenAPI 3.1 at `/v3/api-docs`, committed so contract drift appears in a diff.

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/notifications` | Send (immediate or scheduled) |
| `GET` | `/notifications/{id}` | Aggregate status |
| `GET` | `/notifications/{id}/recipients` | Per-recipient status (paged) |
| `GET` | `/notifications/{id}/attempts` | Delivery attempts |
| `POST` | `/notifications/{id}/cancel` | Cancel if not dispatched |
| `PATCH` | `/notifications/{id}/schedule` | Reschedule |
| `GET/POST/PUT` | `/templates[/{code}][/versions]` | Template CRUD + versioning |
| `GET/PUT/DELETE` | `/users/{userId}/preferences` | Preferences |
| `POST` | `/unsubscribe/{token}` | One-click unsubscribe (signed, unauthenticated) |
| `GET` | `/providers/health` | Provider health + circuit state |
| `POST` | `/webhooks/{providerCode}` | Delivery receipts (HMAC) |
| `GET/POST` | `/admin/v1/dlq[/{id}/replay]` | DLQ triage and replay |

**Send request:**

```json
{
  "trafficClass": "TRANSACTIONAL",
  "channels": ["EMAIL", "PUSH"],
  "template": { "code": "order-shipped", "locale": "en-US" },
  "recipients": { "kind": "USER_IDS", "userIds": ["u_9f2a", "u_7b31"] },
  "variables": { "orderId": "A-4821", "eta": "2026-09-02" },
  "schedule": { "type": "IMMEDIATE" },
  "ttlSeconds": 86400,
  "metadata": { "correlationId": "svc-orders-9931" }
}
```

**`202 Accepted`:**

```json
{
  "notificationRequestId": "01998f2a-7c31-7a04-9e12-6f0b3c1d5a88",
  "status": "ACCEPTED",
  "recipientCount": 2,
  "notifications": [
    { "id": "…5a89", "channel": "EMAIL", "status": "ACCEPTED" },
    { "id": "…5a8a", "channel": "PUSH",  "status": "ACCEPTED" }
  ],
  "links": { "status": "/v1/notifications/…5a88" }
}
```

`202`, not `200` — `200` implies delivery happened. Scheduled sends require an ISO-8601 offset.
Campaigns use `"recipients": { "kind": "S3_MANIFEST", "uri": "s3://…", "count": 10000000 }` and
still return within the p99 250 ms budget.

**Errors — RFC 9457 `application/problem+json`**, so clients branch on a `type` URI rather than
string-matching English messages:

| Status | `type` | When |
|---|---|---|
| `400` | `validation-failed`, `schedule-invalid` | Schema; past/unzoned/beyond-horizon `sendAt` |
| `401`/`403` | `unauthenticated`, `insufficient-scope` | — |
| `404` | `notification-not-found` | Also for cross-tenant access |
| `409` | `idempotency-key-reused`, `request-in-progress`, `already-dispatched` | — |
| `413` | `payload-too-large` | > 256 KB → use `S3_MANIFEST` |
| `422` | `all-recipients-suppressed` | Accepted, nothing sendable |
| `429` | `rate-limited` | + `Retry-After`, `X-RateLimit-*` |
| `503` | `service-degraded` | Load shedding |

Conventions: cursor pagination (never offset — it degrades on partitioned tables),
`ETag`/`If-None-Match` on status reads, `X-Request-Id` echoed, `traceId` in every error body.

---

## 15. Provider Layer

Only the **leaf adapter** is mocked (ADR-005). The SPI, all six decorators, registry, selection
scoring, circuit breaker, rate limiter, failure classification, retry tiers, DLQ, webhook
receiver and state machine are real and fully exercised.

Mocks emulate real vendor **semantics**, not generic success:

| Mock | Emulates | Reproduces |
|---|---|---|
| `MockSmsProvider` | Twilio | Codes `21610`/`21614`/`20429`/`30003`; async `MessageStatus` callback; **no client idempotency key** → forces the `UNKNOWN` path |
| `MockEmailProvider` | Amazon SES | 50-destination bulk, per-destination results, `Throttling`, `Permanent/General` bounce, complaint feedback loop |
| `MockPushProvider` | FCM v1 + APNs | **500-token multicast cap**, `UNREGISTERED` → deactivate token, `QUOTA_EXCEEDED`, `apns-collapse-id`, partial success |

**Deterministic failure injection** — seeded RNG so CI can assert "exactly 3 messages reached
the DLQ"; log-normal latency (real latency is long-tailed; uniform never trips a p99 breaker
realistically); and a `SILENT_SUCCESS` profile where the mock ACKs *after* our timeout and then
fires the webhook anyway — the only way to exercise the `UNKNOWN` state.

**The mock webhook emitter** calls our own public endpoint with a correct HMAC, and deliberately
injects late, out-of-order (5%), duplicated (2%) and lost (1%) receipts — so signature
verification, dedup and the monotonic guard are genuinely tested rather than bypassed.

**Runtime chaos control** makes the failover story demonstrable in 30 seconds:

```
POST /admin/v1/mock-providers/{code}/chaos  { "mode": "HARD_DOWN", "durationSeconds": 120 }
→ circuit OPEN at t+2s → failover to secondary → half-open probe at t+150s → CLOSED
```

**Adding a real provider** is one class + one `provider` row + one `provider_configuration` row
pointing at a Secrets Manager ARN + an error-code mapping. Zero changes to router, retry,
workers or status pipeline. `ProviderCapabilities` drives per-vendor behaviour, so there is no
`if (provider == TWILIO)` anywhere. A `ContractTest` base class enforces ~40 behavioural
assertions that every adapter — mock or real — must satisfy.

---

## 16. Testing Strategy

| Layer | Scope | Tooling |
|---|---|---|
| Unit | Domain invariants, state machine, retry policy, selection scoring, failure classification | JUnit 6, AssertJ, Mockito |
| Architecture | Hexagonal boundaries; **every `Notification` query carries a `created_at` bound**; every repository method takes a `TenantId` | ArchUnit |
| Repository | Real Postgres 18.6, partitioning, `SKIP LOCKED`, monotonic guard, partial indexes | Testcontainers 2 + `@ServiceConnection` |
| Messaging | Producer/consumer contracts, redelivery, rebalance, DLQ routing, offset semantics | Testcontainers Kafka |
| API | Contract tests against the committed OpenAPI; error taxonomy; idempotency semantics | MockMvc + REST Assured |
| Idempotency | Duplicate request, key reuse with different body, concurrent same-key, Kafka redelivery | Integration |
| Concurrency | N schedulers claiming, out-of-order webhooks, concurrent status transitions, lease expiry | Awaitility + parallel executors |
| Retry | Backoff math, jitter distribution, tier routing, `Retry-After` honouring, exhaustion → DLQ | Unit + integration |
| Failover | Circuit open/half-open/closed, provider ordering, all-providers-down | Mock chaos API |
| Failure | Kafka down, Postgres down, Redis down, poison message, worker kill mid-send | Toxiproxy + container pause |
| Load | §17 | k6 + Gatling |

**Non-negotiable tests**, because they encode the design's core claims:

1. Worker killed between the provider call and the attempt write → exactly one delivery, attempt
   resolves from `UNKNOWN`.
2. `DELIVERED` webhook delivered before `SENT` → final state is `DELIVERED`, and
   `status_event_out_of_order_total` increments.
3. Same idempotency key + different body → `409`, and **no second notification exists**.
4. 16 concurrent schedulers over 10k due rows → each dispatched exactly once.
5. Primary provider `HARD_DOWN` → zero delivery loss, failover within one circuit window.
6. Poison message → DLQ, offset committed, **partition keeps flowing**.

---

## 17. Load Testing

Reproducible harness in `load-test/`, driven by k6 against the full Docker Compose stack.

| Scenario | Profile | Measures |
|---|---|---|
| `baseline` | 100 RPS, 10 min | Accept p95/p99, resource floor |
| `ramp` | 50 → 2,000 RPS over 20 min | Breaking point, HPA behaviour |
| `campaign-burst` | 1M recipients in 5 min | Fan-out throughput, consumer lag recovery |
| `provider-outage` | Steady load + `HARD_DOWN` at t+3 min | Failover latency, retry backlog drain, zero loss |
| `mixed-class` | 90% BULK + 10% CRITICAL | **Proves class isolation — CRITICAL p99 must not degrade** |
| `soak` | 300 RPS, 2 h | Memory leaks, connection-pool exhaustion, partition growth |

Reported per run: throughput, p50/p95/p99 accept latency, end-to-end delivery latency, peak
consumer lag and drain time, DB CPU/IOPS/connections, Redis ops/s, DLQ count, error rate.

**No benchmark numbers are published until the tests have actually run.** `LOAD-TEST.md` ships
with the harness, the exact commands, the hardware spec, and an empty results table — filled in
from real runs, with measured local results and extrapolated cloud capacity kept in clearly
separate sections.

---

## 18. Repository Structure

```
notification-platform/
├── pom.xml                       BOM + module aggregation
├── mvnw / .mvn/                  Maven Wrapper 3.3.4 → Maven 3.9.16 (no global install)
├── platform-domain/              entities, value objects, enums, invariants — no Spring
├── platform-application/         use cases, ports, orchestration
├── platform-persistence/         JPA, Flyway, partitioning, repositories
├── platform-messaging/           Kafka producers, consumers, serde, idempotent receiver
├── platform-provider/            SPI, decorators, registry, router, mock adapters
├── platform-resilience/          retry policies, circuit breakers, rate limiters, bulkheads
├── platform-observability/       OTel, Micrometer, tracing propagation
├── platform-security/            authn/z, HMAC, encryption, secrets
├── app-api/                      Spring Boot — REST, idempotency, webhooks, query
├── app-worker/                   Spring Boot — orchestrator, channel workers, status processor
├── app-scheduler/                Spring Boot — due scan, fan-out, outbox sweeper, retry promoter
├── load-test/                    k6 scenarios + Gatling simulations
├── docker/                       compose, Grafana dashboards, Prometheus rules, PG image w/ pg_partman
└── docs/
    ├── README.md  ARCHITECTURE.md  ARCHITECTURE-SUMMARY.md  API.md
    ├── DATABASE.md  KAFKA.md  SCALABILITY.md  FAILURE-MODES.md
    ├── OBSERVABILITY.md  SECURITY.md  RUNBOOK.md  LOAD-TEST.md
    ├── ADDING-A-PROVIDER.md
    ├── adr/                      ADR-001 … ADR-018
    └── diagrams/                 Mermaid sources (rendered on GitHub)
```

Module dependencies are one-directional and **enforced by ArchUnit** — architecture that isn't
enforced by a test is a wish.

---

## 19. Technology Versions

Verified live against Maven Central, Docker Hub and vendor docs on 2026-08-31.

| Component | Version | Note |
|---|---|---|
| Java | 17 (Temurin 17.0.20.1+1) | |
| Spring Boot | **4.1.1** | 3.5.x is OSS-EOL since 2026-06-30 |
| Spring Kafka / kafka-clients | 4.1.1 / 4.2.1 | BOM-managed |
| Kafka broker | `apache/kafka:4.3.1` | **KRaft only**; ZooKeeper removed in 4.0 |
| PostgreSQL | `postgres:18.6` + pg_partman 5.5.0 | pg_partman needs a derived image (PGDG apt) |
| Cache | `valkey/valkey:9.1.1` | BSD-3; Redis 8 is AGPL/RSAL/SSPL tri-licensed |
| Resilience4j | 2.4.0, artifact **`resilience4j-spring-boot4`** | *not* `-spring-boot3` — silent autoconfig failure |
| Flyway | 12.4.0 core **+ `flyway-database-postgresql`** | Second artifact mandatory since Flyway 10 |
| Testcontainers | 2.0.5 | Major bump from 1.21.x |
| OTel agent / Micrometer tracing | 2.31.1 / 1.7.1 | Don't run both agent and bridge for the same spans |
| springdoc-openapi | 3.1.0 | 3.x is the Boot 4 line; 2.9.0 is Boot 3 |
| Jackson | 3.1.5, groupId **`tools.jackson`** | groupId changed |
| JUnit | **6.0.3** | Not 5 |
| MapStruct / Lombok | 1.6.3 / 1.18.46 | 1.7.0.Beta2 is beta — do not pin |
| k6 / Gatling | 2.2.0 / 3.15.1 | Gatling Java DSL is `gatling-core-java` |
| Kafka UI | `kafbat/kafka-ui:v1.5.0` | `provectuslabs/kafka-ui` abandoned since 2024, RCE history |
| Maven Wrapper | 3.3.4 → Maven 3.9.16 | |

**Do not take the newest** for: Lettuce (pin BOM 7.5.2), Flyway (BOM 12.4.0), or any of the
milestone traps (AssertJ 4.0.0-M1, Micrometer 1.18.0-M1, MapStruct 1.7.0.Beta2, Maven 4.0.0-rc-6).
`bitnami/*` images are frozen and unpatched — use `apache/kafka`.

---

## 20. Architecture Decision Records

| # | Decision | Chosen | Rejected | Rationale |
|---|---|---|---|---|
| 001 | Framework | Spring Boot 4.1.1 | 3.5.x | 3.5.x is OSS-EOL |
| 002 | Accept semantics | Outbox + fast path | Kafka-first, dual write | Dual write has no atomicity; Kafka-first breaks read-your-writes |
| 003 | Fan-out placement | Scheduler, own pool | 4th deployable | Bulkhead achieves the isolation |
| 004 | `delivery_attempt` | Postgres, 30-day tail | ClickHouse day one | 48% of write ops at 100M/day → exit at ~50M/day |
| 005 | Providers | Mock only | Real SDKs | Zero-credential clone-and-run; deterministic CI |
| 006 | Broker | Kafka | RabbitMQ, SQS, Pulsar | Replay, keyed ordering, consumer-group rescale, compaction |
| 007 | Delivery semantics | At-least-once + idempotent dispatch | Exactly-once | The provider call cannot enlist in a transaction |
| 008 | Topic split | channel × {tx,bulk} | Shared + priority field | FIFO partitions make a priority field a lie |
| 009 | Retry | Tiered delay topics + `pause()` | `Thread.sleep()`, DB polling | Sleeping holds the partition |
| 010 | Enums | `varchar` + CHECK, one lookup table | Native enum, DOMAIN | Values can't be removed; DOMAIN locks all dependents |
| 011 | Keys | UUIDv7 public / bigint internal | UUIDv4, ULID | v7 gives locality *and* self-describing pruning |
| 012 | Analytics | Deferred; rollup table | ClickHouse day one | 19,000× win without a 4th datastore |
| 013 | Due-scan | Shard-affine + SKIP LOCKED | Naive `FOR UPDATE`, Quartz | Measured 746 vs 159 tps |
| 014 | FKs on hot tables | None | Full RI | FK turns O(1) `DETACH` into a validation scan |
| 015 | Cache | Valkey 9 (BSD-3) | Redis 8 (AGPL) | Cleaner licence; protocol-compatible |
| 016 | Region | Single + warm DR | Active/active | Cross-region dedup outweighs the benefit |
| 017 | GDPR erasure | Crypto-shred per-user DEK | Row rewrite | 1.8B dead tuples avoided; one KMS call covers all stores |
| 018 | Callbacks | Push webhooks + reconciler | Polling status APIs | Polling 100M messages is untenable |

---

## 21. Implementation Phases

| Phase | Deliverable | Exit criteria |
|---|---|---|
| 0 | Repo scaffold, Maven Wrapper, module skeleton, ArchUnit rules, CI, Docker Compose | `./mvnw verify` green on a clean clone |
| 1 | Domain model, enums, state machine, Flyway V1, repositories | Repository tests on Testcontainers PG 18.6 |
| 2 | `app-api` accept path: authn, validation, idempotency, quota, accept tx, outbox | Idempotency test matrix passes |
| 3 | Kafka topology, producers, idempotent consumers, outbox sweeper | Redelivery and rebalance tests pass |
| 4 | Provider SPI, decorators, registry, router, three mock providers | Contract tests pass for all mocks |
| 5 | Channel workers, retry engine, tiered topics, DLQ + replay | Retry, failover, poison-message tests pass |
| 6 | Scheduler: shard-affine due scan, leases, fan-out, jitter | 16-scheduler concurrency test passes |
| 7 | Status pipeline: webhooks, HMAC, monotonic guard, reconciler | Out-of-order and duplicate webhook tests pass |
| 8 | Preferences, templates, suppression, unsubscribe | Filter-chain tests pass |
| 9 | Observability: metrics, tracing, dashboards, alert rules | Dashboards render against a live load run |
| 10 | Load tests executed; results recorded | `LOAD-TEST.md` filled with **measured** numbers |
| 11 | Documentation, ADRs, diagrams, production review | Independent review findings addressed |

---

## 22. Open Items

1. **Diagrams 1 and 6 in the Figma board predate two changes** (dispatch topics became
   channel × lane; due-scan became shard-affine). Regenerate before the repo is published.
2. **AWS cost figures** for RDS and ElastiCache instance classes are linear extrapolations —
   labelled as estimates in `SCALABILITY.md`, not presented as verified.
3. **Load-test results are unpopulated** until the harness has actually run.
4. **`app-fanout` as a 4th deployable** remains a documented option if campaign expansion
   measurably starves the due-scan under load testing (ADR-003 revisit trigger).
