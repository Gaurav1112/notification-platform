# Notification Platform — Complete Project Context

> **Purpose of this file.** A single self-contained briefing you can paste into an LLM to give it
> full context on this project. No external links required. Everything here is either verified
> against the running system or explicitly labelled as unverified.
>
> **State as of 2026-08-31, commit `5bc4316`.** 16 commits. `./mvnw verify` → BUILD SUCCESS,
> 384 tests, 0 failures. All three applications boot. Work is in progress on four remaining items
> listed in §11.

---

## 1. What this is

An enterprise notification platform: **SMS, email and push** for **50 million users** and
**millions of notifications per day**. Built as a portfolio project to evidence senior backend
engineering — distributed systems, concurrency, reliability, observability.

**Original problem statement** (8 requirements):
1. An API receives notification requests
2. Notifications can be immediate or scheduled
3. Multiple providers exist per channel
4. Providers can fail
5. Must support retry
6. Users can receive millions of notifications per day
7. Delivery status must be tracked
8. The system must scale horizontally

**Repository:** `~/PersonalProject/notification-platform` (local; not yet pushed to GitHub).
**Author identity:** Kumar Gaurav `<kgauravis016@gmail.com>` — personal, no employer footprint.

---

## 2. Technology stack (all versions verified live against Maven Central / Docker Hub)

| Component | Version | Note |
|---|---|---|
| Java | 17 (Temurin) | Pinned. Blocks pattern-matching `switch` — a real, recurring cost |
| Spring Boot | **4.1.1** | 3.5.x went OSS-EOL 2026-06-30. Boot 4 brings Spring Framework 7, **Jackson 3 with groupId `tools.jackson`**, **JUnit 6**, **Testcontainers 2** |
| Kafka | 4.3.1 (`apache/kafka`) | KRaft only; ZooKeeper removed in 4.0 |
| PostgreSQL | 18.6 | Native `uuidv7()` |
| Cache | Valkey 9.1.1 | BSD-3. Redis 8 is AGPL/RSAL/SSPL tri-licensed — awkward for a public repo |
| Resilience4j | 2.4.0, artifact **`resilience4j-spring-boot4`** | The `-spring-boot3` artifact gives *silent* autoconfiguration failure |
| Flyway | 12.4.0 + `flyway-database-postgresql` + **`spring-boot-flyway`** | The third is Boot 4's per-technology autoconfig module |
| Build | Maven Wrapper 3.3.4 → Maven 3.9.16 | Committed, so a clean clone needs only a JDK |

### Boot 4 migration traps hit during this build

- **Testcontainers 2.x renamed every module artifact**: `org.testcontainers:postgresql` →
  `org.testcontainers:testcontainers-postgresql`
- **`@EntityScan` moved** to `org.springframework.boot.persistence.autoconfigure.EntityScan`
- **Autoconfiguration split per technology.** `flyway-core` alone gives the library but not
  `FlywayAutoConfiguration`, so every `spring.flyway.*` property is silently inert and no schema
  is ever created. Symptom: `make up` leaves an empty database.

---

## 3. Repository structure

```
notification-platform/
├── pom.xml                 BOM + 11 modules
├── mvnw, .mvn/             Maven Wrapper (committed)
├── LICENSE                 MIT
├── Makefile                up / down / test / test-it / psql / demo
├── .github/workflows/ci.yml
│
├── platform-domain/        PURE JAVA. No Spring, JPA, Kafka or Jackson. ArchUnit-enforced.
├── platform-application/   Use cases + outbound ports
├── platform-persistence/   JPA entities, repositories, Flyway, adapters
├── platform-messaging/     Kafka topics, events, producer, idempotent consumer
├── platform-provider/      Provider SPI, decorator chain, mock adapters, routing
├── platform-resilience/    Retry policy, backoff, circuit breakers, rate limiter
├── platform-observability/ (thin — package only)
├── platform-security/      (thin — package only)
│
├── app-api/                REST, idempotency, webhooks, query      :8080
├── app-worker/             Orchestrator, channel workers, status   :8082
├── app-scheduler/          Due scan, outbox sweeper, retry promoter :8083
│
├── docker/                 compose.yml (postgres, valkey, kafka, kafka-ui, prometheus, grafana)
├── scripts/capture-verification.sh
└── docs/                   ~20 markdown files + 18 ADRs + 8 rendered diagrams
```

Module dependencies are one-directional (`app-*` → libraries → `platform-domain`) and enforced by
ArchUnit, not convention.

---

## 4. Architecture

### The five-stage flow

```
1 ASK      Client POSTs with an Idempotency-Key. Seen before? Replay the stored response.
2 PROMISE  ONE transaction writes notification + outbox row. Return 202. This commit IS the promise.
3 DECIDE   Async: preferences, quiet hours, frequency cap, suppression list → may SUPPRESS.
4 SEND     Score providers on health/cost/rate budget → write attempt row BEFORE calling →
           call through the decorator stack → classify any failure.
5 CONFIRM  Provider webhook arrives minutes later → verify HMAC → apply via a monotonic guard.
```

### Component responsibilities

| Component | Owns | Scaling |
|---|---|---|
| `app-api` | Accept, validate, idempotency, quota, webhooks, query | HPA 3→60 on RPS |
| `app-worker` | Orchestration, channel workers, provider routing, status projection | KEDA on Kafka consumer lag, gated on provider health |
| `app-scheduler` | Leader-elected due scan, campaign fan-out, outbox sweeper, retry promotion | Fixed 3, on-demand nodes only (spot eviction mid-claim is avoidable pain) |
| PostgreSQL | **System of record** | Multi-AZ, partitioned |
| Kafka | Transport | 16 topics, 288 partitions |
| Valkey | Cache + coordination — never the source of truth | 3 shards |

---

## 5. The design decisions that matter

### 5.1 Transactional outbox — why "accepted" is one atomic fact

`save to DB` then `publish to Kafka` has no atomicity. Crash in between and you have accepted a
notification that will never be sent, with no error anywhere. So the accept transaction writes
**three rows and commits once**:

```sql
BEGIN;
  INSERT notification_request
  INSERT notification
  INSERT outbox_message      -- "someone still needs to publish this"
COMMIT;                      -- the single atomic accept decision
```

A best-effort publish runs *after* commit for speed; an `OutboxSweeper` claims unpublished rows
with `SELECT … FOR UPDATE SKIP LOCKED` and publishes them. Double publish is harmless because
consumers are idempotent. Rows are **DELETEd** after publish, never updated with a timestamp — an
update-based design makes the table and its partial index grow without bound.

### 5.2 Traffic classes are physically separate Kafka topics

`CRITICAL` (OTP, 60s TTL, p99 5s) · `TRANSACTIONAL` (24h) · `BULK` (72h, shed first).

A Kafka partition is strictly FIFO. There is no "read the urgent one first" — the consumer
physically must read past every earlier record. So a `priority` **field** on a shared topic is a
lie:

```
9,000,000 campaign messages ÷ 200 msg/s = 12.5 hours before the OTP behind them is read
```

Hence six dispatch topics: `dispatch.{sms,email,push}.{tx,bulk}`. `CRITICAL` and `TRANSACTIONAL`
share the `tx` lane (both drain in seconds, differentiated by a priority field *within* an already
shallow lane); `BULK` is physically separate.

Every large platform that has published its architecture — Netflix, Uber, Airbnb, Pinterest —
arrived at physical separation. None uses a priority field.

### 5.3 The monotonic state machine

Statuses carry a rank; status only ever moves **forwards**:

```sql
UPDATE notification SET status = :new, status_rank = :newRank
 WHERE id = :id
   AND created_at >= :from AND created_at < :to   -- partition pruning
   AND status_rank < :newRank                      -- monotonic
   AND NOT EXISTS (SELECT 1 FROM delivery_status d
                    WHERE d.code = status AND d.is_terminal);
```

**Zero rows returned is not an error** — it means the event was stale, duplicated or illegal, which
is routine at volume. That single property is what makes at-least-once consumers, freely retried
webhooks and DLQ replay all safe.

Two rank choices encode business rules:

- **`CANCELLED = 28 < QUEUED = 30`** — "you cannot cancel something already queued" is enforced by
  *ordering*, not by an `if` a future engineer forgets. Both race orderings are safe: if
  `CANCELLED` commits first it is terminal, so the racing `QUEUED` is rejected by the terminal check.
- **`DELIVERED = 80` is deliberately NOT terminal** — an SMTP 250 means *accepted*, not *in the
  inbox*; a hard bounce legitimately follows. Machines that treat delivered as final silently drop
  bounce events, so the address never reaches the suppression list.

Full ordering: `PENDING 10 · SCHEDULED 20 · SUPPRESSED 25 · EXPIRED 27 · CANCELLED 28 · QUEUED 30 ·
CLAIMED 40 · SENDING 50 · SEND_FAILED 55 · FAILED 58 · SENT 60 · ACCEPTED 70 · DELIVERED 80 ·
BOUNCED 85 · COMPLAINED 88 · UNKNOWN 90`.

### 5.4 Idempotency — five layers

| Layer | Key | Store | Prevents |
|---|---|---|---|
| HTTP ingress | `Idempotency-Key` + SHA-256 body fingerprint | Postgres + Valkey cache | Client/LB retries |
| Kafka consume | `eventId` | Valkey `SETNX` → Postgres fallback | Redelivery, rebalance replay |
| Business dedup | `hash(tenant, user, channel, template, window)` | Valkey | Same logical send via two paths |
| Attempt registration | `(recipient_id, attempt_no)` | Postgres, **written before the call** | Worker crash mid-send |
| Provider dispatch | Vendor idempotency token | Postgres | The provider ACKed and we never saw it |

**The body fingerprint is the part most implementations skip.** Same key + same body = replay the
stored response. Same key + **different** body = `409`, because returning the first response would
silently answer a question the caller did not ask.

### 5.5 Exactly-once is not offered, and the research says why

Verified against vendor documentation:

| Provider | Client dedup key? | Can we ask "did you send X"? |
|---|---|---|
| Twilio | **No** | **No** — `GET /Messages` filters only on To/From/DateSent at whole-day granularity, and there is **no client-reference field** |
| AWS SES | No | Yes, via `EmailTags` |
| SendGrid | No | Yes, via `custom_args` |
| FCM | No | No per-message receipt (aggregate API, up to 5 days lag) |
| APNs | **No** — `apns-id` is correlation for *error reporting* only | No webhook at all |

**No provider we would plausibly integrate offers a usable client idempotency key.** So delivery
semantics differ per channel by cost asymmetry:

- **SMS → at-most-once.** A duplicate OTP costs money and trust, and Twilio cannot be queried. On
  timeout we do **not** resend; we accept a small measured loss rate.
- **Email → at-least-once + reconcile** via `custom_args`.
- **Push → at-least-once** with a collapse key, so a duplicate *replaces* rather than stacks.

There is a first-class **`UNKNOWN`** state for "the provider timed out after possibly delivering".
Never blind-retry from it.

### 5.6 Retry engine

```
delay    = min(initial × multiplier^attempt, cap)
jittered = random(0, delay)          ← FULL jitter, not delay ± noise
```

Without jitter, 100,000 messages failing at the same instant all retry at exactly `t+2s` and
re-kill the provider the moment it recovers.

Five tiered delay topics (5s · 30s · 2m · 10m · 1h). The promoter **pauses the partition** and
re-polls; it never `Thread.sleep()`s, because sleeping holds the partition and breaks
`max.poll.interval.ms`, causing a rebalance → redelivery → sleep → rebalance loop.

On top: a **retry budget** (token bucket, ~10% of calls). AWS's worked example is that a five-deep
stack with three retries per layer amplifies load on a failing dependency **243×**, and Segment
measured that only **~1.5%** of deliveries succeed on a retry.

`FailureType` carries four policy bits — retryable, failoverAfterAttempts, shouldSuppressAddress,
isOutcomeIndeterminate — so the retry-vs-fail-vs-failover decision lives in exactly one place.

### 5.7 Provider layer

```
ChannelWorker → Traced → Metered → CircuitBreaker → RateLimited → Timeout → Idempotent → Adapter
```

Only the leaf adapter is mocked. The three mocks emulate real vendor semantics: Twilio error codes
(`21610` unsubscribed, `21614` invalid number, `20429` rate limited), SES 50-destination bulk,
**FCM's removed `/batch` endpoint** (deprecated 2023-06, dead 2024-06 — `sendEachForMulticast` fans
out to individual HTTP/2 requests). Failure injection is **seeded**, so CI can assert exact DLQ
counts.

Circuit breaker: opens at >50% failure over ≥20 calls in 60s, state shared via Valkey so 40 pods
don't each discover the outage independently. **A 4xx never trips it** — a malformed payload is our
bug, and opening the circuit on it takes a healthy provider offline. The open-state wait is
**jittered per pod**, because `permittedNumberOfCallsInHalfOpenState` is per-JVM and 40 pods × 10
probes = 400 synchronised probes at t+30s.

### 5.8 Database

PostgreSQL is the system of record. Membership test: **is there a transaction where this must be
atomic with something else?**

- **`varchar` + CHECK, never native PG enums.** An enum value can never be removed, and
  `ALTER TYPE … ADD VALUE` then using it in the same transaction fails — and Flyway wraps every
  migration in a transaction. `DOMAIN` was rejected too: `ALTER DOMAIN` locks every dependent table
  in one transaction.
- **`delivery_status` IS a table**, because it carries `rank`/`is_terminal` — data the SQL joins
  against. Adding a status becomes an `INSERT`: zero DDL, zero lock, zero deploy coupling.
- **UUIDv7 for public ids, bigint internally.** v7 embeds a 48-bit timestamp, so the key carries its
  own partition-pruning predicate: `WHERE id = $1` becomes `WHERE id = $1 AND created_at BETWEEN …`.
  Measured: **7 buffers / 0.476 ms** with the bound, versus an `Append` across all 90 partitions
  without it.
- **RANGE partitioning by day** (hourly for `idempotency_record`). Retention is `DROP TABLE` — O(1),
  zero dead tuples. A `DELETE` of 20M rows creates 20M dead tuples that autovacuum never catches up
  with.
- **No foreign keys on the six hot tables.** An FK turns O(1) partition `DETACH` into a validation
  scan, killing the operation retention depends on. Nightly anti-join reconciliation instead.
- **No index on `notification.status`, deliberately.** Status is updated 3–5× per row; indexing it
  forfeits HOT updates, costing ~4 GB/day of extra WAL for a column with 16 values, 90% of them the
  same one. Partial indexes on the retry-eligible and in-flight subsets serve the same queries at
  **3.6 B/row** instead of 40+.

### 5.9 Scheduling — three tiers

1. **PostgreSQL** — durable ledger, partitioned on an **immutable** `due_bucket` (a partition-key
   `UPDATE` is a silent DELETE+INSERT and can raise `40001`, which `SKIP LOCKED` cannot ignore)
2. **A leader-elected hydrator** — ONE writer range-scans the hot partition and pushes the next
   5 minutes into a Valkey sorted set. Single-writer because `SKIP LOCKED` fixes correctness but
   not bloat, and wasted index visits scale as **B·W²/2**
3. **Shard-affine claimers** — 256 shards across pods, so pods never contend

Plus: a 60-second **lease** (not a lock), and a **`claim_count > 5` poison-pill detector** — without
it one malformed row crash-loops a pod forever and never appears in metrics.

**Deterministic jitter**: `dueAt += hash(id) % 300s`. Humans schedule on the hour, so ~5% of a day's
scheduled volume lands in one minute — 20,833/s becomes 4,167/s. Deterministic (not random) so it
survives restarts and stays idempotent.

### 5.10 Timezones

Measured against tzdb 2026c: **312 canonical zones but only 37 distinct UTC offsets**, with minutes
in `{00, 30, 45}` (Nepal +05:45, Chatham +12:45, Eucla +08:45). A single "09:00 local" send produces
37 distinct UTC instants spanning 25 hours. **Hourly ticking is wrong and half-hourly is also
wrong** — 15 minutes is the coarsest correct granularity.

Also: `Australia/Lord_Howe` shifts by **30 minutes**; 2026 has 214 DST transitions with 82.7% on
four dates; and `ZoneRules.getOffset(LocalDateTime)` and `ZonedDateTime.of()` **disagree inside a
DST gap**, which in a fall-back overlap produces a genuine double-send.

**Never compute timezone values in PostgreSQL.** `timezone(text, timestamp)` is marked IMMUTABLE, so
`AT TIME ZONE` is *legal* in a generated column or index — and a tzdata bump then silently
invalidates that index, returning wrong rows with no error.

---

## 6. Capacity model

| | Base (5M/day) | Stretch (100M/day) |
|---|---:|---:|
| Peak notifications/s (PAR 3.0) | 173.6 | 3,472.2 |
| **Burst A** — 10M campaign in 15 min on the evening peak | — | **14,583/s** ← the design point |
| Peak API RPS | 104.2 | 2,083.5 |
| Kafka ingress, compressed | 0.12 MB/s | 2.3 MB/s |
| Postgres row-ops/s at peak | 1,481 | 29,611 |
| Est. AWS/month | ~$4,600 | ~$19,700 |
| **Provider fees/month** | ~$123k | **~$2,460,000** |

**Two findings that shape the design:**

1. **Bytes are never the constraint.** Even at 100M/day with a 10M campaign, Kafka ingests ~16.5
   MB/s compressed — one broker could carry it. Partition counts are derived from *how fast a
   consumer can call Twilio* (SMS 80/s, email 200/s, push 300/s), not from throughput.
2. **AWS is 1.2% of total cost of ownership.** A 20% SMS→push down-route saves **$474k/month — 15×
   the entire AWS bill**. The routing engine matters more than broker tuning.

**Bottleneck ladder** (what saturates first): 1. Provider account quotas (~200–500 msg/s, hit at
~17M/day) → 2. Postgres write path → 3. Consumer parallelism → 4. Valkey shard CPU → 5. Pod
scale-from-zero → 6. **MSK brokers**. AWS infrastructure is *sixth*.

---

## 7. Verified facts (reproducible from the repo)

Run `./scripts/capture-verification.sh`; raw output lands in `docs/verification/`.

| Fact | Value |
|---|---|
| `./mvnw verify` | **BUILD SUCCESS**, 384 tests, 0 failures |
| `-Pintegration` | Green (Testcontainers) |
| Schema on PostgreSQL 18.6 | applies clean under `ON_ERROR_STOP=1` |
| Objects created | **120 tables · 6 partitioned parents · 107 table partitions · 260 indexes · 298 check constraints** |
| Flyway on boot | **0 tables → 121** |
| `app-api` startup | ~4–6 s |
| `POST /v1/notifications` | **202** with a notification id |
| Same key + same body | **202, byte-identical response** |
| Same key + different body | **409 `FINGERPRINT_MISMATCH`** |
| Idempotency records after 3 POSTs | **1** |
| `credentials_ref` CHECK vs a pasted API key | **rejected**; a `mock:`/ARN reference accepted |
| Monotonic guard: late `SENT` after `DELIVERED` | **0 rows** |
| Monotonic guard: duplicate webhook | **0 rows** |
| Monotonic guard: `BOUNCED` after `DELIVERED` | applied |
| Monotonic guard: anything after terminal | **0 rows** |
| `promtool check rules` | SUCCESS, 52 rules |

### Figures that are NOT reproducible here

These came from design-time prototypes on synthetic schemas not in the repository. They explain why
the design is shaped this way; they are **not** benchmarks of this code:

- Due-scan `FOR UPDATE` → `SKIP LOCKED` → shard-affine: **159 → 453 → 746 tps**
- Provider success rate raw scan vs rollup: **290.7 ms → 0.015 ms**
- BRIN vs B-tree on an append-only time column: **48 kB vs 120 MB**

---

## 8. Bugs found and fixed — the most useful material

Every one of these passed a green test suite. That is the theme.

### 8.1 The idempotency replay returned HTTP 500

`AcceptResult` had a record component `replay` **and** a method `isReplay()`. Jackson maps a no-arg
`isXxx()` to a boolean property `xxx`, so the serialiser wrote `"replay": false` over the `Replay`
object. Reading the stored body back threw `MismatchedInputException`.

**Effect:** every idempotent replay — the single behaviour the `Idempotency-Key` header exists to
provide — returned 500. **Nine unit tests covered that class and all nine passed**, because every
one asserted on the in-memory object, never on a serialise/deserialise round trip. The record is
written to `idempotency_record.response_body` and read back, so surviving JSON *is* the feature.

Found by POSTing the same key twice against a running server. Fixed by renaming to `hasReplay()`;
`AcceptResultSerializationTest` is the round-trip oracle.

### 8.2 The circuit breaker was a pass-through

`CircuitBreakerProvider.send()` was literally `return delegate.send(command);` and **zero call sites
anywhere fed a Resilience4j breaker**.

Cascade: every breaker permanently `CLOSED` → the router's "filter out open circuits" step could
never remove a candidate → `getFailureRate()` returned `-1` on an empty window, mapped to a success
rate of `1.0`, pinning the dominant term of the provider score to a constant → the health gate could
never fire. A provider failing 100% of calls would have kept winning selection. The only signal that
could remove a provider was a manual chaos injection — which is why the demo worked while the
mechanism did not.

**Why it survived:** a permanently-CLOSED breaker is indistinguishable from "no provider has failed
yet". Every healthy-path test passes. The fix's tests assert on `getState()` *after driving failures
through it* — the one thing a no-op cannot fake.

Root cause was the builder signature: `circuitBroken()` took no argument, so it was possible to have
the decorator present with nothing to record into. It now takes the registry, making the broken
state unexpressible.

### 8.3 Fan-out duplicated every notification row

`RequestFanOut` created a fresh `notification` row per channel — but the accept transaction had
already written one, and *those* ids are what the 202 returned and what the caller polls.

**Effect:** `GET /v1/notifications/{id}` returned `PENDING` **forever** for every notification ever
sent, because the id handed out belonged to a row nothing advanced while a second invisible row did
the work. Row count doubled.

Fixed by adopting the accept-time rows keyed by channel, advancing them to `QUEUED`.

### 8.4 No `@EnableKafka` in `app-worker`

No `KafkaListenerEndpointRegistry`, so **every `@KafkaListener` was silently inert**. The app booted
and reported healthy while consuming nothing.

### 8.5 Flyway never ran

Boot 4's per-technology autoconfig split. `flyway-core` was present, `spring-boot-flyway` was not, so
every `spring.flyway.*` property was inert and `make up` left an empty database.

### 8.6 API authentication was off by default

`spring.profiles.default: local` + the local profile's `permit-all: true` meant a jar launched with
no `SPRING_PROFILES_ACTIVE` came up **unauthenticated**, with the chaos endpoint (which can take any
provider offline for every tenant) live in the base configuration under every profile.

`SecurityConfig` argues at length that a *property* is safer than a *profile* because "a profile is
trivially copied into a production manifest" — and then shipped the insecure combination as the
default. Fixed by removing the default profile and moving `local` into `spring-boot-maven-plugin`,
so the dev command still needs no flags while the packaged artefact starts secure.

### 8.7 A 52-rule alerting file where almost nothing could fire

A Prometheus rule referencing a metric nothing emits does not error — it produces no series and
stays silent, which on a dashboard is indistinguishable from healthy.

Verification corrected the audit: Micrometer appends `_total` to counters and `_seconds` to timers,
so two "mismatches" already lined up. Only two renames were real. 12 metrics genuinely have no
producer and are now marked `NOT-EMITTED` in place, with a coverage table splitting them into *needs
code* vs *needs infrastructure*.

---

## 9. Testing approach

**384 tests**, no Docker required by default; Testcontainers tests are `@Tag("integration")` and run
under `-Pintegration`.

The suite was audited against **test-oracle quality** — the mechanism that decides whether a test
passed:

1. No oracle (only "it didn't throw")
2. Hand-written example (`assertEquals`) — only catches what the author imagined
3. Invariant/property — asserted over many generated inputs
4. Metamorphic — a relation between two runs when you can't compute the expected output
5. Differential — compared against an independent implementation

Strong existing examples: `ProviderContractTest` (2,000 seeded sends across 3 adapters),
`ScheduleJitterTest` (100k samples, all 300 buckets occupied, peak < 2× mean), `RetryBudgetTest`
(8 threads, exact conservation), `OutboxRepositoryTest` (a `CyclicBarrier` so it cannot pass by
accidental serialisation).

**The unclaimed level-5 oracle:** `DeliveryStatus.transitionTo()` in Java and
`applyStatusTransition` in SQL are two independent implementations of the same rule, and nothing
compares them. Running random event sequences through both and asserting they agree is the single
highest-value test still unwritten.

Test names describe failures, not methods — e.g. *"a late SENT does not overwrite DELIVERED"*,
*"you cannot cancel something already queued — enforced by rank, not an if"*, *"reading another
tenant's notification is a 404 and never a 403, because a 403 confirms it exists"*.

---

## 10. Documentation

| Doc | Contents |
|---|---|
| `LEARN-FROM-ZERO.md` | Teaches Kafka/Redis/partitioning from scratch by following one notification through every row and message, then breaking it 8 ways |
| `UNDERSTANDING-THE-DESIGN.md` | Builds the system from a naive `for` loop, breaking it 13 times |
| `CODE-WALKTHROUGH.md` | The code as written, with a "say in an interview" line per component |
| `ARCHITECTURE.md` / `ARCHITECTURE-SUMMARY.md` | Components, flows, all Mermaid diagrams |
| `DATABASE.md` · `KAFKA.md` · `API.md` | Reference |
| `SCALABILITY.md` · `FAILURE-MODES.md` · `OBSERVABILITY.md` · `SECURITY.md` · `RUNBOOK.md` | Operations |
| `STATUS.md` | What is complete / partial / not started |
| `adr/ADR-001..018` | Each with a *Negative consequences* section |
| `diagrams/*.mmd` + `*.png` | 8 diagrams rendered from versioned Mermaid sources |

---

## 11. Known gaps (honest)

**In progress right now** (a parallel agent workflow is running):
1. `notif.scheduled_notification` has no migration — the scheduler's `SKIP LOCKED` claim SQL has
   never executed. Every scheduled send is dead.
2. Six of seven Kafka producers discard the send future then ack, so a broker blip strands
   recipients in `QUEUED` with no retry, no DLQ, no alert.
3. Tenant scoping is missing on `NotificationRecipientRepository` and `DeliveryAttemptRepository`;
   `applyStatusTransition` and `findInWindow` are unscoped.
4. `STATUS.md` needs rewriting from verified facts.

**Known and not yet addressed:**
- `TracedProvider` and `RateLimitedProvider` are still pass-throughs
- Dedup and the sent-token log are in-memory `HashMap`s in the production beans — **idempotency
  breaks at two worker pods**
- The idempotency claim buckets `created_at` to the hour to make `ON CONFLICT` fire, so two requests
  straddling an hour boundary within milliseconds can both claim
- The request fingerprint hashes the re-serialised DTO, not the arriving bytes
- No load-test harness exists (`LOAD-TEST.md` ships an empty results table deliberately)
- `platform-observability` and `platform-security` are package-only
- No Dockerfile, no Kubernetes manifests, no Terraform — Docker Compose only
- Elasticsearch is absent. There is a genuine gap it would fill: **no index on recipient address
  anywhere**, so "did this user get their OTP last Tuesday?" is unanswerable. The sophisticated
  version of that answer notes the conflict with crypto-shredding — index a keyed hash plus
  ciphertext, never plaintext, so destroying the DEK still works.

---

## 12. Deliberate non-goals

- **Multi-region active/active** — needs conflict-free cross-region dedup; warm DR instead (RTO 30
  min, RPO < 5 min)
- **Exactly-once delivery** — the provider HTTP call cannot enlist in a transaction
- **Real vendor integrations** — mocks emulating real semantics, so CI is deterministic and the repo
  needs zero credentials
- **ClickHouse** — deferred behind a rollup table that delivers the query win without a fourth
  datastore
- **A UI** — the API and Grafana dashboards are the interface

---

## 13. Interview framing

**60-second version:**

> A notification platform — SMS, email and push — for 50 million users. A service calls the API; we
> write the notification and an outbox row in a single database transaction and return 202
> immediately, so one commit is the whole guarantee that nothing is lost. Everything after that is
> asynchronous through Kafka, so a campaign to ten million people doesn't block the caller. Workers
> check preferences, render the template, and send through whichever provider is currently
> healthiest and cheapest; if one fails a circuit breaker opens and we fail over. Failures are
> classified before we retry — a socket reset gets exponential backoff with jitter, an invalid
> phone number never gets retried at all. Delivery receipts come back by webhook and go through a
> state machine that only moves forwards, so out-of-order and duplicate callbacks are safe. The
> thing I'd call out is that I split traffic into separate Kafka topics by priority, because
> partitions are strictly FIFO so a priority *field* doesn't actually work — a marketing blast
> would otherwise sit in front of a login OTP.

**Claims the code supports:** the monotonic state machine; the retry engine's three independent
controls (jitter for *when*, budget for *how many*, delay topics for *where they wait*); physically
separated traffic classes; tests that name the failure they prevent; ArchUnit-enforced architecture.

**Claims to avoid:** "exactly-once delivery"; "it scales horizontally" without noting the in-memory
dedup; quoting the 746 tps figure as a benchmark of this code.

**Best single answer to "what would you do differently":** *"My tests were green and the application
didn't boot, because I had no test that proved the wiring. Nine unit tests covered the class whose
serialisation bug made every idempotent replay return 500. The lesson wasn't 'write more tests' —
it was that unit tests with mocked collaborators cannot see integration failures, and I now add a
three-line `contextLoads()` per application before anything else."*
