# Code Walkthrough

A running explanation of the code as it gets written — what each piece does, why it's shaped that
way, and what to say about it. Grows with the repo.

**Prerequisite reading:** [UNDERSTANDING-THE-DESIGN.md](UNDERSTANDING-THE-DESIGN.md) for the
*why*, this document for the *how*.

Progress: **Phase 1 of 11** · 22 Java files · 16 tests green · schema verified on PostgreSQL 18.6

---

## Part 0 — The build

### Why a Maven Wrapper is committed

```
mvnw                            ← the script a reviewer runs
.mvn/wrapper/maven-wrapper.jar  ← 63 KB, committed on purpose
.mvn/wrapper/maven-wrapper.properties
```

`./mvnw verify` works on a machine that has only a JDK. No `brew install maven`, no version
mismatch. Committing a binary jar feels wrong until you realise the alternative is a README step
that half of reviewers skip and the other half do with the wrong version.

**Say in an interview:** *"The wrapper means a reviewer can clone and build with nothing but a
JDK. Requiring a global Maven install is a small friction that costs you a percentage of the
people who would otherwise have run it."*

### Why 11 modules instead of one

```
platform-domain          ← pure Java. No Spring, no JPA, no Kafka.
platform-application     ← use cases, ports
platform-persistence     ← JPA, Flyway
platform-messaging       ← Kafka
platform-provider        ← provider SPI + adapters
platform-resilience      ← retry, circuit breakers
platform-observability   ← metrics, tracing
platform-security        ← authn/z, crypto
app-api / app-worker / app-scheduler   ← the three deployables
```

The dependency direction is one-way: `app-*` → libraries → `platform-domain`. Nothing points back.

This isn't organisational tidiness. It's what makes the domain **unit-testable in milliseconds
without a Spring context**, and it's why the three apps can scale independently — the API scales
with requests, workers with notifications, and those are different numbers.

### The two Spring Boot 4 traps we hit immediately

Both cost a build failure, and neither is discoverable from a design document:

| Trap | Symptom | Fix |
|---|---|---|
| **Testcontainers 2.x renamed every module** | `'dependencies.dependency.version' for org.testcontainers:postgresql is missing` | `org.testcontainers:postgresql` → `org.testcontainers:**testcontainers-**postgresql` |
| **`@EntityScan` moved package** | `package org.springframework.boot.autoconfigure.domain does not exist` | Now `org.springframework.boot.**persistence**.autoconfigure.EntityScan` |

Also worth knowing for Boot 4: Jackson's groupId changed to `tools.jackson`, JUnit is now 6, and
Resilience4j needs the `resilience4j-spring-**boot4**` artifact — using `-spring-boot3` gives
*silent* autoconfiguration failure, which is worse than a compile error.

---

## Part 1 — The domain

### `TrafficClass` — the enum that shapes the whole system

```java
public enum TrafficClass {
    CRITICAL     (Duration.ofSeconds(60), Duration.ofSeconds(5),  "tx",   0),
    TRANSACTIONAL(Duration.ofHours(24),   Duration.ofSeconds(30), "tx",   1),
    BULK         (Duration.ofHours(72),   Duration.ofMinutes(15), "bulk", 2);

    public String dispatchTopic(Channel channel) {
        return "notification.dispatch." + channel.name().toLowerCase() + "." + topicSuffix;
    }
}
```

Notice the enum **computes its own Kafka topic**. That's deliberate: the mapping from "this is an
OTP" to "therefore it goes on this physical topic" is a domain rule, not infrastructure
configuration. Putting it in a YAML file would let someone change it without thinking about why.

`shedOrder` encodes the load-shedding ladder: under pressure, BULK (2) is dropped before CRITICAL
(0) is touched. Again — a rule, expressed as data.

**Say:** *"Traffic class isn't a label, it selects a physical Kafka topic. Kafka partitions are
strictly FIFO, so a priority field on a shared topic doesn't actually work — the consumer still
has to read past ten million bulk records to reach the OTP."*

### `FailureType` — where the retry decision lives

```java
TRANSIENT_NETWORK  (true,  2,  false, false),
RATE_LIMITED       (true,  0,  false, false),
AUTH_FAILURE       (false, 0,  false, false),
INVALID_RECIPIENT  (false, -1, true,  false),
PROVIDER_TIMEOUT   (true,  2,  false, TRUE),   // ← outcome indeterminate
//                  retry  failover  suppress  indeterminate
```

Four independent policy bits per failure, so there is exactly one place that knows what to do:

- `retryable` — could the *same* provider succeed on a retry?
- `failoverAfterAttempts` — `0` = switch now, `-1` = the message is dead, `n` = after n tries
- `shouldSuppressAddress` — deactivate the address permanently
- `isOutcomeIndeterminate` — **only `PROVIDER_TIMEOUT` sets this**

That last flag is the one that matters. It's the difference between "it failed" and "it may have
worked and we didn't hear back". Blind-retrying the second case is how users get three OTPs.

**Say:** *"A single catch-and-retry branch is the most common bug in this class of system.
Retrying an invalid phone number five times burns budget and helps nobody; not retrying a socket
reset loses a message that would have worked. They're indistinguishable unless you classify
first."*

### `DeliveryStatus` — the monotonic state machine

```java
public Optional<DeliveryStatus> transitionTo(DeliveryStatus proposed) {
    if (this.terminal)                  return Optional.empty();
    if (proposed.rank <= this.rank)     return Optional.empty();
    return Optional.of(proposed);
}
```

Nine lines. It is the single most important method in the codebase, because it's what makes
at-least-once delivery safe.

Two rank choices carry business rules that would otherwise be `if` statements:

```
CANCELLED = 28  <  QUEUED = 30
```
"You cannot cancel something already queued" is now enforced by *ordering*. There is no
`if (status == QUEUED) throw` for a future engineer to forget. And the reverse race is also
correct: if CANCELLED commits first it's terminal, so the racing QUEUED is rejected by the
terminal check instead. **Both orderings are safe** — that's the property you want.

```
DELIVERED = 80, isTerminal = FALSE
```
Counter-intuitive, and correct. An SMTP 250 means "accepted", not "landed in the inbox" — a hard
bounce legitimately follows. State machines that treat delivered as final silently drop bounce
events, which means the address never reaches the suppression list, which means you keep mailing
a dead address and your sender reputation degrades.

**Returning `Optional.empty()` rather than throwing is the design choice.** A rejected transition
isn't an error — it's the *expected* outcome for an out-of-order webhook, and it happens
routinely at volume. The caller records it as an unapplied event and moves on.

### The tests are the documentation

```java
@Test
@DisplayName("you cannot cancel something already queued — enforced by rank, not an if")
void cannotCancelAfterQueueing() { ... }

@Test
@DisplayName("DELIVERED is deliberately NOT terminal, because a hard bounce can follow")
void deliveredIsNotTerminal() { ... }
```

Every test name is a production failure. Read `DeliveryStatusTest` and you learn the hardest idea
in the design in about thirty seconds. That's the point — tests that assert `assertEquals(2,
add(1,1))` document nothing.

### `ArchitectureTest` — rules the build enforces

```java
noClasses().should().dependOnClassesThat().resideInAnyPackage("org.springframework..")
    .because("the domain must be unit-testable without a Spring context")
    .check(domainClasses);
```

Five rules: no Spring, no JPA, no Kafka, no Jackson, no `java.util.Date`.

**Architecture that isn't enforced by a test is a wish.** Framework imports leak into a domain
model gradually — one `@Component` at a time — and by the time anyone notices, you can't test the
domain without booting Spring. This fails the build on the first violation.

The `java.util.Date` rule earns its place specifically here: this platform is timezone-critical,
and `Date` has no timezone, silently uses the system default, and is mutable.

---

## Part 2 — The schema

Verified by applying it to a real PostgreSQL 18.6 container:

```
tables               120     (6 partitioned parents + 335 partitions)
indexes              260
check constraints    298
delivery_status      16 rows
```

### `delivery_status` is a table, not an enum

```sql
CREATE TABLE notif.delivery_status (
    code varchar(24) PRIMARY KEY,
    rank smallint NOT NULL UNIQUE,
    is_terminal boolean NOT NULL,
    is_failure  boolean NOT NULL
);
```

Everything else uses `varchar` + `CHECK`. This one is a table because it carries **data the SQL
reads** — the monotonic guard joins against `is_terminal`. A native enum's implicit ordering can't
express "terminal", and a CHECK constraint can't be joined.

The payoff: **adding a status is an `INSERT`.** No DDL, no lock, no deploy coordination.

### Why not native PostgreSQL enums anywhere

Three reasons, all reproduced against 18.6:

1. `ALTER TYPE … ADD VALUE` then using the value **in the same transaction** fails —
   `unsafe use of new value`. Every Flyway migration runs in a transaction.
2. **You can never remove an enum value.** Roll back a bad deploy and the zombie value lives in
   `pg_enum` forever, invisible to your exhaustive Java `switch`.
3. PgJDBC binds `String` as `varchar`, so you need `stringtype=unspecified` on the JDBC URL —
   which silently weakens type checking for *every* column in the app.

`varchar` + CHECK evolves as `DROP CONSTRAINT` → `ADD CONSTRAINT … NOT VALID` (catalog-only,
instant) → later `VALIDATE` (concurrent DML allowed).

### The constraint that blocks a leaked API key

```sql
CONSTRAINT provider_configuration_secret_ck
    CHECK (credentials_ref ~ '^(arn:aws:secretsmanager:|ssm:|mock:)')
```

Tested against a live database:

```
INSERT ... credentials_ref = 'SK1234567890abcdefTHISISAKEY'
ERROR:  violates check constraint "provider_configuration_secret_ck"

INSERT ... credentials_ref = 'mock:sms-primary'
inserted id 2
```

**The database physically refuses to store a secret.** Code review can miss a pasted key; a check
constraint cannot. This is a good thing to demo.

### The monotonic guard, as real SQL

```sql
UPDATE notif.notification n
   SET status = $1, status_rank = $2, status_at = now()
 WHERE n.id = $3
   AND n.created_at >= $4 AND n.created_at < $5          -- partition pruning
   AND n.status_rank < $2                                 -- monotonic
   AND NOT EXISTS (SELECT 1 FROM notif.delivery_status d
                    WHERE d.code = n.status AND d.is_terminal)
RETURNING n.status;
```

Executed against the live container:

| # | Event | Result |
|---|---|---|
| 1 | `DELIVERED` (80) over `SENT` (60) | ✅ applied |
| 2 | **late `SENT` (60) arriving after DELIVERED** | **0 rows — dropped** |
| 3 | duplicate `DELIVERED` webhook | **0 rows — dropped** |
| 4 | `BOUNCED` (85) after DELIVERED | ✅ applied (delivered isn't terminal) |
| 5 | anything after `BOUNCED` (terminal) | **0 rows — dropped** |

One statement. No `SELECT`-then-`UPDATE`, no optimistic-lock retry loop, no application-level
sequencing. Out-of-order, duplicated and illegal events are all handled by the `WHERE` clause.

**Say:** *"Zero rows returned isn't an error — it means the event was stale or duplicated, which
is exactly what I want. That single property is what lets the Kafka consumers be at-least-once and
makes DLQ replay harmless."*

### Partitioning, and the four things it costs you

`notification`, `notification_recipient`, `delivery_attempt`, `notification_event` and
`notification_request` are RANGE-partitioned by day; `idempotency_record` by **hour**.

Hourly idempotency partitions look excessive until you see the alternative: records expire after
24 hours, and `DELETE FROM idempotency_record WHERE expires_at < now()` would create 20 million
dead rows a day that autovacuum has to reclaim. `DROP TABLE` is O(1) and produces none.

What partitioning costs, all of which shows up in the DDL:

1. **Every unique constraint must include the partition key** — hence `PRIMARY KEY (created_at, id)`
   plus a separate `UNIQUE (id, created_at)` to serve lookup-by-id. Consequence to state out loud:
   the database *cannot* enforce global uniqueness of `id`.
2. **No foreign keys on the hot tables.** An FK turns O(1) `DETACH PARTITION` into a validation
   scan, killing the one operation retention depends on. Integrity comes from same-transaction
   writes plus a nightly reconciliation job that emits a metric.
3. **A partition-key `UPDATE` is a silent DELETE+INSERT** at ~3× the WAL cost, and can raise
   error `40001` under concurrency. So rescheduling is an explicit `DELETE` + `INSERT`.
4. **The DEFAULT partition is a trap.** We create one — a missing partition would otherwise be a
   hard outage — but a row landing in it *blocks creation of the real partition* while holding an
   exclusive lock. So it's a safety net with an alarm on it, never a destination.

### The index that isn't there

There is deliberately **no index on `notification.status`**, and this is worth being able to
defend:

Status is updated 3–5 times per notification. PostgreSQL has an optimisation called a **HOT
update** — if no indexed column changed, it updates in place without touching any index. Index
`status` and every transition writes a new row version *plus* an entry in every index. At 80
million updates a day that's roughly 4 GB/day of extra write-ahead log, for a column with 16
distinct values that are 90% the same one.

Instead there are **partial** indexes:

```sql
CREATE INDEX nrec_retry_ix ON notification_recipient (next_attempt_at)
    WHERE status IN ('SEND_FAILED','QUEUED') AND next_attempt_at IS NOT NULL;
```

The predicate is baked into the index, so it covers ~5% of rows at a fraction of the per-row cost.

### The outbox table

```sql
CREATE INDEX outbox_unpublished_ix ON outbox_message (id) WHERE published_at IS NULL;
ALTER TABLE outbox_message SET (fillfactor = 70, autovacuum_vacuum_threshold = 1000);
```

Two details that matter at throughput:

- The partial index stays a few pages forever, because published rows **leave** the index.
- Rows are **DELETEd** after publish, never `UPDATE`d with a timestamp. An update-based design
  makes the table grow without bound and the index with it.

`fillfactor = 70` leaves free space on each page so updates can stay HOT.

---

## What's next

| Phase | Deliverable |
|---|---|
| **2** | JPA entities + repositories + Testcontainers repository tests |
| **3** | The accept path — `POST /notifications`, idempotency with fingerprint, outbox in one transaction |
| **4** | Provider SPI, decorator stack, mock providers with seeded failure injection |
| 5 | Kafka producers/consumers, idempotent receiver |
| 6 | Channel workers, retry engine, DLQ |
| 7 | Scheduler: hydrator + shard-affine claimers |
| 8 | Webhooks, status processor, reconciler |
| 9 | Preferences, templates, suppression |
| 10 | Observability: metrics, dashboards, alert rules |
| 11 | Load tests with measured results |
