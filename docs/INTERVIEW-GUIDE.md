# Interview Guide

Everything in this project, explained so you can say it out loud. Assumes you know Java, Spring
and REST. Assumes you have **not** used Kafka, Redis or Postgres partitioning in anger.

Read top to bottom once. Then re-read §1, §2 and §8 the morning of the interview.

**If the interview moves to "show me the code", go to [§12](#12-walking-through-the-code)** — six
files, in order, with what to say about each. [§13](#13-questions-the-implementation-now-invites)
covers the questions that only come up once someone has actually opened the repository, including
the uncomfortable ones. Have [STATUS.md](STATUS.md) open in another tab.

---

## 1. The 60-second answer

Memorise the shape, not the words.

> "It's a notification platform — SMS, email and push — designed for 50 million users and
> millions of messages a day.
>
> The API accepts a request, writes it to Postgres in a single transaction, and returns `202`
> immediately. All the actual work happens asynchronously through Kafka, so a campaign to ten
> million people doesn't block the caller.
>
> Workers pick messages up, check the user's preferences, render the template, and send through
> whichever provider is currently healthiest and cheapest. If a provider fails, a circuit breaker
> opens and we fail over to the next one. Failures get classified before we retry — a transient
> network error gets exponential backoff with jitter, an invalid phone number never gets retried
> at all.
>
> Delivery receipts come back asynchronously by webhook and get applied through a state machine
> that only moves forwards, so out-of-order and duplicate callbacks are safe.
>
> The main thing I'd call out is that I split traffic into separate Kafka topics by priority —
> because Kafka partitions are strictly FIFO, so a priority *field* doesn't actually work. A
> marketing blast would otherwise sit in front of a login OTP."

That last paragraph is the one that makes you sound like you've built this rather than read about
it. **Always land it.**

## 2. The whiteboard version

Draw these five boxes left to right. Talk while drawing.

```
   Client              API              Kafka            Workers          Providers
     │                  │                 │                 │                 │
     └──── POST ───────►│                 │                 │                 │
                        │                 │                 │                 │
                   [Postgres]             │                 │                 │
                   one transaction        │                 │                 │
                        ├─── publish ────►│                 │                 │
                        │                 ├──── consume ───►│                 │
                   ◄─── 202               │                 ├──── send ──────►│
                                          │                 │                 │
                                          │◄─── webhook ────┼─────────────────┤
                                                            │
                                                       [Postgres]
                                                       status update
```

Five sentences, one per box:

1. **Client** — a service calls us with an idempotency key so retries are safe.
2. **API** — validates, checks the key, and writes the notification plus an outbox row in one
   transaction. Returns `202` immediately.
3. **Kafka** — carries the work. Separate topics per channel and per priority.
4. **Workers** — resolve preferences, render the template, pick a provider, send.
5. **Providers** — call us back later with a delivery receipt.

Then add the three things that go wrong, because that's what they actually want to discuss:
retry, provider failover, and duplicate prevention.

---

## 3. Kafka — what to know

### What it actually is

Kafka is an **append-only log**. Not a queue in the RabbitMQ sense.

- A **topic** is a named log — `notification.dispatch.sms.tx`.
- A topic is split into **partitions**. Each partition is an ordered, immutable sequence.
- Every message has an **offset** — its position in the partition.
- Consumers **track their own offset**. Reading doesn't delete anything.
- A **consumer group** is a set of consumers sharing the work. Kafka assigns each partition to
  exactly one consumer in the group, which is how you get parallelism without duplicates.

The single most important consequence: **messages are not deleted when read.** They stay for the
retention period (we use 3–7 days). You can reset a consumer group's offset and reprocess
everything.

### How to say why you chose it

> "Three reasons. First, replay — if a bad deploy corrupted a day of delivery statuses, I can
> reset the offset and reprocess. RabbitMQ deletes on ack, so that's gone. Second, ordering per
> key — I partition by recipient ID, so all messages for one user go to the same partition and
> stay in order. Third, consumer groups let me scale workers by adding pods without changing
> anything."

### The four things you must be able to explain

**Partitioning and keys.** The key decides the partition: `hash(key) % partitionCount`. We use
`tenantId|recipientId|channel`. Same key → same partition → guaranteed order for that user.

> Follow-up they'll ask: *"What if one tenant is huge?"*
> "That's a hot partition. It's why the key includes recipient ID, not just tenant — with 50
> million recipients spread over 54 partitions the skew is about 0.3%. The real hot-partition
> risks are bugs: keying on tenant alone, or putting a timestamp in the key."

**Consumer lag.** How far behind the consumer is. Our primary scaling signal, and our primary
alert.

> Follow-up: *"So you autoscale on lag?"*
> "Yes, but with a guard. During a *provider* outage, lag climbs because sends are failing — if
> you scale out on that, you add workers that hammer a provider that's already down. So scale-out
> is gated on provider health. Lag is a symptom of the provider being down, not of insufficient
> capacity."
>
> **This answer is worth a lot. Very few candidates have thought about it.**

**Offset commits.** We commit *after* the database write, never before. So the worst case is
redelivery, never loss. Redelivery is safe because consumers are idempotent.

**Rebalancing.** When a consumer joins or leaves, partitions are reassigned. During that,
processing pauses.

> Follow-up: *"How do you avoid rebalance storms?"*
> "The trap is that a consumer stuck in a slow provider call still sends heartbeats — heartbeats
> are on a background thread — so it looks alive right up until `max.poll.interval.ms` expires and
> it gets kicked out. Then it rejoins, gets the same slow message, and loops. The fix is bounding
> every provider call with a timeout well below the poll interval, and lowering
> `max.poll.records`. Raising the poll interval looks like a fix but just makes failover slower."

### Kafka vs RabbitMQ vs SQS

| | Kafka | RabbitMQ | SQS |
|---|---|---|---|
| Replay | ✅ | ❌ deletes on ack | ❌ |
| Ordering | per partition | per queue | FIFO queues only, low throughput |
| Throughput | very high | moderate | high |
| Delayed delivery | ❌ (we build tiers) | ✅ plugin | ✅ up to 15 min |
| Operational cost | high | medium | none |

> "RabbitMQ has nicer delayed-delivery support and I'd pick it for a smaller system. At this scale
> I needed replay and partition-ordered throughput more than I needed a delay plugin — and the
> tiered retry topics gave me delays anyway."

---

## 4. Redis / Valkey — what to know

### What it is

An in-memory key-value store. Sub-millisecond. **Valkey** is the open-source fork of Redis created
after Redis changed its licence in 2024 — same protocol, same clients, BSD licensed. We use it
purely so the repo has a clean licence; everything you know about Redis applies.

### What we store in it, and why

| Use | Structure | Why not Postgres |
|---|---|---|
| Idempotency key cache | `SET key value EX 86400` | Checked on **every** API call — Postgres is the durable record, Redis is the fast path |
| Rate limits | Token bucket in Lua | Thousands of checks/second; a hot Postgres row would serialise every write |
| Circuit breaker state | Shared flag | 40 pods need to agree a provider is down without each discovering it separately |
| Preference cache | Hash per user | 20M reads/day of data that changes once a year |
| Near-term schedule | Sorted set | Sub-millisecond "what's due in the next 5 minutes" |

### The one principle to state

> "Redis is a cache and a coordination store, never the system of record. Everything in it can be
> rebuilt from Postgres. If Redis dies, we degrade — more duplicate provider calls, weaker
> frequency capping, slower idempotency checks — but we never lose data or send something we
> shouldn't have."

> Follow-up: *"What happens if Redis goes down?"*
> "Quota checks **fail open** — I'd rather accept a few extra notifications than reject paying
> customers because a cache is down. Idempotency falls back to Postgres, which is slower but still
> correct. Circuit breakers fall back to per-pod local state, which means more calls to a failing
> provider, but that's acceptable for the length of a failover."
>
> The phrase **"fail open on quota, never fail open on correctness"** is a good one to have ready.

### Why a sorted set for the scheduler

`ZADD due:{shard} <timestamp> <notificationId>` then `ZRANGEBYSCORE` for everything due now.
Sorted by score, O(log n) insert, range query in milliseconds.

But — and say this unprompted — **Redis is not the source of truth for the schedule.** It holds
only the next five minutes, hydrated from Postgres. Redis's default persistence can lose a second
of writes, which at this volume is thousands of scheduled notifications gone with no way to detect
it.

---

## 5. PostgreSQL at scale — what to know

You know SQL. Here are the four things that are different at 100 million rows a day.

### Partitioning

One logical table, many physical tables underneath, split by a column.

```sql
CREATE TABLE notification (...) PARTITION BY RANGE (created_at);
-- one child table per day
```

**Why:** deleting 90-day-old data becomes `DROP TABLE` — instant, zero cleanup. A
`DELETE FROM notification WHERE created_at < …` on 20 million rows creates 20 million dead rows
that the vacuum process then has to clean up, which at this volume never catches up.

> "Retention by dropping a partition is O(1). Retention by DELETE is O(rows) and generates dead
> tuples faster than autovacuum can reclaim them."

### Why there are no foreign keys on the big tables

> "A foreign key turns detaching a partition into a validation scan across the referencing table.
> That kills the one operation retention depends on. So the six high-volume tables have no FKs —
> integrity comes from always writing parent and child in the same transaction, plus a nightly
> reconciliation job that emits a metric. All the configuration tables have full foreign keys,
> where they're free and catch real bugs."

Say the reconciliation part. Without it, "no foreign keys" sounds careless.

### Why the `status` column isn't indexed

Counter-intuitive, and a great thing to be asked about.

> "Status is updated three to five times per notification. Postgres has an optimisation called
> HOT updates where, if no indexed column changed, it can update in place without touching the
> indexes. Index the status column and you forfeit that — every status change writes a new row
> version plus an entry in *every* index. At 80 million updates a day that's gigabytes of extra
> write-ahead log for a column with fifteen distinct values, ninety percent of them the same one.
> So I use partial indexes instead — one on just the retry-eligible rows, which is about 5% of the
> table and costs 3.6 bytes per row instead of 40."

### `SELECT ... FOR UPDATE SKIP LOCKED`

The standard pattern for "many workers, one work queue in a database". Each worker locks the rows
it claims; `SKIP LOCKED` means other workers step over locked rows instead of waiting.

> "I benchmarked three approaches. Plain `FOR UPDATE` gave 159 transactions a second — no errors,
> it just serialises, because every worker walks the same index in the same order. `SKIP LOCKED`
> got it to 453. Giving each worker its own set of shards so they never look at the same rows got
> it to 746, at a quarter of the latency."

**Having measured it is the point.** Numbers you generated yourself are the single most credible
thing you can bring.

---

## 6. The patterns — say these correctly

### Transactional outbox

**Problem:** `save to DB` then `publish to Kafka` are two operations with no shared transaction.
Crash in between and you've accepted a notification that will never be sent.

**Solution:** write the notification and an `outbox_message` row in the *same* transaction. A
separate process reads unpublished outbox rows and publishes them.

> "The commit is the single atomic decision. Either both rows exist or neither does. I also
> publish immediately after the commit as a fast path, and the sweeper becomes a safety net that
> only catches what the fast path missed — so it's fast when healthy and correct when not."

### Idempotency

**Problem:** clients retry. Load balancers retry. You get the same request twice.

**Solution:** an `Idempotency-Key` header. Store it with a hash of the request body.

> "The part people miss is the body fingerprint. Same key with the same body is a replay — return
> the original response. Same key with a *different* body is a `409`, because returning the first
> response would be silently answering a question they didn't ask."

### Circuit breaker

Three states: **closed** (normal), **open** (failing, don't call), **half-open** (probing).

> "It opens after 20 calls with more than 50% failures in a 60-second window. Two details worth
> mentioning: the state is shared through Redis, so forty pods don't each independently discover
> the provider is dead — that's forty times the damage. And a 4xx never trips the breaker; a
> malformed payload is *our* bug, and opening the circuit on it would take a healthy provider
> offline."

### Exponential backoff with full jitter

> "Retry after 1s, 2s, 4s, 8s — but the crucial bit is jitter. If 100,000 messages fail at the
> same instant because a provider went down, plain exponential backoff means all 100,000 retry at
> exactly t+2 seconds and kill it again the moment it recovers. So the delay is `random(0,
> backoff)`, not `backoff`. That's called full jitter."

### Retry budget

> "On top of backoff I cap retries at about 10% of total calls, using a token bucket. AWS's
> guidance is that a five-deep call stack with three retries per layer amplifies load on the
> failing dependency 243 times. And retries only actually recover about 1.5% of deliveries —
> Segment published that number — so an unbounded retry budget buys very little and can prevent
> recovery."

### Idempotent consumer

> "Every Kafka message carries an event ID. Before processing, the consumer checks whether it's
> already handled that ID. That's what makes at-least-once delivery safe — redelivery is a
> non-event."

---

## 7. Kubernetes and AWS — the minimum

You do not need to be a Kubernetes expert. You need these:

| Term | One line |
|---|---|
| **Pod** | A running instance of your container |
| **Deployment** | Manages N identical pods; handles rolling updates |
| **HPA** | Horizontal Pod Autoscaler — adds pods based on CPU or a custom metric |
| **KEDA** | Like HPA, but scales on external signals — for us, **Kafka consumer lag** |
| **Readiness probe** | "Can this pod take traffic?" Fails → removed from load balancing |
| **Liveness probe** | "Is this pod broken?" Fails → **restarted** |

One detail worth volunteering:

> "Readiness checks Kafka and database connectivity, but liveness deliberately doesn't. If
> Postgres blips and liveness depends on it, Kubernetes restarts every pod simultaneously — you've
> turned a brief database issue into a full outage."

AWS mapping: **EKS** = Kubernetes, **MSK** = managed Kafka, **RDS** = managed Postgres,
**ElastiCache** = managed Redis, **S3** = object storage, **Secrets Manager** = credentials.
Multi-AZ means replicas in three data centres in one region.

---

## 8. The 15 questions you will be asked

**Q: How do you prevent duplicate notifications?**
> "Five layers. Idempotency key at the API. Event ID deduplication on Kafka consume. A business
> dedup key so the same logical notification submitted through two paths only sends once. A unique
> constraint on attempt number. And ideally a token the *provider* deduplicates on.
>
> But I'd be honest in a design review: none of the major providers offer a usable client
> idempotency key. Twilio doesn't have one, and you can't even query them to ask whether your
> message was sent — their API has no client-reference field. So exactly-once is not achievable,
> and I don't claim it. Transport is at-least-once, dispatch is idempotent, and I measure the
> duplicate rate rather than pretending it's zero."

**Q: What happens when a provider goes down?**
> "The circuit breaker opens after 20 calls at over 50% failure. State is shared through Redis so
> every pod knows immediately. Traffic shifts to the next provider by score — health, latency,
> cost, remaining rate limit. After 30 seconds we send probe requests; if they succeed the circuit
> closes and traffic returns to the cheaper provider. If *every* provider for a channel is down,
> messages go to a retry tier and we page someone."

**Q: How do you handle a provider timing out after it already delivered?**
> "That's the hardest case. I model it as an explicit `UNKNOWN` state rather than forcing it into
> sent-or-failed, and I never blind-retry from there. Before calling the provider I commit a
> `PENDING` attempt row, so a crash leaves a visible record instead of a silent gap. Then it
> resolves through the delivery webhook, or by querying the provider where that's possible.
>
> For SMS specifically I chose at-most-once, because a duplicate OTP costs real money and real
> trust and Twilio can't be queried. For email and push I retry, because duplicates are cheap and
> reconciliation works."

**Q: How do you make sure an OTP isn't stuck behind a marketing campaign?**
> "Separate Kafka topics, separately scaled. My first instinct was a priority field, but a Kafka
> partition is strictly FIFO — the consumer physically has to read past ten million records to
> reach the OTP. A priority field on a shared topic is a lie. So `dispatch.sms.tx` and
> `dispatch.sms.bulk` are different topics with different consumer groups."

**Q: How do scheduled notifications work?**
> "Three tiers. Postgres holds the durable schedule, partitioned by due time. A leader-elected
> process scans it and pushes the next five minutes into a Redis sorted set. Worker pods read from
> Redis, each owning its own shards so they never contend.
>
> Two safety details: claims are leases with a 60-second expiry, so a dead worker's rows get
> reclaimed. And there's a claim counter — if a row has been claimed six times it's killing a
> worker every time, so it goes to the dead-letter queue instead of crash-looping a pod forever."

**Q: What about timezones?**
> "Quiet hours are stored as a time plus an IANA zone, and evaluated in the user's zone at
> dispatch time. The thing that surprised me is that there are 37 distinct UTC offsets in force at
> any moment, not 24 — and some are at :45 past the hour, like Nepal at UTC+05:45. So hourly
> batching is wrong and half-hourly is also wrong; you need 15-minute granularity."

**Q: How do you handle out-of-order webhooks?**
> "Every status has a rank, and the update only applies if the new rank is higher. A late 'sent'
> arriving after 'delivered' matches zero rows and gets dropped. Zero rows returned isn't an error
> — it means stale or duplicate, which is exactly what I want, and it's what makes the whole
> pipeline safe to replay."

**Q: How would you scale this from 1 million to 50 million users?**
> "The interesting answer is that the first bottleneck isn't mine. Provider accounts cap out
> around 200 to 500 messages a second, which you hit at about 17 million a day — long before any
> AWS limit. After that it's the Postgres write path, then consumer parallelism, then Redis, then
> pod startup time, and Kafka brokers are sixth on the list."

**Q: What's the biggest weakness of your design?**
> "Two. It's single-region with warm standby, so a region failure costs up to five minutes of
> accepted-but-unreplicated notifications. Active/active would fix it but needs cross-region
> deduplication, and I judged that not worth the complexity here.
>
> And I use mock providers rather than real vendor integrations. That's deliberate — it makes the
> repo runnable with no credentials and CI deterministic — but it does mean no real vendor SDK is
> proven end-to-end. I mitigated it with a contract test suite that any real adapter has to pass."

**Q: How do you know it works?**
> "SLIs with targets. 99.95% API availability, p99 accept latency under 250ms, p99 OTP dispatch
> under 5 seconds, and 99.999% acceptance durability — meaning anything we return 202 for reaches
> a terminal state or sits visibly in the dead-letter queue.
>
> Alerts are on error-budget burn rate, not thresholds. 'CPU is 81%' isn't worth waking someone
> for; 'we're consuming the monthly error budget 14 times faster than sustainable' is."

**Q: Have you actually run it?**
> "Yes — Docker Compose brings up Kafka, Postgres, Valkey and the three services. There's a chaos
> endpoint that takes a provider down so you can watch the circuit open and traffic fail over in
> about two seconds. Load tests run with k6 and the results in the repo are measured, not
> estimated."

**Q: Why three services rather than one, or ten?**
> "They have genuinely different scaling profiles. The API scales with requests. Workers scale
> with notifications, which is a different number entirely, and they burst 250× during campaigns.
> The scheduler is fixed at three and never scales. Ten services would mean network hops between
> things that always change together."

**Q: What would you do differently?**
> "I'd move the delivery-attempt table out of Postgres earlier. It's 48% of write operations at
> full scale and it's append-only, never joined, only ever queried by notification ID — it belongs
> in a columnar store. I kept it in Postgres so the project runs on one database, but I documented
> the exact threshold where you'd move it."

**Q: How does the team fit in?**
> "The API contract is OpenAPI and committed to the repo, so frontend and QA can generate clients
> and mocks without waiting for me. Grafana dashboards and alert rules are in the repo as code, so
> SRE reviews them like any other change. The mock providers are deterministic, so QA gets
> reproducible failure scenarios instead of flaky vendor tests."

**Q: What's the one design decision you'd defend hardest?**
> "Postgres is the system of record and Kafka is transport, not the other way round. It's tempting
> to treat the event log as the truth, but then reconstructing state means replaying a topic, and
> your status API, your dead-letter queue and your audit trail can disagree. Every state
> transition lands in Postgres, so there's one consistent story."

---

## 9. Requirement checklist

They gave eight requirements. Be able to point at each.

| Requirement | Your answer |
|---|---|
| API receives requests | `POST /v1/notifications`, validated, idempotency-keyed, returns `202` |
| Immediate or scheduled | Immediate goes straight to Kafka; scheduled goes to a partitioned table with a three-tier scheduler |
| Multiple providers | Provider interface with a registry; adding one is a single class plus two config rows |
| Providers can fail | Circuit breaker plus scored failover across candidates |
| Retry | Failure classification, then full-jitter backoff across five tiered delay topics, then DLQ |
| Millions/day | Modelled at 5M and 100M/day, sized for a 14,583/sec burst, 288 Kafka partitions derived from consumer throughput |
| Delivery status | 12-state lifecycle, signed webhooks, monotonic state machine, status and attempts APIs |
| Horizontally scalable | Three stateless services, KEDA on consumer lag, documented scaling ladder |

---

## 10. Delivery

**Do:**
- Draw before you talk. Five boxes.
- Give numbers you measured. "159 versus 746 transactions a second" beats "it's faster."
- Name what you didn't build, and why.
- Say "I don't know, here's how I'd find out" rather than guessing.

**Don't:**
- Claim exactly-once delivery. It's the fastest way to lose a senior interviewer.
- List technologies. Justify choices.
- Present it as finished. Say what's next: real provider integration, moving attempts to a
  columnar store, multi-region if the business needs it.

**If you're asked something you genuinely don't know:**

> "I haven't hit that. My instinct would be X, but I'd want to measure before committing —
> I benchmarked the scheduler claim strategies for exactly that reason and the naive version
> was 4.7 times slower than I expected."

That answer is stronger than a confident wrong one, and it's true.

---

## 11. Terms, in one line each

| Term | Meaning |
|---|---|
| **Idempotent** | Doing it twice has the same effect as once |
| **At-least-once** | May arrive more than once, never zero times |
| **Exactly-once** | Not achievable across a network to a third party. Don't claim it |
| **Consumer lag** | How many messages a consumer is behind |
| **Partition** | An ordered slice of a Kafka topic; the unit of parallelism |
| **Offset** | A message's position in a partition |
| **Backpressure** | Slowing intake because downstream can't keep up |
| **Circuit breaker** | Stop calling a service that's clearly failing |
| **Bulkhead** | Isolate resources so one failure can't consume them all |
| **Head-of-line blocking** | One stuck message holds up everything behind it |
| **Thundering herd** | Many clients retrying at the same instant |
| **Metastable failure** | The system's own recovery mechanism sustains the outage |
| **SLI / SLO** | What you measure / the target for it |
| **Error budget** | How much you're allowed to miss the SLO by |
| **Dead letter queue** | Where messages go when retries are exhausted |
| **Outbox** | A table written in the same transaction, published separately |
| **HOT update** | Postgres updating in place because no indexed column changed |
| **Write amplification** | One logical write causing many physical writes |
| **Crypto-shredding** | Deleting data by destroying its encryption key |

---

## 12. Walking through the code

If the interview moves to "show me", this is the tour. **Six files, twelve minutes, in this order.**
Each one earns its place because it contains a decision, not just an implementation.

Have [STATUS.md](STATUS.md) open in another tab. You will be asked what is finished, and the answer
is better delivered than extracted.

> **Open with the honest framing, before they find it themselves:**
>
> *"The domain, the schema, the resilience machinery, the Kafka topology and the scheduler are built
> and tested — 392 tests green. What isn't built is the adapter layer between the API and the
> application module, so `app-api` compiles and its slice tests pass but it can't actually boot. I'd
> rather tell you that up front than have you find it."*
>
> That sentence costs you nothing and buys you the rest of the conversation.

---

### File 1 — `DeliveryStatus.java`

`platform-domain/src/main/java/dev/gaurav/notification/domain/enums/DeliveryStatus.java`

**Why first:** it is nine lines, it is the hardest idea in the design, and it is pure Java with no
framework. You can read it out loud.

```java
public Optional<DeliveryStatus> transitionTo(DeliveryStatus proposed) {
    if (this.terminal)                  return Optional.empty();
    if (proposed.rank <= this.rank)     return Optional.empty();
    return Optional.of(proposed);
}
```

**Say:**

> *"This is what makes at-least-once delivery safe. Every status has a rank, and a transition only
> succeeds if it moves forwards and the current state isn't terminal. So a late `SENT` arriving after
> `DELIVERED` is rejected, a duplicate webhook is rejected, and I don't need any application-level
> sequencing."*

Then point at the two rank choices that are business rules:

> *"`CANCELLED` is 28 and `QUEUED` is 30, so 'you cannot cancel something already queued' is enforced
> by ordering rather than by an `if` statement someone can forget. And both race orderings are safe —
> if `CANCELLED` commits first it's terminal, so the racing `QUEUED` is rejected by the terminal
> check.*
>
> *And `DELIVERED` is rank 80 but deliberately **not** terminal, because an SMTP 250 means 'accepted',
> not 'landed in the inbox'. A hard bounce legitimately follows. State machines that treat delivered
> as final silently drop bounce events, the address never reaches the suppression list, and your
> sender reputation degrades."*

Finish with the return type:

> *"It returns `Optional.empty()` rather than throwing, because a rejected transition isn't an error —
> it's the expected outcome for an out-of-order webhook, and that happens routinely at volume."*

**Then open `DeliveryStatusTest` next to it** and scroll the display names:

```
"you cannot cancel something already queued — enforced by rank, not an if"
"DELIVERED is deliberately NOT terminal, because a hard bounce can follow"
"a late SENT does not overwrite DELIVERED"
```

> *"Every test name is a production failure. Reading this file teaches you the design in thirty
> seconds — that's what I want from a test suite."*

**Likely follow-up:** *"What if you need a status that isn't monotonic?"*
> *"Then the model is wrong and I'd want to know why before adding an escape hatch. The one that
> looks non-monotonic is `DELIVERED` → `BOUNCED`, and that's handled by rank, not by an exception."*

---

### File 2 — `V1__baseline.sql`, three excerpts

`platform-persistence/src/main/resources/db/migration/V1__baseline.sql`

**Why second:** it grounds everything. And you verified it against a real container, which is worth
saying.

> *"120 tables — six partitioned parents and 335 partitions — 260 indexes and 298 check constraints.
> I applied it to a real PostgreSQL 18.6 container and verified the behaviour, not just that it ran."*

**Excerpt A — the secret ban.** Search for `credentials_ref`:

```sql
CONSTRAINT provider_configuration_secret_ck
    CHECK (credentials_ref ~ '^(arn:aws:secretsmanager:|ssm:|mock:)')
```

> *"The database physically refuses to store a secret. I tested it — inserting a fake Twilio key
> fails, inserting `mock:sms-primary` succeeds. Code review can miss a pasted key; a check constraint
> can't."*

**Excerpt B — the index that isn't there.** Search for `nrec_retry_ix`:

```sql
CREATE INDEX nrec_retry_ix ON notification_recipient (next_attempt_at)
    WHERE status IN ('SEND_FAILED','QUEUED') AND next_attempt_at IS NOT NULL;
```

> *"There's deliberately no index on `notification.status`. Status changes three to five times per
> notification, and Postgres has HOT updates — if no indexed column changed, it updates in place
> without touching any index. Index `status` and every transition writes a new row version plus an
> entry in every index. At 80 million updates a day that's roughly 4 GB of extra WAL, for a column
> with 16 values that are 90% the same one.*
>
> *Instead there are partial indexes, where the predicate is baked in, so this one covers about 5% of
> rows."*

**Excerpt C — the monotonic guard as real SQL.** Show it, then the results table:

| # | Event | Result |
|---|---|---|
| 1 | `DELIVERED` (80) over `SENT` (60) | applied |
| 2 | **late `SENT` (60) after DELIVERED** | **0 rows — dropped** |
| 3 | duplicate `DELIVERED` webhook | **0 rows — dropped** |
| 4 | `BOUNCED` (85) after DELIVERED | applied |
| 5 | anything after `BOUNCED` | **0 rows — dropped** |

> *"One statement. Out-of-order, duplicated and illegal events are all handled by the `WHERE` clause,
> and zero rows returned isn't an error — it means the event was stale, which is exactly what I want.
> That single property is what lets the Kafka consumers be at-least-once and makes DLQ replay
> harmless."*

**Likely follow-up:** *"Why is `delivery_status` a table when everything else is varchar + CHECK?"*
> *"Because the SQL reads it. The guard joins against `is_terminal`, and you can't join a CHECK
> constraint. The payoff is that adding a status is an INSERT — no DDL, no lock, no deploy
> coordination."*

---

### File 3 — `ProviderDecoratorChain.java`

`platform-provider/src/main/java/dev/gaurav/notification/provider/decorator/ProviderDecoratorChain.java`

**Why third:** it is the design pattern question and the resilience question in one file, and the
Javadoc argues each ordering choice.

```
Traced → Metered → CircuitBreaker → RateLimited → Timeout → Idempotent → adapter
```

> *"The builder assembles this order regardless of what order the builder methods are called in.
> That's the whole reason the class exists — decorator order has observable consequences, and someone
> wiring it by hand gets it subtly wrong once and nobody notices for six months."*

Pick **two** boundaries. Do not recite all five.

> *"Metered sits **outside** the circuit breaker, because a short-circuited call is an outcome the
> caller experienced. If you measure inside the breaker, an open circuit looks like zero traffic and
> a hundred percent success rate — the dashboard says the provider is perfectly healthy at the exact
> moment you've stopped calling it.*
>
> *And Timeout sits **above** Idempotent, because the token has to be written before the clock can
> cut the call off. A timed-out call is precisely the case where that record is the only evidence a
> send ever happened."*

**Then be honest, unprompted:**

> *"Three of the six are pass-throughs right now — circuit breaker, rate limiter and tracing. The
> components they'll delegate to are built and tested; the breaker is real in `platform-resilience`
> and the worker's router already filters open circuits out of the candidate list, so failover works
> today, just not through the decorator. I left the empty stages in the chain deliberately, because
> inserting a stage into an existing chain later is where the ordering bug gets introduced."*

**Likely follow-up:** *"Why not use Spring AOP for this?"*
> *"An aspect would hide the ordering, and the ordering is the design. I want a reader of this class
> to see the sequence and the reasoning in one place."*

---

### File 4 — `TimeoutProvider.java`

`platform-provider/src/main/java/dev/gaurav/notification/provider/decorator/TimeoutProvider.java`

**Why fourth:** it is the best single example of "a bug in one layer becomes an outage in another",
which is the thing senior interviewers are listening for.

```java
} catch (TimeoutException e) {
    inFlight.cancel(true);
    return new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT,
            "no response from " + code() + " within " + budget.toMillis() + "ms", …);
}
```

> *"An HTTP client's socket timeout doesn't cover connection-pool acquisition, DNS or TLS, so a
> 'five second read timeout' routinely becomes a ninety-second call. On a Kafka consumer that blows
> through `max.poll.interval.ms`. The group rebalances, the in-flight batch is redelivered, and now
> one slow provider is a cluster-wide stall **plus** duplicate sends. That's the failure this class
> exists to prevent."*

Then the return type, which is the real point:

> *"It returns `Indeterminate`, not a failure. The request was on the wire and the provider may well
> have delivered it. Reporting that as a plain failure is what produces three one-time passcodes for
> one login."*

Two details if there is time:

> *"Never `ForkJoinPool.commonPool` — its width is `availableProcessors() - 1`, which on a two-vCPU
> container is one thread, shared with every parallel stream in the JVM. And the queue is
> deliberately tiny: if the pool is full I shed to the next provider rather than parking the calling
> thread, because parking hands the provider's backlog straight to my Kafka consumer."*

**Likely follow-up:** *"What if the adapter throws instead of returning?"*
> *"Then it's an adapter bug, and I rethrow it wrapped so the worker DLQs it loudly. The SPI forbids
> throwing for a business failure — converting an NPE into a retryable result would hide it forever
> and retry a bug five times."*

---

### File 5 — `ProviderCircuitBreakerConfiguration.java`

`platform-resilience/src/main/java/dev/gaurav/notification/resilience/circuitbreaker/ProviderCircuitBreakerConfiguration.java`

**Why fifth:** this is the answer most candidates do not have. Everyone can describe a circuit
breaker. Almost nobody has thought about what half-open does across a fleet.

```java
Duration jitteredWaitDuration(Duration base) {
    long baseMillis = base.toMillis();
    return Duration.ofMillis(baseMillis + jitterSource.nextLong(baseMillis));
}
```

> *"`permittedNumberOfCallsInHalfOpenState` is enforced inside a single JVM. Nothing in Resilience4j
> coordinates across pods.*
>
> *So a provider outage fails calls on all forty worker pods at roughly the same second. All forty
> breakers open at roughly the same second. Thirty seconds later all forty go half-open at roughly the
> same second and each admits its three probes — so the recovering provider takes **120 synchronised
> probes** instead of the three the config appears to promise. Enough of them fail, all forty re-open
> together, and you've built a thirty-second oscillator that keeps the provider down.*
>
> *The configured number is off by a factor of the fleet size, and the fleet size is exactly what
> grows during an incident.*
>
> *The fix here is the cheap one: each JVM adds a random offset up to the base wait, drawn once at
> startup. Forty pods then probe spread across a thirty-to-sixty second band. It's `SecureRandom`
> specifically so identical pods rolled from one image don't draw identical offsets — a seeded PRNG
> would reproduce the very synchronisation it exists to break."*

Then the 4xx rule, from `FailureClassifier`:

> *"And a 4xx never trips the breaker. A malformed phone number is our bug — the provider answered
> correctly and quickly. Count it and one bad campaign drives the failure rate to a hundred percent,
> opens the circuit, and takes a healthy provider offline for every other tenant. The metric that
> should say 'your payloads are wrong' instead says 'Twilio is down'.*
>
> *The two exceptions are `AUTH_FAILURE` and `QUOTA_EXCEEDED`. They arrive as 4xx but they're
> account-scoped, not message-scoped — every call with revoked credentials will fail, so opening the
> circuit is exactly right."*

**Likely follow-up:** *"Why not share breaker state through Redis?"*
> *"That's the longer-term answer for state sharing, and I considered it. It doesn't by itself
> desynchronise the probe *instant*, and it puts a dependency on the failure path — the path most
> likely to already be degraded. The jitter is cheaper and it fixes the specific problem."*

---

### File 6 — `AcceptNotificationUseCase.java`

`platform-application/src/main/java/dev/gaurav/notification/application/usecase/AcceptNotificationUseCase.java`

**Why last:** it ties the whole thing together, and it is where the delivery contract lives.

Walk the five steps in order:

```java
var claim = idempotencyStore.claim(tenantId, key, command.requestFingerprint());   // 1
enforceQuota(tenantId, command.recipientCount());                                  // 2
var records = acceptanceWriter.persist(command, requestId, acceptedAt, expiresAt); // 3
idempotencyStore.complete(tenantId, key, 202, serializer.serialize(result));       // 4
publishBestEffort(command, records);                                               // 5
```

> *"Step three is the whole contract. The request row, one notification per channel and the outbox
> row commit together. Before that commit nothing happened and the client's retry is free; after it,
> the message will be delivered even if this pod dies on the next instruction. There's no in-between
> state to reconcile, which is why the API can be stateless and lose zero in-flight work on a rolling
> deploy."*

Then the two error paths:

> *"The claim carries a **request fingerprint** — a SHA-256 of the canonical body. Same key with a
> different body is a 409, never a silent replay of an unrelated response. That's the check most
> implementations skip.*
>
> *And the quota **fails open**. If the limiter is unreachable I admit the request, because a Valkey
> outage that reads as 'every tenant is over quota' turns a degraded cache into a total outage of the
> send API."*

Then the publish:

> *"The Kafka publish is after the commit and it's best-effort. It's a latency optimisation, not the
> hand-off — the row is already durable, so a broker outage costs about two seconds until the outbox
> sweeper notices, and nothing else. Letting a publish failure propagate would turn a committed
> accept into a 500, and then the client's retry would replay a response we never actually sent
> them."*

Finally, what it deliberately does not do:

> *"No preference resolution, no template rendering, no provider selection. Two reasons. Each of
> those is a dependency that can be slow or down, and none of them should be able to fail
> `POST /notifications`. And they're only *correct* at dispatch time — quiet hours evaluated at accept
> time freeze an answer for a send that happens next week, and a template pinned now renders with the
> typo the tenant fixed yesterday."*

**Likely follow-up:** *"What if step 4 fails after step 3 committed?"*
> *"The claim stays `IN_PROGRESS` and expires with its lock, so a retry gets `request-in-progress` and
> then a clean run. I deliberately don't release the claim on the failure path — that means writing to
> the idempotency store on the path most likely to be failing."*

---

### The two-minute version, if they only want one file

Open **`DeliveryStatus.java`** and its test. Nine lines, no framework, and it contains the hardest
idea in the design.

If they want one *system* file instead, open **`ProviderCircuitBreakerConfiguration.java`** — the
half-open stampede is the answer almost nobody has.

---

### Files to have ready but not lead with

| File | If they ask about |
|---|---|
| `TopicPartitions.java` | Kafka sizing — the formula and every consumer rate are in the Javadoc |
| `PartitionKeys.java` | Hot partitions — "every real hot partition is a bug or an adversary" |
| `BackoffStrategy.java` | Retry — the FULL vs `backoff ± noise` distinction |
| `RetryBudget.java` | Retry amplification — 3⁵ = 243, and Segment's 1.5% figure |
| `RetryTier.java` / `RetryTierListener.java` | Why not `Thread.sleep()` — KIP-62 background heartbeats |
| `ShardAffineClaimer.java` | The 746 vs 159 tps measurement |
| `DueScanHydrator.java` | Why leader-elect the scan — the B·W²/2 bloat term |
| `FailureInjector.java` | Testing strategy — seeded per-message RNG, log-normal latency |
| `WebhookSignatureVerifier.java` | Security — constant-time comparison, signed timestamp |
| `ProblemType.java` | API design — RFC 9457, and why two 400s and three 409s |
| `HealthWeightedSelectionStrategy.java` | Routing — the capped exploration draw, cold start at 1.0 |

---

### Things to avoid on the tour

- **Do not open a DTO or a config class.** They contain no decision.
- **Do not scroll.** Have the file open at the method you are going to read.
- **Do not read Javadoc aloud verbatim.** Say it in your own words; the Javadoc is there so they can
  check afterwards.
- **Do not claim a file is finished when it is a stub.** They will open the file.
- **Do not tour all eleven modules.** Six files. Twelve minutes. Then stop and let them drive.

---

## 13. Questions the implementation now invites

Section 8 covers the design questions. These are the ones that only come up once someone has looked
at the code.

**Q: Your `CircuitBreakerProvider` is a pass-through. Is the circuit breaker actually built?**
> "The breaker is built and tested — `ProviderCircuitBreakerConfiguration`, `ProviderCircuitBreakers`
> and `FailureClassifier`, 58 tests in that module. What's not wired is the decorator. Failover works
> today because the worker's `ChannelProviderRouter` filters open circuits out of the candidate list
> before scoring, so the routing path uses the breaker; the per-call short-circuit doesn't. I kept the
> empty decorator in the chain so the ordering doesn't move when it's wired."

**Q: `app-api` has three ports with no implementations. Why did you ship it like that?**
> "I wouldn't call it shipped. The controllers, the RFC 9457 error handling, the interceptors and the
> webhook verifier are real and tested against mocked ports — 29 tests. What's missing is the adapter
> layer that maps DTOs to commands and translates exceptions, plus a raw-webhook store the schema
> doesn't have a table for. That's a design step rather than build repair, and the four use cases
> behind it exist and have 33 tests of their own. It's the first thing I'd finish."

**Q: How do you test failure injection deterministically? Isn't that flaky by definition?**
> "The RNG is re-seeded per call from `(base seed, provider, recipient, attempt)` rather than being one
> shared stream. So the outcome of a send is a pure function of its identity — independent of thread,
> of ordering, of how many messages are in flight. That's what makes 'exactly three of these reach the
> DLQ' a legitimate assertion under sixteen threads.
>
> The attempt number is in the key on purpose. Without it a retry draws the same fate forever, so a
> transient failure could never recover and the retry engine would be untestable."

**Q: Why is provider latency log-normal rather than uniform?**
> "Because real latency is long-tailed and a uniform draw has essentially no tail. It never populates
> a realistic p99 bucket and never trips a latency-based breaker — so every latency test passes and
> every latency alert is untested. I solve mu and sigma from the two numbers an operator actually
> knows, the median and the p99."

**Q: You have 392 tests. What are they actually testing?**
> "Mostly behaviour under failure rather than happy paths. The contract tests assert that every mock
> adapter maps every failure profile to a distinct vendor code and that a timeout produces
> `Indeterminate` rather than `Rejected`. The domain tests assert the state machine's rejection cases.
> `MonotonicGuardIntegrationTest` runs the real SQL against a real Postgres container.
>
> What I don't have is an end-to-end test, because `app-api` can't boot. That's the honest gap."

**Q: Your default build skips the Testcontainers tests. Isn't that hiding failures?**
> "The integration tests are tagged and excluded by default so a clean clone builds green without a
> Docker daemon. `-Pintegration` runs everything — 392 instead of 371. The reason for the tag is that
> the container starts in a static initialiser, so without Docker all four subclasses die with
> `NoClassDefFoundError` before any test method runs, and the whole reactor aborts. Tag filtering
> happens at discovery, so an excluded class is never initialised."

**Q: You claim adding a provider is one class and two config rows. Prove it.**
> "The registry takes a `List<NotificationProvider>` from Spring, so there's no registration step —
> that's the property that makes it true. The class is the adapter plus its error mapping, the rows
> are a `provider_configuration` row and a webhook secret. `ADDING-A-PROVIDER.md` walks it against the
> real SPI with a Twilio adapter.
>
> What I can't claim is that I've *done* it — there's no real adapter in the repo, deliberately."

**Q: What's the worst decision in here?**
> "Probably letting nine modules be built in parallel without a shared integration point. It's why
> `app-api` and `platform-application` both have an `AlreadyDispatchedException` and a `TemplateRef`,
> and why the ports never got connected. Every module is internally coherent and two of them don't
> talk to each other.
>
> The fix isn't hard, but the *reason* it happened is worth naming: I optimised for parallel progress
> and paid for it at the seam."

**Q: Three decorators are stubs, two modules are empty, and the load test hasn't run. What did you
actually finish?**
> "The parts where the design decisions live. The state machine, the schema and its verification, the
> failure taxonomy, the decorator chain and its ordering, all three retry controls, the circuit breaker
> including the half-open fix, the Kafka topology with derived partition counts, the scheduler's three
> tiers with the measured claim strategy, and the SLO rules.
>
> What's missing is mostly wiring and two modules I'd rather leave empty than fake. I'd rather show you
> a real half-open stampede fix than a mocked OAuth filter."

**Q: If you had two more weeks, what would you do?**
> "In order: the three app-api adapters so it boots and there's an end-to-end path. Then wire the three
> stub decorators. Then the reconciler, because `UNKNOWN` currently only resolves if a webhook happens
> to arrive. Then run the load tests and fill in the empty table — specifically the `mixed-class`
> scenario, because that's the one that falsifies the topic-split decision if it's wrong.
>
> Not the security module. That's a month, not two weeks, and half-built security is worse than none."
