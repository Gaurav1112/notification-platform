# Interview Guide

Everything in this project, explained so you can say it out loud. Assumes you know Java, Spring
and REST. Assumes you have **not** used Kafka, Redis or Postgres partitioning in anger.

Read top to bottom once. Then re-read §1, §2 and §8 the morning of the interview.

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
