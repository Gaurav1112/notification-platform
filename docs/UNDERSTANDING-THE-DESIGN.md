# Understanding the Design

Read this before anything else. It builds the system up from the simplest thing that could
possibly work, breaking it one problem at a time. Every complication below exists because
something simpler failed first.

No prior context needed. ~15 minutes.

---

## Version 1 — the obvious solution

```java
@PostMapping("/notifications")
public void send(@RequestBody SendRequest req) {
    for (String userId : req.userIds()) {
        String email = userRepo.findEmail(userId);
        emailProvider.send(email, render(req.template(), req.variables()));
    }
}
```

This genuinely works. For a few hundred users a day it's the correct amount of engineering, and
anything more would be waste.

Now let's break it.

---

## Problem 1: the request takes four hours

Someone sends a campaign to 10 million users. The loop runs for hours. The HTTP connection times
out. The load balancer kills the request. Nobody knows what was sent.

**Fix: accept the request, do the work later.** Write it down, return immediately, process
asynchronously.

```
POST /notifications  →  save to DB  →  return 202 Accepted
                                       (a background worker picks it up)
```

This is the single most important change, and it introduces everything that follows. The moment
work happens *after* the response, you need to answer: what if the worker crashes? what if it
runs twice? how does the caller find out what happened?

---

## Problem 2: we lost a notification

The code is:

```java
notificationRepo.save(notification);   // committed
kafka.send(notification);              // ← crash here
```

The notification is in the database, marked pending, and **nothing will ever pick it up.** The
customer sees "accepted" and never receives anything.

This is the *dual write* problem: two systems, no shared transaction, no atomicity.

**Fix: the transactional outbox.** Write the notification and a "to-send note" in the *same*
database transaction. The commit is now a single atomic decision.

```java
@Transactional
void accept(SendRequest req) {
    notificationRepo.save(notification);
    outboxRepo.save(new OutboxMessage(notification));   // same transaction
}   // ← either both exist, or neither does
```

A separate sweeper reads unpublished outbox rows and pushes them to Kafka. If it crashes, the rows
are still there. If it publishes twice, that's fine — everything downstream is idempotent (we'll
get to that).

> One refinement: a pure sweeper adds latency. So we *also* publish immediately after the
> transaction commits, as a best-effort fast path, and the sweeper becomes a safety net that only
> catches what the fast path missed. Fast when healthy, correct when not.

---

## Problem 3: the same order confirmation went out twice

The caller's HTTP client timed out and retried. We now have two notifications.

**Fix: an idempotency key.** The caller sends `Idempotency-Key: order-4821`. We record it. A
second request with the same key returns the *first* response and sends nothing.

The subtlety most implementations miss: **what if the same key arrives with a different body?**

```
Key "order-4821" + body A   →  process, store response
Key "order-4821" + body B   →  ??? 
```

Returning A's response would be silently wrong — the caller asked for something else and got a
stale answer. So we store a **SHA-256 fingerprint of the body** and return `409 Conflict` on a
mismatch. Same key, same body = replay. Same key, different body = error.

---

## Problem 4: Twilio is down

Every SMS fails. We retry. They still fail. We retry harder. Now we're making 10,000 requests a
second to a service that is already on fire.

**Fix, part 1 — classify the failure before reacting to it.** Not all failures are equal:

| Failure | What to do |
|---|---|
| Connection reset, HTTP 500 | Retry — probably transient |
| HTTP 429 rate limited | Retry, but honour their `Retry-After` |
| "Invalid phone number" | **Never retry.** It will never work |
| "User has unsubscribed" | Never retry, **and stop sending to them forever** |
| Auth failure | Never retry — page a human, our credentials are broken |

A single "retry on error" branch is the most common bug in this class of system. Retrying an
invalid phone number 5 times burns budget, adds latency and helps nobody.

**Fix, part 2 — back off, with randomness.** Retry after 1s, 2s, 4s, 8s. But here's the trap:

> 100,000 messages fail at the same instant because the provider went down. With plain exponential
> backoff, **all 100,000 retry at exactly t+2s** — killing the provider again the moment it
> recovers.

So the delay is `random(0, backoff)`, not `backoff`. This is called **full jitter**, and it's the
difference between a recovery and a second outage.

**Fix, part 3 — stop calling a service that's obviously dead.** A circuit breaker: after 20 calls
with >50% failures, stop trying for 30 seconds, then send a few probes to check.

**Fix, part 4 — have a second provider.** Score the available ones on health, cost and remaining
rate limit; use the best. If it breaks, use the next.

---

## Problem 5: the retry blocked everything behind it

Naive retry inside the consumer:

```java
while (attempt < 5) {
    try { provider.send(msg); break; }
    catch (Exception e) { Thread.sleep(backoff); attempt++; }  // ← disaster
}
```

Kafka gives each consumer a partition — an ordered log. While this thread sleeps, **every other
message in that partition is stuck behind it.** Worse, Kafka thinks a consumer that hasn't polled
in 5 minutes is dead, kicks it out of the group, and triggers a rebalance. Which redelivers the
message. Which sleeps again.

**Fix: tiered delay topics.** Instead of sleeping, publish the message to a topic dedicated to
that delay and move on:

```
failed, retry in ~5s   →  notification.retry.5s
failed, retry in ~30s  →  notification.retry.30s
failed, retry in ~2m   →  notification.retry.2m
                          … 10m, 1h
```

A consumer on each tier waits until the message is due — by **pausing the partition**, not by
sleeping — then republishes it to the main dispatch topic. Nothing blocks.

---

## Problem 6: the marketing blast delayed a login OTP

10 million campaign messages are queued. A user requests a password reset. It goes to the back of
the queue, behind 10 million other messages, and arrives 40 minutes later. Useless.

The instinct is to add a `priority` field and sort by it.

**That doesn't work, and the reason is worth internalising: a Kafka partition is strictly
first-in-first-out.** There is no "read the high-priority one first" — the consumer physically
must read past the 10 million records to reach the OTP. A priority field on a shared topic is a
lie.

**Fix: physically separate topics** with separately-scaled consumers.

```
notification.dispatch.sms.tx     ← OTPs, password resets. Never busy.
notification.dispatch.sms.bulk   ← campaigns. Can be 10M deep. Nobody cares.
```

Every large notification platform that has published its architecture — Netflix, Uber, Airbnb,
Pinterest — arrived at physical separation. None uses a priority field.

---

## Problem 7: the worker died mid-send

This is the hardest problem in the system, and it is worth reading twice.

```
1. worker calls Twilio
2. Twilio receives it and starts sending
3. worker is killed (OOM, deploy, spot eviction) before recording anything
4. Kafka redelivers the message to another worker
5. that worker calls Twilio again
   →  the user receives two OTPs
```

Now the same shape without a crash:

```
1. worker calls Twilio
2. we time out at 5 seconds
3. Twilio actually succeeded at 5.2 seconds
   →  we believe it failed. If we retry, the user gets two OTPs.
```

**We genuinely do not know whether it was delivered.** Both "retry" and "give up" are wrong.

**Fix, part 1 — write down the intent before acting.**

```java
attemptRepo.save(new Attempt(PENDING, token));   // COMMITTED before the call
provider.send(payload, token);                   // now safe to crash
attemptRepo.markSucceeded(...);
```

A crash now leaves a visible `PENDING` row instead of an invisible gap. Recovery can *see* that
something happened.

**Fix, part 2 — model "I don't know" as a real state.** Most designs have `SENT | FAILED`. We add
**`UNKNOWN`**, and we never blind-retry from it. We wait for the delivery receipt, or query the
provider, or eventually give up and flag it.

**Fix, part 3 — check what the provider can actually do.** Ideally you send them a key and they
deduplicate. Reality:

| Provider | Can they deduplicate for us? |
|---|---|
| Twilio | No — and worse, no way to query "did you send my message X" |
| SES / SendGrid | No key, but our tag comes back on the webhook, so we can reconcile |
| FCM / APNs | No |

So the honest answer differs per channel:

- **SMS** — a duplicate OTP is bad and costs real money, and Twilio can't be queried. So on
  `UNKNOWN` we **do not resend**. We accept a small, measured loss rate.
- **Email** — duplicates are cheap and reconciliation works. Retry and reconcile.
- **Push** — duplicates are free, and we set a collapse key so a duplicate *replaces* the first
  notification on the device rather than stacking.

**This is the part that separates a real design from a textbook one.** "Exactly-once delivery" is
not achievable across a network boundary to a third party. Saying so, and then choosing sensibly
per channel, is the answer.

---

## Problem 8: the delivery receipt arrived before the send confirmation

The provider webhook says `DELIVERED` at 09:14:22. Our own worker writes `SENT` at 09:14:23,
because it was slower. Naive code:

```java
notification.setStatus(newStatus);   // last writer wins
```

The record now says `SENT`. Our delivery metrics are wrong, and if retries are driven off status,
**we re-send something the user already received.**

**Fix: statuses have a rank, and status only ever moves forwards.**

```sql
UPDATE notification SET status = :new
 WHERE id = :id AND status_rank < :new_rank;
```

A late `SENT` (rank 60) against a stored `DELIVERED` (rank 80) matches zero rows and is discarded.

Two things make this elegant rather than merely correct:

- **Zero rows returned is not an error.** It means "stale, duplicate, or illegal" — exactly what
  we want, and it makes the whole pipeline safe to replay.
- **The ranks encode business rules.** `CANCELLED` is rank 28, below `QUEUED` at 30 — so
  "you cannot cancel something already queued" is enforced *by the data*, with no `if` statement
  anywhere that someone can forget to write next year.

---

## Problem 9: 20 scheduler instances fought over the same rows

Scheduled notifications live in a table. Every instance runs:

```sql
SELECT * FROM scheduled WHERE due_at <= now() LIMIT 500 FOR UPDATE;
```

All 20 walk the same index in the same order and block on the row the first one holds. **Measured:
159 transactions/sec, 100 ms latency.** No errors, no deadlocks — it just serialises. Which is why
it presents as "the database is slow" rather than as a design bug.

Adding `SKIP LOCKED` helps — **453 tps** — because instances step over locked rows instead of
waiting.

But there's a documented case of this pattern hitting a hard wall at 128 concurrent workers on an
80-core machine, because each arriving worker has to skip past every row the others have locked.
The wasted work grows as the *square* of the worker count.

**Fix: give each instance its own shards, so they never look at the same rows.**
**Measured: 746 tps, 21.5 ms** — 4.7× the naive version.

Plus two safety mechanisms:

- **A lease, not a lock.** `claimed_until = now() + 60s`. If an instance dies, its work is
  reclaimed in a minute. (This is also why the system is at-least-once, not exactly-once — a
  network-partitioned worker can still be sending while its lease expires.)
- **A claim counter.** If a row has been claimed 6 times, it is killing a worker every time it's
  picked up. Send it to the dead-letter queue instead of claiming it a seventh time. Without this,
  one malformed row crash-loops a pod forever and never shows up in metrics.

---

## Problem 10: everything fires at 9:00:00

Humans schedule things on the hour. About 5% of a day's scheduled volume lands in a single
minute — **20,833 per second** in a spike.

**Fix, one line:**

```java
dueAt = dueAt.plusSeconds(Math.floorMod(id.hashCode(), 300));
```

Deterministic (so it survives restarts and stays idempotent), and it spreads a 60-second cliff
over 5 minutes. 20,833/s becomes 4,167/s. No infrastructure, no cost.

Incidentally, Google's FCM documentation independently tells you to *"avoid sending messages
within a 2 minute window of each of the :00, :15, :30, and :45 minute marks"* — the same problem,
observed from the receiving end.

---

## Problem 11: we woke someone at 3 a.m.

Quiet hours: don't send between 22:00 and 07:00 **in the user's timezone**.

The obvious approach — 24 hourly batches, one per timezone — is wrong twice over. Measured against
the current timezone database:

- There are **37 distinct UTC offsets** in force at any moment, not 24.
- Offsets exist at **:45 past the hour** — Nepal is UTC+05:45, Chatham Islands +12:45.

So hourly ticking is wrong, **and so is half-hourly.** 15 minutes is the coarsest granularity that
is actually correct.

Three more traps:

- Not every daylight-saving shift is one hour. `Australia/Lord_Howe` moves by **30 minutes**.
- On the day the clocks go back, 01:30 happens **twice**. Two workers can resolve the same local
  time to instants an hour apart and **send twice**. So we store the resolved offset alongside the
  timestamp, making it idempotent.
- The timezone database changes several times a year, sometimes with *days* of notice. Your JVM's
  copy, your database's copy and your operating system's copy all lag differently. We stamp each
  row with the timezone-database version used to compute it, so a post-update sweep is one query.

And a trap that looks harmless: PostgreSQL lets you put `AT TIME ZONE` in a stored generated
column or an index, because the function is (incorrectly) marked immutable. **A timezone database
update then silently invalidates that index — wrong rows, no error, no warning.** We compute these
values in Java and store plain timestamps.

---

## Problem 12: everything is fine, and we can't tell

The system is running. Is it healthy?

"CPU is under 80%" tells you nothing about whether notifications are arriving. So we define what
good means *before* we measure it:

| We promise | We measure | Target |
|---|---|---|
| The API answers | non-5xx ÷ total | 99.95% |
| Accepting is fast | time to `202` | p99 < 250 ms |
| OTPs are fast | accept → provider call | **p99 < 5 s** |
| Nothing is silently lost | accepted → reached a terminal state | 99.999% |
| Duplicates are rare | delivered twice ÷ total | < 0.01% |

Then we alert on **burn rate**, not thresholds. "We are consuming our monthly error budget 14×
faster than sustainable" is worth waking someone for. "CPU is at 81%" is not.

And some alerts only make sense as **conjunctions**:

> Consumer lag rising **and** commit rate ≈ 0 → a poison message is blocking a partition.

Lag alone is ambiguous — it's high during any traffic spike. Lag *plus* zero commits means the
consumer is alive, healthy-looking, and processing exactly nothing.

---

## Problem 13: the fix made it worse

Two feedback loops that are easy to build by accident:

**Autoscaling on queue depth during a provider outage.** Twilio goes down → messages stop being
processed → the queue grows → the autoscaler adds workers → **more workers hammer the recovering
provider.** Queue depth is a symptom of the provider being down, not of insufficient capacity. So
scale-out is gated on provider health.

**Every worker probing at the same time.** The circuit breaker opens on 40 pods simultaneously.
All wait exactly 30 seconds. All send 10 probe requests. The recovering provider receives **400
simultaneous probes** — the exact stampede the breaker was meant to prevent, recreated by having
more than one instance. So the wait is jittered per pod.

This class of bug — where the system's own recovery mechanism sustains the outage — is called
**metastable failure**. A study of 22 production incidents found roughly half had *retry policy*
as the sustaining cause, and the diagnostic is counter-intuitive:

> If offered load drops and the error rate doesn't, you are metastable, not overloaded.
> **Stop scaling. Start shedding.**

---

## Where we ended up

| Started with | Ended with | Because |
|---|---|---|
| Loop over users | Accept + process async | 10M recipients don't fit in a request |
| Save, then publish | One transaction with an outbox | Crashing between two writes loses data |
| — | Idempotency key + body fingerprint | Clients retry; same key + different body is an error |
| Retry on error | Classify, then decide | Retrying an invalid number never works |
| Exponential backoff | **Full** jitter | Synchronised retries re-kill the provider |
| `Thread.sleep` | Tiered delay topics | Sleeping blocks the whole partition |
| Priority field | Separate topics | Partitions are FIFO; a priority field is a lie |
| `SENT` / `FAILED` | `+ UNKNOWN` | The provider timed out after possibly delivering |
| Set status | Only move forwards | Webhooks arrive out of order, routinely |
| `SELECT … FOR UPDATE` | Shard-affine + lease | 159 tps → 746 tps, measured |
| Send at 09:00 | Deterministic jitter | 5% of a day lands in one minute |
| 24 timezone buckets | 15-minute ticks | 37 offsets exist, some at :45 |
| Alert on CPU | Alert on SLO burn rate | CPU doesn't tell you if notifications arrive |
| Scale on queue depth | Gate on provider health | Otherwise you amplify the outage |

Every row is a bug we chose not to ship.

---

## Where to go next

| You want | Read |
|---|---|
| The 2-page summary | [ARCHITECTURE-SUMMARY.md](ARCHITECTURE-SUMMARY.md) |
| Components and diagrams | [ARCHITECTURE.md](ARCHITECTURE.md) |
| The API | [API.md](API.md) |
| Schema, indexes, partitioning | [DATABASE.md](DATABASE.md) |
| Topics and partition maths | [KAFKA.md](KAFKA.md) |
| Every failure mode | [FAILURE-MODES.md](FAILURE-MODES.md) |
| Why each decision | [adr/](adr/) |
| Everything, in order | [the full spec](superpowers/specs/2026-08-31-notification-platform-design.md) |
