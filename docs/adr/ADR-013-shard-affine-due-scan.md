# ADR-013: Shard-affine due-scan with `SKIP LOCKED`, and a leader-elected hydrator

> **Status caveat (verified 2026-08-31).** The table this ADR governs, `notif.scheduled_notification`, is **not yet in a migration**, so the SQL described here has never executed in this repository. The 159 / 453 / 746 tps figures were measured on a design-time prototype, not on this code. Tracked in [STATUS.md](../STATUS.md).


**Status:** Accepted
**Date:** 2026-08-31

## Context

Scheduled notifications sit in PostgreSQL with a `due_at` and a `READY` state. When they come due,
some number of scheduler pods has to pick them up and publish them to the dispatch lane — exactly
once each, without two pods taking the same row, and fast enough to hold a **p99 < 30 s** scheduler-lag
SLO through a 20,833/s scheduled cliff.

The textbook answer is `SELECT … FOR UPDATE SKIP LOCKED`. It is correct. The question is whether it is
fast enough at sixteen concurrent claimers, and the answer turns out to be "faster than the naive
form, and still leaving 65% on the table".

## Decision

**Two stages with two different concurrency models:**

1. **Hydrator — leader-elected, one pod.** Range-scans PostgreSQL for work due in the next five
   minutes and pushes it into per-shard Redis sorted sets. A plain read, from one session.
2. **Claimers — every pod, shard-affine, `FOR UPDATE SKIP LOCKED`.** Each pod claims only from the
   shards assigned to it.

## The measurement

16 concurrent claimers, 3M `READY` rows, 100 rows per transaction:

| Strategy | tps | rows/s | avg latency |
|---|---:|---:|---:|
| `FOR UPDATE` (stampede) | **159** | 15,900 | 100.5 ms |
| `FOR UPDATE SKIP LOCKED` | **453** | 45,300 | 35.3 ms |
| **shard-affine + `SKIP LOCKED`** | **746** | 74,600 | 21.5 ms |

The naive version is **4.7× slower** than the chosen one, and it **raises no errors at all**. It
serialises, because all sixteen pods walk the same index in the same order and each blocks on the
lock the one in front holds.

**That is the dangerous part.** The symptom presents as "the database is slow", which is why the
instinct is to add claimers — and adding claimers makes it worse.

## Why the *scan* is leader-elected and the *claim* is not

This is the non-obvious half, and it is the reason a plain `SKIP LOCKED` on everything stops at 453.

`SKIP LOCKED` fixes correctness and serialisation. **It does not fix bloat.**

PlanetScale measured the recursive-CTE and `SKIP LOCKED` patterns side by side and found their
"degradation curves almost identical". A documented pgsql-general case hit a hard wall at **128
concurrent claimers on 80 cores**; Thomas Munro's diagnosis was that each session must skip every dead
or non-matching tuple left behind at the start of the table by all the other sessions, and "it all
gets a bit explosive".

Wasted index visits scale as:

```
B · W² / 2        B = batch size, W = worker count
```

**Quadratic in worker count, and linear in batch size.** So the intuitive fix — raise the batch size
to do more work per pass — *multiplies the quadratic term*. It is precisely the wrong lever, and it is
the first thing anyone tries.

The fix is to stop having W sessions walk the same index. The scan runs on **one** pod, from one
session, as a plain read that produces no dead tuples at all. `SKIP LOCKED` is then reserved for the
claim, where shard affinity has already made contention rare — it earns its place only during a
rebalance window, and that narrow role is the whole 746-versus-453 gap.

**River and Oban both arrived at the same split**, independently, which is decent evidence it is not
an over-fit.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Naive `FOR UPDATE`** | 159 tps. Serialises silently |
| **`SKIP LOCKED` with no affinity** | 453 tps. Leaves 65% on the table, and the W² bloat term is unbounded as the fleet grows |
| **Quartz / ShedLock** | Quartz's clustered mode uses `SELECT … FOR UPDATE` on a lock row — the 159 tps case with extra tables. It also brings its own schema, its own scheduler semantics, and a misfire policy that has to be reasoned about |
| **Advisory locks per row** | One lock per row at 74,600 rows/s exhausts the lock table |
| **Kafka as the scheduler** | Kafka has no per-message delay. The whole reason the retry tiers exist ([ADR-009](ADR-009-tiered-retry-topics.md)) |
| **A single scheduler pod doing everything** | Simple, and the claim throughput of one pod is the ceiling. Also a single point of failure with no warm standby |
| **Redis sorted set as the source of truth** | Fast, and loses everything on a Redis failure. Redis is the *index*; PostgreSQL stays the source of truth, so a Redis loss degrades latency rather than losing scheduled sends |

## Supporting decisions that come with it

### The leadership campaign is inside the tick

```java
var leadership = settings.leaderElection()
        ? election.campaign(settings.leaseName(), settings.leaseDuration()).orElse(null)
        : unelected;
```

A separate renewal thread keeps advertising a healthy leader while the scan thread is wedged on a hung
query — the lease never expires and a replica that could make progress never gets the chance. Renewing
only when a pass actually starts makes a stuck hydrator **lose** the lease, which is the behaviour that
heals.

### Writes are fenced

```java
if (result == DueIndex.FENCED_OUT) {
    election.resign(settings.leaseName());
    leading = false;
    return;
}
```

A monotonic fencing token, checked on every shard write. A GC pause longer than the lease is not
hypothetical, and without fencing the result is two hydrators writing the same horizon.

The hydrator stands down **immediately** on a fenced write rather than finishing the loop — every
further write in that pass would be a zombie write.

### The transaction never spans the network call

Claim commits, then the publish happens, then a second short transaction records the outcome.

Holding the claim transaction open across a 10-second Kafka timeout would **pin `xmin`**, and a pinned
`xmin` stops autovacuum reclaiming the twenty million dead tuples a day that status updates generate.
That is the failure that takes the cluster down at 3 a.m., and it always starts as a convenience.

### A failed publish is a no-op

The row stays `CLAIMED`, its lease expires, and `LeaseReaper` returns it to `READY`. There is no
compensating write to get wrong.

## Consequences

### Positive

- **746 tps, measured**, against 159 for the form most implementations reach for.
- The bloat term is bounded: one scanning session instead of W.
- **Losing the leader is a delay, not a loss.** Rows stay `READY` until a claimer takes them, so a
  hydrator that dies mid-pass costs at most one scan interval.
- `sum(scheduler_hydrator_leader)` is an invariant that must equal 1 — a metric with a provable
  property is a metric you can page on without further context.

### Negative

- **Leader election is a component**, with a lease, a fencing token, a resignation path and a
  development-mode bypass. That is `LeaderElection`, `LeaderLockStore`, `RedisLeaderLockStore`,
  `Leadership` and `NodeIdentity` — five types for one job.
- **Hydration throughput is single-pod.** If one pod cannot scan the horizon fast enough, the answer is
  a bigger pod or a shorter horizon, not more pods. That is a real ceiling and it is not horizontal.
- **Shard assignment is another moving part.** A rebalance during a deploy means two pods briefly
  believe they own the same shard, which is exactly the window `SKIP LOCKED` covers — but it has to be
  correct, and `ShardAssignment` is a class that can be wrong.
- **Redis is on the scheduling path.** Not as the source of truth, but a Redis outage degrades
  scheduling to whatever the PostgreSQL fallback does, and that fallback is design-only today.
- The three-stage design is harder to explain than "poll the table".

## Related

- [ADR-006](ADR-006-kafka-as-broker.md) — why Kafka is not the scheduler
- [ADR-009](ADR-009-tiered-retry-topics.md) — the same "no native delay" constraint, solved differently
- `ScheduleJitter` — the deterministic 300 s spread that keeps the scheduled cliff from being the
  binding constraint
