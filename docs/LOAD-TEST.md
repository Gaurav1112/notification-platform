# Load Testing

The harness, the scenarios, how to run them, and an **empty results table**.

> **No benchmark numbers are published in this repository, because the tests have not been run.**
>
> The `load-test/` directory does not exist yet. This document is the specification for it: the
> scenarios, the exact commands, the hardware to record, and the table those runs will fill in.
> Everything below the "Results" heading is blank on purpose and will stay blank until real runs
> produce real numbers.
>
> The two measurements this project *does* have are listed in
> [STATUS.md](STATUS.md#measurements) and are labelled as measured everywhere they appear.

---

## Why an empty table is the honest artefact

A systems-design project that publishes invented throughput figures is worse than one that publishes
none. The figures are unfalsifiable, they will be wrong, and the first interviewer who asks "what was
the p99 at 2,000 RPS and what was the bottleneck?" gets an answer that unravels.

So: the harness ships, the commands ship, the hardware spec ships, and the numbers arrive when they
are earned.

**Say in an interview:** *"The load-test doc has an empty results table. I'd rather say 'I haven't run
it' than publish a number I made up — and the two numbers I do quote, the scheduler claim benchmark
and the schema verification, I ran against real Postgres and I can tell you exactly how."*

---

## What the harness targets

The full local Docker Compose stack plus the three Spring applications running on the host:

```
docker/compose.yml     postgres 18.6 · valkey 9.1.1 · kafka 4.3.1 (KRaft)
                       kafka-init (16 topics) · kafka-ui · prometheus · grafana
host JVMs              app-api :8080 · app-worker · app-scheduler
```

The apps run on the host rather than in containers so a profiler and a debugger attach directly and a
recompile is instant. Prometheus reaches them via `host.docker.internal`.

> **Blocker:** `app-api` cannot boot today — its three inbound ports have no adapters
> ([STATUS.md](STATUS.md#app-api-cannot-boot)). No load test can run end to end until that is closed.
> The scenarios below are written against the intended surface.

---

## Tooling

| Tool | Version | Role |
|---|---|---|
| **k6** | 2.2.0 | The primary driver. Scenario definitions in JavaScript, thresholds as pass/fail gates |
| **Gatling** | 3.15.1 (`gatling-core-java`) | Second opinion on the ramp and soak scenarios; better report artefacts for a long run |
| **Prometheus + Grafana** | already in the compose stack | Server-side truth. Client-side numbers alone hide queueing on the client |

Both drivers, because a load generator that saturates before the system under test produces a
beautiful graph of the load generator. Running two independent drivers over the same scenario is the
cheapest way to notice.

---

## Scenarios

| Scenario | Profile | What it measures |
|---|---|---|
| `baseline` | 100 RPS, 10 min | Accept p95/p99, resource floor. The control run every other number is compared against |
| `ramp` | 50 → 2,000 RPS over 20 min | Breaking point and HPA behaviour. Where does p99 leave the 250 ms objective, and what saturates |
| `campaign-burst` | 1M recipients in 5 min | Fan-out throughput, consumer lag peak and drain time. A scaled-down Burst A |
| `provider-outage` | Steady load + `HARD_DOWN` at t+3 min | Failover latency, retry backlog drain, **zero loss** |
| `mixed-class` | 90% BULK + 10% CRITICAL | **Proves class isolation — CRITICAL p99 must not degrade.** The single most important scenario in the list |
| `soak` | 300 RPS, 2 h | Memory leaks, connection-pool exhaustion, partition growth, autovacuum keeping up |

### Why `mixed-class` is the one that matters

Every other scenario measures capacity. This one measures whether the **central design decision is
real**.

The claim is that separating `.tx` and `.bulk` into physically distinct Kafka topics keeps a
10M-recipient campaign from delaying a login OTP, and that a priority *field* on a shared topic could
not do the same because partitions are strictly FIFO.

That claim is falsifiable in one run: put 90% BULK and 10% CRITICAL through the system simultaneously
and check whether CRITICAL p99 moves. If it does, the topic split is not delivering what the design
says it delivers, and that is worth knowing.

**Pass criterion:** CRITICAL dispatch p99 under mixed load is within 20% of its `baseline` value.

### Why `provider-outage` asserts zero loss, not fast failover

Failover speed is nice. The contract is that **nothing accepted is lost**, so the assertion is:

```
count(202 responses) == count(rows reaching a terminal state)
```

with the DLQ counted as a terminal state, because the delivery contract says *"will reach a terminal
state or sit visibly in the DLQ"*.

`ChaosState` makes this deterministic: the fault is injected by API call, it is bounded, and it lifts
on its own.

---

## Running it

```bash
# 1. Infrastructure
make up                      # postgres, valkey, kafka + 16 topics, prometheus, grafana

# 2. Build and install every module
make build

# 3. The three apps, three terminals
./mvnw -pl app-api       spring-boot:run
./mvnw -pl app-worker    spring-boot:run
./mvnw -pl app-scheduler spring-boot:run

# 4. Wait for readiness
make wait

# 5. A scenario
k6 run load-test/k6/baseline.js
k6 run load-test/k6/mixed-class.js
k6 run --out experimental-prometheus-rw load-test/k6/ramp.js
```

Between runs, to start from a known state:

```bash
make reset                   # DESTRUCTIVE: drops every volume, replays V1 on next API start
make up
```

Watching a run:

```
Grafana     http://localhost:3000/d/np-operational
Prometheus  http://localhost:9090
Kafka UI    http://localhost:8081
```

The operational dashboard is the one to have open — it already pairs consumer lag with commit rate,
which is the pair you need to tell "saturated" from "stuck".

---

## Hardware — record this with every run

A throughput number without the machine it came from is not a measurement.

| Field | Value |
|---|---|
| Machine | _(e.g. Apple MacBook Pro, M-series)_ |
| CPU | _(model, physical cores, logical cores)_ |
| Memory | _(GB)_ |
| Storage | _(NVMe / SATA, free space)_ |
| OS | _(e.g. macOS 15.x, Darwin 24.6.0)_ |
| Container runtime | _(e.g. Rancher Desktop x.y, Docker Desktop x.y, native Docker Engine)_ |
| Runtime CPU/memory limits | _(the VM allocation, not the host — this is usually the real ceiling)_ |
| JDK | _(e.g. Temurin 17.0.20.1+1)_ |
| Heap per app | _(`-Xmx` for api / worker / scheduler)_ |
| k6 version | 2.2.0 |
| Load generator location | _(same host / separate machine)_ |

**The container-runtime allocation is the field people forget and the one that most often explains the
result.** Docker Desktop defaulting to 4 vCPU and 8 GB means PostgreSQL, Kafka, Valkey, Prometheus and
Grafana are sharing four cores with the thing being measured.

**If the load generator runs on the same host as the system under test, say so.** It competes for
exactly the CPU you are trying to measure, and it is the most common reason a local benchmark plateaus
early.

---

## What to record per run

| Metric | Source |
|---|---|
| Throughput (accepted/s, dispatched/s) | k6 + `notification_accepted_total` |
| Accept latency p50 / p95 / p99 | k6 client-side **and** `sli:api_accept_latency:p99_5m` server-side |
| End-to-end delivery latency | `notification_delivery_latency_seconds` |
| Dispatch latency p99 **by traffic class** | `sli:dispatch_latency:p99_5m` |
| Peak consumer lag and drain time | `kafka_consumergroup_lag` |
| DB CPU / IOPS / connections | `pg_stat_activity`, container stats |
| Redis ops/s | `INFO commandstats` |
| DLQ count | `notification_dlq_total` |
| Error rate by status and `ProblemType` | k6 + API logs |
| Outbox depth and oldest-row age | `outbox_pending_count`, `outbox_oldest_age_seconds` |
| **The bottleneck** | Whatever was saturated at the plateau — name it |

Record client-side **and** server-side latency. A divergence between them is queueing in the load
generator, and it invalidates the run.

---

## Results

**Empty. No runs have been performed.**

### Local measured

| Scenario | Date | Throughput | Accept p50 | p95 | p99 | E2E p99 | Peak lag | Drain | DLQ | Errors | Bottleneck |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `baseline` | — | — | — | — | — | — | — | — | — | — | — |
| `ramp` | — | — | — | — | — | — | — | — | — | — | — |
| `campaign-burst` | — | — | — | — | — | — | — | — | — | — | — |
| `provider-outage` | — | — | — | — | — | — | — | — | — | — | — |
| `mixed-class` | — | — | — | — | — | — | — | — | — | — | — |
| `soak` | — | — | — | — | — | — | — | — | — | — | — |

### Class-isolation check (`mixed-class`)

| | `baseline` CRITICAL p99 | `mixed-class` CRITICAL p99 | Delta | Pass (< 20%) |
|---|---|---|---|---|
| Dispatch latency | — | — | — | — |

### Zero-loss check (`provider-outage`)

| | Count |
|---|---:|
| `202` responses | — |
| Rows reaching a terminal state | — |
| Rows in the DLQ | — |
| **Unaccounted** | **—** |

### Extrapolated cloud capacity

**Deliberately a separate section.** Local numbers on a laptop with a shared container runtime do not
extrapolate linearly to `db.r7g.8xlarge` and six MSK brokers, and presenting them in one table invites
exactly that mistake.

Nothing will be written here until there is a local baseline to extrapolate *from*, and any figure
that does appear will state its scaling assumption explicitly.

| Target | Basis | Projected | Assumption |
|---|---|---|---|
| — | — | — | — |

---

## Known measurement traps in this stack

Things that will produce a wrong number if you do not control for them.

| Trap | Effect | Control |
|---|---|---|
| **Load generator on the same host** | Plateau is the generator, not the system | Note it; ideally run k6 from a second machine |
| **Container runtime CPU allocation** | The real ceiling is the VM, not the host | Record the allocation |
| **JIT warm-up** | First 30–60 s is 2–5× slower | Discard the first minute of every run |
| **`Sleeper.REAL` in the mocks** | Mock providers really do sleep — log-normal latency with a real tail | Intended. Do **not** switch to `Sleeper.NONE`: it removes the tail the whole design is shaped around |
| **Deterministic failure injection** | Failure rate is a pure function of message identity, so it is stable across runs | Good for reproducibility. It also means a *different* recipient-ID generator changes your failure rate |
| **Flyway on first start** | The V1 migration creates 335 partitions | Let it finish before starting the clock |
| **Partition pre-creation** | A run crossing midnight needs tomorrow's partitions | `PartitionMaintenanceJob` handles it; verify before a soak |
| **`InMemorySentTokenLog` bounds** | 100k entries, 1 h TTL — a long soak evicts | Watch `evictions()`; a non-zero value means the TTL is too generous for the run |
| **Kafka retention during a soak** | Two hours of retry-tier traffic is real volume | Check disk before, not after |

---

## Prerequisites for the harness to be buildable

Listed so the gap is explicit:

1. **`app-api` must boot** — the three inbound ports need adapters.
2. **A seeded tenant and credential** so k6 can authenticate, or a documented dev bypass.
3. **`load-test/k6/*.js`** — six scenario files.
4. **A recipient generator** that produces realistic address distributions; because failure injection
   is seeded on recipient ID, a degenerate generator produces a degenerate failure rate.
5. **A results-capture script** that pulls the Prometheus series listed above at the end of a run, so
   the table is filled from data rather than from screenshots.
