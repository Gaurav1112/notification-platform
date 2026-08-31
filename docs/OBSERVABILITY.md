# Observability

SLIs, the metric catalogue, burn-rate alerting, and the dashboards.

Source: [§12 of the design spec](superpowers/specs/2026-08-31-notification-platform-design.md#12-observability).
The rules described here are **real files in this repository**:

| File | Contents |
|---|---|
| `docker/prometheus/rules/notification-slo.yml` | 24 SLI recording rules, 5 burn-rate alerts, 18 symptom alerts, 2 meta alerts |
| `docker/prometheus/prometheus.yml` | Scrape config for the three deployables plus infra targets |
| `docker/grafana/dashboards/notification-operational.json` | The operational dashboard, provisioned automatically |

---

## The two principles

Both are stated at the top of the rule file, and both are load-bearing.

### 1. An SLO you cannot compute is a slogan

Every SLI in §1.4 of the spec has a recording rule **named after it**, so "what is our accept
latency" has one answer rather than one per dashboard.

```yaml
- record: sli:api_availability:error_ratio_5m
- record: sli:api_accept_latency:p99_5m
- record: sli:dispatch_latency_critical:bad_ratio_1h
- record: sli:scheduler_lag:bad_ratio_30m
```

A rule that only exists inside a Grafana panel **cannot page anyone**, which is why the rules live in
Prometheus and the dashboard reads *from* them rather than recomputing.

Availability is recorded as the **error** ratio, not the success ratio. Burn-rate arithmetic is
expressed in error budget consumed, and `(1 - success)` loses float precision exactly where the
numbers are small and it matters.

### 2. Alert on burn rate and on conjunctions — never on a raw threshold, never on lag alone

Consumer lag alone is equally consistent with:

- a poison pill (lag rising, commits flat)
- a stuck poll (lag rising, no commits, no rebalance)
- a provider outage (lag rising, sends failing, commits normal)
- a healthy catch-up after a deploy (lag falling)

Four different responses. A single `lag > N` alert therefore trains the on-call to ignore it, so the
rule file breaks it into conjunctions: `KafkaPoisonPillSuspected`, `KafkaStuckPoll`,
`KafkaRebalanceStorm`, `ConsumerLagGrowing`, `MetastableFailureSuspected`.

### A third, quieter one: NaN is load-bearing

With zero traffic every ratio is `0/0 = NaN`, and every comparison against NaN is false. An idle
laptop running the compose stack does not page. That property is deliberate and it is why the ratios
are not wrapped in `or vector(0)`.

---

## SLIs and SLOs

30-day window.

| SLI | Definition | SLO | Recording rule |
|---|---|---|---|
| `api_availability` | non-5xx ÷ total on `POST /v1/notifications` | **99.95%** (21.6 min/mo) | `sli:api_availability:error_ratio_{5m,30m,1h,6h}`, `:ratio_30d` |
| `accept_latency` | request received → 202 returned | p95 < 100 ms, **p99 < 250 ms** | `sli:api_accept_latency:bad_ratio_*`, `:p95_5m`, `:p99_5m` |
| `dispatch_latency` | accept → first provider call, per class | CRITICAL **p99 < 5 s** | `sli:dispatch_latency:p99_5m`, `sli:dispatch_latency_critical:bad_ratio_*` |
| `delivery_success_rate` | DELIVERED ÷ (DELIVERED + FAILED), excluding invalid recipient | per-channel, tracked | `sli:delivery_success:ratio_30m` |
| `duplicate_rate` | delivered ≥2× to the same recipient ÷ total | **< 0.01%** | `sli:duplicate_delivery:ratio_1h` |
| `scheduler_lag` | `now − scheduled_at` at dispatch | **p99 < 30 s** | `sli:scheduler_lag:p99_5m`, `:bad_ratio_30m` |
| `acceptance_durability` | accepted requests reaching a terminal state | **99.999%** | `sli:acceptance_durability:error_ratio_{1h,6h}` |

Two supporting ratios that are not SLIs but drive tickets: `sli:retry:ratio_15m` and
`sli:kafka_partition_skew:ratio`.

### Why `delivery_success_rate` excludes invalid recipients

A dead phone number is not a platform failure. Including it makes the SLI a measure of the tenant's
data quality, and a metric that moves for reasons you cannot act on is a metric the on-call learns to
discount.

### Why `acceptance_durability` is the strictest number here

99.999% is the delivery contract from §1.5: *once we return 202, the request will reach a terminal
state or sit visibly in the DLQ.* Everything else on this page is about quality of service. This one
is about whether the platform is telling the truth.

Its alert says so:

> *This is the one guarantee we publish. A non-zero value means the delivery contract is broken, not
> degraded. **Do not close this by adjusting the threshold.***

---

## Metric catalogue

Labels omitted for brevity. Metrics marked **✓** are emitted by code that exists today; the rest are
named in the design and referenced by rules that will simply produce no series until they are.

### Ingest

| Metric | Type | Emitted |
|---|---|---|
| `notification_accept_duration_seconds` | histogram | design |
| `notification_accepted_total` | counter | design |
| `idempotency_outcome_total` | counter (`CLAIMED` / `REPLAY` / `CONFLICT` / `IN_PROGRESS`) | design |

### Pipeline

| Metric | Type | Emitted |
|---|---|---|
| `kafka_consumergroup_lag` | gauge | ✓ (container `micrometerEnabled`) |
| `outbox_pending_count` | gauge | design |
| `outbox_oldest_age_seconds` | gauge | design |
| `notification_dispatch_latency_seconds` | histogram | design |

### Providers

| Metric | Type | Emitted |
|---|---|---|
| `notification.provider.send` | timer, tags `provider`, `channel`, `traffic_class`, `outcome`, `failure` | **✓ `MeteredProvider`** |
| `notification.provider.cost.micros` | counter | **✓ `MeteredProvider`** |
| `notification.provider.retry_after.honoured` | counter | **✓ `MeteredProvider`** |
| `provider_success_rate` | derived from the timer | ✓ |
| `provider_circuit_state` | gauge, 0 CLOSED · 1 OPEN · 2 HALF_OPEN · 3 FORCED_OPEN | ✓ via Resilience4j |
| `provider_failover_total` | counter | design |

`MeteredProvider` bounds tag cardinality deliberately: every tag is an enum or a
configuration-fixed identifier — `provider` (single digits), `channel` (3), `traffic_class` (3),
`outcome` (4), `failure` (14 including `none`). **The vendor's error *message* is deliberately not a
tag** — free text from a provider is how a metrics backend acquires a million-series label.

It also sits **outside** the circuit breaker in the decorator chain, because a short-circuited call is
an outcome the caller experienced. Measured inside, an open circuit would look like zero traffic and
100% health.

### Retry and failure

| Metric | Type | Emitted |
|---|---|---|
| `notification_retry_total` | counter | design |
| `notification_dlq_total` | counter | design |
| `dlq_backlog_size` | gauge | design |
| Retry budget available / throttled | gauge / counter | **✓ `RetryBudget.availableRetries()`, `.throttledCount()`** |

### Delivery and status

| Metric | Type | Emitted |
|---|---|---|
| `notification_delivery_latency_seconds` | histogram | design |
| `status_event_out_of_order_total` | counter | **✓ `MonotonicDeliveryStatusService`** |
| `webhook_signature_invalid_total` | counter | **✓ `WebhookSignatureVerifier.INVALID_COUNTER`** |

`status_event_out_of_order_total` is more useful than it looks. Every increment corresponds to an
`applied = false` row in `notification_event`, and those rows are the only thing that can answer *"why
is this stuck in SENT"* with *"three later signals arrived and every one was correctly discarded"*.

### Scheduler

| Metric | Type | Emitted |
|---|---|---|
| `scheduler.hydrator.scan` | timer | **✓ `DueScanHydrator`** |
| `scheduler.hydrator.leader` | gauge — **summed across the fleet it must always read exactly 1** | **✓** |
| `scheduler.hydrator.fenced` | counter | **✓** |
| `scheduler.hydrator.scanned` / `.indexed` | counters | **✓** |
| `scheduler_dispatch_lag_seconds` | timer, `now − dueAt` at dispatch | **✓ `ShardAffineClaimer`** |
| `scheduler_lease_expired_total` | counter | ✓ `LeaseReaper` |
| `scheduler_claim_count_exceeded_total` | counter | design |

The leader gauge is the best kind of metric: it has an invariant. `sum(scheduler_hydrator_leader)`
must be exactly 1. Two means split-brain, zero means nothing is hydrating, and both are pageable
without knowing anything else about the system.

### Degradation

| Metric | Type | Emitted |
|---|---|---|
| `redis_fallback_active` | gauge | design |
| `shedding_level` | gauge | design (`SheddingLevel` controller not built) |

---

## Burn-rate alerting

**Two windows per alert, always.** The long window says "the budget really is burning"; the short
window says "it is *still* burning right now". Without the short arm the alert stays lit for a full
long-window after the incident is over, and the on-call learns to close it unread.

```
fast: 14.4× for 1h  (2% of a 30-day budget in one hour)  → page,   for 2m
slow:  6.0× for 6h  (5% of a 30-day budget in six hours) → ticket, for 15m
```

The short window is **1/12 of the long one** in both cases (5m / 30m) — the Google SRE pairing, which
keeps the detection/reset trade-off symmetric.

```yaml
- alert: NotificationApiErrorBudgetFastBurn
  expr: |
    (sli:api_availability:error_ratio_1h > (14.4 * 0.0005))
      and
    (sli:api_availability:error_ratio_5m > (14.4 * 0.0005))
  for: 2m
  labels: { severity: page, slo: api_availability, burn: fast }
```

The five burn-rate alerts:

| Alert | SLO | Budget | Severity |
|---|---|---|---|
| `NotificationApiErrorBudgetFastBurn` | api_availability | 0.0005 | page |
| `NotificationApiErrorBudgetSlowBurn` | api_availability | 0.0005 | ticket |
| `NotificationAcceptLatencyFastBurn` | accept_latency | 0.01 slow-request budget | page |
| `NotificationAcceptLatencySlowBurn` | accept_latency | 0.01 | ticket |
| `CriticalDispatchLatencyFastBurn` | dispatch_latency (CRITICAL) | 0.01 | page |
| `AcceptanceDurabilityBudgetBurn` | acceptance_durability | 1e-5 | page |

`AcceptanceDurabilityBudgetBurn` deliberately has **no fast-burn arm**: the nightly reconciliation job
feeds it, so a 5-minute window would be measuring the absence of a nightly job rather than a defect.

Two alert descriptions are worth quoting because they encode diagnosis, not just detection:

> **`CriticalDispatchLatencyFastBurn`** — *"If this is firing while BULK is healthy, the `.tx` lane is
> starved — check whether a campaign is being produced onto a tx topic, which is the failure the lane
> split exists to prevent."*

> **`NotificationAcceptLatencyFastBurn`** — *"The accept path is three writes and a Kafka produce.
> Latency here almost always means Postgres (lock wait, connection saturation) or the outbox fast path
> blocking on an unreachable broker."*

---

## Symptom alerts

### Pages

| Alert | Condition |
|---|---|
| `ConsumerLagBacklog` | Absolute lag over threshold |
| `ConsumerLagGrowing` | Lag rising for 10 minutes |
| `KafkaPoisonPillSuspected` | **Lag rising AND commit rate flat** |
| `KafkaStuckPoll` | No polls, no rebalance, no commits |
| `KafkaRebalanceStorm` | Repeated rebalances in a short window |
| `DlqRateHigh` | DLQ > 100/min |
| `DlqBacklogGrowing` | DLQ depth rising |
| `AllProvidersOpenForChannel` | Every circuit for a channel OPEN |
| `SchedulerLagHigh` | scheduler_lag p99 > 60 s |
| `SchedulerLeaseExpiryHigh` | Claimers dying mid-flight |
| `OutboxAgeHigh` | Oldest unpublished row > 120 s |
| `OutboxDepthShedding` | Depth at the shedding threshold |
| `DuplicateDeliverySystemic` | Duplicate rate > 0.1% |

**The duplicate-rate page fires at 10× the SLO** (0.1% vs the 0.01% target) *deliberately*. The SLO is
a 30-day budget tracked by the slow-burn rule; the page is for a systemic break — a provider replaying
callbacks, or an idempotency layer failing open. A page at exactly the SLO threshold would fire on
statistical noise at low volume.

### Tickets

| Alert | Why it is a ticket and not a page |
|---|---|
| `SingleProviderDegraded` | Failover already handled it |
| `HalfOpenProbeStampede` | The jitter should prevent this; if it fires, the jitter is misconfigured |
| `RetryRateHigh` | Retry rate > 20% |
| `RedisFallbackActive` | Quality degraded, correctness intact |
| `KafkaPartitionSkew` | Skew > 3× — almost always a key bug |
| `DefaultPartitionNonEmpty` | A row landed in the DEFAULT partition. It blocks creation of the real one while holding an exclusive lock, so this is urgent-but-not-3am |
| `ReplicationSlotRetainingWal` | The classic silent disk-full |
| `PostgresReplicaLagHigh` | Replica lag > 30 s |
| `MetastableFailureSuspected` | Sustained high retry rate with a falling success rate — the system's recovery mechanism sustaining the outage |

### Meta

`ScrapeTargetDown` and `PrometheusRuleEvaluationFailing`. **An alerting system that cannot tell you
it has stopped working is worse than no alerting system**, because it produces silence that looks like
health.

There is a related trap called out in `prometheus.yml`: `spring-boot-starter-security` is on the
classpath, and until `platform-security` ships a `SecurityFilterChain` permitting
`EndpointRequest.to(HealthEndpoint, InfoEndpoint, PrometheusScrapeEndpoint)`, every actuator endpoint
answers 401 and the scrape target silently reports `down` — **which disarms every burn-rate alert.**
The local config works around it with basic auth and a comment saying so.

---

## Tracing

`traceparent` rides in Kafka headers so async hops join one trace: accept → fan-out → dispatch →
provider → webhook → status.

Sampling: **100% of errors and CRITICAL, 1% of BULK.** Sampling uniformly at 1% means the traces you
most need are the ones you are least likely to have.

`TracedProvider` is the outermost decorator specifically so the span covers time spent on **our own**
rate limiter and thread pool. If the span started below those, the trace would report a fast provider
and a mysteriously slow system, and the fix would get applied to the wrong component.

**Not built.** `platform-observability` contains a `package-info.java` and nothing else, and
`TracedProvider` is currently a pass-through carrying a TODO that names the exact span attributes it
must set.

---

## Dashboards

One dashboard is provisioned today: **`notification-operational`**, at
`http://localhost:3000/d/np-operational`.

| Row | Panels |
|---|---|
| **SLOs** | API availability (30d) · availability error-budget burn rate (page at 14.4×) · accept latency p99 against the 250 ms objective |
| **Pipeline** | Dispatch latency p99 by traffic class · **consumer lag by group** · **consumer commit rate** · DLQ entries/min · outbox oldest-row age |
| **Providers** | Circuit state by provider · delivery success rate by channel (INVALID_RECIPIENT excluded) |

The two pipeline panels in bold are placed adjacent on purpose, and their titles say why:

> *"Consumer lag by group — read WITH the commit rate below, never alone"*
> *"Consumer commit rate — flat zero here with rising lag above is a poison pill"*

That pairing is the same idea as the conjunction alerts, expressed in a layout: **a dashboard that
lets you read a number without its disambiguating partner will be misread.**

### Designed but not built

Three more dashboards are in the design:

- **Executive** — throughput, success rate, cost per 1,000, SLO burn
- **Provider** — per-provider health matrix, failover events, cost by vendor
- **Data** — partition sizes, replica lag, autovacuum progress, DEFAULT partition count, outbox depth

---

## Scrape configuration

`scrape_interval: 15s`, not the 1-minute default. CRITICAL dispatch latency has a 5-second p99
objective; a 1-minute scrape cannot see a 5-second regression before the burn-rate windows have
already smeared it.

`honor_labels: false` so `job` always wins over anything the app self-reports — the recording rules
key off `job`.

The three deployables run on the **host**, not in containers, so targets are `host.docker.internal`.
On native Linux Docker Engine that name does not exist:

```bash
HOST_ALIAS='host.docker.internal:host-gateway' make up
```

In production this file does not exist. The apps export OTLP to an OpenTelemetry Collector which
remote-writes to Amazon Managed Prometheus, and **the rule files are applied unchanged as AMP rule
groups.** That is the point of keeping them in Prometheus rather than in a Grafana panel.

Rules referencing a metric that is not emitted yet simply produce no series — they do not error.
Anything requiring an exporter that is not in the local compose stack (`postgres_exporter`,
`kafka_exporter`) is marked `LOCAL: NO DATA` in the file.

---

## Say in an interview

> *"Every SLI has a Prometheus recording rule named after it, because an SLO you can't compute is a
> slogan. Alerts are multi-window burn rate — 14.4× over an hour AND still burning over five minutes —
> so a blip that already recovered doesn't page. And the Kafka alerts are conjunctions, never lag
> alone: lag rising with commits flat is a poison pill, lag rising with commits normal is a provider
> outage, and those need completely different responses. One `lag > N` alert just teaches the on-call
> to ignore it."*
