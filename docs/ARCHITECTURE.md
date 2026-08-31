# Architecture

Components, flows, patterns and the full diagram set.
Numbers are derived in [SCALABILITY.md](SCALABILITY.md); schema in [DATABASE.md](DATABASE.md).

---

## 1. System diagram

```mermaid
flowchart TB
    subgraph Clients
        SVC["Internal Services"]
        CAMP["Campaign Tools"]
        PROV_WH["Provider Webhooks"]
    end

    subgraph Edge
        WAF["WAF"] --> ALB["ALB / API Gateway"]
    end

    subgraph API["app-api (stateless, HPA 3-60)"]
        AUTH["AuthN/Z"]
        VAL["Validation"]
        IDEM["Idempotency Filter"]
        QUOTA["Tenant Quota"]
        INGEST["Ingest — persist + outbox in ONE tx"]
        WH["Webhook Receiver — HMAC"]
        QRY["Query API"]
    end

    subgraph Store["State"]
        PG[("PostgreSQL — SYSTEM OF RECORD")]
        RD[("Valkey — cache + coordination")]
        S3[("S3 — bodies, manifests, archive")]
    end

    subgraph Bus["Kafka"]
        T_REQ["notification.requested"]
        T_DISP["dispatch.{push,email,sms}.{tx,bulk}"]
        T_RETRY["retry.{5s,30s,2m,10m,1h}"]
        T_STAT["notification.status"]
        T_DLQ["notification.dlq"]
    end

    subgraph Sched["app-scheduler (3, shard-assigned)"]
        SCAN["Due Scanner — shard-affine leases"]
        FANOUT["Campaign Fan-out"]
        OUTBOX["Outbox Sweeper"]
        RETRYP["Retry Promoter"]
    end

    subgraph Work["app-worker (KEDA on lag)"]
        ORCH["Orchestrator — preferences, template, dedup"]
        CW["Channel Workers"]
        ROUTER["Provider Router"]
        ADPT["Provider Adapters"]
        SEP["Status Processor"]
    end

    subgraph Ext["Providers"]
        P["Mock SMS / Email / Push"]
    end

    SVC & CAMP --> WAF
    PROV_WH --> WAF
    ALB --> AUTH --> VAL --> IDEM --> QUOTA --> INGEST
    ALB --> WH
    ALB --> QRY

    IDEM <--> RD
    QUOTA <--> RD
    INGEST -->|"same tx"| PG
    INGEST -.->|"fast path"| T_REQ
    CAMP -.->|"large list"| S3

    OUTBOX -->|"safety net"| T_REQ
    SCAN --> PG
    SCAN -.-> T_REQ
    FANOUT --> S3
    RETRYP --> T_RETRY
    T_RETRY -.-> T_DISP

    T_REQ -.-> ORCH --> CW --> ROUTER --> ADPT --> P
    ORCH <--> RD
    ORCH -.-> T_DISP -.-> CW
    CW --> PG
    ADPT -.-> T_STAT
    ADPT -.->|"exhausted"| T_DLQ
    WH --> PG
    WH -.-> T_STAT
    T_STAT -.-> SEP --> PG
    P -.->|"delivery receipt"| WH
    QRY --> PG & RD
```

## 2. Synchronous vs asynchronous

### The sync path — everything that must be correct before we say yes

Budget: **p99 < 250 ms**.

```
1. authenticate + authorise (JWT, tenant scope)
2. validate payload
3. idempotency  → Valkey GET (hit: return stored response, ~2 ms)
                → miss: INSERT idempotency_record ON CONFLICT (durable claim)
4. tenant quota → Valkey token bucket (fail-open if Valkey is down)
5. BEGIN TX
     INSERT notification_request
     INSERT notification (+ recipients, or S3 pointer if large)
     INSERT outbox_message
   COMMIT                        <-- the single atomic accept decision
6. store response in idempotency_record, warm the cache
7. return 202
8. [after commit, best-effort] publish to Kafka
```

Deliberately **absent**: preference lookup, template rendering, dedup, provider selection. Each
is a dependency that could be slow or down, and none should be able to make
`POST /notifications` fail.

### The async path

Orchestrator → preferences (may `SUPPRESS`) → template render → dedup → per-recipient dispatch →
channel worker → provider router → adapter → status event → monotonic state machine → Postgres.
Retries and webhooks re-enter the same spine.

## 3. Components

| Component | Owns | If it dies |
|---|---|---|
| **app-api** | Accept, validate, idempotency, quota, webhooks, query | Stateless. Zero in-flight loss: accept is a committed tx or it never happened |
| **Ingest** | The atomic accept transaction | Crash before commit → client sees 5xx, retries with the same key → no duplicate. Crash after commit before publish → sweeper recovers in ~2 s |
| **app-scheduler** | Due scan, fan-out, outbox sweep, retry promotion | Shard-assigned; leases expire in 60 s so claimed rows are reclaimed. Idempotent by design |
| **Orchestrator** | Preferences, templating, dedup, per-recipient explode | Rebalance; uncommitted offsets redeliver; dedup absorbs it |
| **Channel Worker** | Provider selection, call, attempt recording | The dangerous one — crash *after* the call, *before* the write. Mitigated by pre-registering the attempt + provider-side idempotency token |
| **Status Processor** | Monotonic lifecycle transitions | Must tolerate out-of-order and replayed events; only advances, never regresses |

## 4. Request flow (immediate send)

```mermaid
sequenceDiagram
    participant C as Client
    participant API as app-api
    participant R as Valkey
    participant PG as PostgreSQL
    participant K as Kafka
    participant W as Worker
    participant P as Provider

    C->>API: POST /v1/notifications (Idempotency-Key)
    API->>R: check idempotency key
    R-->>API: miss
    API->>PG: claim key, then commit accept transaction
    API-->>C: 202 Accepted
    API-)K: produce notification.requested
    K->>W: consume, resolve preferences, render
    W->>PG: INSERT delivery_attempt (PENDING) + idempotency_token
    W->>P: send(payload, token)
    P-->>W: 202 providerMessageId
    W->>PG: attempt SUCCEEDED, status SENT
    P--)API: webhook delivery receipt (seconds to minutes later)
    API->>PG: monotonic transition to DELIVERED
```

The `PENDING` attempt row is committed **before** the network call. That converts an invisible
failure into a visible, reconcilable one — see [FAILURE-MODES.md](FAILURE-MODES.md).

## 5. Scheduled flow

```mermaid
flowchart LR
    A[/"Schedule request"/] --> B["Persist notification + schedule row"] --> C(["202"])
    B --> D["Shard-affine scan: state=READY AND shard=ANY(mine)"]
    D --> E["Mark CLAIMED, lease 60s, fencing token"]
    E --> F{"Recipients over 1000"}
    F -->|No| G["Publish to dispatch"]
    F -->|Yes| H["Chunk from S3 manifest"] --> G
    G --> I["Mark DISPATCHED"]
    J["Lease reaper, 30s"] --> K{"claim_expires_at past"}
    K -->|Yes| L["Reset to READY, claim_count++"] -.-> D
    M{"claim_count over 5"} -->|Yes| N["Poison pill to DLQ"]
    style L fill:#FFCDC2
    style N fill:#FFCDC2
```

Scheduler pods own disjoint shards, so they never contend. `SKIP LOCKED` remains only as a
rebalance-window safety net. Measured 746 tps vs 159 for the naive `FOR UPDATE` form — the naive
version throws no errors, it just serialises, which is why it reads as "the database is slow"
rather than a design bug.

`claim_count > 5` is a poison-pill detector: without it one malformed row crash-loops a pod
forever and never appears in metrics.

## 6. Retry flow

```mermaid
flowchart LR
    A(["Provider call failed"]) --> B["Classify into FailureType"] --> C{Retryable}
    C -->|Invalid recipient| D["FAILED, deactivate address"]
    C -->|Auth or quota| E["Failover now, page on-call"]
    C -->|Transient or 5xx| F{"Attempts left and not expired"}
    F -->|No| G["DLQ"] --> H["Operator triage and replay"]
    F -->|Yes| I["Full jitter: random(0, backoff)"]
    I --> J{"Retry-After present"}
    J -->|Yes| K["max(jitter, Retry-After)"] --> L["Tiered delay topic"]
    J -->|No| L
    L --> M["Promoter: pause partition, re-poll"] --> N{"Still eligible"}
    N -->|No| O["EXPIRED or CANCELLED"]
    N -->|Yes| P["Republish to dispatch"]
    style I fill:#FFECBD
    style G fill:#FFCDC2
    style P fill:#CDF4D3
```

**Full jitter, not backoff-plus-noise.** With 100k messages failing simultaneously, deterministic
backoff means all 100k retry at exactly `t+2s`, re-killing the provider the moment it recovers.

## 7. Provider failover

```mermaid
flowchart LR
    A(["Dispatch"]) --> B["Candidates by priority"] --> C["Filter: circuit CLOSED/HALF_OPEN"]
    C --> D["Filter: rate limit + daily cap"] --> E{Any left}
    E -->|No| F["ALL_PROVIDERS_UNAVAILABLE, page"]
    E -->|Yes| G["Score: success rate, latency, cost, priority"]
    G --> H["Top pick, weighted-random across top two"] --> I["Send via decorator stack"]
    I --> J{Outcome}
    J -->|2xx| K["Success, close circuit"]
    J -->|429| L["Shrink limit, DEGRADED, failover"]
    J -->|4xx| M["Does NOT trip breaker"]
    J -->|5xx/timeout| N["Failure counter++"] --> O{"over 50 percent of 20 calls"}
    O -->|Yes| P["OPEN, publish to Valkey"] --> Q["Half-open after 30s"]
    Q -.->|probe ok| K
    Q -.->|probe fails| P
    style P fill:#FFCDC2
    style M fill:#FFE0C2
```

Circuit state is **shared through Valkey** so 40 worker pods don't each independently discover a
dead provider. A **4xx never trips the breaker** — a malformed payload is our bug, and opening
the circuit on it would take a healthy provider offline.

## 8. Delivery status ingestion

```mermaid
sequenceDiagram
    participant P as Provider
    participant WH as Webhook Endpoint
    participant PG as PostgreSQL
    participant K as Kafka
    participant SEP as Status Processor

    P->>WH: POST /v1/webhooks/{provider}
    WH->>WH: HMAC constant-time compare
    WH->>WH: timestamp within 5 minutes
    WH->>PG: INSERT provider_callback (dedup_hash UNIQUE)
    WH-->>P: 200 immediately
    WH-)K: status event keyed by recipientId
    K->>SEP: consume
    SEP->>PG: SELECT recipient FOR UPDATE
    SEP->>SEP: monotonic guard, drop if rank not higher
    SEP->>PG: UPDATE status, append notification_event
    SEP->>PG: INSERT suppression on hard bounce
    SEP->>K: commit offset AFTER db commit
```

The raw payload is persisted **before** interpretation, so a verification bug is replayable
rather than lost. Committing the Kafka offset after the DB commit means the worst case is
redelivery — absorbed by the monotonic guard.

## 9. Delivery lifecycle

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Pending
    Pending --> Suppressed: preference or dedup
    Pending --> Queued: eligible
    Queued --> Processing: worker claimed
    Queued --> Cancelled: cancel API
    Queued --> Expired: TTL
    Processing --> Sent: provider ACK
    Processing --> Retrying: retryable failure
    Processing --> Failed: permanent failure
    Processing --> Unknown: timeout after send
    Retrying --> Queued: backoff elapsed
    Retrying --> Failed: attempts exhausted
    Unknown --> Delivered: late webhook
    Unknown --> Failed: reconciled as lost
    Sent --> Delivered: delivery receipt
    Sent --> Bounced: hard bounce
    Delivered --> [*]
    Failed --> [*]
    Bounced --> [*]
    Suppressed --> [*]
    Expired --> [*]
    Cancelled --> [*]
```

`UNKNOWN` is the state most designs omit — the provider timed out *after* possibly delivering.
Never blind-retry from here.

## 10. Deployment

```mermaid
flowchart TB
    subgraph AZ1["AZ-1"]
        A1["api pods"]
        W1["worker pods"]
        K1["MSK broker"]
        PG1[("RDS primary")]
    end
    subgraph AZ2["AZ-2"]
        A2["api pods"]
        W2["worker pods"]
        K2["MSK broker"]
        PG2[("RDS standby")]
    end
    subgraph AZ3["AZ-3"]
        A3["api pods"]
        W3["worker pods"]
        K3["MSK broker"]
        S1["scheduler pods"]
    end
    ALB["ALB"] --> A1 & A2 & A3
    PG1 -.->|"sync replication"| PG2
    PG1 -.->|"async, RPO ~1s"| DR[("DR region replica")]
    K1 -.->|"MSK Replicator"| DRK[("DR Kafka")]
```

RTO 30 min, RPO < 5 min. Workers run 70% spot — safe precisely *because* the design is
crash-tolerant; spot eviction is just another instance of the failure model already handled.

## 11. Design patterns

### Architectural
Hexagonal / Ports & Adapters (ArchUnit-enforced) · DDD tactical (aggregate owns invariants) ·
CQRS-light (separate read projections) · modular monolith with three deployables.

### Enterprise integration
Transactional Outbox · Idempotent Receiver · Competing Consumers · Content-Based Router ·
Splitter (fan-out) · Aggregator (campaign rollup) · **Claim Check** (S3 for large recipient
lists) · Dead Letter Channel · Invalid Message Channel (separate from DLQ) · Message Translator ·
Correlation Identifier · Wire Tap (audit) · **Delayed Message** (tiered retry topics) ·
Guaranteed Delivery.

### Resilience
Circuit Breaker (Valkey-shared) · Bulkhead (per channel *and* class) · Retry with full jitter ·
Timeout (always < `max.poll.interval.ms`) · Rate Limiter · Fallback/Failover · Leader Election
with fencing tokens · Cache-Aside · Health Endpoint Monitoring · Backpressure · Graceful
Degradation.

### GoF
Strategy (selection, retry policy) · Adapter (providers) · **Decorator** (the call stack) ·
Template Method (`AbstractChannelWorker`) · Chain of Responsibility (`PreferenceFilterChain`) ·
State (`DeliveryStateMachine`) · Registry · Builder · Observer · Specification · Null Object.

**The decorator stack** — each layer independently testable, and the mock adapter gets identical
resilience behaviour to a real one, which is what makes the failure tests meaningful:

```
ChannelWorker → Traced → Metered → CircuitBreaker → RateLimited → Timeout → Idempotent → Adapter
```

### Deliberately rejected

| Rejected | Why |
|---|---|
| Dual write (DB then Kafka) | No atomicity |
| Full Event Sourcing | Replaying 500M notifications for a status lookup |
| Saga / 2PC | One aggregate, one DB — ceremony |
| Exactly-once on the dispatch path | The provider call cannot enlist in a transaction |
| `Thread.sleep()` retry in-consumer | Holds the partition |
| Naive `WHERE due_at < now()` polling | Measured 159 tps vs 746 |
| Anemic domain model | Invariants belong in the aggregate |
