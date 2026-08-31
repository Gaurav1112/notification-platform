# Kafka

Apache Kafka 4.3.1, KRaft mode (ZooKeeper was removed in 4.0). 16 topics, 288 partitions.

---

## 1. How partition counts were derived

Nothing here is a round number picked by feel:

```
P = ceil( peakRate / consumerRatePerPartition × 1.30 × K )   rounded to a multiple of 3 (AZs)

  1.30 = operational headroom (broker restart, rolling upgrade, rebalance)
  K    = 1.5 for KEYED topics   (cannot repartition without breaking per-key order)
       = 1.0 for unkeyed topics (--alter --partitions is safe and online)
```

The binding input is **measured work per message**, not bytes:

| Consumer | Rate/partition | Why |
|---|---:|---|
| dispatch SMS | **80/s** | Single-send, ~250 ms round trip, hard provider caps |
| dispatch EMAIL | **200/s** | SES bulk 50 destinations, ~150 ms |
| dispatch PUSH | **300/s** | ~200 ms, **one HTTP/2 request per token** (FCM has no batch endpoint since June 2024) |
| requested (expander) | 400/s | Valkey lookup + batched PG insert |
| delivery writer | 1,500/s | Batched multi-row INSERT |
| status projector | 2,000/s | Batched upsert |
| retry promoter | 1,000/s | No external I/O — pause and republish |

**Bytes are never the constraint.** Even at 100M/day with a 10M campaign superimposed the cluster
ingests **~16.5 MB/s compressed** — a single `express.m7g.large` broker is rated at 15 MB/s.
Sizing Kafka by MB/s produces a cluster that is ~5× short on partitions and ~2× oversized on
brokers.

## 2. Topics

| Topic | P | Key | RF / minISR | Retention | Cleanup |
|---|---:|---|---|---|---|
| `notification.requested` | 12 | `tenantId\|idempotencyKey` | 3 / 2 | **7 d** | delete |
| `notification.scheduled` | 6 | `tenantId\|requestId` | 3 / 2 | 7 d | delete |
| `notification.dispatch.push.tx` | 18 | `tenantId\|recipientId\|PUSH` | 3 / 2 | 3 d | delete |
| `notification.dispatch.push.bulk` | **54** | same | 3 / 2 | 3 d | delete |
| `notification.dispatch.email.tx` | 12 | `…\|EMAIL` | 3 / 2 | 3 d | delete |
| `notification.dispatch.email.bulk` | 36 | same | 3 / 2 | 3 d | delete |
| `notification.dispatch.sms.tx` | 12 | `…\|SMS` | 3 / **3** | 3 d | delete |
| `notification.dispatch.sms.bulk` | 6 | same | 3 / 2 | 3 d | delete |
| `notification.delivery` | 24 | `notificationId` | 3 / 2 | 7 d | delete |
| `notification.status` | 48 | `notificationId` | 3 / 2 | 7 d | **compact,delete** |
| `notification.retry.5s` | 24 | `…\|channel` | 3 / 2 | 2 d | delete |
| `notification.retry.30s` | 12 | same | 3 / 2 | 2 d | delete |
| `notification.retry.2m` | 6 | same | 3 / 2 | 2 d | delete |
| `notification.retry.10m` | 6 | same | 3 / 2 | 2 d | delete |
| `notification.retry.1h` | 6 | same | 3 / 2 | 2 d | delete |
| `notification.dlq` | 6 | `notificationId` | 3 / 2 | **30 d** | delete |

`sms.tx` carries OTPs and uses `minISR=3`, trading availability for zero loss. Everything else
uses `minISR=2` — AWS guidance is `minISR ≤ RF−1`, because `minISR = RF` prevents producing at all
during a rolling broker update.

`notification.requested` keeps **7 days** specifically so a cross-region failover can replay from
the mirror since the last known-good offset (see [RUNBOOK.md](RUNBOOK.md)).

`288 leaders × RF 3 = 864 replicas ÷ 6 brokers = 144/broker` — 14% of the AWS-recommended ceiling.
Deliberate, because partition count is the one thing you cannot cheaply change later.

## 3. Why the topics are split this way

### Per channel, not one shared dispatch topic

Sizing a shared topic to the slowest channel (SMS at 80/s) needs **182 partitions vs 138** split.
Worse, one stalled SMS record — a Twilio 429 with a 30-second backoff — **head-of-line blocks
every push and email record behind it in that partition.** A single provider incident would
degrade all three channels.

Per-channel topics also give independent consumer scaling, independent `pause()`/`resume()` during
an incident, independent retention and independent DLQs. Cost: 3× the topic surface, and loss of
cross-channel ordering for one recipient — which nobody needs, because the real contract is
per-`(recipient, channel)` FIFO.

### `.tx` and `.bulk` lanes

Without them, a 10M blast puts ~11,000 records ahead of a password reset in every partition. At
300 msg/s that is a **37-second delay on a password reset.**

`CRITICAL` and `TRANSACTIONAL` share the `.tx` lane, differentiated by the `priority` field —
which is meaningful *there* because the lane is already shallow and low-latency. The FIFO
objection applies to mixing `BULK` with everything else, not to ordering two classes that both
drain in seconds. If `CRITICAL` p99 is ever observed degrading behind `TRANSACTIONAL`, the remedy
is a third lane per channel: a topic addition, not a redesign.

**This is the industry-convergent answer.** Netflix uses a queue and cluster per priority; Airbnb
a consumer group and Temporal namespace per category; Uber a per-domain Priority Assigner;
Pinterest a Kubernetes worker pool per queue. Nobody operating one of these at scale uses a
priority field in a shared queue.

### Five retry tiers, shared across channels

5 s → 30 s → 2 m → 10 m → 1 h; total budget 1 h 12 m 35 s.

Shared across channels is safe because the retry consumer performs **no external I/O** — it pauses
and republishes — so head-of-line blocking inside a tier is bounded by the tier delay itself, not
by provider latency. Tiers must stay *tight*: one 1-hour message at the head of a "5-second"
partition would stall it, so messages **hop tiers** rather than waiting long in one.

`retry.5s` is sized for a **total provider outage** (9,861/s), not the 12.8% steady retry rate.
Retry tiers are the shock absorber; size them for the worst provider day.

## 4. Producer and consumer configuration

```properties
# producer — all topics
acks=all
enable.idempotence=true
max.in.flight.requests.per.connection=5
retries=2147483647
delivery.timeout.ms=120000
compression.type=zstd
linger.ms=25
batch.size=262144
# default murmur2 partitioner — custom partitioners are banned in code review

# consumer
group.protocol=consumer            # KIP-848, GA in Kafka 4.0 — broker-side coordinator
max.poll.records=100               # low, deliberately (see §7)
max.poll.interval.ms=300000
enable.auto.commit=false           # offsets committed AFTER the DB commit
isolation.level=read_committed
```

Transactions are used **only** on the expander (`requested` → `dispatch.*` + PG outbox) and the
status read-process-write hop. Never on the dispatch consumer — the provider HTTP call cannot
enlist in a Kafka transaction, so exactly-once there would be correctness theatre at ~30%
throughput cost. Worse, if the transaction aborts *after* Twilio accepted the SMS, Kafka replays
the input and you send a second SMS — and that path is invisible and automatic.

## 5. Ordering

**Guaranteed:** FIFO per `(tenantId, recipientId, channel)`. That is the only contract published.

- Producer retries don't reorder: with `enable.idempotence=true` the Java client re-sequences,
  so `max.in.flight=5` is safe.
- **Deliberately broken by retry tiers.** A message that fails and lands in `retry.30s` re-enters
  after a later message for the same key. 99%+ of notifications are mutually independent; where
  ordering genuinely matters, an opt-in `sequence_no` guard checks that `n−1` is terminal before
  dispatch, costing one indexed read per ordered message.
- **Intra-partition parallelism must use `KEY` ordering mode** (Confluent Parallel Consumer), never
  `UNORDERED`. This is also the lever that lets consumer throughput grow ~4× with **zero** topic
  changes.
- **`notification.status` compaction keeps the highest offset, not the latest state.** Carry a
  monotonic `version` in the payload and have the projector drop `version <= current`. Never rely
  on compaction for correctness.

## 6. Hot partitions

With `recipientId` in the key there are ~50M distinct keys over 54 partitions:

```
mean = 100,000,000 / 72 = 1,388,889 records/partition
sd   = sqrt(n · (1/p) · (1 − 1/p)) = 1,170
4σ / mean = 0.337%          ← statistically negligible
```

**Every real hot partition is a bug or an adversary**, never hashing:

| Cause | Fix |
|---|---|
| Key = `tenantId` alone | Always include `recipientId` |
| Time component in the key | Banned |
| Custom partitioner on sequential keys | Banned in code review — keep murmur2 |
| Runaway recipient (bot, retry storm) | Per-recipient token bucket at the expander; shed to DLQ |
| Campaign emitted in ID order | `Collections.shuffle` each 10k chunk before produce — free |
| Mega-tenant at 40% of platform volume | Kafka client quotas + dedicated topics for tier-1 tenants |

**Sticky partitioning (KIP-480) does nothing here** — it applies to null-key records only. Don't
let it appear on a mitigation list.

**Client quotas enforce by delay, not error.** The broker returns the response with a computed
delay and mutes the channel, so a throttled producer sees *latency*, not exceptions — which means
your producer timeout and buffer settings decide whether throttling degrades gracefully or turns
into `BufferExhaustedException`.

Alarm on `max(partition_lag) / mean(partition_lag) > 3` sustained 5 minutes, and on per-tenant
produce rate exceeding N% of topic volume. Most teams lack the second one and discover the whale
post-incident.

## 7. Poison messages and rebalance storms

**The poison-pill signature:** offsets commit only after successful processing, so the consumer
seeks back forever. **Throughput for that partition is exactly zero while the consumer looks
perfectly healthy.** With `hash(tenant) % partitions`, a deterministic subset of tenants receives
*nothing* while every group-level dashboard is green.

Handling: 3 in-place attempts → DLQ with full context → **commit the offset**. A poison message
must never block a partition. Schema-invalid messages go to a *separate* invalid-message channel,
so "bad data" and "the provider was down" don't get mixed during triage.

**Replay goes to the first retry topic, not the main topic** — naive bulk replay of a
deterministic poison recreates the identical storm. Every replayed message carries an attempt-count
header with a ceiling.

**Rebalance storms are invisible to liveness monitoring.** KIP-62 moved heartbeats to a background
thread, so a consumer stuck in a slow provider call **still heartbeats and looks alive** while the
`max.poll.interval.ms` clock runs out. The loop: provider call hangs → poll interval exceeded →
consumer leaves → rebalance → backlog grows → on resume every consumer pulls a full batch against
a larger backlog → the hanging record is reassigned → repeat. Throughput collapses to zero while
the Kafka cluster reports perfect health, so teams debug the broker for hours.

Mitigations, in order of leverage:

1. **Bound every provider call** — connect + read timeouts strictly below the per-record budget.
   Non-negotiable for a platform whose whole job is calling flaky third parties.
2. **`max.poll.records=100`**, not the default 500.
3. **`group.protocol=consumer` (KIP-848)** — assignment moves to the broker-side coordinator,
   eliminating the group-wide synchronisation barrier entirely.
4. Decouple processing from polling with a worker pool plus `pause()`/`resume()`.
5. `group.initial.rebalance.delay.ms` to coalesce rolling-deploy churn.

**Never raise `max.poll.interval.ms` as the fix** — it costs failover time, and worst-case
rebalance detection takes up to 2× the interval.

## 8. Repartitioning without downtime

**Rule 0 — avoid it.** Partitions can be increased but never decreased, and increasing re-maps
`murmur2(key) % N`, which permanently breaks per-key ordering across the boundary, breaks log
compaction (two answers for "latest value per key"), and breaks any Kafka Streams state. Hence
`K = 1.5` on every keyed topic.

1. **Scale inside the partition first.** Parallel Consumer `KEY` mode gives ~4× with zero topic
   changes and zero ordering loss. Exhaust this before touching partition counts — it covers
   roughly a 4× growth step.
2. **Unkeyed topics:** `--alter --partitions` is safe and instantaneous.
3. **Keyed topics: shadow-topic cutover.** No dual-write — that duplicates sends.
   - Create `<topic>.v2`, deploy the v2 consumer group **subscribed but feature-flagged off** so
     it builds group state and commits.
   - **Quiesce:** `producer.flush()` on v1, then flip the topic-name config atomically.
   - **Drain:** v1 consumers run until lag is zero on all partitions. **Measure it, don't guess.**
   - Enable v2 dispatch only after zero lag is confirmed — the ordering-hazard window is then
     empty by construction.
   - Retain v1 for its full retention as a rollback path.
   - Expect 3–8 minutes of drain. **Never during a campaign window.**
4. **Adding brokers is not repartitioning.** New brokers get no existing partitions. Use
   `kafka-reassign-partitions --throttle` (AWS: ≤10 partitions per call) or Cruise Control, and
   never above 70% broker CPU.

## 9. Broker sizing

Sized by **message rate**, TLS/compression CPU and the 60% headroom rule — not bytes:

```
Burst A ingress                     = 57,222 msg/s
+ replication fetch (×2)            = 171,666 msg/s of broker-side record handling
÷ 12,000 msg/s per vCPU at 60% CPU  = 14.3 vCPU required

6 × kafka.m7g.xlarge = 24 vCPU  →  60% utilised at Burst A   OK
6 × kafka.m7g.large  = 12 vCPU  →  119%                      insufficient
```

Storage:

```
967 GB retained (single copy) × RF 3 = 2,901 GB ÷ 6 brokers = 483.5 GB
÷ 0.60 headroom = 806 GB  →  provision 1 TB/broker
```

Volumes above 334 GB get **250 MB/s baseline EBS throughput**; peak per-broker disk write during
Burst A is **8.2 MB/s = 3.3% of baseline**, so MSK provisioned storage throughput is pure waste
here — do not enable it.

Also set: `unclean.leader.election.enable=false`, `auto.create.topics.enable=false`,
`num.replica.fetchers=4`, `num.recovery.threads.per.data.dir` = vCPU count (single-threaded log
recovery after an unclean shutdown takes *hours* with thousands of partitions), rack-aware
fetch-from-follower (`client.rack`) to zero cross-AZ consumer transfer cost, IAM auth, TLS,
KMS at rest.

**Node-level:** `vm.max_map_count` defaults to **65,530**. Kafka mmaps index files, so this is a
hard ceiling at surprisingly modest partition counts. Raise to ≥1,000,000 in the node bootstrap.

## 10. Local development

```bash
docker compose -f docker/compose.yml up -d
```

Single-broker `apache/kafka:4.3.1` in KRaft mode with `RF=1` and reduced partition counts
(`docker/kafka-topics-local.sh`), plus `kafbat/kafka-ui` on `http://localhost:8081`.

> `provectuslabs/kafka-ui` is **abandoned** — last commit 2024-07-26, with an unpatched RCE
> history. Use `kafbat/kafka-ui` (the maintainer-led fork) or `redpandadata/console`.

Integration tests use Testcontainers 2 with `@ServiceConnection`, so no local broker is needed for
`./mvnw verify`.
