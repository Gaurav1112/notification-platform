# Runbook

On-call procedures for the notification platform. Every section is written to be
executed under pressure by someone who did not write the code.

**Rules of engagement**

1. **Mitigate before you diagnose.** Stop the bleeding, then find out why.
2. **Never restart a consumer to "clear" lag.** If it is a poison pill you will
   re-read the identical record and lose the diagnostic evidence.
3. **Never raise `max.poll.interval.ms`.** It costs failover time and worst-case
   rebalance detection takes up to 2× the interval. Bound the provider call instead.
4. **Never bulk-replay a DLQ without triaging by `failure_type` first.** A
   deterministic poison replayed in bulk recreates the identical storm.
5. Write what you do in the incident channel as you do it. The timeline is the
   deliverable, not the fix.

Dashboards: Grafana `http://localhost:3000/d/np-operational` (locally),
alerts at `http://localhost:9090/alerts`. Rule definitions live in
[`docker/prometheus/rules/notification-slo.yml`](../docker/prometheus/rules/notification-slo.yml).

---

## First five minutes of an incident

Do these in order. Do not skip to the interesting one.

**1. What is the actual user impact?** Not "lag is high" — is anything failing to
be *accepted*, or failing to be *delivered*, and for which traffic class?

```
sli:api_availability:error_ratio_5m            # accepts failing?
sli:dispatch_latency:p99_5m                    # by class — is CRITICAL affected?
sum by (channel) (rate(notification_dlq_total[5m])) * 60
```

If CRITICAL is healthy and only BULK is degraded, **you are not in an incident, you
are in a capacity event.** Downgrade the severity and stop paging people.

**2. Is the contract broken, or only the quality?** The published guarantee is that
an accepted request reaches a terminal state or sits visibly in the DLQ. Check:

```
sli:acceptance_durability:error_ratio_1h       # non-zero = contract broken
```

Non-zero here is the only condition that justifies waking additional people.

**3. What changed?** In order of prior probability: a deploy, a tenant, a provider.

```
sum by (tenant_id) (rate(notification_accepted_total[5m]))   # a new whale?
provider_circuit_state                                       # a vendor?
kube_deployment_status_observed_generation                   # a deploy?
```

**4. Pick the shape.** Match the symptom to a signature — the conjunction table in
[`docs/KAFKA.md` §7](KAFKA.md) and the alert rules encode these:

| Signature | Section |
|---|---|
| Lag rising **and** commit rate ≈ 0 | [Poison message](#poison-message) |
| Lag flat non-zero **and** consumed rate = 0 | [Consumer lag spike](#consumer-lag-spike) |
| One provider's circuit OPEN | [Provider outage](#provider-outage) |
| Accepts slow, outbox age climbing | [Outbox age growing](#outbox-age-growing) |
| Scheduled sends late | [Scheduler lag](#scheduler-lag) |
| Offered load **down**, error rate **flat** | [Metastable failure](#metastable-failure) |

**5. Apply the shedding ladder if you need headroom.** It is a published decision,
not an improvisation:

```
1. BULK ingest         -> 503 + Retry-After
2. BULK dispatch       -> consumers paused, lag builds deliberately
3. Non-critical status -> buffered in Kafka, PG writes deferred
4. TRANSACTIONAL       -> degraded latency, never dropped
5. CRITICAL            -> protected to the last drop of capacity
```

```bash
curl -X POST $API/admin/v1/shedding -d '{"level": 2}'
```

Raising the shedding level is **reversible and cheap**. Reach for it early; the cost
of shedding BULK for ten minutes is far below the cost of a metastable collapse.

---

## DLQ triage and replay

**Alert:** `DlqRateHigh` (page, > 100/min) · `DlqBacklogGrowing` (ticket)

A record is in the DLQ because it exhausted three in-place attempts and the offset
was committed so the partition could keep moving. That is by design — the DLQ is
where a failure becomes *visible*, and per the delivery contract, sitting in the DLQ
is not "lost".

### Triage first — never replay blind

```bash
# What kind of failures, and are they one tenant or many?
curl -sS "$API/admin/v1/dlq?since=1h&groupBy=failureType" | jq .
curl -sS "$API/admin/v1/dlq?since=1h&groupBy=tenantId"    | jq .
curl -sS "$API/admin/v1/dlq?since=1h&groupBy=providerCode" | jq .
```

Read the answer against `FailureType`:

| Dominant `failure_type` | What it means | Action |
|---|---|---|
| `TRANSIENT_NETWORK`, `PROVIDER_5XX`, `PROVIDER_TIMEOUT` | The provider had a bad window | Replay after the provider recovers |
| `RATE_LIMITED`, `QUOTA_EXCEEDED` | We exceeded a vendor cap | Fix the rate limiter first, then replay slowly |
| `INVALID_RECIPIENT`, `DEVICE_UNREGISTERED`, `UNSUBSCRIBED` | The address is dead | **Do not replay.** Confirm suppression happened, then discard |
| `TEMPLATE_ERROR`, `CONTENT_REJECTED`, `PAYLOAD_TOO_LARGE` | Our bug or the caller's | **Do not replay until the code or template ships.** Replaying reproduces it exactly |
| `AUTH_FAILURE` | Credentials rotated or revoked | Fix the secret; replay is pointless until then |
| Mixed / no pattern | Probably not a provider problem | Look at a deploy |

If `failure_type` is one value and one tenant, you are looking at a data problem
that the platform correctly refused to send. Close it with the tenant, not with a replay.

### Replay

```bash
# One record, to prove the fix
curl -X POST "$API/admin/v1/dlq/$DLQ_ID/replay"

# A filtered batch, rate-limited
curl -X POST "$API/admin/v1/dlq/replay" -H 'Content-Type: application/json' -d '{
  "filter": { "failureType": "PROVIDER_5XX", "since": "2026-08-31T09:00:00Z" },
  "ratePerSecond": 200,
  "maxRecords": 50000
}'
```

Three properties of replay that are not negotiable, and are worth knowing before you
argue with them:

- **Replay publishes to `notification.retry.5s`, never to the main dispatch topic.**
  Bulk-replaying onto the main topic recreates the original storm and puts the
  replayed records ahead of live traffic.
- **Every replayed record carries an attempt-count header with a ceiling.** A record
  cannot loop DLQ → replay → DLQ forever.
- **Replay is safe by construction.** The monotonic guard drops any status event
  that is stale or duplicated, so a record replayed twice cannot corrupt state. It
  can still produce a duplicate *send* — check TTL before replaying anything old.

### Before you close

`ttlSeconds` may have expired while the record sat in the DLQ. Replaying a
2-hour-old OTP delivers a code the user cannot use and looks like a security event
to them. Filter on TTL, or discard:

```bash
curl -X POST "$API/admin/v1/dlq/discard" -d '{"filter":{"expired":true},"reason":"TTL elapsed in DLQ"}'
```

---

## Provider outage

**Alert:** `SingleProviderDegraded` (ticket) · `AllProvidersOpenForChannel` (page) ·
`HalfOpenProbeStampede` (ticket)

### One provider degraded — usually nothing to do

The circuit opened, the router failed over, sends continue on the secondary. Confirm
that is actually what happened:

```bash
curl -sS $API/v1/providers/health | jq -c '.providers[] | {code,channel,circuitState,healthy}'
```

```
sum by (provider) (rate(provider_failover_total[5m]))     # failover is happening
sli:delivery_success:ratio_30m                            # and it is working
```

If `delivery_success` is holding, **do not intervene.** Open a ticket with the
vendor and go back to sleep.

### All providers for a channel are OPEN

No failover target remains. Traffic is parking in `notification.retry.1h`, which is
correct but has a 1h12m total budget — after that, records go to the DLQ.

1. **Confirm it is them and not us.** An `AUTH_FAILURE` on every provider at once is
   a secrets-rotation bug on our side, not a coordinated vendor outage.
   ```
   sum by (provider, failure_type) (rate(provider_call_total{outcome="failure"}[5m]))
   ```
2. **Decide explicitly about CRITICAL.** If the vendor is partially up, forcing a
   circuit closed for CRITICAL only is legitimate:
   ```bash
   curl -X POST "$API/admin/v1/providers/mock-sms-primary/circuit" \
     -d '{"state":"CLOSED","trafficClasses":["CRITICAL"],"durationSeconds":600}'
   ```
   Set a duration. A forced-closed circuit with no expiry is how the next incident
   starts.
3. **Shed BULK for that channel** so the retry tiers are not competing with new work
   (shedding level 1, then 2).
4. **Pause dispatch rather than let it churn.** Paused partitions cost nothing;
   failing calls cost the retry budget and keep the provider under load while it
   tries to recover.
   ```bash
   curl -X POST "$API/admin/v1/consumers/dispatch-sms-tx/pause"
   ```

### Circuit flapping between OPEN and CLOSED

`permittedNumberOfCallsInHalfOpenState` is **per JVM**. Forty pods that opened at the
same instant and wait the same duration send 40 × 10 = 400 synchronised probes at a
recovering provider — the exact herd the breaker exists to prevent.

Check that jitter is actually on (`resilience4j.circuitbreaker.configs.provider.enable-randomized-wait`)
and that the shared Valkey circuit state is reachable — with `redis_fallback_active = 1`
every pod is deciding alone and the herd is guaranteed.

### Do not scale out

Lag climbing during a provider outage is a *symptom of the provider being down*, not
of insufficient consumer capacity. KEDA scale-out here points more consumers at a
recovering provider. Verify the suppression is working:

```
notification_worker_scale_out_suppressed{reason="circuit_open"}
```

---

## Consumer lag spike

**Alert:** `ConsumerLagBacklog` · `ConsumerLagGrowing` · `KafkaStuckPoll` · `KafkaRebalanceStorm`

Lag alone tells you nothing. Get the signature first — one query:

```
# is it moving?
deriv(sum by (consumergroup) (kafka_consumergroup_lag)[10m:30s])
# is it committing?
kafka_consumer_coordinator_commit_rate
# is it consuming at all?
kafka_consumer_fetch_manager_records_consumed_rate
# is it rebalancing?
kafka_consumer_coordinator_rebalance_rate_per_hour
```

| Lag | Commit rate | Consumed rate | Diagnosis |
|---|---|---|---|
| Rising | **≈ 0** | > 0 | [Poison message](#poison-message) — go there now |
| Flat, non-zero | ≈ 0 | **0** | Stuck poll — an unbounded provider call inside the loop |
| Rising | > 0 | > 0 | Genuine capacity shortfall, **or** a provider outage |
| Rising on one partition only | > 0 | > 0 | Partition skew — see below |
| Sawtooth | erratic | erratic | Rebalance storm |

### Genuine capacity shortfall

Only after you have ruled out the provider (`provider_circuit_state` all CLOSED):

1. **Scale inside the partition first.** Parallel Consumer `KEY` mode gives roughly
   4× with zero topic changes and zero ordering loss. Exhaust this before touching
   partition counts.
2. Then add consumers, up to the partition count. Beyond that they idle.
3. Only then consider partitions — and read
   [`docs/KAFKA.md` §8](KAFKA.md) first, because increasing partitions on a keyed
   topic permanently breaks per-key ordering across the boundary. Never during a
   campaign window.

### Partition skew

```
sli:kafka_partition_skew:ratio        # > 3 sustained is a bug, not hashing
```

With `recipientId` in the key, 4σ spread is 0.337%. **Every real hot partition is a
bug or an adversary:** a `tenantId`-only key, a time component in the key, a campaign
emitted in ID order, or a runaway recipient. Find the tenant:

```
topk(5, sum by (tenant_id) (rate(notification_accepted_total[5m])))
```

Mitigate with a per-recipient token bucket at the expander and Kafka client quotas.
Note that **quotas throttle by delay, not by error** — the broker mutes the channel,
so a throttled producer sees latency, not exceptions. Watch for
`BufferExhaustedException` on the producer side as the second-order effect.

### Rebalance storm

KIP-62 moved heartbeats to a background thread, so a consumer stuck in a slow
provider call **still heartbeats and looks alive** while `max.poll.interval.ms` runs
out. The loop: provider call hangs → poll interval exceeded → consumer leaves →
rebalance → backlog grows → on resume every consumer pulls a full batch against a
larger backlog → the hanging record is reassigned → repeat.

Fix in this order:
1. Confirm every provider call has connect + read timeouts below the per-record
   budget (`resilience4j.timelimiter.configs.provider.timeout-duration`).
2. Confirm `max.poll.records` is 100, not 500.
3. Confirm `group.protocol=consumer` (KIP-848) is actually in effect — if the broker
   does not offer it, the client silently falls back to `classic`:
   ```bash
   docker compose -f docker/compose.yml exec kafka \
     /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 \
     --describe --group notification-worker --state
   ```
4. **Do not raise `max.poll.interval.ms`.**

---

## Poison message

**Alert:** `KafkaPoisonPillSuspected` (page)

**The signature:** offsets commit only after successful processing, so a record that
always throws makes the consumer seek back forever. Throughput for that partition is
**exactly zero while the consumer looks perfectly healthy** — it polls, it heartbeats,
its liveness probe passes. With `hash(key) % partitions`, a deterministic subset of
tenants receives *nothing* while every group-level dashboard is green.

**Do not restart the consumer.** It will re-read the same record.

### 1. Find the offset

```bash
docker compose -f docker/compose.yml exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 \
  --describe --group notification-worker
```

The stuck partition is the one whose `CURRENT-OFFSET` has not moved while `LOG-END-OFFSET`
climbs. Note the topic, partition and current offset.

### 2. Capture the record before you touch anything

The payload is the entire diagnostic. Once the offset moves you cannot easily get it back.

```bash
docker compose -f docker/compose.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
  --topic notification.dispatch.sms.tx --partition 3 --offset 84172 \
  --max-messages 1 --property print.headers=true --property print.key=true
```

### 3. Route it out and commit

Preferred — the admin endpoint does capture, DLQ-with-context and commit atomically:

```bash
curl -X POST "$API/admin/v1/consumers/dispatch-sms-tx/quarantine" \
  -H 'Content-Type: application/json' \
  -d '{"topic":"notification.dispatch.sms.tx","partition":3,"offset":84172,"reason":"INC-2026-0831 deserialisation failure"}'
```

Break-glass, only if the app is down and the partition must move — this loses the
DLQ context, so **you must have captured the payload in step 2**:

```bash
docker compose -f docker/compose.yml exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 \
  --group notification-worker --topic notification.dispatch.sms.tx:3 \
  --reset-offsets --to-offset 84173 --execute
```

Skipping the offset without a DLQ record **silently violates the delivery contract**.
Record the id in the incident log so the reconciler's count is explainable.

### 4. Classify it

- **Schema-invalid** (deserialisation failure) → the invalid-message channel, not the
  DLQ. Keeping "bad data" and "the provider was down" in separate places is what makes
  DLQ triage tractable.
- **Valid schema, handler throws** → a code bug. The DLQ record is the reproduction case.

### 5. Ask why three attempts did not catch it

Three in-place attempts then DLQ is the designed behaviour. If a record blocked a
partition instead, the retry-then-DLQ path did not run — that is a bigger bug than
the poison record itself.

---

## Outbox age growing

**Alert:** `OutboxAgeHigh` (page, > 120 s) · `OutboxDepthShedding` (page, > 500k)

The accept transaction writes the notification row and the outbox row together and
commits. **Nothing is lost** — the guarantee holds. But nothing is moving either.

```
outbox_oldest_age_seconds
outbox_pending_count
rate(outbox_published_total[5m])        # is the sweeper doing anything?
```

| Publish rate | Meaning |
|---|---|
| 0 | Sweeper is down, or cannot reach Kafka |
| > 0 but < ingest rate | Sweeper is running and losing; scale it or shed BULK |

Checks, in order:

1. **Is Kafka reachable from the API pods?** This is the usual answer.
   ```bash
   docker compose -f docker/compose.yml exec kafka \
     /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:19092 | head -3
   ```
2. **Is the sweeper leader elected?** Like the hydrator, it is single-writer.
3. **Is the partial index still small?**
   ```sql
   SELECT pg_size_pretty(pg_relation_size('notif.outbox_unpublished_ix')),
          count(*) FILTER (WHERE published_at IS NULL) AS pending
     FROM notif.outbox_message;
   ```
   The index should stay a few pages forever, because published rows are **DELETEd**,
   not updated. If it has grown to megabytes, someone changed delete-after-publish to
   update-after-publish and the table now grows without bound.
4. **Autovacuum keeping up?** `fillfactor = 70` exists so outbox updates stay HOT.
   ```sql
   SELECT relname, n_dead_tup, last_autovacuum
     FROM pg_stat_user_tables WHERE relname = 'outbox_message';
   ```

Above 500k pending, BULK ingest is shed with `503 + Retry-After` automatically and
CRITICAL keeps flowing. That is working as designed; do not disable it to "clear the
alert".

---

## Scheduler lag

**Alert:** `SchedulerLagHigh` (page, p99 > 60 s) · `SchedulerLeaseExpiryHigh` (ticket)

```
sli:scheduler_lag:p99_5m
rate(scheduler_lease_expired_total[10m])
rate(scheduler_claim_count_exceeded_total[10m])
```

**Read the shape of the curve — it names the cause.**

| Shape | Cause |
|---|---|
| Lag climbing **linearly with wall clock** | Nothing is scanning. The hydrator leader is lost |
| Lag spikes on the :00 / :15 / :30 / :45 marks | Everything scheduled on the mark; jitter is off or too small |
| Lag climbing with high `lease_expired` | Claimers slower than the lease; work is being done twice |
| Lag climbing with high `claim_count_exceeded` | Rows repeatedly claimed and never dispatched — a downstream failure |

### Hydrator leader lost

```bash
curl -sS $API/actuator/health | jq '.components.schedulerLeader'
```

The lease is 30 s with a 10 s renew. A leader that cannot renew — usually a Valkey
blip, in which case it falls back to Postgres advisory locks — leaves nobody scanning.
Lag then grows exactly one second per second, which is the giveaway.

### Claimers falling behind

Do **not** raise the batch size, and do **not** add claimers. Both make it worse.

`SKIP LOCKED` fixes correctness and serialisation; it does not fix bloat. Each session
must skip every dead or non-matching tuple left behind by every other session, so
wasted index visits scale as **B·W²/2** in batch size B and worker count W. Raising B
multiplies the quadratic term. A documented case hit a hard wall at 128 concurrent
claimers on 80 cores.

The lever is **shard affinity**: increase `notification.scheduler.claimer.shards` so
each claimer owns a disjoint hash range and skips nothing. Then, if still short,
add claimers up to the shard count.

### Quiet hours delivering late

The tick is 15 minutes, not hourly, because UTC+05:45 (Nepal), UTC+05:30 (India) and
UTC+08:45 (Eucla) are not on hour boundaries — an hourly tick delivers up to 45
minutes into someone's quiet window.

If a specific timezone is wrong, check that the value was resolved in the
application and not in Postgres. `AT TIME ZONE` resolves against the tzdata bundled
with the *server*, so after a DST rule change the database and the application
disagree until the next minor upgrade, and only for the affected zone.

---

## DEFAULT partition non-empty

**Alert:** `DefaultPartitionNonEmpty` (ticket) — fires on `> 0`, because any row here
is actionable.

A DEFAULT partition exists so a missing daily partition is not a hard outage. But it
is a **safety net with an alarm on it, never a destination**: a row sitting in DEFAULT
**blocks creation of the real partition** while holding an exclusive lock. The trap is
that this is silent for a week and then fails at 02:00 during partition maintenance.

### 1. Find the rows and what range they need

```sql
SELECT min(created_at), max(created_at), count(*)
  FROM notif.notification_default;
```

### 2. Move them out, create the partition, put them back

Inside one transaction, with a `lock_timeout` so you cannot wedge the table:

```sql
SET lock_timeout = '3s';
BEGIN;

CREATE TEMP TABLE stranded AS
  DELETE FROM notif.notification_default RETURNING *;

CREATE TABLE notif.notification_p2026_09_02
  PARTITION OF notif.notification
  FOR VALUES FROM ('2026-09-02') TO ('2026-09-03');

INSERT INTO notif.notification SELECT * FROM stranded;

COMMIT;
```

`DELETE … RETURNING` into a temp table rather than `INSERT … SELECT` then `DELETE`:
the partition-key move is a DELETE+INSERT anyway, and doing it in two statements
leaves a window where the row exists twice.

### 3. Fix the cause

Always one of three:

- **Partition pre-creation job did not run.** It should maintain 7 days ahead. Check
  the schedule, then widen the horizon.
- **A row arrived with an out-of-range `created_at`** — a client-supplied timestamp,
  or a clock skew. Server-generate it.
- **A backfill or replay wrote historical rows** whose partitions were already dropped
  by retention.

Verify the pre-creation horizon before you close:

```sql
SELECT max(substring(relname from '\d{4}_\d{2}_\d{2}$'))
  FROM pg_class WHERE relname LIKE 'notification_p%';
```

---

## Replication slot growth

**Alert:** `ReplicationSlotRetainingWal` (ticket, > 10 GB)

An inactive replication slot pins WAL **forever**. The failure mode is that a disabled
downstream consumer quietly becomes a **primary database outage** when the data volume
fills. This is one of the few tickets that turns into a page if ignored.

```sql
SELECT slot_name, active, slot_type,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS unconfirmed
  FROM pg_replication_slots
 ORDER BY pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) DESC;
```

```sql
SELECT pg_size_pretty(sum(size)) FROM pg_ls_waldir();
```

**Decide explicitly. There is no safe middle option.**

| `active` | Retained growing | Do this |
|---|---|---|
| `t` | yes | The consumer is up but slow. Fix the consumer; do not drop the slot |
| `f` | yes | Nobody is consuming. Either resume it now, or drop it |
| `f` | > 50% of volume free space | **Drop it.** A dead slot is not worth the primary |

```sql
SELECT pg_drop_replication_slot('cdc_notification_events');
```

Dropping is irreversible: the downstream consumer must re-snapshot. That is a day of
CDC backfill, versus an outage. Take the backfill.

Prevention: `max_slot_wal_keep_size` bounds the damage by invalidating a slot instead
of filling the disk. It converts a database outage into a consumer outage, which is
the trade you want.

---

## Metastable failure

**Alert:** `MetastableFailureSuspected` (page)

**The signature is counter-intuitive:** offered load has *dropped* and the error rate
has *not*. The trigger is gone; the system is now sustaining its own failure. An
OSDI'22 study of 22 incidents found at least 4 of the 15 major AWS outages of the
prior decade were metastable failures, lasting 1.5–73 hours, with **~50% having retry
policy as the sustaining effect**.

**Every instinct is wrong here.** Do not scale out. Do not restart pods in a rolling
fashion. Both add load to a system that is already saturated by its own retries.

1. **Cut the sustaining loop.** Retry budget to zero first — it is the most common
   sustaining effect and the cheapest thing to remove.
   ```bash
   curl -X POST "$API/admin/v1/retry-budget" -d '{"ratio": 0.0, "durationSeconds": 900}'
   ```
2. **Shed hard.** Level 3 or 4. Get offered load *below* the degraded capacity, not
   near it.
3. **Wait for error rate to fall.** It will fall sharply, not gradually. That is the
   confirmation you were in a metastable state.
4. **Reintroduce load in steps**, restoring the retry budget last.
5. **Post-incident: fix the sustaining loop, not the trigger.** The next trigger will
   be different and the failure state identical.

---

## DR failover

**Objective: RTO 30 minutes, RPO < 5 minutes.** Warm standby, not active/active —
active/active would need conflict-free cross-region dedup, which was cut deliberately.

Exercised quarterly as a game day. If you are reading this for the first time during
an actual region loss, follow it literally and do not optimise.

### Decision gate (do this first — 2 min)

Failover is not free: it costs up to 5 minutes of accepted-but-unreplicated
notifications and a re-snapshot of anything CDC-attached.

**Fail over only if** the primary region is expected to be down longer than 30
minutes *and* the API is failing there. A degraded-but-working region is not a
failover; it is a shedding problem.

Declare it out loud in the incident channel with a named decision maker.

### 1. Stop the writers in the primary region (2 min)

If the region is partially alive, this prevents a split brain where both regions
accept and neither replicates.

```bash
kubectl --context primary scale deploy/notification-api --replicas=0
kubectl --context primary scale deploy/notification-worker-{sms,email,push}-{tx,bulk} --replicas=0
kubectl --context primary scale deploy/notification-scheduler --replicas=0
```

If the region is unreachable, skip this and rely on Route 53 (step 4) — but **record
that you skipped it**, because it changes how you reconcile duplicates afterwards.

### 2. Promote the database (5–10 min)

```bash
aws rds promote-read-replica --db-instance-identifier notification-dr \
  --region us-west-2
aws rds wait db-instance-available --db-instance-identifier notification-dr \
  --region us-west-2
```

Verify before you point anything at it — a promoted replica that is missing the last
migration will fail `ddl-auto: validate` on every pod simultaneously:

```sql
SELECT version, description, success, installed_on
  FROM notif.flyway_schema_history ORDER BY installed_rank DESC LIMIT 3;
SELECT max(created_at) FROM notif.notification;   -- how far behind are we?
```

`now() - max(created_at)` is your **actual RPO for this incident**. Write it down; it
is the number you will be asked for.

### 3. Scale up the DR compute (5 min)

Terraform keeps the DR EKS cluster at zero. Scale it, do not create it.

```bash
terraform -chdir=infra/dr apply -var 'desired_capacity=full' -auto-approve
kubectl --context dr rollout status deploy/notification-api --timeout=300s
```

Readiness is gated on Kafka and the database, so pods will not take traffic until
both are actually reachable. Do not remove the gate to "speed things up".

### 4. Repoint Route 53 (2 min + TTL)

```bash
aws route53 change-resource-record-sets --hosted-zone-id $ZONE \
  --change-batch file://infra/dr/failover-recordset.json
```

The record TTL is 60 s deliberately. Clients that cache DNS longer will keep hitting
the dead region — that is expected and self-resolving; do not chase it.

### 5. Verify translated Kafka offsets (5 min) — the step people skip

MSK Replicator does **not** preserve offsets across clusters. It maintains a
translation, and consumer groups must be positioned using it. Get this wrong in
either direction and you either **replay hours of already-sent notifications** or
**silently skip a window**.

```bash
kafka-consumer-groups.sh --bootstrap-server $DR_BOOTSTRAP \
  --describe --group notification-worker
```

Every partition must show a `CURRENT-OFFSET` that is set and plausible. `-` means the
group has no committed offset in this cluster and will apply `auto.offset.reset`
(`earliest` — a full replay). Fix it before starting workers, not after.

### 6. Replay `notification.requested` from the last known-good offset (10 min)

This is why that topic has **7 days** of retention while the dispatch topics have 3.

Requests accepted in the primary region within the replication gap were durably
recorded there but never expanded here. Replay closes the RPO window:

```bash
curl -X POST "$DR_API/admin/v1/replay" -H 'Content-Type: application/json' -d '{
  "topic": "notification.requested",
  "fromTimestamp": "2026-08-31T09:52:00Z",
  "toTimestamp":   "2026-08-31T09:58:00Z",
  "ratePerSecond": 500
}'
```

Use the **last known-good offset timestamp**, and overlap it by a minute. Overlapping
is safe and skipping is not: the idempotency layer collapses the duplicates, keyed on
`(tenantId, idempotencyKey)`, and the monotonic guard drops the stale status events.

### 7. Confirm and hand over

```
sli:api_availability:error_ratio_5m       # accepting
rate(notification_accepted_total[5m])     # at a plausible rate
sli:dispatch_latency:p99_5m               # dispatching
sum(kafka_consumergroup_lag)              # draining, not growing
```

Post in the incident channel: RTO achieved, measured RPO from step 2, whether step 1
was skipped, and the replay window from step 6.

### Failing back

**Do not fail back during business hours, and never automatically.** The primary is
now the stale side and must be rebuilt as a replica of the DR region, re-verified,
and cut over with the same procedure in reverse. Treat it as a planned change with a
maintenance window, not as "returning to normal".

---

## Appendix — local reproduction

Most of the above can be rehearsed locally against `docker/compose.yml`.

```bash
make up                     # infrastructure + the 16 topics
make demo                   # provider outage, circuit open, failover, recovery
make psql                   # the DEFAULT-partition and replication-slot SQL
make logs S=kafka           # broker-side view
```

The local stack is single-node with `RF=1`, so anything involving ISR shrink, unclean
leader election, `min.insync.replicas` or cross-AZ behaviour **cannot** be reproduced
here. Do not conclude from a green local run that the durability configuration works;
that belongs to Testcontainers and staging.
