# ADR-016: Single region with warm DR, not active/active

**Status:** Accepted
**Date:** 2026-08-31

## Context

The platform is a system of record for accepted notifications with a published **99.999% acceptance
durability** SLO and a **99.95% API availability** SLO. A regional AWS outage takes it down.

The two credible postures are:

- **Active/active** — both regions accept traffic, replicating bidirectionally.
- **Single region + warm standby** — one region serves, a second is provisioned and replicating,
  promoted on failure.

## Decision

**Single region, warm standby DR. RTO 30 minutes, RPO < 5 minutes.**

Replica promotion for RDS, MSK Replicator for Kafka, EKS rebuilt from IaC, DNS failover. Exercised as
a quarterly game day.

## Why active/active does not work for this system

The blocker is not compute or storage. It is **cross-region deduplication.**

### The idempotency key problem

Layer 1 of the idempotency stack is `(tenantId, Idempotency-Key)`. A client retry after a timeout is
routed by DNS or an ALB, and there is no guarantee it lands in the same region as the original.

Two regions receiving the same key must agree that it is the same request, or the client's retry sends
a second SMS. Three ways to arrange that, all bad:

| Approach | Problem |
|---|---|
| **Synchronous cross-region check** | 60–150 ms round trip added to a path with a 250 ms p99 budget — 24–60% of the entire budget, on every request, permanently, to defend against an event that happens once every few years |
| **Asynchronous replication of the key store** | Replication lag *is* the duplicate window. A retry inside that lag sends twice, and a client retrying a timeout retries fast — the retry is far more likely to land inside the window than outside it |
| **Sticky routing by tenant** | Now it is not active/active. It is two active/passive deployments with a routing rule, and a failover still has the same problem for the tenants that move |

### And it is not only the idempotency key

The same argument applies to four other pieces of shared state:

- **Frequency capping** — "no more than 3 marketing messages per week" is meaningless if each region
  counts separately.
- **Suppression** — an unsubscribe processed in one region must stop sends from the other
  *immediately*. Replication lag here is a compliance failure, not a quality one.
- **Circuit-breaker state** — two regions independently discovering the same provider outage doubles
  the probe load on a recovering vendor.
- **Provider rate-limit budgets** — a contracted 100 msg/s divided across two regions that cannot see
  each other is either exceeded or half-wasted.

**Cross-region consistency requirements outweigh the availability benefit.** That is the finding.

## What warm standby actually costs

| | |
|---|---|
| **RTO 30 min** | Detect, decide, promote the RDS replica, repoint MSK Replicator, apply EKS IaC, cut DNS |
| **RPO < 5 min** | Bounded by RDS replica lag and MSK Replicator lag |
| **Data at risk** | Up to 5 minutes of accepted-but-unreplicated notifications — accepted rows whose `202` was honest at the time and that the standby has never seen |

That last row is the honest statement of the loss, and it is published in the delivery contract rather
than buried.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Active/active, both regions accepting** | Cross-region dedup, per above |
| **Active/active with a global consensus store** (Spanner, DynamoDB Global Tables with transactions) | Solves dedup at the cost of a cross-region write on the accept path — the same latency problem, relocated |
| **Multi-region read replicas, single-region writes** | Helps read latency, not availability. The write path — which is 14.8× the read path — is still single-region |
| **Cold DR: backups and a runbook** | Cheaper. RTO measured in hours, which fails the availability SLO on a single incident |
| **Pilot light: data replicated, compute at zero** | Cheaper than warm, RTO 60–90 min. Rejected because scale-from-zero during a regional failover means bringing up 18 pods *and* draining a backlog simultaneously, which is the worst moment to be cold |
| **Multi-AZ only, no DR at all** | Multi-AZ is already in place and survives an AZ loss. It does not survive a region loss, and a system of record with a 99.999% durability SLO needs an answer for that |

## Consequences

### Positive

- **The idempotency guarantee is unqualified.** One region, one key store, one answer. No replication
  window in which a client retry duplicates.
- Suppression and frequency capping are strongly consistent, which matters for compliance.
- Provider rate-limit budgets and circuit-breaker state are globally coherent, so a recovering vendor
  sees one fleet's probes rather than two.
- **Substantially cheaper.** No duplicate compute at full scale, no cross-region data transfer on the
  hot path.
- Vastly simpler to operate, debug and reason about. During an incident there is one place to look.

### Negative

- **A regional outage is an outage.** 30 minutes of unavailability, and the SLO absorbs it — 30 minutes
  is more than the 21.6-minute monthly error budget, so a single regional event burns a month of
  availability budget entirely.
- **Up to 5 minutes of accepted work can be lost.** Those requests received a `202`. That is a
  contract violation during a regional failover, and it is stated in §1.5 rather than hidden.
- **The failover is a procedure, not a mechanism.** Procedures rot. This one needs a quarterly game
  day, and a DR plan that has not been exercised in a year is a document, not a capability.
- Standby capacity is paid for and idle.
- **None of this is built.** There is no DR tooling, no IaC and no game-day runbook in this repository
  — the local Docker Compose stack is the only deployable environment.

## Revisit when

- A customer contract requires a regional RTO under 30 minutes.
- The platform's write path becomes idempotent without a shared key store — for example if every
  client supplied a globally-unique, deterministically-derived id that both regions could agree on
  without communicating.
- Managed cross-region strongly-consistent stores become cheap enough that the accept path can afford
  one.

## Related

- [ADR-007](ADR-007-at-least-once.md) — the idempotency stack this protects
- [FAILURE-MODES.md](../FAILURE-MODES.md#10-region-loss) — the operational view
