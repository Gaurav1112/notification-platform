# ADR-003: Campaign fan-out runs in the scheduler on its own pool

**Status:** Accepted
**Date:** 2026-08-31

## Context

A campaign request is O(1) at the edge and O(10,000) behind it. The blended fan-out is 1.67
recipients per request, but the tail is a 10-million-recipient blast that must be expanded into
individual dispatch records.

Expansion cannot happen in the request thread — that is settled by the 250 ms accept objective. The
open question is *where* it happens: which deployable owns it, and how it is isolated from the work
around it.

The risk is that a 10M expansion starves the thing next to it. If fan-out shares a thread pool with
the due-scan claimers, a campaign delays every scheduled OTP for as long as it takes to expand.

## Decision

**Fan-out runs in the existing worker/scheduler tier, in its own bulkhead — a dedicated, bounded
thread pool — rather than in a fourth deployable.**

The isolation requirement is satisfied by the bulkhead. A separate deployable would satisfy it too,
and would also add a service to build, deploy, monitor, scale, page on and reason about.

## Alternatives considered

### A fourth deployable, `app-expander`

The clean answer, and the one a purist reaches for. It gives true resource isolation: separate pods,
separate memory, separate scaling policy, and a campaign cannot touch the scheduler at all.

Rejected because the isolation it buys over a bulkhead is small, and the cost is not:

- A fourth Helm chart, HPA, dashboard, alert set and on-call surface.
- Another consumer group and another set of offsets to reason about during an incident.
- Another deployable that can be at a different version during a rolling deploy.

The design's own rule is that a new deployable has to earn its existence by needing a genuinely
different scaling signal. Expansion scales with the same signal as the rest of the async tier —
consumer lag — so it does not.

### Fan-out in the API request thread

Fails the 250 ms accept objective by four orders of magnitude on a large campaign. Not seriously
considered; listed because it is what a first implementation does.

### Fan-out in the channel workers

Puts expansion on the same threads as provider calls. A campaign expansion would then compete with
the OTP sends it is meant not to delay, which is precisely the failure to avoid.

## Consequences

### Positive

- **Three deployables, not four.** Each has a distinct scaling signal: API scales on requests, workers
  on consumer lag, scheduler on a fixed small count.
- The bulkhead is visible in the code (`ChannelExecutors`, `WorkerProperties`) and its saturation is
  attributable — a full queue names the pool that filled.
- Expansion can use `COPY` for the insert path without any cross-service coordination, because it is
  in the same JVM as the repository.

### Negative

- **Isolation is by convention plus a pool boundary, not by process boundary.** A memory-hungry
  expansion can still cause GC pressure that the co-located claimers feel. A bulkhead bounds threads;
  it does not bound heap.
- Scaling is coupled: you cannot scale expansion capacity without scaling the deployable it lives in.
- The pool size becomes a tuning parameter that has to be revisited as campaign sizes grow, and
  getting it wrong is a queue that fills silently.
- If a future campaign workload genuinely needs different hardware — more memory, no provider
  network access — the fourth deployable comes back, and the migration is not free.

## Revisit when

A single campaign expansion regularly saturates the bulkhead for longer than one scheduler scan
interval, or when expansion needs a different instance type from the rest of the tier.
