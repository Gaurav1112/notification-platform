# ADR-006: Kafka as the message broker

**Status:** Accepted
**Date:** 2026-08-31

## Context

The asynchronous spine of the platform carries accepted requests to fan-out, dispatch records to
channel workers, delayed retries to tiers, outcomes to the status pipeline, and failures to a DLQ.
At the stretch operating point that is ~604M records/day, peaking at 57,222 msg/s during a campaign
burst.

Four properties are needed:

1. **Replay** — a bad deploy that corrupted a day of statuses must be recoverable by reprocessing.
2. **Per-key ordering** — messages for one `(tenant, recipient, channel)` must arrive in order.
3. **Consumer-group rescale** — adding worker pods must redistribute work without reconfiguration.
4. **Compaction** — the projected current status per notification is a keyed, last-value-wins
   dataset.

## Decision

**Apache Kafka 4.3.1, KRaft mode.** 16 topics, 288 partitions.

## Alternatives considered

### RabbitMQ

The most credible alternative, and it loses on the first property.

**RabbitMQ deletes on ack.** Replay is not a configuration option; it is a feature that does not
exist. Once a delivery-status consumer has acked a bad message, the message is gone, and recovering
from a projection bug means reconstructing from PostgreSQL or not at all.

Secondary problems: ordering guarantees weaken with competing consumers and requeues; consistent
hash exchanges exist but are a plugin rather than the model; and there is no compaction.

RabbitMQ is better at per-message routing, priorities and TTLs. This design deliberately does not use
priorities ([ADR-008](ADR-008-topic-split-by-class.md)), which removes its main advantage.

### AWS SQS + SNS

Fully managed, no cluster to run, and genuinely attractive operationally.

Rejected on ordering and replay. Standard queues are unordered and at-least-once with no key
affinity. FIFO queues cap at 3,000 msg/s with batching per message group — an order of magnitude
short of Burst A. Neither offers replay, and neither offers compaction.

The `MessageDeduplicationId` on FIFO is interesting for idempotency, with a 5-minute minimum window
that is too short for the 7-day dedup horizon this design uses.

### Apache Pulsar

Technically the best fit on paper: tiered storage, per-message TTL, multi-tenancy, and both queue and
stream semantics. Rejected on ecosystem and operational familiarity — Spring integration is thinner,
the operational literature is far smaller, and running BookKeeper alongside brokers is a second
distributed storage system to understand.

This is a "not now" rather than a "no". If the platform ever needs per-message delay natively, Pulsar
does what the five retry tiers emulate.

### Redis Streams

Already have Valkey. Consumer groups exist. But durability is Redis durability, replay is bounded by
memory, and putting the delivery spine on the same instance as the cache couples two failure domains
that the design deliberately keeps separate — Redis loss must degrade quality, never correctness.

### PostgreSQL as the queue

Genuinely viable at the base operating point and worth saying so. It fails at the stretch point on
the same write-amplification grounds that put the PostgreSQL write path second on the bottleneck
ladder, and `SKIP LOCKED` contention degrades quadratically in worker count — the measurement that
drives [ADR-013](ADR-013-shard-affine-due-scan.md).

## Consequences

### Positive

- **Replay is a consumer-group offset reset.** That is the property that makes the DLQ useful and
  makes a projection bug survivable.
- **Per-key ordering for free**, because `murmur2(key) % partitions` is deterministic and partitions
  are FIFO.
- **Scaling is adding pods.** Consumer groups redistribute partitions with no configuration change.
- **Compaction** gives `notification.status` a keyed last-value-wins projection without a separate
  materialised view.
- KRaft removes ZooKeeper — one fewer distributed system, and ZooKeeper was removed outright in
  Kafka 4.0 anyway.
- KIP-848 (GA in 4.0) moves assignment to the broker-side coordinator, removing the group-wide
  synchronisation barrier that makes a rebalance stop *every* consumer.

### Negative

- **Partition count is the one decision you cannot cheaply change.** Increasing partitions on a keyed
  topic re-maps `murmur2(key) % N` and **permanently breaks per-key ordering across the boundary**.
  That is why keyed topics carry a 1.5× over-provisioning factor and why the total is 288 rather than
  something derived purely from today's load.
- **Consumer liveness is subtle and the failure mode is silent.** `max.poll.interval.ms` combined with
  KIP-62's background heartbeat means a consumer stuck in a slow provider call *keeps heartbeating and
  looks alive* while the poll clock runs out. The resulting rebalance storm presents as perfect broker
  health. Three separate mechanisms in this codebase exist to avoid causing it.
- **No native per-message delay.** Retry backoff needs five delay topics and a pausing consumer
  ([ADR-009](ADR-009-tiered-retry-topics.md)). Pulsar and RabbitMQ both have this natively.
- **A cluster to operate**: 6 brokers, 288 partitions, 864 replicas, rebalances, ISR, retention,
  storage. Managed MSK reduces but does not remove this.
- Local development needs a Kafka container, which is the largest single component of the compose
  stack.

## Notes on the topology

The 288 partitions are derived, not chosen — the formula and every input consumer rate are in
`TopicPartitions`. The binding input is **measured work per message, never bytes**: even at 100M/day
with a campaign superimposed, the cluster ingests ~16.5 MB/s compressed, which one broker handles.
Sizing Kafka by MB/s produces a cluster roughly 5× short on partitions and 2× oversized on brokers.

## Related

- [ADR-008](ADR-008-topic-split-by-class.md) — why 6 dispatch topics and not 1 with a priority field
- [ADR-009](ADR-009-tiered-retry-topics.md) — how delay is implemented without native support
- [ADR-007](ADR-007-at-least-once.md) — the delivery semantics Kafka's at-least-once implies
