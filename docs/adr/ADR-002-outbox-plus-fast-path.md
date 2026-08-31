# ADR-002: Transactional outbox with a post-commit fast path

**Status:** Accepted
**Date:** 2026-08-31

## Context

`POST /v1/notifications` must do two things that cannot happen atomically: write rows to PostgreSQL,
and publish an event to Kafka. There is no transaction that spans both.

The API also has a **p99 < 250 ms** accept-latency objective and a **99.999% acceptance durability**
SLO — once we return `202`, the request must reach a terminal state or sit visibly in the DLQ. Those
two constraints pull in opposite directions: durability wants a synchronous confirmation, latency
wants none.

## Decision

**Write the notification rows and an `outbox_message` row in one database transaction. After that
transaction commits, publish to Kafka on a best-effort fast path. A separate `OutboxSweeper` drains
anything the fast path missed.**

```java
// 3. The single atomic accept decision.
var records = acceptanceWriter.persist(command, requestId, acceptedAt, expiresAt);

// 5. Post-commit, best-effort. The outbox sweeper is the guarantee.
try {
    eventPublisher.publishRequested(notice);
} catch (RuntimeException e) {
    log.warn("fast-path publish failed for request={}, outbox sweeper will recover", …);
}
```

The commit in step 3 **is** the accept decision. Before it, nothing happened and the client's retry is
free. After it, the message will be delivered even if the pod dies on the next instruction.

## Alternatives considered

### Dual write — write to Postgres, then publish, both inline

The default thing people do, and it has **no atomicity at all**. Four failure orderings, three of
them bad:

| | Publish succeeds | Publish fails |
|---|---|---|
| **Commit succeeds** | fine | notification recorded, never sent — silent loss |
| **Commit fails** | sent, never recorded — phantom delivery, no audit trail | fine |

There is no ordering that fixes this, only orderings that change which failure you get.

### Kafka-first — publish, then persist from a consumer

Atomic in the sense that Kafka is the source of truth. It breaks **read-your-writes**: the client gets
a `202` with an id, immediately `GET`s it, and the row does not exist yet. Every client then has to
tolerate a 404 that means "not yet" rather than "never", which is an API contract nobody wants.

It also puts the durability of an accepted request on the broker's availability, which inverts the
dependency the SLO cares about.

### Kafka transactions spanning the DB write

The provider call cannot enlist in a Kafka transaction, and neither can a JDBC connection without
XA. Two-phase commit across PostgreSQL and Kafka is technically possible and operationally
miserable — prepared transactions that survive a crash pin `xmin` and stop autovacuum, which is the
failure mode this design works hardest to avoid.

### Debezium / CDC on the outbox table instead of a sweeper

Genuinely good, and the right answer at larger scale. Rejected here as a fourth stateful component to
run, monitor and fail over, for a table the sweeper drains at ~10k/s. Noted as a future option rather
than a rejection on merit.

## Consequences

### Positive

- **No loss window.** A broker outage costs latency, not messages. The client's `202` stays honest.
- **The accept path never fails because Kafka is unavailable.** The catch block is three lines and it
  is the reason `POST /notifications` has one hard dependency instead of two.
- **Latency stays low.** The fast path means the common case does not wait for a sweeper tick.
- The outbox depth is a natural back-pressure signal — `outbox_oldest_age_seconds` drives both a
  ticket and a page, and the load-shedding ladder keys off it.

### Negative

- **Duplicate publishes are guaranteed, not merely possible.** The fast path and the sweeper will both
  send the same record whenever a publish succeeds and the delete does not. Every consumer therefore
  has to be an idempotent receiver, and every status write has to go through the monotonic guard.
  That is a real cost paid by every consumer in the system.
- **A fourth table to operate.** `outbox_message` needs `fillfactor = 70`, a partial index
  `WHERE published_at IS NULL`, aggressive autovacuum settings, and rows must be **DELETEd** rather
  than stamped — an update-based design makes the table one of the largest in the database.
- **Two publishers to keep in step.** The fast path and the sweeper must agree on topic and key. The
  design forces this by storing both on the outbox row and forbidding the relay from re-deriving
  them; a disagreement would put the same event on two topics, invisible until a consumer group
  reports zero lag on a topic nobody produces to any more.
- One more moving part in the accept path to explain to a new engineer.

## Related

- [ADR-007](ADR-007-at-least-once.md) — at-least-once delivery, which this makes safe
- [ADR-018](ADR-018-webhooks-over-polling.md) — the same idempotency machinery on the inbound side
