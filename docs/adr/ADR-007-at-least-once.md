# ADR-007: At-least-once transport with idempotent dispatch, not exactly-once

**Status:** Accepted
**Date:** 2026-08-31

## Context

The obvious thing to promise is exactly-once delivery. Every user of a notification platform wants
it, and Kafka's transactional producer plus `read_committed` consumers advertises exactly-once
*processing*.

The problem is the last hop. The provider call is an HTTP request to a third party that cannot enlist
in a Kafka transaction or a database transaction. There is a moment where the request is on the wire
and no amount of local transactionality tells us what happened to it.

Making the promise anyway means one of two things: quietly meaning "exactly-once within our own
system", which is a different and much weaker claim than the reader hears; or building a
reconciliation layer and calling its output exactly-once, which is at-least-once with better
marketing.

## Decision

**Transport is at-least-once. Dispatch is idempotent. Exactly-once delivery is explicitly not
offered.**

The published contract:

> **Guaranteed:** once we return `202 Accepted`, the request is durably recorded and will reach a
> terminal state (`DELIVERED`, `FAILED`, `EXPIRED`, `SUPPRESSED`) or sit visibly in the DLQ. It will
> never be silently dropped.
>
> **Not guaranteed:** exactly-once *delivery*. A provider that ACKs after our client timeout can still
> produce a genuine duplicate. We target **< 0.01%**, measure it, and publish it.
>
> **Ordering:** per `(recipient, channel)` only. Global ordering is not offered.

## The five layers that make at-least-once safe

| # | Boundary | Key | Store | TTL | Defends against |
|---|---|---|---|---|---|
| 1 | HTTP ingress | `Idempotency-Key` scoped `(tenantId, key)` | Postgres + Redis | 24 h | Client and ALB retries |
| 2 | Kafka consume | `eventId` | Redis `SETNX` → Postgres | 7 d | Redelivery, rebalance replay |
| 3 | Business dedup | `hash(tenant, user, channel, template, window)` | Redis cuckoo filter | 24 h | Same logical send via two paths |
| 4 | Attempt registration | `(recipient_id, attempt_number)` UNIQUE | Postgres, **before** the call | permanent | Worker crash mid-send |
| 5 | Provider dispatch | `idempotency_token` sent to the provider | Postgres UNIQUE | provider | Provider ACKed and we never saw it |

Layer 1 also checks a **request fingerprint** — a SHA-256 of the canonical body. Same key with a
*different* body is a `409`, never a silent replay of an unrelated response. That check is the one
most implementations skip.

## Why layer 5 cannot be relied on — the table most designs get wrong

Verified 2026-08-31:

| Provider | Client idempotency key | Reconciliation by our reference |
|---|---|---|
| **Twilio SMS** | **NO** | **NO** — `GET /Messages` filters only on To/From/DateSent at *whole-day* granularity, and there is no client-reference field on Messages |
| AWS SNS | Partial — FIFO only, 5-minute minimum window | via SQS |
| **AWS SES** | **NO** | Yes — `EmailTags`. AWS explicitly documents that a timeout may follow acceptance and a retry sends a **second email with a different message ID** |
| SendGrid | **NO** | Yes — `custom_args` echoed on every Event Webhook payload |
| **FCM HTTP v1** | **NO** (open feature request since 2018) | **NO** per-message receipt — aggregate Data API only, up to 5 days lag, sampled |
| **APNs** | **NO** — `apns-id` is correlation for error reporting only | **NO** — no webhook at all |

**No provider we would plausibly integrate offers a usable client idempotency key.** Layer 5 is
therefore *aspirational*, and the design must not assume it.

## The consequence: delivery semantics differ per channel

An idempotency key converts duplicate risk into *loss* risk; the ledger plus a reconciler converts it
back — **but only where reconciliation is actually possible.** So the platform states per channel
which trade it has chosen:

| Channel | Duplicate cost | Semantic on `UNKNOWN` |
|---|---|---|
| **SMS** | Real money, user trust, carrier spam flags — and no way to reconcile on Twilio | **At-most-once.** Do not resend. Accept a small, measured loss rate: a lost OTP is recoverable by the user retrying; **a duplicate OTP is not recoverable at all** |
| **Email** | Negligible; exact reconciliation via `custom_args` / `EmailTags` | At-least-once + reconcile |
| **Push** | ~zero | At-least-once, with `apns-collapse-id` / `collapse_key` set to our dedup key so a duplicate **replaces** rather than stacks |

That table is the real content of this ADR. A uniform guarantee across three channels with three
different cost asymmetries would be wrong for at least two of them.

## Alternatives considered

### Kafka exactly-once semantics (transactional producer + `read_committed`)

**Used**, on the internal hops — the expander produces inside a Kafka transaction and consumers read
committed. It solves Kafka-to-Kafka and Kafka-to-Postgres. It does nothing for Kafka-to-Twilio,
which is the only hop where a duplicate is user-visible.

Advertising "we use Kafka EOS, therefore exactly-once delivery" is the specific conflation this ADR
exists to refuse.

### Two-phase commit with the provider

Providers do not offer a prepare phase. Not available.

### Always resend on `UNKNOWN`

Maximises delivery, guarantees duplicates on the timeout path. Wrong for SMS by a wide margin.

### Never resend on `UNKNOWN`

Minimises duplicates, loses messages that genuinely failed before reaching the provider. Wrong for
push, where a duplicate costs nothing.

Neither is right for all three channels, which is why the decision is per channel.

## Consequences

### Positive

- **The contract is true.** Everything the platform promises, it can deliver.
- Five independent layers mean no single failure produces a duplicate. Redis can be down (layer 2
  fails open) and layers 4 and 5 still hold.
- **The duplicate rate is measured and published** — `sli:duplicate_delivery:ratio_1h`, with a page at
  0.1% (10× the SLO, deliberately, so noise at low volume does not page).
- `SendResult.Indeterminate` being a first-class case in a sealed interface means the compiler asks
  every caller about the `UNKNOWN` branch.

### Negative

- **A small number of users will get two OTPs.** Under 0.01%, but not zero, and that is a real product
  consequence to defend to a stakeholder who wanted a guarantee.
- **Five idempotency layers is real complexity** — five stores, five TTLs, five failure modes,
  documented in five places.
- **SMS accepts measured loss.** On `UNKNOWN` we do not resend, so an OTP that genuinely failed
  in-flight is simply not delivered. The justification is asymmetric recoverability, and it is a
  judgement call, not a derivation.
- The reconciler that resolves `UNKNOWN` on email and push **is not built** — see
  [STATUS.md](../STATUS.md). Until it is, the `UNKNOWN` population only resolves by webhook.

## Say it plainly

> *"I don't offer exactly-once, and I'd push back on anyone who claims it for this problem. The
> provider call can't join a transaction. What I offer is at-least-once transport, idempotent dispatch
> at five layers, a first-class UNKNOWN state, and a measured duplicate rate under 0.01% — and the
> semantics differ per channel because a duplicate SMS costs real money and a duplicate push costs
> nothing."*

## Related

- [ADR-002](ADR-002-outbox-plus-fast-path.md) — the outbox, which guarantees duplicates on the publish
  side and is safe for the same reasons
- [ADR-018](ADR-018-webhooks-over-polling.md) — the monotonic guard that makes duplicate inbound
  events harmless
