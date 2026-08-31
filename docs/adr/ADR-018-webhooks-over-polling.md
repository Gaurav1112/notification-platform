# ADR-018: Push webhooks for delivery status, with a reconciler backstop

**Status:** Accepted
**Date:** 2026-08-31

## Context

A provider accepting a message is not a delivery. SMS reaches a handset seconds to minutes later;
email is accepted by the receiving MTA and may bounce hours later; push may find the device offline.

The platform needs to know the final outcome for every message, because `DELIVERED`, `BOUNCED` and
`COMPLAINED` drive the suppression list, the delivery-success SLI and the tenant-facing status API.

Two ways to find out: ask the provider (poll), or be told (webhook).

## Decision

**Push webhooks as the primary mechanism, with a reconciler sweeping the residue.**

Inbound webhooks pass four verification gates and are applied through the monotonic status guard.

## Why polling does not scale

```
100,000,000 notifications/day
polling each once = 1,157 status queries/s sustained
polling each 3 times (accepted → sent → delivered) = 3,472/s
```

Against provider APIs whose rate limits are in the **hundreds** of requests per second. The polling
traffic would exceed the send traffic, and it would consume the same rate-limit budget the sends need.

And it does not even work, because for the two most important providers **there is nothing to poll**:

| Provider | Can we query the status of a specific message by our own reference? |
|---|---|
| **Twilio SMS** | **No.** `GET /Messages` filters only on To/From/DateSent, at *whole-day* granularity. There is no client-reference field on Messages |
| **FCM** | **No** per-message receipt. Aggregate Data API only, up to 5 days lag, sampled |
| **APNs** | **No** webhook and no query. Aggregate console metrics only |
| AWS SES | Yes, via `EmailTags` |
| SendGrid | Yes, via `custom_args` |

So polling is untenable at scale *and* impossible for SMS and push. Webhooks are not a preference;
they are the only mechanism that exists.

## What a webhook costs you

Webhooks arrive **out of order, twice, late, forged, or not at all.** All five happen routinely at
volume, and each needs an answer.

### Out of order → the monotonic guard

```sql
UPDATE notif.notification n
   SET status = $1, status_rank = $2, status_at = now()
 WHERE n.id = $3
   AND n.created_at >= $4 AND n.created_at < $5      -- partition pruning
   AND n.status_rank < $2                             -- monotonic
   AND NOT EXISTS (SELECT 1 FROM notif.delivery_status d
                    WHERE d.code = n.status AND d.is_terminal)
```

One statement. A late `SENT` arriving after `DELIVERED` matches zero rows and is dropped. No
`SELECT`-then-`UPDATE`, no optimistic-lock retry loop, no application-level sequencing.

**Zero rows is not an error.** It is the expected outcome for a stale or duplicated event, and the
event is recorded with `applied = false` and a counter incremented. Those rows are the most valuable
debugging artefact in the system — the only thing that can answer *"why is this stuck in SENT"* with
*"three later signals arrived and every one was correctly discarded"*.

One rank choice is worth calling out:

```
DELIVERED = 80, is_terminal = FALSE
```

Counter-intuitive and correct. An SMTP 250 means "accepted", not "landed in the inbox" — a hard bounce
legitimately follows. State machines that treat delivered as final **silently drop bounce events**,
which means the address never reaches the suppression list, which means you keep mailing a dead
address and your sender reputation degrades.

### Twice → `dedup_hash` UNIQUE

A unique index on `(dedup_hash, occurred_at)`. The application pre-checks to keep the common case out
of the exception path, and that pre-check races — deliberately. The index is the real defence; a lost
race throws, the listener retries, and the second pass sees the row.

### Forged → four gates

1. HMAC-SHA256 over `timestamp + "." + rawBody`, compared with `MessageDigest.isEqual` in constant
   time
2. Timestamp inside a ±5 minute window, **both directions**
3. Source IP allowlist
4. `dedup_hash` UNIQUE

And the blast radius if all four somehow fail is still bounded: the monotonic guard means a forged
event cannot move anything backwards or out of a terminal state. **A defence built for out-of-order
callbacks turns out to also bound a forgery.**

### Not at all → the reconciler

The residue. `UNKNOWN` attempts older than a timeout, and messages that never received a terminal
event.

This is where the polling that was rejected as a *primary* mechanism comes back as a *backstop*, on a
population three orders of magnitude smaller — and only for the providers where a query is possible at
all. For Twilio SMS the answer is that the attempt stays `UNKNOWN`, which is why the SMS channel's
documented semantic is at-most-once.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Poll every message** | 1,157–3,472 queries/s against APIs limited to hundreds/s; impossible for Twilio, FCM and APNs |
| **Poll only non-terminal messages** | Better, and still thousands/s during a campaign, and still impossible for the same three |
| **Webhooks only, no reconciler** | Leaves `UNKNOWN` attempts unresolved forever. The acceptance-durability SLO says every accepted request reaches a terminal state or sits visibly in the DLQ |
| **Trust the send response as final** | Reports `SENT` as `DELIVERED`. Bounces never reach suppression, sender reputation degrades, the delivery-success SLI becomes a fiction |
| **Provider SDK callbacks / streaming** | Vendor-specific, not universally available, and would put a long-lived connection per provider in a stateless service |

## Consequences

### Positive

- **Scales.** Provider push cost is theirs, and the volume is proportional to actual state changes
  rather than to polling frequency.
- **Timely.** A bounce is known when it happens, not at the next poll interval.
- The monotonic guard means the entire out-of-order, duplicate and illegal-transition problem is
  handled by a `WHERE` clause. That single property is what lets the Kafka consumers be at-least-once
  and makes DLQ replay harmless.
- The webhook endpoint is stateless and horizontally scalable like the rest of `app-api`.
- `webhook_signature_invalid_total` is a real security signal with an alert on it.

### Negative

- **A public inbound endpoint**, which is attack surface the platform would not otherwise have. It is
  the only unauthenticated-by-token path in the API, defended by HMAC instead.
- **Per-provider signature schemes.** Every vendor signs differently — header names, canonical string,
  encoding. Each needs its own verifier configuration and its own secret.
- **Webhook delivery is the provider's at-least-once**, so duplicates are guaranteed and the
  `dedup_hash` index is on the hot path.
- **The raw payload should be persisted before interpretation** so a signature dispute can be settled
  from evidence. `V1__baseline.sql` has no table for it — a real gap.
- **Providers that offer nothing** — APNs has no webhook at all — mean push delivery confirmation is
  aggregate-only, and the platform's status for a push message tops out at `SENT` in practice.
- **The reconciler is not built.** Today, an `UNKNOWN` resolves only if a webhook eventually arrives.
  See [STATUS.md](../STATUS.md).

## Related

- [ADR-007](ADR-007-at-least-once.md) — the `UNKNOWN` state and the per-channel semantics
- [SECURITY.md](../SECURITY.md) — the four gates in detail, and why `MessageDigest.isEqual`
- [CODE-WALKTHROUGH.md](../CODE-WALKTHROUGH.md) Part 2 — the monotonic guard's five verified cases
