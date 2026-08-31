# ADR-005: Mock providers only, with seeded failure injection

**Status:** Accepted
**Date:** 2026-08-31

## Context

The platform routes to SMS, email and push vendors. A real integration means Twilio, Amazon SES and
Firebase Cloud Messaging — each with an account, a credential, a billing relationship and a rate
limit.

For a project whose value is in the resilience machinery around the vendor call rather than in the
vendor call itself, that has three problems:

1. **A reviewer cannot run it.** Clone, `make up`, and nothing sends without three sets of
   credentials the reviewer does not have.
2. **CI cannot assert on failure behaviour.** Real vendors fail when they feel like it. A test that
   says "exactly three of these reach the DLQ" cannot be written against a real provider.
3. **Secrets in a public repository** is a category of accident this design goes out of its way to
   make impossible at the schema level.

## Decision

**Mock adapters only. Five of them: SMS ×2, email ×2, push ×1.** They emulate vendor *semantics* —
error codes, batch limits, callback behaviour — and inject failures from a **deterministic, seeded**
source.

**Only the leaf adapter is mocked.** The SPI, all six decorators, the registry, the router, the
circuit breaker, the rate limiter, the retry engine, the DLQ, the webhook verification and the status
pipeline are real and are exercised by the mocks.

## What "emulate semantics" means concretely

| Mock | Models | Behaviour that matters |
|---|---|---|
| `MockSmsProvider` | Twilio | Real error codes (21610 opt-out, 21211 invalid, 20429 throttle, 20003 auth). **No client idempotency key**, because Twilio has none. Magic test numbers via `vendorOverride` |
| `MockEmailProvider` | Amazon SES | `supportsBatching = true`, `maxBatchSize = 50` — the real `SendBulkEmail` hard limit, with per-destination status. Permanent bounces that must reach the suppression list, because SES pauses an account above a 5% bounce rate |
| `MockPushProvider` | FCM / APNs | One HTTP/2 request per token (FCM removed its batch endpoint in June 2024). `UNREGISTERED`. `apns-collapse-id` |

And the fact that shapes the whole idempotency design: **no provider we would plausibly integrate
offers a usable client idempotency key.** The mocks say so, which keeps the platform honest about
layer 5 being aspirational rather than universal.

## The determinism decision, which is the interesting half

```java
static long seedFor(long baseSeed, ProviderCode provider, UUID recipientId, int attempt) {
    var h = mix(baseSeed);
    h = mix(h ^ fnv1a(provider.value()));
    h = mix(h ^ recipientId.getMostSignificantBits());
    h = mix(h ^ recipientId.getLeastSignificantBits());
    return mix(h ^ attempt);
}
```

A fresh `Random` per draw, seeded from the message's identity, rather than one shared stream.

A shared stream is reproducible only single-threaded. With sixteen worker threads, draw *n* goes to
whichever thread got there first, and "exactly three messages reached the DLQ" becomes flaky.

Seeding on identity makes the outcome of a send **a pure function of that send** — independent of
thread, ordering and concurrency. `attempt` is in the key so a retry draws a *different* fate;
without it a `TRANSIENT_NETWORK` failure could never recover and the retry engine would be
untestable.

Latency is **log-normal**, solved from a configured median and p99, because real provider latency is
long-tailed and a uniform draw has no tail — so it never populates a realistic p99 bucket and never
trips a latency-based breaker.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Real vendor SDKs with credentials** | Reviewer cannot run it; secrets risk; non-deterministic CI; real money per test run |
| **Real SDKs pointed at vendor sandboxes** | Sandboxes still need credentials, still rate-limit, and their failure behaviour is not configurable — you cannot ask a sandbox for a 4% `SILENT_SUCCESS` rate |
| **WireMock / Mountebank stubs over HTTP** | Closer to the wire, and it would exercise the HTTP client. But the failure injection would then live in JSON stub files rather than in typed, seeded Java, and the determinism story gets much harder |
| **A single trivial mock per channel** | Cannot test failover — you cannot fail over to nothing. Two providers per channel where failover matters is a deliberate part of this decision |
| **Recorded/replayed vendor traffic (VCR)** | Good for request-shape fidelity, useless for failure injection, which is the point |

## Consequences

### Positive

- **Clone and run with zero credentials.** `make up && make build && make demo` is the whole tour.
- **CI can assert exact counts.** `DeterministicFailureInjectionTest` and the three contract tests
  depend on it.
- **The chaos demo is real.** `POST /admin/v1/mock-providers/{code}/chaos {"mode":"HARD_DOWN"}` drives
  a genuine circuit-breaker open, a genuine failover and a genuine half-open recovery, through the
  same code production traffic would use.
- **`SILENT_SUCCESS` exists.** No real vendor will produce an acknowledge-after-your-deadline on
  demand, and that gap is where every duplicate-OTP incident lives. A mock that cannot produce it
  cannot test the code that closes it.

### Negative

- **The HTTP layer is never exercised.** No connection pooling behaviour, no TLS handshake cost, no
  DNS, no real socket timeouts. `TimeoutProvider`'s whole rationale is that a client's socket timeout
  does not cover pool acquisition — and that specific failure cannot be reproduced here.
- **No vendor drift.** Twilio's error codes change; ours never will. The mappings are correct as of
  2026-08-31 and will silently age.
- **Latency shape is modelled, not measured.** The log-normal parameters are plausible, not observed.
- **"Adding a real provider is one class" is asserted, not proven.** It is argued in
  [ADDING-A-PROVIDER.md](../ADDING-A-PROVIDER.md) against the real SPI, but no real adapter exists to
  demonstrate it.
- Anyone evaluating the project has to be told which parts are mocked, or they will assume more than
  is there. Hence the explicit paragraph in the README and this ADR.

## Related

- [ADDING-A-PROVIDER.md](../ADDING-A-PROVIDER.md) — the path from here to a real vendor
- [ADR-007](ADR-007-at-least-once.md) — the delivery semantics the mocks are shaped to test
