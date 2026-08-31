# Status

An honest account of what is built, what is partial and what is not started, as of **2026-08-31**.

Nothing here is aspirational. Where the design document describes something that does not exist in
code, this page says so.

---

## Read this first

If you have ten minutes and want to judge the project, open these in order:

1. **`platform-domain/.../enums/DeliveryStatus.java`** — the monotonic state machine, and
   `DeliveryStatusTest` next to it. Nine lines that make at-least-once delivery safe.
2. **`platform-persistence/src/main/resources/db/migration/V1__baseline.sql`** — 120 tables, applied
   and verified against a real PostgreSQL 18.6 container.
3. **`platform-resilience/.../circuitbreaker/ProviderCircuitBreakerConfiguration.java`** — the
   half-open stampede, and why the fix is per-JVM jitter.
4. **[CODE-WALKTHROUGH.md](CODE-WALKTHROUGH.md)** — the guided version of all of it.

---

## Build state

| Command | Result |
|---|---|
| `./mvnw -B -DskipTests compile` | **SUCCESS**, zero warnings |
| `./mvnw -B clean verify` | **SUCCESS** — 371 tests, 0 failures, 0 errors, 0 skipped |
| `./mvnw -B clean verify -Pintegration` | **SUCCESS** — 392 tests, 0 failures, 0 errors, 0 skipped |
| `docker compose -f docker/compose.yml config` | valid, 7 services |

The default profile excludes `@Tag("integration")` so a clean clone builds green **without a Docker
daemon**. The Testcontainers suite runs under `-Pintegration`.

Tests by module, integration profile: domain 16 · application 33 · persistence 21 · messaging 56 ·
resilience 58 · provider 107 · api 29 · worker 56 · scheduler 16.

11 modules · 273 main Java files · 63 test Java files.

---

## Complete

These are built, wired, and covered by tests that name real failure scenarios.

| Area | What exists |
|---|---|
| **Domain model** | All 9 enums with behaviour on them; `DeliveryStatus` monotonic transitions; `FailureType` four-way policy classification; `TrafficClass` computing its own Kafka topic. ArchUnit rules fail the build on a Spring, JPA, Kafka, Jackson or `java.util.Date` import in the domain |
| **Database schema** | `V1__baseline.sql` — 6 partitioned parents, 335 partitions, 260 indexes, 298 check constraints, the `delivery_status` lookup table, the outbox with its partial index, the secret-shaped `CHECK` on `credentials_ref`. Verified live on PostgreSQL 18.6 |
| **Persistence** | JPA entities, repositories, the monotonic-guard SQL, idempotency and outbox repositories. Testcontainers tests including `MonotonicGuardIntegrationTest` |
| **Provider SPI** | `NotificationProvider`, `SendCommand`, the three-case sealed `SendResult`, `ProviderCapabilities`, `ProviderCode`. Locked contract, contract-tested across all five mocks |
| **Decorator chain** | Fixed-order builder; `TimeoutProvider` (dedicated pool, `Indeterminate` on timeout, shed-on-saturation), `IdempotentProvider` + `SentTokenLog`, `MeteredProvider` (bounded tag cardinality) |
| **Mock providers** | Five adapters — SMS ×2, email ×2, push ×1 — modelling Twilio / SES / FCM semantics. Seeded per-message failure injection, log-normal latency, `SILENT_SUCCESS`, bounded chaos windows |
| **Provider routing** | `ProviderRegistry` (duplicate codes fail startup), `HealthWeightedSelectionStrategy` with the capped exploration draw and cold-start handling |
| **Retry engine** | Four `BackoffStrategy` variants including FULL jitter, `DefaultRetryPolicy` with `Retry-After` as a floor, five `RetryTier` delay topics, `RetryBudget` token bucket, `RetryRouter` with the ordered decision ladder |
| **Circuit breakers** | Resilience4j config with per-JVM jittered open duration, `FailureClassifier` (the 4xx rule), `ProviderCircuitBreakers` keyed on `(provider, channel)` |
| **Rate limiting** | `RateLimiter` interface and `RedisTokenBucketRateLimiter` |
| **Kafka** | 16 topics and 288 partitions as constants, `PartitionKeys`, producer and consumer configuration, `KafkaErrorHandlingConfig`, `IdempotentConsumer`, the six event records, `NotificationEventPublisher`. `KafkaPipelineIntegrationTest` on Testcontainers |
| **Application use cases** | `AcceptNotificationUseCase`, `CancelNotificationUseCase`, `GetNotificationStatusUseCase`, `ApplyDeliveryStatusUseCase` — with nine outbound ports, 33 tests |
| **Workers** | Three channel workers on the shared eight-step template, `ChannelProviderRouter`, `DeliveryAttemptRecorder`, `RetryTierListener` (partition pause, never sleep), `RequestFanOut`, `StatusEventListener`, `MonotonicDeliveryStatusService` |
| **Scheduler** | `DueScanHydrator` (leader-elected, fenced), `ShardAffineClaimer`, `LeaseReaper`, `OutboxSweeper`, `RetryPromoter`, `PartitionMaintenanceJob`, `ScheduleJitter`, Redis leader election |
| **API surface** | Controllers, DTOs, RFC 9457 `ProblemType` catalogue and exception handler, idempotency-key and payload-size interceptors, `ApiCaller` argument resolver, OpenAPI config, `WebhookSignatureVerifier`, chaos and provider-health endpoints |
| **Local stack** | `docker/compose.yml` — PostgreSQL 18.6, Valkey 9.1.1, Kafka 4.3.1 (KRaft), kafka-init creating the 16 topics, kafbat Kafka UI, Prometheus, Grafana. `Makefile` with a chaos-failover demo target |
| **Observability config** | `notification-slo.yml` — SLI recording rules for every SLI in the spec, multi-window burn-rate alerts, conjunction-based symptom alerts. One provisioned Grafana dashboard |
| **Documentation** | Design spec, architecture, database, Kafka, API, runbook, learn-from-zero, interview guide, code walkthrough, 18 ADRs, and the reference set this page sits in |

---

## Partial — built but not fully wired

### `app-api` cannot boot

This is the one real gap, and it is worth stating plainly.

`app-api/.../port/` declares three inbound ports — `NotificationCommandPort`,
`NotificationQueryPort`, `WebhookIngestPort`. `NotificationController` and `WebhookController` take
them by constructor injection, and `NotificationApiApplication` component-scans all of
`dev.gaurav.notification`. **No class implements any of the three.** A real context start fails with
`UnsatisfiedDependencyException`.

The 29 app-api tests pass because they are MockMvc slices with mocked ports — they test the
controllers, the error mapping, the interceptors and the signature verifier, all of which are real.
What is missing is the adapter layer between them and `platform-application`.

`app-worker` and `app-scheduler` are internally consistent and **do** have implementations for their
own ports.

### `platform-application` has no production caller

Six modules declare it as a Maven dependency, but no production code outside the module references
`dev.gaurav.notification.application`. Its nine outbound ports have test fakes only, and
`CancelNotificationUseCase` / `GetNotificationStatusUseCase` have no production caller at all.

So the four use cases that should sit behind app-api's ports exist and are tested — they are just not
connected.

Building the connection means DTO ↔ command mapping, exception translation, cursor pagination for
`recipients()`, and a raw-webhook store that `V1__baseline.sql` has no table for. That is a design
step, not build repair.

### Duplicated concepts from parallel development

The modules were written in parallel and some concepts landed twice:

| Concept | Duplicated as |
|---|---|
| Already-dispatched error | `api.error.AlreadyDispatchedException` · `application.exception.AlreadyDispatchedException` |
| Template reference | `api.dto.TemplateRef` · `application.command.TemplateRef` |
| Preference resolution | `worker.orchestrator.PreferenceResolver` · `application.port.PreferenceResolver` |
| Apply-status use case | `worker.status.ApplyDeliveryStatusUseCase` · `application.usecase.ApplyDeliveryStatusUseCase` |

None of these break anything. They are the seam that the app-api adapter work should close.

### Three decorators are pass-throughs

`CircuitBreakerProvider`, `RateLimitedProvider` and `TracedProvider` currently forward to their
delegate. Each carries a `TODO` naming exactly what it must do and which built component it should
delegate to. The components exist and are tested:

- The circuit breaker is real (`platform-resilience`), and `ChannelProviderRouter` already filters
  open circuits out of the candidate list — so failover works today, just not via the decorator.
- The rate limiter is real (`RedisTokenBucketRateLimiter`).
- Tracing has no implementation at all yet — see below.

The empty stages stay in the chain so the ordering does not move when they are filled in.

### Preferences, templates and suppression

`PermissivePreferenceResolver` admits everything. `EchoTemplateRenderer` returns the body unchanged.
`LoggingSuppressionWriter` logs rather than writing to `suppression_entry`. The schema and the ports
for all three exist; the real implementations do not. Phase 8 of the spec.

### Aggregate counters

`MonotonicDeliveryStatusService` advances per-recipient state correctly but does not maintain
campaign-level `delivered_count`. Doing it with `SET delivered_count = delivered_count + 1` would
serialise every worker in the fleet on one row; the design calls for a Redis increment with a
periodic flush, and that is not built.

---

## Not started

| Area | Note |
|---|---|
| **`platform-observability`** | Package and `package-info.java` only. Micrometer is used directly by the modules that emit metrics; there is no OpenTelemetry wiring, no trace propagation through Kafka headers, and `TracedProvider` is therefore a pass-through |
| **`platform-security`** | Package and `package-info.java` only. `app-api` has a `SecurityConfig` and `ApiSecurityProperties`, and the webhook HMAC verifier is real and tested — but OAuth2 / JWKS validation, field-level AES-GCM encryption, tenant scoping enforced at the repository layer, and secrets-manager integration are all design-only. See [SECURITY.md](SECURITY.md), which labels every control accordingly |
| **`load-test/`** | The directory does not exist. No k6 scenarios, no Gatling simulations, no measured results. [LOAD-TEST.md](LOAD-TEST.md) ships the designed harness and an **empty** results table |
| **Reconciler** | The `UNKNOWN` → resolved sweep is described in the design and referenced by `SentTokenLog.startedAt()`, but there is no reconciler job |
| **DLQ replay API** | The DLQ topic exists and `RetryRouter` routes to it. There is no operator replay endpoint |
| **Real provider adapters** | Deliberate — [ADR-005](adr/ADR-005-mock-providers.md). [ADDING-A-PROVIDER.md](ADDING-A-PROVIDER.md) is the guide |
| **AWS / Terraform / Helm** | Design only. The local Docker Compose stack is the only deployable environment |
| **`SheddingLevel` controller** | The load-shedding ladder is designed and `ProblemType.SERVICE_DEGRADED` exists. Nothing sets or reads a shedding level |
| **Distributed dedup store** | `InMemoryDeduplicationStore` and `InMemorySentTokenLog` are single-JVM. The Valkey-backed versions are not written; both classes document the gap and `isFullyProtected()` reports it |

---

## Measurements

Two numbers in this repository are measured rather than modelled, and they are labelled as such
everywhere they appear:

- **Scheduler claim strategies** — 159 / 453 / 746 tps for naive `FOR UPDATE`, `SKIP LOCKED`, and
  shard-affine + `SKIP LOCKED` at 16 concurrent claimers over 3M READY rows.
- **Schema verification** — the monotonic guard's five-case table in
  [CODE-WALKTHROUGH.md](CODE-WALKTHROUGH.md) Part 2, executed against a live PostgreSQL 18.6
  container.

Everything else with a number attached — capacity, partition counts, cost — is **derived from the
model in the design spec**, not observed. No end-to-end throughput or latency benchmark has been run.

---

## Known environment issue

Testcontainers cannot find Docker automatically under Rancher Desktop on macOS: the socket is at
`unix:///Users/<you>/.rd/docker.sock`, there is no `/var/run/docker.sock`, and the `rancher-desktop`
Docker context is not picked up. Ryuk also fails to start.

```bash
DOCKER_HOST=unix://$HOME/.rd/docker.sock TESTCONTAINERS_RYUK_DISABLED=true \
  ./mvnw -B verify -Pintegration
```

Both settings are deliberately kept out of the committed build — they are machine-specific, and
baking `TESTCONTAINERS_RYUK_DISABLED` into the repo would disable container cleanup on CI.

---

## The honest one-paragraph summary

The hard parts are built: the state machine, the schema, the failure taxonomy, the decorator chain,
the retry engine with all three of its controls, the circuit breaker with the half-open fix, the
Kafka topology, the scheduler's three tiers, and the SLO rules. What is missing is mostly *wiring* —
the adapter layer that would let `app-api` boot — plus two empty modules (`observability`,
`security`) and the load-test harness. The project demonstrates the design decisions it set out to
demonstrate; it is not a running end-to-end system today.
