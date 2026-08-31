# Status

What is built, what is partial, and what is not started — at commit `b5c9f48`, **2026-08-31**.

Every line below is either something you can open in this repository, or a number produced by a
command that was run and whose output is quoted. Where the design describes something that does not
exist in code, this page says so. Where something exists but has never been observed working, this
page says that too, which is a different and weaker claim.

---

## How the numbers on this page were produced

| Claim | Command |
|---|---|
| Test counts | `./mvnw -B clean verify` and `./mvnw -B clean verify -Pintegration` |
| Schema counts | `V1__baseline.sql` and `V2__scheduled_notification.sql` applied to an empty PostgreSQL 18.6 database, then `pg_class` / `pg_indexes` / `pg_constraint` counted |
| Boot times | `./mvnw -pl <app> spring-boot:run`, the `Started …Application in Ns` line |
| Accept / idempotency behaviour | `curl` against a running `app-api`, output in [verification/](verification/) |

Anything not in that table is a statement about code, and names the file.

---

## Build and test state

| Command | Result |
|---|---|
| `./mvnw -B -DskipTests compile` | **SUCCESS** |
| `./mvnw -B clean verify` | **SUCCESS** — **406 tests**, 0 failures, 0 errors, 0 skipped, 36.7 s |
| `./mvnw -B clean verify -Pintegration` | **SUCCESS** — **444 tests**, 0 failures, 0 errors, 0 skipped, 47.9 s |
| `docker compose -f docker/compose.yml config --services` | 7 services |

Tests by module:

| Module | default | `-Pintegration` |
|---|---:|---:|
| platform-domain | 16 | 16 |
| platform-application | 33 | 33 |
| platform-messaging | 62 | 62 |
| platform-persistence | 4 | 38 |
| platform-resilience | 58 | 58 |
| platform-provider | 114 | 114 |
| app-api | 33 | 33 |
| app-worker | 69 | 69 |
| app-scheduler | 17 | 21 |
| **total** | **406** | **444** |

The default profile excludes `@Tag("integration")`, so a clean clone builds green **without a Docker
daemon**. `-Pintegration` clears the exclusion and adds 38 tests, all of them in the two places that
genuinely need a container: `platform-persistence` (34) and `app-scheduler` (4).

**Only two test classes use Testcontainers** — `AbstractPostgresTest` (the base class for every
persistence integration test) and `ScheduledWorkStoreIntegrationTest`. `KafkaPipelineIntegrationTest`
uses `@EmbeddedKafka`, not a container; it runs in the default build.

11 modules · 301 main Java files · 76 test Java files · 18 ADRs.

CI: [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) — four jobs. `build` on a JDK 17/21
matrix with no Docker, `integration` with Testcontainers, `schema` applying `V1` to a real
PostgreSQL 18.6 service container and asserting the object counts, `supply-chain` producing a
CycloneDX SBOM and a Trivy scan (reporting, not blocking).

---

## Runtime state

**All three applications start and stay up.** Measured by running them, not inferred.

| App | Port | Startup | Verified |
|---|---|---:|---|
| `app-api` | 8080 | **3.782 s** | `/actuator/health` UP after 877 s uptime |
| `app-worker` | 8082 | **3.457 s** | `/actuator/health` UP after 580 s uptime |
| `app-scheduler` | 8083 | **8.004 s** | `/actuator/health` UP after 580 s uptime |

`spring-boot-maven-plugin` activates the `local` profile in each app's `pom.xml`, so
`./mvnw -pl app-api spring-boot:run` needs no flags. A packaged jar does not get that profile and
starts with authentication on.

Each app module has a `contextLoads()` test that starts the real context —
`app-api/src/test/java/.../ApplicationContextSmokeTest.java` and its two siblings. These were added
after a release in which 371 green unit tests coexisted with three applications that could not boot;
the javadoc on the app-api one records why nothing else could have caught it.

**Flyway.** Only `app-api` runs migrations (`spring.flyway.enabled: false` on worker and scheduler).
Three migrations apply from an empty schema in ~150 ms total:

```
1   | baseline                 | success | 117ms
2   | scheduled notification   | success |  30ms
900 | local development tenant | success |   2ms
Successfully applied 3 migrations to schema "notif", now at version v900
```

`V900` lives in `app-api/src/main/resources/db/local/` and is on the migration path only under the
`local` profile. The Flyway history table is `notif.flyway_schema_history`, not `public`.

**Schema object counts**, from applying the migrations to an empty PostgreSQL 18.6 database:

| | base tables | partitioned parents | partitions | plain tables | indexes | CHECK constraints |
|---|---:|---:|---:|---:|---:|---:|
| after `V1` | 120 | 6 | 107 | 7 | 260 | 298 |
| after `V1` + `V2` | 139 | 7 | 125 | 7 | 336 | 450 |

`V2` adds `notif.scheduled_notification` — one partitioned parent, 18 daily partitions, and the
CHECK constraints those partitions inherit.

---

## Complete

Built, wired into a running application, and covered by tests that name a real failure.

| Area | Evidence |
|---|---|
| **Domain model** | `platform-domain/.../enums/` — **10 enums**, each with behaviour rather than only constants: `DeliveryStatus` (monotonic rank), `FailureType` (four-way policy classification), `TrafficClass` (derives its own Kafka topic), `AttemptState`, `Channel`, `CircuitState`, `Priority`, `ScheduleState`, `ScheduleType`, `SuppressionReason` |
| **Domain purity, enforced** | `platform-domain/src/test/.../ArchitectureTest.java` fails the build on a Spring, JPA, Kafka, Jackson or `java.util.Date` import in the domain |
| **Tenant isolation, enforced** | `platform-persistence/src/test/.../TenantScopedQueryArchTest.java` reads every `@Query` reflectively and fails the build on a query over one of six tenant-owned tables with no `tenantId` predicate. Cross-tenant sweeps are allowlisted individually with a reason, and two further tests fail the build when an allowlist entry rots |
| **Database schema** | `V1__baseline.sql` and `V2__scheduled_notification.sql`. Counts above. Includes the `delivery_status` lookup table, the outbox with its partial index, and the `CHECK` on `credentials_ref` that rejects a pasted API key |
| **Persistence** | JPA entities and repositories, the monotonic-guard SQL, idempotency and outbox repositories, `MonotonicGuardIntegrationTest` and `TenantIsolationIntegrationTest` against a real PostgreSQL 18.6 container |
| **Outbound adapters** | **11 of 11** `platform-application` ports have a production implementation: `JpaAcceptanceWriter`, `JpaCancellationWriter`, `JpaDeliveryStatusWriter`, `JpaIdempotencyStore`, `JpaNotificationQuery`, `OutboxOwnedEventPublisher`, `JacksonResponseSerializer`, `RedisQuotaGuard`, `RedisDispatchTombstoneStore`, and — as deliberate stand-ins, see Partial — `PermissivePreferenceResolver` and `EchoTemplateRenderer` |
| **Provider SPI** | `NotificationProvider`, `SendCommand`, the three-case sealed `SendResult`, `ProviderCapabilities`, `ProviderCode`. `ProviderContractTest` is one abstract suite of 8 contract tests with **three** concrete subclasses, one per adapter class |
| **Mock providers** | Three adapter classes (`MockSmsProvider`, `MockEmailProvider`, `MockPushProvider`) configured as **five beans** — SMS ×2, email ×2, push ×1 — modelling Twilio / SES / FCM semantics. Per-message seeded failure injection, log-normal latency, `SILENT_SUCCESS` |
| **Decorator chain** | `ProviderDecoratorChain` builds a fixed order. Real: `TimeoutProvider` (dedicated pool, `Indeterminate` on timeout, shed-on-saturation), `IdempotentProvider` + `SentTokenLog`, `MeteredProvider` (bounded tag cardinality), `CircuitBreakerProvider` |
| **Circuit breaker** | `platform-resilience/.../ProviderCircuitBreakers` keyed on `(provider, channel)` with a per-JVM jittered open duration, `FailureClassifier` (a 4xx does not count), and `CircuitBreakerProvider` feeding it. `CircuitBreakerProviderTest` asserts on **breaker state**, which is the one thing a pass-through cannot fake. `provider_circuit_state` is exported on the worker's `/actuator/prometheus` |
| **Provider routing** | `ProviderRegistry` (duplicate codes fail startup), `HealthWeightedSelectionStrategy` with a capped exploration draw, `ChannelProviderRouter` filtering open circuits out of the candidate list |
| **Retry engine** | Four `BackoffStrategy` variants including FULL jitter, `DefaultRetryPolicy` with `Retry-After` as a floor, five `RetryTier` delay topics, `RetryBudget` token bucket, `RetryRouter`'s ordered decision ladder |
| **Kafka** | **16 topics** in `Topics.java`, all 16 created by `docker/kafka/create-topics.sh` and present on the running broker; `TopicPartitions.TOTAL = 288` for the production model (local uses the same ratios ÷ 6). `PartitionKeys`, producer/consumer config, `KafkaErrorHandlingConfig`, `IdempotentConsumer`, `NotificationEventPublisher`, and `PublishBatch`, which blocks on every produce future before the offset is committed |
| **Event schema** | One sealed interface `NotificationEvent` permitting **five** records: `NotificationRequestedEvent`, `NotificationDispatchEvent`, `DeliveryStatusEvent`, `RetryScheduledEvent`, `DeadLetterEvent` |
| **Application use cases** | `AcceptNotificationUseCase`, `CancelNotificationUseCase`, `GetNotificationStatusUseCase`, `ApplyDeliveryStatusUseCase` — 33 tests |
| **Accept path** | `POST /v1/notifications` → `202` with a `Location` header and a real notification id; the id in the body is the id in `notif.notification`. Reproduced across five requests, exactly one row each |
| **Idempotency** | Same key + same body → `202` with a byte-identical response and the same ids. Same key + different body → `409` RFC 9457 `FINGERPRINT_MISMATCH`. Exactly one `notif.idempotency_record` row. `AcceptResultSerializationTest` is the round-trip oracle that a nine-test unit suite was missing |
| **Workers** | Three channel workers on a shared eight-step template, `ChannelProviderRouter`, `DeliveryAttemptRecorder`, `RetryTierListener` (pauses the partition, never sleeps the thread), `RequestFanOut`, `StatusEventListener`, `MonotonicDeliveryStatusService` |
| **Scheduler** | `DueScanHydrator`, `ShardAffineClaimer`, `LeaseReaper`, `OutboxSweeper`, `RetryPromoter`, `PartitionMaintenanceJob`, `ScheduleJitter`, Redis leader election. Live evidence: `LeaderElection: acquired leadership of notification-hydrator … with fencing token 8`, and zero `relation does not exist` and zero `ERROR` lines in a ten-minute run |
| **API surface** | Controllers, DTOs, an RFC 9457 `ProblemType` catalogue and exception handler, idempotency-key and payload-size interceptors, `ApiCaller` argument resolver, OpenAPI config, `WebhookSignatureVerifier`, provider-health endpoint |
| **Local stack** | `docker/compose.yml` — PostgreSQL 18.6, Valkey 9.1.1, Kafka 4.3.1 (KRaft), kafka-init, kafbat Kafka UI, Prometheus, Grafana. Six containers plus the one-shot init, all healthy |
| **Observability config** | `docker/prometheus/rules/notification-slo.yml` — 22 recording rules and 30 alerting rules, 52 total, `promtool check rules` clean. One provisioned Grafana dashboard |

---

## Partial

### The one defect that loses work: inline `content` is accepted and then dead-lettered

`SendNotificationRequest` validates `@AssertTrue "exactly one of template or content must be
supplied"` — so an inline `content` body is a **documented, validated, accepted** input, and returns
`202` with a polling id.

`NotificationRequestedEvent` has no field for it. Grep the record for `content`, `body` or `subject`
and the count is **zero**. So a content-only request emits `templateCode: null`, and in the worker:

```
cause-fqcn: dev.gaurav.notification.worker.orchestrator.TemplateRenderer$TemplateRenderingException
message:    no templateCode on the request
at EchoTemplateRenderer.render(EchoTemplateRenderer.java:40)
at RequestFanOut.expand(RequestFanOut.java:175)
at RequestedEventListener.onRequested(RequestedEventListener.java:115)
```

Retried three times, then `notification.requested-1@18 → notification.dlq-0@75`. The caller sees
`202` and a notification that never leaves `PENDING`; no error surfaces anywhere.

Nothing is silently dropped — the message is in the DLQ, which is the correct destination for a
request the platform cannot execute. But the API should not have accepted it. Swapping the dev
`EchoTemplateRenderer` for a real renderer would **not** fix this: the event record has nowhere to
carry a body. The fix is either a content field on the event or a `400` at the edge, and it is a
schema decision, not a bug fix.

Until then: **use `template`, not `content`.**

### Delivery past `QUEUED` has never been observed

The furthest a notification has been watched go is `QUEUED` (`rank=30`), once, on the template path.
No `delivery_attempt` row has ever been seen written by a running worker. A provider send, a `SENT`,
a `DELIVERED`, and the webhook round trip are all **untested end to end**, on any commit.

The components exist and are unit-tested — `AbstractChannelWorker`, `DeliveryAttemptRecorder`,
`SendResultHandler`, the five mock providers, `MonotonicDeliveryStatusService`. What has not been
demonstrated is that they are correctly wired to each other at runtime. Treat "the dispatch half
works" as unproven rather than as broken.

### Two decorators are still pass-throughs

`RateLimitedProvider` and `TracedProvider` are `return delegate.send(command);` with a `TODO` naming
what they must do. The third one, `CircuitBreakerProvider`, was a pass-through and now is not.

The rate limiter itself is real and **does** have a production caller — `RedisQuotaGuard` wraps
`RedisTokenBucketRateLimiter` and is the bean behind the `QuotaGuard` port, so per-tenant accept-time
quota is enforced. What is missing is the per-provider limiter at the send edge.

Tracing has no implementation at all — see Not started.

The empty stages stay in the chain so the ordering does not move when they are filled in.

### The chaos endpoint cannot be reached, and could not work if it were

`notification.providers.mock-chaos.enabled: false` in `app-api/src/main/resources/application.yml`,
and there is **no override in the `local` profile**. `ChaosController` is
`@ConditionalOnProperty(havingValue = "true")`, so the endpoint returns `404` under every profile.
`make demo` and the chaos steps in the README describe something that does not run.

Enabling the flag would not be enough. `ChaosState` is a plain in-JVM `ConcurrentHashMap`,
`ChaosController` lives only in `app-api`, and `app-worker` does not depend on `app-api` — the class
is not in the worker jar. With no shared store, an HTTP call to the API cannot affect the providers
the worker calls. Making the demo real means moving chaos state into Valkey, which is a small piece
of work and an honest one to name.

Failover itself is not blocked by this: `ChannelProviderRouter` filters open circuits, and
`CircuitBreakerProvider` now actually opens them.

### Preferences, templates and suppression are stand-ins

`PermissivePreferenceResolver` admits everything. `EchoTemplateRenderer` returns the body unchanged
and throws when there is no `templateCode`. `LoggingSuppressionWriter` logs instead of writing to
`suppression_entry`. All three ports and all three tables exist; the real implementations do not.

### Aggregate counters are not maintained

`MonotonicDeliveryStatusService` advances per-recipient state correctly and does not touch
campaign-level `delivered_count`. `SET delivered_count = delivered_count + 1` would serialise the
whole worker fleet on one row; the design calls for a Redis increment with a periodic flush, and that
is not built.

### Deduplication is single-JVM

`InMemoryDeduplicationStore` and `InMemorySentTokenLog` are `ConcurrentHashMap`s. Idempotency at the
consumer and at the provider therefore holds within one pod and not across two. Both classes document
the gap and `isFullyProtected()` reports it. The Valkey-backed versions are not written.

### Duplicated concepts from parallel development

| Concept | Duplicated as |
|---|---|
| Already-dispatched error | `api.error.AlreadyDispatchedException` · `application.exception.AlreadyDispatchedException` |
| Template reference | `api.dto.TemplateRef` · `application.command.TemplateRef` |
| Preference resolution | `worker.orchestrator.PreferenceResolver` · `application.port.PreferenceResolver` |
| Apply-status use case | `worker.status.ApplyDeliveryStatusUseCase` · `application.usecase.ApplyDeliveryStatusUseCase` |

None of these break anything. They are seams left by building the modules in parallel.

---

## Not started

| Area | Note |
|---|---|
| **`platform-observability`** | `package-info.java` and nothing else. Micrometer is used directly by the modules that emit metrics; there is no OpenTelemetry wiring and no trace propagation through Kafka headers, which is why `TracedProvider` is a pass-through |
| **`platform-security`** | `package-info.java` and nothing else. `app-api` has a real `SecurityConfig`, `ApiSecurityProperties` and a tested HMAC webhook verifier — but OAuth2 / JWKS validation, field-level AES-GCM encryption and secrets-manager integration are design-only. [SECURITY.md](SECURITY.md) labels every control |
| **Load-test harness** | There is no `load-test/` directory, no k6 scenario, no Gatling simulation, and no measured result. [LOAD-TEST.md](LOAD-TEST.md) is a specification for one |
| **Reconciler** | The `UNKNOWN` → resolved sweep is referenced by `SentTokenLog.startedAt()` and described in the design. No such job exists — `find . -iname '*reconcil*'` returns nothing |
| **DLQ replay API** | The DLQ topic exists and `RetryRouter` routes to it. There is no operator replay endpoint |
| **`SheddingLevel` controller** | The load-shedding ladder is designed and `ProblemType.SERVICE_DEGRADED` exists. No class sets or reads a shedding level |
| **Real provider adapters** | Deliberate — [ADR-005](adr/ADR-005-mock-providers.md). [ADDING-A-PROVIDER.md](ADDING-A-PROVIDER.md) is the guide |
| **AWS / Terraform / Helm** | Design only. The local Docker Compose stack is the only deployable environment |
| **Screenshots** | `docs/screenshots/` does not exist. The captured terminal evidence is in [verification/](verification/) |

---

## Measurements

Numbers in this repository fall into three buckets, and every document that quotes one says which.

**Measured here, reproducible by you:** the test counts, boot times, schema object counts, Flyway
timings and accept/idempotency behaviour on this page. `./scripts/capture-verification.sh`
regenerates the text captures in [verification/](verification/); note that `--text` skips the PNG
render, so after a `--text` run the images are older than the text beside them.

**Measured in a design-time prototype that is not in this repository:** the scheduler claim
benchmark — 159 / 453 / 746 tps for naive `FOR UPDATE`, `SKIP LOCKED`, and shard-affine +
`SKIP LOCKED` at 16 concurrent claimers over 3M rows. It explains why `ShardAffineClaimer` is shaped
the way it is. It is not a benchmark of this code.

**Derived from the capacity model, not observed:** every throughput, partition-count, storage and
cost figure in [SCALABILITY.md](SCALABILITY.md) and the design spec. No end-to-end throughput or
latency benchmark has been run against this code.

---

## Known environment issue

Testcontainers cannot find Docker automatically under Rancher Desktop on macOS: the socket is at
`unix:///Users/<you>/.rd/docker.sock`, there is no `/var/run/docker.sock`, and the `rancher-desktop`
Docker context is not picked up. Ryuk also fails to start.

```bash
DOCKER_HOST=unix://$HOME/.rd/docker.sock TESTCONTAINERS_RYUK_DISABLED=true \
  ./mvnw -B clean verify -Pintegration
```

Both settings are deliberately kept out of the committed build — they are machine-specific, and
baking `TESTCONTAINERS_RYUK_DISABLED` into the repo would disable container cleanup on CI.

---

## The honest one-paragraph summary

The accept half of this platform runs: three applications boot in under nine seconds, Flyway builds a
139-table schema from empty, `POST /v1/notifications` returns `202` with an id that is really in the
database, and replaying the idempotency key returns the same bytes while reusing it with a different
body returns `409`. 406 tests pass without Docker and 444 with it. The dispatch half is built and
unit-tested but has never been watched work end to end — nothing has been observed past `QUEUED`, and
a request that uses inline `content` instead of a template is accepted and then dead-lettered because
the event record has no field to carry the body. Two provider decorators are still pass-throughs, two
modules are empty packages, deduplication does not survive a second pod, and there is no load test.
The design decisions this project set out to demonstrate are demonstrable in the code; the end-to-end
system is not yet demonstrable in a terminal.

## Defects found by running the system, 2026-08-31

Five of these were invisible to the test suite because every layer reported success in its own
terms. They are recorded here rather than quietly fixed, because the reason each one hid is more
useful than the fix.

### `notif.provider` was empty, so nothing was ever delivered

`V1__baseline.sql` creates the `provider` table and seeds no rows. The five mock adapters are Spring
beans, so `ProviderRegistry` found them all and `/v1/providers/health` reported every circuit CLOSED
and healthy. But `delivery_attempt.provider_id` is a `smallint` FK into that table, so the worker
resolves the code to an id *before* the network call, and `ProviderIds` throws when the row is
absent. Every dispatch failed at that step and went to `notification.dlq`: 177 dead letters, 0 rows
in `delivery_attempt`.

The whole platform looked healthy while delivering nothing. `202` on accept, rows advancing to
`QUEUED`, every circuit CLOSED — because **a circuit breaker only records calls that happen**, and
zero calls is indistinguishable from zero failures. Fixed by `db/local/V901__local_mock_providers.sql`.
`ProviderIds` throwing rather than defaulting is what made it findable at all: the DLQ header named
the exact missing row.

### The chaos endpoint was unreachable, and in the wrong process

Both `app-api` and `app-worker` carried the comment "enabled only in the `local` profile". No `local`
document ever set it, so the bean never registered and every call answered `404`. Worse, the
controller lived in `app-api` while `ChaosState` is an in-process object read at send time — in the
worker. Even switched on, breaking a provider through the API would have mutated state no sender
consults: `200 OK` and no effect. Moved to `platform-provider` beside the adapters, so the endpoint
is always co-located with the state it controls.

### `/v1/providers/health` was served by the process that never sends

Same root cause. Everything it reads — the Resilience4j registry, the send timer, `isHealthy()` — is
per-JVM state written by the send path, and it was hosted only by the API tier. It reported the same
unchanging answer forever: all CLOSED, all `successRate5m` 1.0, all `p95LatencyMs` **exactly 0.0**.
That last figure is the tell; a provider that has merely never been slow still records a latency
once it has been called. Moved alongside the chaos endpoint.

### Swagger UI could not call the API at all

Three separate faults, each individually enough to break "Try it out" on the one endpoint that
creates work:

- **`Idempotency-Key` was not in the OpenAPI document.** The key is claimed by an interceptor and
  passed to the controller as a request attribute, so springdoc had nothing to infer it from. The
  header was described in prose and absent from the contract, so the UI offered no field and every
  attempt returned `400`.
- **`ApiCaller` leaked into the document** as a required parameter named `caller` with no input,
  because springdoc does not know it comes from the authenticated principal.
- **`servers` was hardcoded to `http://localhost:8080`.** Swagger UI resolves requests against the
  selected server, so on any other port — including the 9080 the captures use — every call failed
  with "Failed to fetch". Now a relative `/`, which follows the page's own origin.

The OpenAPI `info.license` also said `Proprietary` while the repository ships MIT.

### `ProviderHealthGate` does not fire on a health-only outage — open, not fixed

With both email providers `HARD_DOWN`, the worker logs `no eligible provider for channel EMAIL` on
every record, but `notification.dispatch.gated{channel=EMAIL}` stays `0.0`. The gate tests
`allCircuitsOpen(channel)`, and a provider excluded by `isHealthy()` is never called, so its breaker
stays CLOSED. The router excludes on **health**; the gate keys on **circuit state**. In the outage
shape the gate exists to handle, it does not engage, and records burn through the retry ladder
instead. The gate should consider a provider unavailable when it is either open or unhealthy.
