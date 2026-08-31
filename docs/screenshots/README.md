# Screenshots of the running system

Captured from a live local stack, not mocked up. Reproduce with:

```bash
make up                                        # postgres, valkey, kafka, prometheus, grafana
./mvnw -pl app-api       spring-boot:run -Dspring-boot.run.arguments=--server.port=9080
./mvnw -pl app-worker    spring-boot:run -Dspring-boot.run.arguments=--server.port=9082
./mvnw -pl app-scheduler spring-boot:run -Dspring-boot.run.arguments=--server.port=9083
```

Alternate ports are used so the capture does not collide with an already-running stack.

| File | What it shows |
|---|---|
| `01-swagger-ui.png` | The generated OpenAPI surface — every endpoint in `docs/API.md`, live |
| `02-kafka-topics.png` | All 16 topics with real message counts and partition counts |
| `03-prometheus-targets.png` | Scrape targets |
| `04-prometheus.png` | Prometheus query surface |
| `05-health.png` | `/actuator/health` with the database and Valkey components |

## What `02-kafka-topics.png` actually shows, including the bad news

The topic list is the design made concrete: `dispatch.{sms,email,push}.{tx,bulk}` as six physically
separate topics rather than one topic with a priority field, and the five retry tiers
(`5s · 30s · 2m · 10m · 1h`). Partition counts differ per topic because they are derived from
measured consumer throughput, not chosen uniformly — see `docs/KAFKA.md`.

It also shows **75 messages in `notification.dlq`**, and that is left in deliberately.

Those are inline-content requests (`"content": {...}` instead of `"template": {...}`). They fail
at fan-out because `NotificationRequestedEvent` carries a template reference and has no field for
inline content, so the renderer throws on a null template code. The retry ladder ran, the attempts
were exhausted, and the messages landed in the dead-letter queue rather than being silently
dropped — which is the behaviour the design promises.

Template-based requests complete the full pipeline: accept → outbox → Kafka → fan-out → recipient
row. Inline content is tracked in [STATUS.md](../STATUS.md).
