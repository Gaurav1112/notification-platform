# ADR-001: Spring Boot 4.1.1 as the application framework

**Status:** Accepted
**Date:** 2026-08-31

## Context

The platform needs a JVM application framework for three deployables (`app-api`, `app-worker`,
`app-scheduler`) with dependency injection, configuration binding, actuator endpoints, Kafka
integration, JPA and a web layer.

The obvious choice is Spring Boot. The non-obvious part is the version. Spring Boot 3.5.x is what
most production systems run today and what most reference material assumes. It is also **OSS
end-of-life since 2026-06-30** — commercial support continues, free security patches do not.

For a project intended to be read and evaluated rather than merely run, shipping on an OSS-EOL branch
is a visible signal, and not a good one.

## Decision

**Spring Boot 4.1.1**, with Java 17 as the language level.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Spring Boot 3.5.x** | OSS-EOL since 2026-06-30. Every dependency scanner flags it, and "we pinned to an EOL framework for familiarity" is a hard answer to defend |
| **Quarkus** | Faster startup and lower memory, which matters for scale-from-zero. But the Kafka, JPA and Micrometer ecosystem this design leans on is deepest in Spring, and the project's purpose is to demonstrate systems design, not framework novelty |
| **Micronaut** | Same reasoning as Quarkus, with a smaller ecosystem |
| **Plain Java + libraries** | Defensible, and would make the domain module's Spring-free purity trivially true. Costs configuration binding, actuator, listener containers and transaction management — a lot of undifferentiated work |

## Consequences

### Positive

- Supported, patched, and not flagged by a dependency scanner.
- Kafka 4.x client support, `resilience4j-spring-boot4`, springdoc 3.x and Jackson 3 all line up on
  one coherent dependency set.
- The `ConcurrentKafkaListenerContainerFactory` with `MANUAL_IMMEDIATE` acknowledgement — the
  mechanism the whole at-least-once story rests on — is first-class.

### Negative

- **Most search results are wrong.** Boot 4 moved packages and groupIds, and the mismatch is silent
  more often than it is a compile error:

  | Trap | Symptom |
  |---|---|
  | Jackson groupId is `tools.jackson`, not `com.fasterxml.jackson` | Two Jackson versions on the classpath |
  | `@EntityScan` moved to `org.springframework.boot.persistence.autoconfigure` | `package does not exist` |
  | Testcontainers 2.x renamed every module (`testcontainers-postgresql`) | Missing-version build failure |
  | JUnit is 6, not 5 | Test discovery differences |
  | `resilience4j-spring-boot3` on Boot 4 | **Silent autoconfiguration failure** — no error, no breakers |

  That last one is the worst kind: the artefact resolves, the app starts, and nothing works.

- Spring 7 deprecations surface immediately — `HttpStatus.PAYLOAD_TOO_LARGE` and
  `UNPROCESSABLE_ENTITY` became `CONTENT_TOO_LARGE` and `UNPROCESSABLE_CONTENT`. Cosmetic, but it is
  a build warning on day one.
- A smaller pool of engineers has production experience with it.

## Notes

Java 17 rather than 21 is a deliberate separate choice: it is the version the target environment
pins, and the design uses records, sealed interfaces, switch expressions and text blocks — all of
which 17 has. The one place it shows is `MeteredProvider`, where pattern matching in a `switch` is
still preview, so the sealed `SendResult` is destructured with an `instanceof` chain and a comment
saying why.
