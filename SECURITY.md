# Security Policy

This is a portfolio project running mock providers with no real credentials, so there is no
production deployment to compromise. The policy below is written as it would be for a real system,
because how a platform handles secrets and untrusted input is part of the design being demonstrated.

## Reporting a vulnerability

Open a GitHub issue, or use the repository's private vulnerability reporting.
There is no bounty and no SLA.

---

## What is actually implemented

Two controls are genuinely built and verifiable; the rest is designed and documented. That
distinction is stated here rather than left for a reader to discover.

### 1. Secrets cannot be stored in the database

`provider_configuration.credentials_ref` carries a CHECK constraint:

```sql
CONSTRAINT provider_configuration_secret_ck
    CHECK (credentials_ref ~ '^(arn:aws:secretsmanager:|ssm:|mock:)')
```

The column holds a **reference** — a Secrets Manager ARN or an SSM path — never a secret. Pasting
an API key is rejected by the database:

```
INSERT ... credentials_ref = 'SK1234567890abcdefTHISISAKEY'
ERROR:  new row for relation "provider_configuration"
        violates check constraint "provider_configuration_secret_ck"
```

Verified output is in [`docs/verification/05-behaviour.png`](docs/verification/05-behaviour.png).

Code review can miss a pasted key. A constraint cannot. This is the difference between a policy and
a control.

### 2. Webhook signature verification

`WebhookSignatureVerifier` applies four gates, in order:

1. **HMAC-SHA256 over `timestamp + "." + rawBody`**, compared with `MessageDigest.isEqual` — a
   constant-time comparison. `String.equals` returns early on the first differing byte, which an
   attacker can time to guess a signature one byte at a time.
2. **Timestamp within ±5 minutes.** A valid signature is valid forever without this; the bound is
   what stops a captured webhook being replayed.
3. **Source IP allowlist** per provider.
4. **`dedup_hash` UNIQUE** so a duplicate callback is a no-op.

The raw `byte[]` body is used, never a re-serialised POJO — re-serialisation reorders keys and
normalises whitespace, so the signature fails on *valid* payloads, and teams then "fix" it by
disabling verification.

The failure reason is logged but never returned: every rejection is an identical 401.

---

## Designed, not built

Documented in [`docs/SECURITY.md`](docs/SECURITY.md) and honestly marked as unimplemented in
[`docs/STATUS.md`](docs/STATUS.md):

- **AuthN/Z** — OAuth2 client credentials with JWT and per-tenant scoping. `SecurityConfig` exists;
  the local profile runs permit-all for the demo
- **Field-level encryption** — recipient addresses under per-user DEKs wrapped by a KMS CMK
- **Crypto-shredding for GDPR erasure** — destroy the DEK and every ciphertext in Postgres, S3,
  Kafka and the archive becomes unreadable at once, with zero rows rewritten. This is the only
  approach that scales: rewriting 90 partitions of a 1.8-billion-row table to null one user
  generates 1.8 billion dead tuples
- **Tenant isolation at the repository layer** — partially implemented. Several repository methods
  are still unscoped, which is listed in `STATUS.md` rather than glossed over

---

## Two security defects found and fixed in this repository

Both are recorded because how a codebase handles its own mistakes is more informative than a list
of controls.

### The default profile shipped unauthenticated

`spring.profiles.default: local` combined with the `local` profile's `permit-all: true` meant a jar
launched with no `SPRING_PROFILES_ACTIVE` came up **with authentication disabled**, and with the
chaos endpoint — which can take any provider offline for every tenant — enabled in the base
configuration under every profile.

`SecurityConfig` argues at length that gating on a *property* is safer than a *profile* because
"a profile is trivially copied into a production manifest"… and then shipped the insecure
combination as the default.

**Fixed** by removing the default profile and moving `local` into `spring-boot-maven-plugin`, so
`./mvnw spring-boot:run` still needs no flags while a packaged artefact starts secure. The chaos
endpoint is now `enabled: false` in the base document.

### The chaos endpoint

`POST /admin/v1/mock-providers/{code}/chaos` can disable any provider. It is now behind a property
that defaults to false and is enabled only under the `local` profile. A packaged jar does not
expose it.

---

## Scope

No real provider credentials exist in this repository, by design
([ADR-005](docs/adr/ADR-005-mock-providers.md)). `docker/compose.yml` and the local profile use
development-only values (`password: local`) that are inert outside a local stack.
