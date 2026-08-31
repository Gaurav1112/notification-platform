# Security

Authentication, authorisation, encryption, secrets, PII, webhook verification and the threat model.

Source: [§13 of the design spec](design/DESIGN-SPEC.md#13-security).

> **Read this column first.** `platform-security` contains a `package-info.java` and nothing else.
> Every control below is marked **Built**, **Partial** or **Design only**, and the honest summary is
> that two of them are genuinely built — the webhook verifier and the schema-level secret ban — and
> the rest are design. See [STATUS.md](STATUS.md).

---

## Control summary

| Control | Design | State |
|---|---|---|
| **AuthN** | OAuth2 client-credentials (JWT RS256), JWKS cached with rotation; mTLS available; admin API on a separate ingress with SSO + MFA | **Design only** — `app-api` has a `SecurityConfig` and `ApiSecurityProperties` shell |
| **AuthZ** | Tenant scoping at the **repository layer**, ArchUnit-enforced. Scopes: `notifications:send\|read`, `templates:write`, `providers:read`, `admin:*` | **Design only** — `ApiCaller` carries the tenant, and controllers pass it explicitly, so the seam exists |
| **In transit** | TLS 1.3 everywhere; `sslmode=verify-full` to RDS; IAM + TLS to MSK | Design only |
| **At rest** | KMS CMK on RDS/MSK/ElastiCache/S3/EBS; **field-level** AES-GCM on addresses and bodies under per-user DEKs | Design only |
| **Secrets** | Secrets Manager + External Secrets Operator. **Zero secrets in git, config or the DB** | **Partial — the database enforcement is built** |
| **Provider credential isolation** | One secret per `(provider, tenant)`; only the worker IRSA role can decrypt. **The API tier cannot decrypt provider credentials at all** | Design only |
| **PII** | Addresses never logged plaintext — `address_hint` (`g***@example.com`) only. Log filter redacts by key. Bodies in S3, never the heap | **Partial** — `RecipientAddressVault` / `DevelopmentAddressVault` exist as the seam |
| **Rate limiting** | WAF per-IP → Redis token bucket per `(tenant, endpoint)` → per-`(tenant, provider)` budget. **Fails open** | **Partial** — `RedisTokenBucketRateLimiter` built; WAF and the per-provider budget are not |
| **Audit** | Write-once, monthly partitions, 13 mo hot → S3 Object Lock (WORM) 7 yr | Schema exists; no writer |
| **Webhook verification** | HMAC-SHA256 constant-time · timestamp ±5 min · IP allowlist · `dedup_hash` UNIQUE. Raw payload persisted **before** interpretation | **Built** (except the raw-payload store) |
| **Supply chain** | Dependabot, OWASP Dependency-Check, CycloneDX SBOM, Trivy, distroless, `runAsNonRoot`, read-only root FS | Design only |

---

## 1. The two controls that are actually built

### 1.1 The database physically refuses to store a secret

```sql
CONSTRAINT provider_configuration_secret_ck
    CHECK (credentials_ref ~ '^(arn:aws:secretsmanager:|ssm:|mock:)')
```

Tested against a live PostgreSQL 18.6 container:

```
INSERT ... credentials_ref = 'SK1234567890abcdefTHISISAKEY'
ERROR:  violates check constraint "provider_configuration_secret_ck"

INSERT ... credentials_ref = 'mock:sms-primary'
inserted id 2
```

**Code review can miss a pasted key; a check constraint cannot.** The column holds a *reference*, and
the shape of a valid reference is enforced by the database rather than by convention.

The `mock:` prefix is in the allowlist because this repository ships mock providers only
([ADR-005](adr/ADR-005-mock-providers.md)); in a real deployment it would be dropped.

### 1.2 Webhook signature verification

`app-api/.../webhook/WebhookSignatureVerifier.java` — four gates, and each one closes a specific
attack.

```java
var expected  = hmac(provider.secret(), timestampHeader, rawBody);
var presented = decodeHex(stripPrefix(signatureHeader));
if (presented == null || !MessageDigest.isEqual(expected, presented)) {
    throw reject(providerCode, Reason.SIGNATURE_MISMATCH);
}
```

**Gate 1 — HMAC-SHA256, constant-time.** `String.equals` returns as soon as two characters differ, so
its runtime is a function of how many leading characters were correct — and that difference is
measurable across a network given enough samples. An attacker who can post to this endpoint
repeatedly recovers a valid signature **one hex character at a time**: sixty-four rounds of a few
thousand requests each, no secret required. `MessageDigest.isEqual` compares every byte regardless.

The comparison is on decoded **bytes**, not hex strings, for the same reason — a length check on the
string would short-circuit and leak the expected length, and case handling on hex would introduce a
branch.

**Gate 2 — timestamp inside the window, both directions.** ±5 minutes. A timestamp in the *future* is
not harmless clock skew; it is how a captured payload is made valid for longer than the window
allows.

The timestamp is part of the signed material:

```java
mac.update(timestamp.getBytes(UTF_8));
mac.update((byte) '.');          // not decoration — see below
mac.update(rawBody);
```

Signing the body alone would make every captured payload valid forever. And the separator matters:
without it, `(ts="1", body="23")` and `(ts="12", body="3")` produce the same MAC input, so a provider
that controls part of the body could forge a signature for a different timestamp.

**Gate 3 — source IP allowlist.** Per provider.

**Gate 4 — `dedup_hash` UNIQUE.** Enforced by the database downstream, not in this class. A replayed
payload that somehow passed gates 1–3 still cannot be applied twice.

**The raw bytes are signed, never a re-serialised object.** Re-serialising a parsed body changes key
order and whitespace and breaks every signature — a bug that looks like "the provider's signatures are
wrong" and takes a day to find.

Every gate failure renders as an **identical 401**. Distinguishing "bad signature" from "unknown
provider" in the response tells an attacker which half of their guess was right.

`webhook_signature_invalid_total` is a counter with an alert on it: a non-zero rate means either a
misconfigured provider or somebody probing.

**What is missing:** the design requires the raw payload to be persisted **before** interpretation, so
a signature dispute can be settled from evidence. `V1__baseline.sql` has no table for it.

---

## 2. Authentication and authorisation

### Design

OAuth2 client-credentials with RS256 JWTs, JWKS cached with rotation. mTLS available for tenants that
want it. The admin API — chaos injection, provider configuration, DLQ replay — sits on a **separate
ingress** with SSO and MFA, because it is the only surface that can change platform behaviour rather
than send a message.

Scopes: `notifications:send`, `notifications:read`, `templates:write`, `providers:read`, `admin:*`.

### The one authorisation decision worth arguing about

**Tenant scoping belongs at the repository layer, and it should be ArchUnit-enforced.**

The alternative — scoping in the service layer — fails the first time somebody adds a query that
bypasses the service. Enforcing it at the repository means every read path is scoped by construction,
and enforcing *that* with an ArchUnit rule means the build fails on the first violation rather than
the first incident.

The existing `ArchitectureTest` already demonstrates the technique for the domain module (no Spring,
no JPA, no Kafka, no Jackson, no `java.util.Date`). The tenant-scoping rule would be the same shape.

### A cross-tenant read returns 404, not 403

```java
/** Unknown id — and also the answer for a cross-tenant read.
 *  See NotificationNotFoundException for why this must never be a 403. */
NOTIFICATION_NOT_FOUND("notification-not-found", HttpStatus.NOT_FOUND, "Notification not found"),
```

**A 403 confirms the resource exists.** That is an enumeration oracle: an attacker with a valid token
for tenant A can discover which notification IDs belong to tenant B by watching which ones return 403
instead of 404.

The design calls for an integration test asserting the 404 explicitly, because this is exactly the
kind of behaviour a well-meaning refactor "improves" into a 403.

### The seam that exists today

Every controller handler takes an explicit `ApiCaller`:

```java
public NotificationStatusResponse status(ApiCaller caller, @PathVariable UUID id) {
    return queries.status(caller, id);
}
```

Resolved by `ApiCallerArgumentResolver` — a **parameter, not a thread-local**. A thread-local tenant
is invisible at the call site, survives into async work it should not, and is the single most common
source of cross-tenant bugs. Making it a parameter means a method that forgets to scope its query
does not compile without an unused variable warning at minimum, and reads wrong at review.

**Built?** The resolver and the parameter threading are built. There is no token validation behind
them — `ApiCaller` is currently populated from headers.

---

## 3. Encryption

### In transit

TLS 1.3 everywhere. `sslmode=verify-full` to RDS — *verify-full*, not `require`, because `require`
encrypts without authenticating the server and therefore does not stop a MITM. IAM + TLS to MSK.

### At rest

Two layers, and the second is the one that matters:

1. **KMS CMK** on RDS, MSK, ElastiCache, S3 and EBS. This protects against a stolen disk and satisfies
   most compliance checklists. It protects against nothing else — anyone with database access reads
   plaintext.
2. **Field-level AES-GCM** on recipient addresses and message bodies, under **per-user DEKs** wrapped
   by a KMS CMK. This is what makes insider exfiltration and crypto-shredding both work.

`RecipientAddressVault` is the interface seam for this; `DevelopmentAddressVault` is the
non-encrypting local implementation.

---

## 4. PII handling

**Addresses are never logged in plaintext.** Logs carry an `address_hint` — `g***@example.com` — and a
log filter redacts by key so a new field cannot leak by omission.

**Bodies live in S3, never on the heap.** Kafka carries a pointer (Claim Check), which also keeps the
`dispatch` record at ~1,250 bytes instead of unbounded.

### Retention

| Table | Hot | Archive |
|---|---|---|
| `notification` | 90 d (1.28 TB) | S3 Parquet 13 mo → Glacier 7 yr |
| `delivery_attempt` | 30 d | S3 Parquet 7 yr (billing evidence) |
| `notification_event` | 7 d | S3 7 yr |
| `audit_record` | 13 mo | S3 Object Lock (WORM) 7 yr |
| `idempotency_record` | 24–48 h | none |

Kafka: PII-bearing topics get `retention.ms=7d`, which is **the actual guarantee**. Compaction
tombstones are best-effort with no SLA, so a compacted topic is not a deletion mechanism.

### GDPR erasure by crypto-shredding

Rewriting 90 partitions of a 1.8-billion-row table to null one user generates 1.8B dead tuples and a
multi-day vacuum. Instead:

**Erasure = destroy the per-user DEK. One KMS call.** Every ciphertext in Postgres, S3, Kafka and the
archive becomes permanently unreadable simultaneously, with zero rows rewritten. The 30-day SLA is met
in minutes. [ADR-017](adr/ADR-017-crypto-shredding.md).

**Two honest caveats, both from the spec:**

- `address_hmac` is **tenant-keyed, not user-keyed** — suppression has to match across users — so it
  is not covered by the shred and must be nulled separately. A few thousand rows per user via
  `n_user_hist_ix`, batched. Seconds, not days.
- **`suppression_entry` must survive erasure.** Deleting someone's unsubscribe means you may lawfully
  mail them again, which is a *worse* compliance outcome than retaining a pseudonym. Rewrite with
  `reason='GDPR_ERASURE'`, drop the provider attribution, and get legal sign-off rather than deciding
  it in code review.

That second caveat is the interesting one, because the naive reading of "erasure" produces the worse
privacy outcome.

---

## 5. Secrets

**Zero secrets in git, in config, or in the database.**

AWS Secrets Manager with the External Secrets Operator; the database holds a `credentials_ref` whose
shape is enforced by the CHECK constraint in §1.1.

### Credential isolation is the part most designs skip

One secret per `(provider, tenant)`. Only the **worker** IRSA role can decrypt, scoped by tag.

**The API tier cannot decrypt provider credentials at all.** The API is the internet-facing surface
and the one most likely to be compromised; it has no business holding a credential that can send SMS
on the tenant's behalf. Separating the roles means an API compromise cannot become a spam incident.

---

## 6. Rate limiting

Three layers, and **all of them fail open**:

```
WAF per-IP  →  Redis token bucket per (tenant, endpoint)  →  per-(tenant, provider) budget
```

```java
try {
    admitted = quotaGuard.tryConsume(tenantId, permits);
} catch (RuntimeException e) {
    // Fail-open: an unreachable limiter must not read as "everyone is over quota".
    return;
}
```

**Failing open on a rate limiter is a security decision, and it is the right one here.** A Valkey
outage that reads as "every tenant is over quota" turns a degraded cache into a total outage of the
send API. The exposure is a bounded window of over-quota traffic; the alternative is a self-inflicted
denial of service against paying customers.

The quota is charged **per recipient**, not per request, so one call cannot bypass the limit by
carrying ten million of them.

`RateLimiter.tryAcquire` returns a boolean rather than throwing or blocking: throwing would cost a
stack trace at thousands per second for an expected outcome, and blocking would hold a Kafka consumer
thread and break `max.poll.interval.ms`.

---

## 7. Audit

Write-once `audit_record`, monthly partitions, 13 months hot, then S3 with **Object Lock (WORM)** for
7 years.

Object Lock is the point. An audit log an administrator can delete is not evidence, and the whole
reason to keep one is for the case where the administrator is the subject.

**Built?** The table exists in `V1__baseline.sql`. Nothing writes to it.

---

## 8. Threat model

| Threat | Mitigation | State |
|---|---|---|
| **Stolen token → mass spam** | Per-tenant quota + anomaly alert at >3× baseline + per-tenant kill switch | Quota built; anomaly and kill switch design |
| **Forged webhook marks everything delivered** | HMAC + timestamp + IP allowlist; **the monotonic guard bounds the blast radius** | Built |
| **Template injection → phishing** | Auto-escaping engine; `variables_schema` validated; no raw HTML from variables | Design (`EchoTemplateRenderer` today) |
| **Tenant A reads tenant B** | Repository scoping + ArchUnit + integration test asserting a cross-tenant **404**, not 403 | Design; the `ApiCaller` seam exists |
| **Insider exfiltration** | Field-level encryption — DB access alone yields ciphertext; KMS decrypt audited in CloudTrail | Design |
| **Replay of a captured request** | Idempotency key + request fingerprint → returns the original response, sends nothing | **Built** |
| **Timing attack on webhook HMAC** | `MessageDigest.isEqual`, byte comparison | **Built** |
| **Enumeration of notification IDs** | 404 for cross-tenant, UUIDv7 public ids | Partial |

### The one worth expanding: a forged webhook

Suppose an attacker gets past the HMAC — a leaked secret, say. What can they do?

They can post `DELIVERED` for a notification they know the ID of. What they **cannot** do:

- Move anything *backwards*. The monotonic guard rejects any transition to a rank ≤ the current one.
- Move anything out of a terminal state. `BOUNCED`, `COMPLAINED`, `FAILED`, `SUPPRESSED`, `CANCELLED`
  and `EXPIRED` are terminal, and the guard's first check is `if (this.terminal) return empty`.
- Replay the same payload twice — `dedup_hash` is UNIQUE.
- Cause an unbounded write. A rejected transition returns zero rows; it does not throw, retry or
  allocate.

**The blast radius of a forged webhook is bounded by a state machine that was designed for a
completely different reason** — handling out-of-order provider callbacks. That is the kind of
defence-in-depth worth pointing at, because it costs nothing.

### And the one that is genuinely open

`ChaosController` is mounted in the same application as the public API:

```java
log.warn("chaos endpoint is ACTIVE; mock providers can be broken on demand. "
        + "This must never be true in production.");
```

The warning is real and it fires at startup, but the endpoint is only protected by whatever
`SecurityConfig` does — which today is very little. In the design it lives on a separate admin ingress
behind SSO and MFA. **Do not deploy this repository as-is.**

---

## 9. Supply chain

Design: Dependabot, OWASP Dependency-Check, CycloneDX SBOM, Trivy image scanning, distroless base
images, `runAsNonRoot`, read-only root filesystem.

**None of it is set up.** There is no CI pipeline in this repository.

One supply-chain decision *is* made and is visible: `bitnami/*` images are frozen and unpatched, so
the compose stack uses `apache/kafka` and `kafbat/kafka-ui` — the latter specifically because
`provectuslabs/kafka-ui` has been abandoned since 2024 and has an RCE history.

---

## Say in an interview

> *"Two things I'd point at. First, the schema physically refuses to store a secret — there's a CHECK
> constraint on the credentials column that only accepts a Secrets Manager ARN or an SSM path, and I
> tested it by trying to insert a fake Twilio key. Code review can miss a pasted key; a check
> constraint can't.*
>
> *Second, the webhook verifier uses `MessageDigest.isEqual` rather than `String.equals`, because
> `equals` short-circuits on the first differing character and that timing difference is measurable
> across a network — you recover a valid HMAC one hex digit at a time. And I sign the timestamp
> alongside the body, because signing the body alone makes every captured payload valid forever.*
>
> *What I'd be honest about is that the security module is a package declaration. OAuth, field-level
> encryption and tenant scoping are designed and the seams are there — `ApiCaller` is a parameter
> rather than a thread-local specifically so scoping is visible at every call site — but they're not
> implemented."*
