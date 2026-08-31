# API Reference

Base path `/v1`. OpenAPI 3.1 served at `/v3/api-docs` and committed to the repo, so contract
drift appears in a diff.

---

## Conventions

| Concern | Rule |
|---|---|
| Auth | `Authorization: Bearer <JWT>` (OAuth2 client-credentials, RS256) |
| Idempotency | `Idempotency-Key` header **required** on all `POST` that create work |
| Content type | `application/json`; errors are `application/problem+json` (RFC 9457) |
| Pagination | Cursor only — `?cursor=&limit=` (max 200). Never offset; it degrades on partitioned tables |
| Caching | `ETag` / `If-None-Match` on status reads |
| Correlation | `X-Request-Id` echoed; `traceId` present in every error body |
| Timestamps | RFC 3339 with offset. A bare local time is a `400` |

## Endpoints

| Method | Path | Scope |
|---|---|---|
| `POST` | `/v1/notifications` | `notifications:send` |
| `GET` | `/v1/notifications/{id}` | `notifications:read` |
| `GET` | `/v1/notifications/{id}/recipients` | `notifications:read` |
| `GET` | `/v1/notifications/{id}/attempts` | `notifications:read` |
| `POST` | `/v1/notifications/{id}/cancel` | `notifications:send` |
| `PATCH` | `/v1/notifications/{id}/schedule` | `notifications:send` |
| `GET/POST/PUT` | `/v1/templates[/{code}][/versions]` | `templates:read\|write` |
| `GET/PUT/DELETE` | `/v1/users/{userId}/preferences` | `preferences:read\|write` |
| `POST` | `/v1/unsubscribe/{token}` | none (signed token) |
| `GET` | `/v1/providers/health` | `providers:read` |
| `POST` | `/v1/webhooks/{providerCode}` | none (HMAC) |
| `GET/POST` | `/admin/v1/dlq[/{id}/replay]` | `admin:dlq` |
| `POST` | `/admin/v1/mock-providers/{code}/chaos` | `admin:chaos` (local/dev only) |

---

## Send a notification

```http
POST /v1/notifications
Authorization: Bearer <jwt>
Idempotency-Key: 7f3c1a90-order-4821
Content-Type: application/json
```

```json
{
  "trafficClass": "TRANSACTIONAL",
  "channels": ["EMAIL", "PUSH"],
  "template": { "code": "order-shipped", "locale": "en-US" },
  "recipients": { "kind": "USER_IDS", "userIds": ["u_9f2a", "u_7b31"] },
  "variables": { "orderId": "A-4821", "eta": "2026-09-02" },
  "schedule": { "type": "IMMEDIATE" },
  "ttlSeconds": 86400,
  "metadata": { "campaignId": "cmp_2026q3", "correlationId": "svc-orders-9931" }
}
```

**`202 Accepted`**

```json
{
  "notificationRequestId": "01998f2a-7c31-7a04-9e12-6f0b3c1d5a88",
  "status": "ACCEPTED",
  "acceptedAt": "2026-08-31T09:14:22.481Z",
  "recipientCount": 2,
  "notifications": [
    { "id": "01998f2a-7c31-7a04-9e12-6f0b3c1d5a89", "channel": "EMAIL", "status": "ACCEPTED" },
    { "id": "01998f2a-7c31-7a04-9e12-6f0b3c1d5a8a", "channel": "PUSH",  "status": "ACCEPTED" }
  ],
  "links": { "status": "/v1/notifications/01998f2a-7c31-7a04-9e12-6f0b3c1d5a88" }
}
```

`202`, not `200`. A `200` implies delivery happened; it hasn't — the request is durably queued.

### Fields

| Field | Type | Notes |
|---|---|---|
| `trafficClass` | enum | `CRITICAL` \| `TRANSACTIONAL` \| `BULK`. Determines topic, TTL and shed order |
| `channels` | array | One or more of `SMS`, `EMAIL`, `PUSH`. One `notification` per channel |
| `template` | object | `{ code, locale }`. Mutually exclusive with `content` |
| `content` | object | Raw `{ subject, body }` for templateless sends |
| `recipients.kind` | enum | `INLINE` \| `USER_IDS` \| `S3_MANIFEST` \| `AUDIENCE_REF` |
| `variables` | object | Validated against the template version's `variablesSchema` |
| `schedule.type` | enum | `IMMEDIATE` \| `SCHEDULED` \| `RECURRING` |
| `schedule.sendAt` | string | Required when `SCHEDULED`. **Must carry an offset** |
| `ttlSeconds` | int | Defaults per traffic class. Expired notifications become `EXPIRED`, never sent |

### Scheduled

```json
{ "schedule": { "type": "SCHEDULED", "sendAt": "2026-09-01T09:00:00+05:30" } }
```

`"2026-09-01T09:00:00"` without an offset is a `400`. "9am" in whose timezone is not something
to guess — guessing is how OTPs arrive at 3 a.m.

### Campaign

```json
{
  "trafficClass": "BULK",
  "recipients": { "kind": "S3_MANIFEST", "uri": "s3://tenant-bucket/audience.ndjson", "count": 10000000 }
}
```

Returns `202` within the same p99 250 ms budget. Expansion is asynchronous (Claim Check pattern);
`recipientCount` is echoed as declared and reconciled after fan-out.

---

## Read status

### `GET /v1/notifications/{id}` — aggregate

```json
{
  "id": "01998f2a-7c31-7a04-9e12-6f0b3c1d5a89",
  "channel": "EMAIL",
  "trafficClass": "TRANSACTIONAL",
  "status": "PARTIALLY_COMPLETED",
  "createdAt": "2026-08-31T09:14:22.481Z",
  "dispatchedAt": "2026-08-31T09:14:23.102Z",
  "counts": { "total": 2, "delivered": 1, "failed": 0, "suppressed": 1, "pending": 0 },
  "links": { "recipients": "…/recipients", "attempts": "…/attempts" }
}
```

### `GET /v1/notifications/{id}/recipients`

```json
{
  "items": [
    { "recipientId": "…", "addressHint": "g***@example.com", "status": "DELIVERED",
      "deliveredAt": "2026-08-31T09:14:41Z", "attemptCount": 1 },
    { "recipientId": "…", "addressHint": "b***@example.com", "status": "SUPPRESSED",
      "suppressionReason": "QUIET_HOURS" }
  ],
  "nextCursor": null
}
```

Addresses are never returned in plaintext — only `addressHint`.

### `GET /v1/notifications/{id}/attempts`

```json
{
  "items": [
    { "attemptNumber": 1, "provider": "mock-sms-primary", "state": "FAILED",
      "failureType": "PROVIDER_TIMEOUT", "latencyMs": 5001,
      "startedAt": "2026-08-31T09:14:23Z", "retryScheduledAt": "2026-08-31T09:14:31Z" },
    { "attemptNumber": 2, "provider": "mock-sms-secondary", "state": "SUCCEEDED",
      "providerMessageId": "SM9f3a…", "latencyMs": 212, "costMicros": 9200 }
  ]
}
```

`state: "UNKNOWN"` is a documented, first-class outcome — the provider may or may not have
delivered. It resolves by webhook or reconciliation.

---

## Cancel and reschedule

```http
POST /v1/notifications/{id}/cancel
```

| Response | When |
|---|---|
| `200` | Was `PENDING`/`SCHEDULED`; now `CANCELLED` |
| `409 already-dispatched` | Already claimed or dispatched. A best-effort tombstone is written to Valkey and workers check it before the provider call, but no guarantee |

```http
PATCH /v1/notifications/{id}/schedule
{ "sendAt": "2026-09-02T14:00:00+05:30" }
```

Implemented as `DELETE` + `INSERT` on the schedule row, not `UPDATE` — a partition-key update is
a silent DELETE+INSERT at ~3× the WAL cost, and that cost should be visible in the code.

---

## Preferences

```http
PUT /v1/users/{userId}/preferences
```

```json
{
  "preferences": [
    { "channel": "EMAIL", "category": "marketing", "optedIn": false },
    { "channel": "SMS", "category": "*", "optedIn": true,
      "quietHours": { "start": "22:00", "end": "07:00", "timezone": "Asia/Kolkata" },
      "frequencyCap": { "maxPerDay": 5, "maxPerWeek": 20 },
      "digestMode": "IMMEDIATE" }
  ]
}
```

`category: "*"` is the channel-wide default; a specific category overrides it. Quiet hours carry
their own IANA zone — the timezone *is* the semantics, and `22:00` without one is meaningless.

Evaluation order at dispatch:
`OptOut → GlobalUnsubscribe → Suppression → QuietHours → FrequencyCap → Dedup → Consent`.

`CRITICAL` bypasses quiet hours — an OTP at 2 a.m. was requested by the user.

---

## Provider health

```http
GET /v1/providers/health
```

```json
{
  "providers": [
    { "code": "mock-sms-primary", "channel": "SMS", "circuitState": "OPEN",
      "successRate5m": 0.12, "p95LatencyMs": 4890, "rateLimitUtilization": 0.0,
      "lastFailure": { "type": "PROVIDER_5XX", "at": "2026-08-31T09:12:44Z" } },
    { "code": "mock-sms-secondary", "channel": "SMS", "circuitState": "CLOSED",
      "successRate5m": 0.994, "p95LatencyMs": 240, "rateLimitUtilization": 0.31 }
  ]
}
```

---

## Webhooks (inbound)

```http
POST /v1/webhooks/{providerCode}
X-Signature: sha256=<hex>
X-Timestamp: 1756631662
```

Four verification gates, in order:

1. HMAC-SHA256 over `timestamp + "." + rawBody`, **constant-time** compare
2. Timestamp within ±5 minutes (replay window)
3. Source IP allowlist per provider
4. `dedup_hash` UNIQUE insert

Returns `200` immediately after persisting the raw payload — never blocks on processing. A
duplicate also returns `200`; anything else makes the provider retry forever.
Invalid signature → `401` + audit record + `webhook_signature_invalid_total`.

---

## Error contract (RFC 9457)

```json
{
  "type": "https://docs.notification-platform.dev/errors/idempotency-key-reused",
  "title": "Idempotency key reused with a different payload",
  "status": 409,
  "detail": "Key '7f3c1a90-order-4821' was used at 2026-08-31T09:14:22Z with a different body.",
  "instance": "/v1/notifications",
  "traceId": "0af7651916cd43dd8448eb211c80319c",
  "errors": [ { "field": "variables.orderId", "code": "FINGERPRINT_MISMATCH" } ]
}
```

A machine-readable `type` URI means clients branch on a stable identifier instead of
string-matching English messages — which is what they will do if you don't give them a choice.

| Status | `type` slug | When |
|---|---|---|
| `400` | `validation-failed` | Schema, recipient format, missing template variable |
| `400` | `schedule-invalid` | `sendAt` in the past, beyond the 1-year horizon, or missing an offset |
| `401` | `unauthenticated` | Missing/expired/invalid JWT |
| `403` | `insufficient-scope` | Valid token, wrong scope |
| `404` | `notification-not-found` | Also returned for cross-tenant access — a `403` would confirm existence |
| `409` | `idempotency-key-reused` | Same key, different request fingerprint |
| `409` | `request-in-progress` | A concurrent request holds the key |
| `409` | `already-dispatched` | Cancel or reschedule after dispatch |
| `413` | `payload-too-large` | Body > 256 KB → use `S3_MANIFEST` |
| `422` | `all-recipients-suppressed` | Accepted, but nothing was sendable |
| `429` | `rate-limited` | + `Retry-After`, `X-RateLimit-Limit`, `X-RateLimit-Remaining` |
| `500` | `internal-error` | Never leaks a stack trace; `traceId` only |
| `503` | `service-degraded` | Load shedding. `BULK` sheds before `TRANSACTIONAL`; `CRITICAL` last |

### Idempotency semantics

| Situation | Response |
|---|---|
| New key | Processed; response stored for 24 h |
| Same key, **same** fingerprint | The original response replayed verbatim, including the original ID |
| Same key, **different** fingerprint | `409 idempotency-key-reused` |
| Same key, still in progress | `409 request-in-progress` |
| Key older than 24 h | Treated as new |

The fingerprint check is what most implementations skip. Same key with a different body must be
an error, not a silent replay of an unrelated response.
