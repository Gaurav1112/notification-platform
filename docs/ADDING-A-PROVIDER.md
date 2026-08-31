# Adding a Provider

**One class and two config rows.** This page shows exactly what that means, using the real SPI.

The claim is made in the README and in [ADR-005](adr/ADR-005-mock-providers.md); this document is
where it has to hold up.

---

## What you do and do not have to write

| You write | You do not write |
|---|---|
| A class implementing `NotificationProvider` | A timeout — `TimeoutProvider` |
| Its vendor error-code → `FailureType` mapping | A circuit breaker — `ProviderCircuitBreakers` |
| A `@Bean` method | A rate limiter — `RedisTokenBucketRateLimiter` |
| Two configuration rows | Idempotency recording — `IdempotentProvider` |
| | Metrics — `MeteredProvider` |
| | Tracing — `TracedProvider` |
| | Registration — `ProviderRegistry` collects `List<NotificationProvider>` |
| | Routing and failover — `HealthWeightedSelectionStrategy` |
| | Retry policy, backoff, DLQ routing — `RetryRouter` |
| | Anything in the worker, scheduler or API |

The reason this works is `ProviderRegistry`:

```java
public ProviderRegistry(List<NotificationProvider> providers) { … }
```

Spring collects every `NotificationProvider` bean into that constructor. There is **no registration
call to forget, no enum to extend, no switch to update.** The failure mode of a plugin system that
requires registration is that someone adds the class, forgets the registration, and the provider
silently never receives traffic.

---

## Step 1 — implement `NotificationProvider`

Five methods, and the contract is locked:

```java
public interface NotificationProvider {
    Channel channel();
    ProviderCode code();
    ProviderCapabilities capabilities();
    SendResult send(SendCommand command);
    boolean isHealthy();
}
```

### The input

```java
public record SendCommand(
        UUID recipientId,
        Channel channel,
        TrafficClass trafficClass,
        String address,            // E.164, email address, or device token
        String subject,
        String body,
        String idempotencyToken,
        Map<String, String> attributes,
        Duration deadline) { }     // hard upper bound; defaults to 5s
```

`deadline` is already enforced above you by `TimeoutProvider` — you do not have to honour it, but
passing it to your HTTP client as a request timeout means the call ends cleanly rather than being
cancelled from outside.

### The output — three cases, and you must return one

```java
public sealed interface SendResult permits Accepted, Rejected, Indeterminate {

    record Accepted(String providerMessageId, Duration latency, long costMicros) { }

    record Rejected(FailureType type, String code, String message,
                    Duration latency, Optional<Duration> retryAfter) {
        static Rejected of(FailureType type, String code, String message, Duration latency);
    }

    record Indeterminate(FailureType type, String message, Duration latency) { }
}
```

**`send` must not throw for a business outcome.** An exception carries no `retryAfter`, no cost, no
latency and no classification, so it forces every caller to re-derive information you already had.
`TimeoutProvider` treats a thrown exception as an adapter bug and wraps it in
`ProviderAdapterException` so the message is dead-lettered loudly rather than retried five times.

### A worked example

```java
package com.example.notification.provider.twilio;

/**
 * Twilio SMS.
 *
 * <p>Twilio offers <strong>no client idempotency key</strong> and no reconciliation by our own
 * reference — {@code GET /Messages} filters only on To/From/DateSent at whole-day granularity.
 * That is why {@code supportsIdempotencyKey} is false and why an {@link SendResult.Indeterminate}
 * on this channel must never be blind-retried: there is no way to find out afterwards, and a
 * duplicate OTP is not recoverable.
 */
public final class TwilioSmsProvider implements NotificationProvider {

    private static final ProviderCode CODE = ProviderCode.of("twilio-sms");

    private final TwilioRestClient client;
    private final String messagingServiceSid;
    private final long costMicros;

    public TwilioSmsProvider(TwilioRestClient client, String messagingServiceSid, long costMicros) {
        this.client = Objects.requireNonNull(client, "client");
        this.messagingServiceSid = Objects.requireNonNull(messagingServiceSid, "messagingServiceSid");
        this.costMicros = costMicros;
    }

    @Override public Channel channel()   { return Channel.SMS; }
    @Override public ProviderCode code() { return CODE; }

    @Override
    public ProviderCapabilities capabilities() {
        // No batch endpoint, no client idempotency key, status callbacks yes, status query yes.
        return new ProviderCapabilities(false, 1, false, true, true);
    }

    @Override
    public SendResult send(SendCommand command) {
        var startedAt = System.nanoTime();
        try {
            var message = client.messages().create(
                    command.address(), messagingServiceSid, command.body(),
                    command.deadline());
            return new SendResult.Accepted(message.sid(), elapsed(startedAt), costMicros);

        } catch (TwilioApiException e) {
            return mapError(e, elapsed(startedAt));

        } catch (IOException | TimeoutException e) {
            // The request may have reached Twilio. Never a Rejected — see the class javadoc.
            return new SendResult.Indeterminate(
                    FailureClassifier.classify(e), e.getMessage(), elapsed(startedAt));
        }
    }

    @Override
    public boolean isHealthy() {
        // A state read, never a vendor call. A health endpoint that pings the provider multiplies
        // every Kubernetes probe by the number of providers and pods.
        return client.isConnected();
    }

    private static Duration elapsed(long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos);
    }
}
```

### Step 1b — the error mapping, which is the part that actually matters

This is the only genuinely vendor-specific logic, and it is where an integration is right or wrong.

```java
private SendResult mapError(TwilioApiException e, Duration latency) {
    return switch (e.getCode()) {
        // 21610 — the recipient replied STOP. Permanent, and it must reach the suppression list.
        case 21610 -> SendResult.Rejected.of(FailureType.UNSUBSCRIBED,
                "21610", "Recipient has opted out", latency);

        // 21211 / 21614 — the number is not a valid mobile destination. Never retry.
        case 21211, 21614 -> SendResult.Rejected.of(FailureType.INVALID_RECIPIENT,
                String.valueOf(e.getCode()), e.getMessage(), latency);

        // 20429 — throttled. Our back-pressure, not their ill health; carry Retry-After.
        case 20429 -> new SendResult.Rejected(FailureType.RATE_LIMITED,
                "20429", "Too many requests", latency, e.retryAfter());

        // 20003 — auth. Account-scoped, so this SHOULD trip the breaker, and it should fail over.
        case 20003 -> SendResult.Rejected.of(FailureType.AUTH_FAILURE,
                "20003", "Authentication failed", latency);

        default -> e.getStatus() >= 500
                ? SendResult.Rejected.of(FailureType.PROVIDER_5XX,
                        String.valueOf(e.getCode()), e.getMessage(), latency)
                : SendResult.Rejected.of(FailureClassifier.classify(e.getStatus()),
                        String.valueOf(e.getCode()), e.getMessage(), latency);
    };
}
```

`FailureClassifier.classify(int httpStatus)` in `platform-resilience` covers the generic HTTP cases,
so you only hand-map the vendor codes that carry more meaning than their status does.

Two mappings people get wrong, and the platform will punish both:

| Mistake | Consequence |
|---|---|
| Mapping an opt-out to a retryable type | Five retries into a STOP, budget burned, carrier spam flag |
| Mapping a gateway timeout (504) to `PROVIDER_5XX` | Treated as definitely-not-delivered, blind-retried, duplicate SMS. `FailureClassifier` maps 504 → `PROVIDER_TIMEOUT` for exactly this reason |

### Step 1c — get `ProviderCapabilities` honest

```java
public record ProviderCapabilities(
        boolean supportsBatching,
        int maxBatchSize,
        boolean supportsIdempotencyKey,
        boolean supportsWebhook,
        boolean supportsStatusQuery) { }
```

| Field | Say true only if |
|---|---|
| `supportsBatching` / `maxBatchSize` | There is a real bulk endpoint and you know its hard cap. SES is 50 destinations — that is an API limit, not a tuning knob, and sending 51 is an error, not a slow request |
| `supportsIdempotencyKey` | A retried call with the same key genuinely does **not** send twice. For Twilio, SES, SendGrid, FCM and APNs the honest answer is **false** |
| `supportsWebhook` | The vendor pushes delivery events to us |
| `supportsStatusQuery` | We can look up the outcome of a specific message **by a reference we chose**. Twilio: no. SES: yes, via `EmailTags` |

`supportsStatusQuery` is what the reconciler needs to resolve an `Indeterminate`. Lying about it
means the reconciler queries something that cannot answer, and the `UNKNOWN` never resolves.

For the single-send case there is a shortcut:

```java
ProviderCapabilities.singleSend(true);   // no batching, no idempotency key, webhook yes
```

---

## Step 2 — the bean

```java
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "notification.providers.twilio", name = "enabled",
        havingValue = "true")
public class TwilioProviderConfiguration {

    @Bean
    public TwilioSmsProvider twilioSms(TwilioProperties properties, SecretResolver secrets) {
        var client = TwilioRestClient.using(secrets.resolve(properties.credentialsRef()));
        return new TwilioSmsProvider(client, properties.messagingServiceSid(), 7_900L);
    }
}
```

That is the whole registration. `ProviderRegistry` picks it up.

**Duplicate codes fail the context start**, deliberately, with both bean names in the message. Two
beans claiming `twilio-sms` would leave the router and the chaos endpoint each resolving to whichever
one the map happened to keep — so a fault injected into "the" primary would land on a provider nobody
is routing to.

---

## Step 3 — two configuration rows

### Row 1 — `notif.provider_configuration`

```sql
INSERT INTO notif.provider_configuration
    (provider_code, channel, enabled, priority, weight,
     cost_micros, rate_limit_rps, daily_cap, credentials_ref)
VALUES
    ('twilio-sms', 'SMS', true, 1, 100,
     7900, 100, 5000000, 'arn:aws:secretsmanager:eu-west-1:123456789012:secret:twilio/prod-AbC123');
```

**`credentials_ref` is a reference, never a secret.** The database enforces it:

```sql
CONSTRAINT provider_configuration_secret_ck
    CHECK (credentials_ref ~ '^(arn:aws:secretsmanager:|ssm:|mock:)')
```

Paste an actual API key here and the `INSERT` fails. Code review can miss a pasted key; a check
constraint cannot.

| Column | Feeds |
|---|---|
| `priority` | `ProviderCandidate.priority` — 1 = first choice. **Nudges, never decides**: the score is `1/priority × 0.15`, so an operator preference cannot keep traffic on a provider that has stopped delivering |
| `weight` | Static share for a vendor migration |
| `cost_micros` | `ProviderCandidate.costMicros` and the `notification.provider.cost.micros` counter |
| `rate_limit_rps` | The contracted ceiling the `RateLimitedProvider` stage shapes to. 0 = unmetered |
| `daily_cap` | Eligibility filter in `ChannelProviderRouter` |

### Row 2 — the webhook secret

Whatever the vendor signs its callbacks with, so `WebhookSignatureVerifier` can verify them:

```yaml
notification:
  webhooks:
    timestamp-tolerance: 5m
    providers:
      twilio-sms:
        secret: ${TWILIO_WEBHOOK_SECRET}        # from Secrets Manager, never committed
        allowed-ips:
          - 54.172.60.0/23
          - 54.244.51.0/24
```

If the vendor does not push callbacks, skip this and set `supportsWebhook = false`. The reconciler
then becomes the only path to a terminal state, which is exactly why `supportsStatusQuery` has to be
honest.

---

## What happens automatically once the bean exists

```
ProviderRegistry          indexes it by channel and by code
DecoratedProviders        wraps it: traced → metered → broken → limited → bounded → idempotent
ChannelProviderRouter     filters it on circuit state, health and daily cap
HealthWeighted…Strategy   scores it, and gives it a warm minority share as runner-up
ProviderCircuitBreakers   creates twilio-sms:sms lazily on first call
MeteredProvider           tags every call with provider / channel / traffic_class / outcome / failure
RetryRouter               classifies its failures and routes them
/v1/providers/health      lists it with its circuit state
Grafana                   the provider panels pick up the new label value
```

You never touch any of them.

### Two things you get for free that are easy to miss

**Cold start is handled.** A newly registered provider has no measurements, and
`ProviderCandidate.freshlyRegistered` starts it at `successRate5m = 1.0`, not 0.0. Start at 0.0 and it
scores last forever and never earns the traffic it needs to earn a score — the deadlock that makes an
operator disable the router and hard-code a vendor.

**A single-provider channel is logged, loudly:**

```
INFO  channel SMS has a single provider (twilio-sms); there is nothing to fail over to
```

---

## Step 4 — the tests you should write

The contract test is generic. `ProviderContractTest` in `platform-provider` already asserts the
properties every adapter must satisfy, and a new adapter should be added to it:

| Property | Why |
|---|---|
| `send` never throws for a business failure | `TimeoutProvider` treats a throw as an adapter bug and DLQs it |
| Identity survives the decorator chain | The registry, the router and the chaos endpoint all key on `code()` |
| Every `FailureProfile` maps to a distinct vendor code | A mapping gap shows up as a `PERMANENT_UNKNOWN` that no policy handles |
| A timeout produces `Indeterminate`, never `Rejected` | This is the duplicate-OTP case |
| `capabilities()` matches the vendor's documented limits | `maxBatchSize` is consulted, not a config value |

Name the tests after the failure, in the house style:

```java
@Test
@DisplayName("a Twilio 21610 opt-out is permanent and reaches the suppression list, never a retry")
void optOutIsPermanent() { … }

@Test
@DisplayName("a 504 from Twilio is PROVIDER_TIMEOUT, not PROVIDER_5XX — the SMS may have gone")
void gatewayTimeoutIsIndeterminate() { … }
```

---

## Step 5 — switch off the mocks

```yaml
notification:
  providers:
    mock:
      enabled: false          # MockProviderConfiguration is @ConditionalOnProperty
    twilio:
      enabled: true
```

Or leave both on during a migration: the router will score them together, and `priority` plus
`weight` let you shift traffic gradually while watching `provider_success_rate` and
`notification.provider.cost.micros` per provider.

---

## The whole thing, counted

| Artefact | Size |
|---|---|
| `TwilioSmsProvider.java` | ~120 lines, of which ~40 are the error mapping |
| `TwilioProviderConfiguration.java` | ~15 lines |
| `provider_configuration` row | 1 |
| Webhook secret row | 1 |
| Changes to existing platform code | **0** |

**Say in an interview:** *"Adding a real vendor is one class plus two config rows, and the reason
isn't the interface — it's that the registry takes a `List<NotificationProvider>` from Spring, so
there's no registration step to forget. The only vendor-specific code is the error-code mapping, and
that's the part I'd actually spend the review time on, because mapping an opt-out to a retryable
failure means five retries into someone who texted STOP."*

---

## Reference: what the mocks already model

Worth reading before writing a real adapter — the mapping tables in each class are the shape yours
should take.

| Mock | Models | Notable |
|---|---|---|
| `MockSmsProvider` | Twilio | No client idempotency key; magic test numbers via `vendorOverride`; 21610 opt-out |
| `MockEmailProvider` | Amazon SES | `supportsBatching = true`, `maxBatchSize = 50`; permanent bounce must reach suppression or SES pauses the account above 5% |
| `MockPushProvider` | FCM / APNs | One HTTP/2 request per token (FCM removed its batch endpoint in June 2024); `UNREGISTERED`; `apns-collapse-id` |

And the fact that governs all three: **no provider we would plausibly integrate offers a usable
client idempotency key.** Layer 5 of the idempotency stack is aspirational rather than universal, and
the design does not assume it.
