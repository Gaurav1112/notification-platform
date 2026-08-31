# ADR-008: Dispatch topics split by channel × traffic class

**Status:** Accepted
**Date:** 2026-08-31

## Context

Notifications are not one workload:

| Class | Example | Latency budget (accept → provider) | TTL |
|---|---|---|---|
| `CRITICAL` | OTP, 2FA, fraud alert | **p99 < 5 s** | 60 s |
| `TRANSACTIONAL` | Receipt, password reset | p99 < 30 s | 24 h |
| `BULK` | Marketing, digest | p99 < 15 min | 72 h |

The design point is a **10-million-recipient campaign in 15 minutes landing on the 20:00 peak**:
14,583 notifications/s. During that fifteen minutes, someone is logging in and needs an OTP within
five seconds.

## Decision

**Six dispatch topics: `{sms, email, push} × {tx, bulk}`.** Physically separate topics with
independent consumer groups.

```
notification.dispatch.sms.tx      notification.dispatch.sms.bulk
notification.dispatch.email.tx    notification.dispatch.email.bulk
notification.dispatch.push.tx     notification.dispatch.push.bulk
```

The mapping is computed by the domain enum, not by infrastructure configuration:

```java
public enum TrafficClass {
    CRITICAL     (…, "tx",   0),
    TRANSACTIONAL(…, "tx",   1),
    BULK         (…, "bulk", 2);

    public String dispatchTopic(Channel channel) {
        return "notification.dispatch." + channel.name().toLowerCase() + "." + topicSuffix;
    }
}
```

## Why a priority field cannot work

This is the whole argument, and it is worth being able to state precisely.

**Kafka partitions are strictly FIFO.** A consumer reads a partition in offset order. There is no
mechanism — none — for it to skip ahead to a higher-priority record. A `priority` field on a record
is metadata the consumer can *read*, after it has already read every record in front of it.

So with one shared dispatch topic:

```
10,000,000 campaign records spread over the partitions of one topic
≈ 11,000 records ahead of the password reset in whichever partition it lands on
at 300 msg/s per partition → 37-second delay
```

A 37-second delay on a password reset, and a 60-second TTL on an OTP that is now most of the way
expired. **The priority field is read 37 seconds too late to matter.**

The only fix is that the OTP is never behind those 11,000 records in the first place, and the only
way to arrange that is a physically separate topic with its own consumer group.

## Why per-channel as well as per-class

A stalled SMS record — a Twilio 429 with a 30-second backoff — **head-of-line blocks every push and
email record behind it in the same partition.** One provider incident would degrade all three
channels.

Per-channel topics also let each channel have the partition count its consumer rate justifies:

| Topic | Partitions | Why |
|---|---:|---|
| `dispatch.sms.tx` | 12 | 80/s per partition — the slowest consumer in the system, hard provider caps |
| `dispatch.sms.bulk` | 6 | Bulk SMS is rare and expensive; deliberately the narrowest |
| `dispatch.email.tx` | 12 | 200/s per partition thanks to SES's 50-destination bulk API |
| `dispatch.email.bulk` | 36 | Campaign email, still cheap per message |
| `dispatch.push.tx` | 18 | 300/s, latency-critical |
| `dispatch.push.bulk` | **54** | The widest topic — FCM removed its batch endpoint in June 2024, so one HTTP/2 request per token |

One shared topic would have to be sized for the widest consumer and would waste the difference.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **One topic + a `priority` field** | FIFO partitions make it a lie. See above |
| **One topic + separate consumer groups filtering by class** | Every group reads every record and discards most. At 604M records/day that is 3× the read amplification for zero isolation — the CRITICAL group still reads past the campaign |
| **Per-class only, not per-channel** | A stuck SMS blocks push and email in the same partition |
| **Per-channel only, not per-class** | The campaign still blocks the OTP within a channel — the primary failure |
| **Three classes → three suffixes (`critical`, `tx`, `bulk`)** | Nine dispatch topics. `CRITICAL` and `TRANSACTIONAL` share the `tx` lane because their latency budgets (5 s and 30 s) are close relative to BULK's 15 minutes, and the campaign volume is entirely in BULK. The isolation that matters is bulk-from-everything-else |
| **A separate Kafka cluster per class** | True isolation, and enormous operational cost for a problem topics already solve |

## Consequences

### Positive

- **Class isolation is physical, not advisory.** A campaign cannot delay an OTP, because they are not
  in the same log.
- Independent consumer groups mean independent scaling: the bulk push group can run 54 consumers
  while the SMS tx group runs 12.
- **Load shedding has somewhere to act.** Pausing the bulk consumer groups sheds bulk dispatch without
  touching tx — step 2 of the shedding ladder, and it is a consumer-group operation rather than a
  code path.
- Per-topic retention and `minISR` differ where it matters: `sms.tx` is the only topic with
  `minISR=3`, because an OTP is worth more than the availability that third replica costs.
- The mapping lives on the domain enum, so "this is an OTP, therefore this topic" is a rule in code
  rather than a YAML file someone can change without thinking about why.

### Negative

- **Six dispatch topics instead of one**, plus five retry tiers, plus five others: 16 topics, 288
  partitions to provision, monitor and reason about.
- **Six consumer groups with six lag series.** An operator has more dashboards to read, and "is the
  system behind?" is six questions rather than one.
- **A misrouted record is invisible.** Producing a campaign onto a `.tx` topic breaks the isolation
  silently — there is no error, just a starved lane. The `CriticalDispatchLatencyFastBurn` alert
  description names this explicitly as the first thing to check.
- Partition counts are effectively fixed. Increasing them on a keyed topic re-maps
  `murmur2(key) % N` and permanently breaks per-key ordering across the boundary, which is why every
  keyed topic carries a 1.5× over-provisioning factor.
- Cross-class ordering does not exist. That is fine — ordering is published as per
  `(recipient, channel)` only — but it means a BULK and a CRITICAL message to the same recipient can
  arrive out of send order.

## How it is verified

The `mixed-class` load scenario — 90% BULK plus 10% CRITICAL — exists specifically to falsify this
decision. Pass criterion: **CRITICAL dispatch p99 under mixed load within 20% of its baseline.** If it
degrades, the split is not delivering what this ADR claims.

That scenario **has not been run** — see [LOAD-TEST.md](../LOAD-TEST.md).

## Related

- [ADR-006](ADR-006-kafka-as-broker.md) — why Kafka, and the FIFO property this rests on
- [ADR-009](ADR-009-tiered-retry-topics.md) — the five retry tiers, shared across channels for the
  opposite reason
