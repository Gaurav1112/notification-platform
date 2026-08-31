# ADR-015: Valkey 9 rather than Redis 8

**Status:** Accepted
**Date:** 2026-08-31

## Context

The platform needs an in-memory data store for idempotency caching, business dedup, preference
caching, rate-limit token buckets, circuit-breaker state sharing, scheduler leader locks and the
per-shard due-index sorted sets. At the stretch operating point that is ~35,847 ops/s and 44.62 GB
naive, or 11.50 GB with the TTL and cuckoo-filter levers applied.

Redis is the obvious choice. In March 2024 Redis Ltd. relicensed Redis from BSD-3 to a dual
RSALv2/SSPLv1 model; Redis 8 later added AGPLv3 as a third option. The Linux Foundation forked Redis
7.2.4 as **Valkey**, which remains BSD-3.

## Decision

**Valkey 9.1.1.** Protocol-compatible with Redis, driven through the standard Lettuce client, run as
`valkey/valkey:9.1.1` locally and ElastiCache for Valkey in AWS.

## Why the licence matters here and not everywhere

For a self-hosted internal service, SSPL is mostly a theoretical concern — the copyleft trigger is
offering the software *as a service* to third parties.

Three reasons it still decides this:

1. **This is a portfolio project.** It should be clonable, forkable and reusable without anyone having
   to reason about SSPL. A BSD-3 dependency needs no thought.
2. **A notification platform is close to the trigger.** A multi-tenant notification API offered to
   external customers is not obviously outside the "offering as a service" definition, and "not
   obviously outside" is a conversation with legal that a licence change makes unnecessary.
3. **AGPLv3 in Redis 8 is worse, not better.** It is a well-understood licence, and it is a strong
   copyleft licence that most corporate policies flag on sight.

## Why it is a low-risk swap

| | Detail |
|---|---|
| **Protocol** | Wire-compatible. `RedisTokenBucketRateLimiter` uses standard commands and a Lua script; nothing is Valkey-specific |
| **Client** | Lettuce, unchanged. Spring Data Redis works as-is |
| **Managed service** | ElastiCache offers a Valkey engine, at a lower per-node price than the Redis engine |
| **Governance** | Linux Foundation, with AWS, Google and Oracle contributing — a broader base than a single vendor |
| **Reversibility** | Switching back is a container image change and a connection string |

That last row is the real reason the decision is cheap: it is not a one-way door.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Redis 8 (AGPL / RSAL / SSPL)** | Tri-licensed, all three of which need a legal conversation for a project intended to be freely reused |
| **Redis 7.2.4** — the last BSD-3 release | Frozen. No security patches, and pinning to an unmaintained release to avoid a licence is a worse position than forking |
| **KeyDB** | Multithreaded Redis fork, BSD-3. Much smaller community and no managed offering; Valkey has the Linux Foundation and ElastiCache |
| **Dragonfly** | Genuinely faster on paper, and BSL-licensed with its own restrictions. Also a much younger codebase for something on the correctness path |
| **Memcached** | No sorted sets, no Lua, no persistence. The scheduler due-index and the token-bucket limiter both need capabilities it does not have |
| **Hazelcast / Infinispan** | Embedded JVM caches. Would remove a component, and would make state per-pod at exactly the points where it must be shared — the rate-limit budget must be global, or dividing 100/s by the replica count is wrong the moment the HPA scales |
| **PostgreSQL for everything** | Already the bottleneck. Putting 35,847 ops/s of cache traffic on the datastore that is second on the bottleneck ladder is the opposite of the right direction |

## What the platform actually depends on

Worth listing, because it bounds how much this decision could ever cost:

| Use | Structure | Fallback if unavailable |
|---|---|---|
| Idempotency cache | `SETNX` + TTL | PostgreSQL `idempotency_record` |
| Consumer dedup | `SETNX` + 7-day TTL | Process anyway; layers 4 and 5 hold |
| Preference cache | Hash + TTL | Read through to PostgreSQL |
| Rate-limit token bucket | Lua script over a hash | Per-pod approximation |
| Circuit-breaker state sharing | Key per `(provider, channel)` | Per-pod local state |
| Scheduler leader lock | `SET NX PX` with a fencing token | PostgreSQL advisory lock |
| Due index | Sorted set per shard | PostgreSQL range scan |

**Every one of them has a fallback, and every fallback fails open.** Redis loss degrades quality —
more duplicate work, weaker frequency capping, slower scheduling — never correctness. That principle
is what makes the choice of *which* Redis-compatible store low-stakes.

## Consequences

### Positive

- BSD-3. No licence conversation, ever.
- Lower ElastiCache pricing on the Valkey engine.
- Linux Foundation governance with multiple large contributors, rather than a single vendor that has
  already changed the licence once.
- Zero code impact — the client, the commands and the Lua script are unchanged.

### Negative

- **Smaller ecosystem for the newest Redis modules.** RedisJSON, RediSearch and RedisBloom are Redis
  Ltd. products; Valkey has its own module story that is younger. This platform uses none of them
  today, but the **cuckoo filter** lever in the capacity model would want RedisBloom or an equivalent
  — and that lever is the difference between 44.62 GB and 11.50 GB.
- **Divergence risk.** Valkey and Redis will drift. Today they are protocol-compatible; in three
  years' time a feature may exist on only one side.
- Less operational literature and fewer StackOverflow answers than Redis, though the overlap is still
  near-total.
- Some monitoring tooling still assumes the Redis name and version string.

## Note on the cuckoo filter

The one real functional gap is the dedup-set memory lever. The naive exact set is 17 GB; a cuckoo
filter at 1.2 bytes/entry brings the total from 44.62 GB to 11.50 GB. On Redis that is RedisBloom.

On Valkey the options are the community module port, or implementing the filter in the application
and storing it as a bitmap. Neither is free, and this is the one place the licence decision has a
technical cost rather than just a legal benefit.

**Neither the dedup set nor the filter is built today** — see [STATUS.md](../STATUS.md).
