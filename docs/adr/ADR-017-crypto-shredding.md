# ADR-017: GDPR erasure by crypto-shredding, not row rewrite

**Status:** Accepted
**Date:** 2026-08-31

## Context

GDPR Article 17 gives a data subject the right to erasure, and the practical expectation is a 30-day
SLA. The personal data here is recipient addresses — phone numbers, email addresses, device tokens —
and message bodies, which routinely contain names, order numbers and amounts.

That data is in:

| Store | Volume |
|---|---|
| PostgreSQL `notification`, `notification_recipient`, `delivery_attempt` | ~1.8 billion rows across 90 daily partitions |
| S3 message bodies | Claim Check pointers from Kafka |
| Kafka topics | 7-day retention on PII-bearing topics |
| S3 Parquet archive | 13 months to 7 years |

The obvious implementation is `UPDATE … SET address = NULL WHERE user_id = ?` across all of them.

## Decision

**All PII is AES-GCM encrypted under a per-user data encryption key (DEK), wrapped by a KMS customer
master key. Erasure is destroying the DEK — one KMS call.**

Every ciphertext in PostgreSQL, S3, Kafka and the archive becomes permanently unreadable
simultaneously, with **zero rows rewritten**.

## Why row rewrite does not work

### The dead-tuple arithmetic

PostgreSQL's MVCC means an `UPDATE` writes a new row version and marks the old one dead. Nulling one
user's addresses across a 1.8-billion-row table touches every partition that contains one of their
rows.

```
An UPDATE over 90 partitions of a 1.8B-row table
→ up to 1.8B dead tuples requiring autovacuum
→ a multi-day vacuum on the primary
→ table and index bloat throughout
→ WAL volume proportional to rows rewritten
```

And that is **one erasure request**. At 50 million users, erasure requests are a steady stream, not an
event.

The whole design works hard to avoid dead tuples — hourly `idempotency_record` partitions so expiry is
`DROP TABLE` rather than `DELETE`, outbox rows DELETEd rather than stamped, no index on
`notification.status` so transitions stay HOT. An erasure mechanism that generates billions of them
undoes all of it.

### It only covers what you remember

A rewrite has to reach PostgreSQL, S3 bodies, Kafka topics inside retention, the Parquet archive, and
any backup within its own retention window. Miss one and the erasure is incomplete — and *proving*
completeness means auditing every store.

### It does not cover backups at all

A database backup taken before the erasure still contains the data. Rewriting live rows does nothing
about it, and the honest answers are either "we restore, re-erase and re-backup" or "we wait out the
backup retention".

## Why crypto-shredding does

```
Erasure = KMS DeleteKey on the user's DEK
```

- **One operation**, not a distributed rewrite.
- Covers **every store simultaneously**, including backups and archives, because they all hold the
  same ciphertext.
- **Zero dead tuples.** No vacuum, no bloat, no WAL.
- **Provable.** A destroyed KMS key is a CloudTrail record with a timestamp. "We deleted the key at
  14:32" is stronger audit evidence than "we ran an UPDATE that we believe touched everything".
- The 30-day SLA is met **in minutes**.

## The two honest caveats

Both are in the design, and both matter more than the mechanism.

### 1. `address_hmac` is tenant-keyed, not user-keyed

Suppression has to match across users — the same phone number appearing under two accounts must hit
the same suppression entry — so the address HMAC is keyed per tenant, not per user. It is therefore
**not covered by the shred** and has to be nulled directly.

The scale is manageable: a few thousand rows per user via the `n_user_hist_ix` index, batched. Seconds,
not days. But it is a second mechanism, and forgetting it means the erasure is incomplete in a way the
KMS audit trail will not reveal.

### 2. `suppression_entry` must survive erasure

This is the counter-intuitive one and it is the most important paragraph on this page.

**Deleting someone's unsubscribe means you may lawfully mail them again.** That is a *worse* privacy
outcome than retaining a pseudonymous record that they opted out.

The handling: rewrite the entry with `reason = 'GDPR_ERASURE'`, drop the provider attribution, keep the
suppression in force.

And: **get legal sign-off rather than deciding it in code review.** The naive reading of "erase
everything" produces the worse outcome for the data subject, and that is not a judgement an engineer
should make alone.

## Kafka is a separate story

Compaction tombstones are **best-effort with no SLA** — a tombstone guarantees eventual removal only
after the compaction cycle runs, and there is no bound on when.

So the actual guarantee on Kafka is **`retention.ms = 7d` on PII-bearing topics**. Crypto-shredding
covers the window regardless, which is why it is the mechanism rather than an optimisation.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Row rewrite / null-out** | 1.8B dead tuples, multi-day vacuum, does not reach backups |
| **`DELETE` the rows** | Worse than nulling — same dead tuples, plus it destroys the audit trail and the billing evidence the design keeps `delivery_attempt` for |
| **Tokenisation / a vault** | Store a token in the main tables, real PII in a vault, erase the vault entry. Very close to crypto-shredding in effect, with a lookup on every read — a network hop on the hot dispatch path for every recipient. Crypto-shredding puts the ciphertext inline and only needs the key |
| **One DEK per tenant, not per user** | Cheaper key management, and erasing one user would destroy every user in that tenant. Unusable |
| **Application-level encryption with a locally-held key** | Removes the KMS dependency and the audit trail, and puts key custody in the application — where it can be logged, leaked or backed up |
| **Retain nothing; short retention everywhere** | Would genuinely simplify this. Fails against the 7-year billing-evidence requirement on `delivery_attempt` and the WORM audit requirement |

## Consequences

### Positive

- **Erasure is one KMS call**, provable in CloudTrail, complete across every store including backups.
- **No dead tuples, no vacuum pressure**, which preserves the write-path headroom the whole capacity
  model depends on.
- Field-level encryption also defends against **insider exfiltration** — database access alone yields
  ciphertext, and every KMS decrypt is audited.
- The 30-day regulatory SLA is met with three orders of magnitude of margin.

### Negative

- **A per-user DEK is a per-user key to manage.** At 50 million users that is 50 million wrapped keys,
  their storage, their caching, their rotation and their own backup story. This is the largest hidden
  cost of the decision.
- **Every read of an address or body is a decrypt.** Cached DEKs make it cheap in the common case and
  a cache miss is a KMS call on the dispatch path.
- **Destroying a key is irreversible, and it is a single call.** An erroneous shred cannot be undone
  from any backup, because the backup is ciphertext too. That needs a confirmation workflow, and a
  KMS key deletion window (7–30 days) is the safety net.
- **Encrypted columns cannot be indexed or searched.** Suppression matching therefore needs the
  tenant-keyed HMAC, which is the source of caveat 1 above — the caveat is *caused by* the encryption
  scheme.
- **Two erasure mechanisms**, not one: the shred, plus the HMAC nulling. A partial implementation looks
  complete.
- KMS availability becomes a dependency of reading any PII.

### And the honest one

**None of this is built.** `platform-security` contains a `package-info.java`. There is no
field-level encryption, no DEK management and no erasure workflow in this repository. The design is
sound and the seams exist — `RecipientAddressVault` is the interface — but the implementation is not
there. See [STATUS.md](../STATUS.md) and [SECURITY.md](../SECURITY.md).

## Related

- [SECURITY.md](../SECURITY.md) — the full PII and encryption picture
- [ADR-004](ADR-004-delivery-attempt-in-postgres.md) — the 7-year billing-evidence requirement that
  rules out simply deleting
