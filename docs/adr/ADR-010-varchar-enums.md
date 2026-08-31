# ADR-010: `varchar` + CHECK for enums, with one lookup table

**Status:** Accepted
**Date:** 2026-08-31

## Context

The schema has a lot of enumerated columns: channel, traffic class, priority, schedule type, attempt
state, schedule state, circuit state, suppression reason, failure type, delivery status. Ten domains
of closed values.

PostgreSQL offers native `ENUM` types, `DOMAIN` types with a CHECK, plain `varchar` with a CHECK, and
lookup tables with foreign keys. All four are defensible in isolation.

## Decision

**`varchar` + a CHECK constraint everywhere, except `delivery_status`, which is a table.**

```sql
status varchar(24) NOT NULL,
CONSTRAINT notification_status_ck CHECK (status IN ('PENDING','SCHEDULED', …))
```

```sql
CREATE TABLE notif.delivery_status (
    code        varchar(24) PRIMARY KEY,
    rank        smallint NOT NULL UNIQUE,
    is_terminal boolean NOT NULL,
    is_failure  boolean NOT NULL
);
```

## Why native `ENUM` was rejected — three reasons, all reproduced against 18.6

### 1. You cannot add a value and use it in the same transaction

```
ALTER TYPE … ADD VALUE 'NEW_STATUS';
INSERT … VALUES ('NEW_STATUS');
ERROR:  unsafe use of new value "NEW_STATUS" of enum type
```

**Every Flyway migration runs in a transaction.** So adding an enum value and backfilling with it
requires two migrations, and the ordering constraint is invisible until it fails in staging.

### 2. You can never remove an enum value

There is no `ALTER TYPE … DROP VALUE`. Roll back a bad deploy and the zombie value lives in `pg_enum`
forever — invisible to an exhaustive Java `switch`, present in the database, and impossible to remove
without recreating the type and every dependent column.

### 3. It forces `stringtype=unspecified` on the JDBC URL

PgJDBC binds a Java `String` as `varchar`, and PostgreSQL will not implicitly cast `varchar` to an
enum type in a parameterised comparison. The standard workaround is `stringtype=unspecified` on the
connection URL, **which silently weakens type checking for every column in the application**, not just
the enum ones.

Trading application-wide type safety for a storage optimisation on ten columns is a bad trade.

## Why `delivery_status` is a table

Because it carries **data the SQL reads**.

The monotonic guard is a single UPDATE that has to know whether the current status is terminal:

```sql
UPDATE notif.notification n
   SET status = $1, status_rank = $2, status_at = now()
 WHERE n.id = $3
   AND n.status_rank < $2
   AND NOT EXISTS (SELECT 1 FROM notif.delivery_status d
                    WHERE d.code = n.status AND d.is_terminal)
```

A native enum's implicit ordering cannot express "terminal", and **a CHECK constraint cannot be
joined**. The rank and the terminal flag have to be queryable rows.

The payoff is worth stating: **adding a status is an `INSERT`.** No DDL, no lock, no deploy
coordination between the migration and the application.

Two rank choices in that table carry business rules that would otherwise be `if` statements:

```
CANCELLED = 28  <  QUEUED = 30      "you cannot cancel something already queued"
DELIVERED = 80, is_terminal = FALSE  because a hard bounce legitimately follows an SMTP 250
```

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Native `ENUM`** | Three reasons above, all reproduced |
| **`DOMAIN` with a CHECK** | Better than an enum — real `varchar` underneath. But altering a domain's constraint **locks every dependent table simultaneously**, so one status change takes an exclusive lock on six partitioned tables and all 335 partitions at once |
| **Lookup table + FK for every enum** | Correct in a textbook. Fails here because there are **no foreign keys on the hot tables** at all ([ADR-014](ADR-014-no-foreign-keys.md)) — an FK turns O(1) `DETACH PARTITION` into a validation scan. Ten unenforceable lookup tables are worse than ten CHECK constraints |
| **`smallint` codes with meaning in the application** | Compact and fast, and a nightmare in a production incident. `SELECT status FROM notification` returning `55` instead of `SEND_FAILED` means every ad-hoc query needs a decoder ring, and someone will get it wrong at 3am |
| **No constraint at all** | The database stops being the last line of defence. A typo in one code path silently writes `'DELIVRED'` and it never fails until a report is wrong |

## How a value is added or removed

The evolution path is the reason this decision holds up:

```sql
-- Adding a value: catalogue-only, instant, no table scan
ALTER TABLE notif.notification DROP CONSTRAINT notification_status_ck;
ALTER TABLE notif.notification ADD CONSTRAINT notification_status_ck
    CHECK (status IN (…, 'NEW_VALUE')) NOT VALID;

-- Later, at leisure, with concurrent DML allowed
ALTER TABLE notif.notification VALIDATE CONSTRAINT notification_status_ck;
```

`NOT VALID` means the new constraint applies to new rows immediately without scanning existing ones.
`VALIDATE` takes only a `SHARE UPDATE EXCLUSIVE` lock, so reads and writes continue.

For `delivery_status`, it is simply `INSERT INTO notif.delivery_status VALUES ('NEW', 95, false, false)`.

## Consequences

### Positive

- **Values can be added and removed**, which enums cannot do.
- **No `stringtype=unspecified`** — full type checking on every column in the application.
- Readable in every tool: `psql`, a Grafana panel, a CSV export, a support ticket.
- `delivery_status` being joinable is what makes the monotonic guard a **single statement** with no
  `SELECT`-then-`UPDATE` and no optimistic-lock retry loop.
- Adding a delivery status is an INSERT with no deploy coordination.

### Negative

- **More storage.** `varchar(24)` against a 4-byte enum OID, on tables with 1.8 billion rows. That is
  real, and it is accepted because TOAST and compression absorb most of it and the columns are short.
- **Slightly slower comparisons.** String comparison against integer comparison, on a hot path.
  Mitigated by `status_rank smallint` being the column the guard actually compares.
- **Two mechanisms to keep in step.** The Java enum and the SQL CHECK constraint are separate
  declarations of the same closed set, and nothing enforces that they agree. A value added to one and
  not the other fails at runtime. This is the real cost of the decision and there is no clean fix short
  of code generation.
- The `delivery_status` table is an exception to the pattern, so a reader has to be told why.
- **298 check constraints** in the schema. They are cheap, but they are 298 things that show up in
  `\d+` output.

## Related

- [ADR-014](ADR-014-no-foreign-keys.md) — why lookup tables with FKs were not an option
- The monotonic guard, explained in [CODE-WALKTHROUGH.md](../CODE-WALKTHROUGH.md) Part 2
