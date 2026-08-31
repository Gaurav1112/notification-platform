-- =============================================================================
-- V2 — notif.scheduled_notification
--
-- The table JdbcScheduledWorkStore has always targeted and V1 never created.
-- Consequence, before this migration: app-scheduler booted fine and then raised
-- `relation "notif.scheduled_notification" does not exist` on every claimer
-- tick — 100 ms, across 256 shards — and on every hydrator scan. Nothing
-- crashed, nothing was lost, and every scheduled and deferred send simply never
-- happened. That is the failure this file exists to end.
--
-- Conventions are V1's, deliberately: varchar + CHECK rather than native enums
-- (an enum label can never be removed, and a migration that adds one then uses
-- it in the same transaction fails outright), timestamptz everywhere, no
-- foreign keys on a partitioned hot table, and a composite primary key leading
-- with the partition column because PostgreSQL requires every unique constraint
-- to contain the partition key.
--
-- The one choice that is specific to this table, and the only one worth
-- arguing about, is the partition key. Read the note on due_bucket first.
-- =============================================================================

SET lock_timeout = '3s';
SET statement_timeout = '0';

SET search_path = notif, public;

CREATE TABLE notif.scheduled_notification (
    -- The partition key, and immutable by construction: derived from due_at at
    -- insert, guarded by a trigger, never updated.
    --
    -- due_at is the obvious key and it is the wrong one, for a reason that only
    -- shows up under load. A partition-key UPDATE is not an UPDATE: PostgreSQL
    -- turns it into a DELETE plus an INSERT in another partition, at roughly
    -- triple the WAL cost. And it breaks the claim outright — `FOR UPDATE SKIP
    -- LOCKED` can step over a tuple another session has *locked*, but it cannot
    -- silently ignore one that was *moved* out from under it. The concurrent
    -- session gets ERROR 40001, "tuple to be locked was already moved to
    -- another partition due to concurrent update", on the hottest loop in the
    -- platform. Rescheduling is therefore an explicit DELETE + INSERT, and
    -- nothing in JdbcScheduledWorkStore ever moves a row between partitions.
    --
    -- `date` rather than a truncated timestamptz because notif.ensure_daily_partition
    -- (V1) takes a date, and because the scan predicate compares against
    -- ((… AT TIME ZONE 'UTC')::date) — see the AT TIME ZONE note on the trigger.
    due_bucket              date         NOT NULL,
    id                      uuid         NOT NULL,          -- UUIDv7 from the writer
    due_at                  timestamptz  NOT NULL,

    -- 0..255, hashed from the recipient and stamped once at insert. Fixed for
    -- the row's whole life because it decides which pod owns it: a shard that
    -- moves is a row two pods reach for at the same time. The upper bound is
    -- hard-coded rather than configurable for the same reason ShardAssignment
    -- documents SHARD_COUNT as fixed forever — changing it orphans every
    -- already-written row and every hydrated Redis key.
    shard                   smallint     NOT NULL,

    tenant_id               bigint       NOT NULL,
    notification_id         uuid         NOT NULL,
    -- Carried, not looked up. notif.notification is partitioned by created_at,
    -- so without this any join back to it is an Append across every partition.
    notification_created_at timestamptz  NOT NULL,
    recipient_id            uuid         NOT NULL,
    channel                 varchar(16)  NOT NULL,
    traffic_class           varchar(16)  NOT NULL,

    state                   varchar(16)  NOT NULL DEFAULT 'READY',

    -- --- lease columns -------------------------------------------------------
    -- The lease, not the row lock, is the correctness boundary. The claim
    -- transaction commits *before* the Kafka publish on purpose (holding it
    -- open across a 10 s produce pins xmin and stops autovacuum reclaiming the
    -- dead tuples status updates generate). Between that commit and the publish
    -- the row belongs to a pod that may be OOM-killed, and nothing releases it.
    -- claim_expires_at is what bounds that window.
    claimed_by              varchar(96),
    claim_expires_at        timestamptz,

    -- NOT a retry counter. Incremented only by LeaseReaper, on reclaim, so it
    -- counts exactly one thing: the number of times taking this row ended with
    -- the pod that took it failing to finish. A row that does that repeatedly
    -- is killing the process that touches it, and past the threshold it is
    -- routed to the DLQ instead of being handed to a fourth victim. A counter
    -- incremented on every claim would instead fire on every rolling deploy and
    -- be ignored within a week.
    claim_count             smallint     NOT NULL DEFAULT 0,

    -- Reserved for a fenced release. The intended use is that markDispatched
    -- carries the token it claimed with, so a pod that paused past its lease
    -- cannot mark work that a successor now owns. Nothing stamps it today —
    -- the claim path guards on claimed_by alone — and it is here now because
    -- adding a column later means an ALTER across every partition on a table
    -- that is written at 74,600 rows/s.
    fencing_token           bigint       NOT NULL DEFAULT 0,

    -- The fully-formed NotificationDispatchEvent, serialised at schedule time.
    -- Carrying the whole event rather than a pointer is what keeps the claim
    -- path free of joins; a lookup per row would put the dispatch fan-out back
    -- on the database that the Redis near-horizon exists to keep out of the loop.
    payload                 jsonb        NOT NULL,

    created_at              timestamptz  NOT NULL DEFAULT now(),

    PRIMARY KEY (due_bucket, id),

    CONSTRAINT sn_state_ck   CHECK (state IN ('READY','CLAIMED','DISPATCHED','CANCELLED','FAILED')),
    CONSTRAINT sn_shard_ck   CHECK (shard BETWEEN 0 AND 255),
    CONSTRAINT sn_channel_ck CHECK (channel IN ('SMS','EMAIL','PUSH')),
    CONSTRAINT sn_class_ck   CHECK (traffic_class IN ('CRITICAL','TRANSACTIONAL','BULK')),
    CONSTRAINT sn_count_ck   CHECK (claim_count >= 0),
    CONSTRAINT sn_fencing_ck CHECK (fencing_token >= 0),

    -- Biconditional, not two nullable columns and a hope. It makes "CLAIMED
    -- with nobody holding it" and "owned but not CLAIMED" both unrepresentable,
    -- which is what catches a buggy reclaim at the INSERT instead of three
    -- hours later as a duplicate send. Every write path in
    -- JdbcScheduledWorkStore already satisfies it; the point is that a future
    -- one cannot quietly stop satisfying it.
    CONSTRAINT sn_owner_ck   CHECK ((state = 'CLAIMED') = (claimed_by IS NOT NULL)),
    -- Same argument for the expiry. A lease with an owner and no deadline never
    -- expires, so LeaseReaper would never see the row and the work would be
    -- lost silently — the one failure mode the lease exists to prevent.
    CONSTRAINT sn_lease_ck   CHECK ((claimed_by IS NULL) = (claim_expires_at IS NULL))
) PARTITION BY RANGE (due_bucket);

COMMENT ON TABLE notif.scheduled_notification IS
    'Durable ledger of future work. Partitioned by due_bucket, which is immutable; '
    'rescheduling is DELETE + INSERT, never an UPDATE of due_at.';
COMMENT ON COLUMN notif.scheduled_notification.claim_count IS
    'Times a claim ended in an expired lease. A poison-pill signal, not a retry tally.';

-- -----------------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------------

-- The due scan, and the reason it is index-only. The predicate is exactly the
-- index's own WHERE clause and `id` rides in the INCLUDE list, so the hydrator's
-- 500-row pass never visits the heap. Partial because READY is a small and
-- shrinking fraction of the table: claimed and dispatched rows leave the index
-- entirely, so it stays roughly the size of the backlog rather than of the
-- retention window.
CREATE INDEX sn_due_shard_ix ON notif.scheduled_notification (due_at, shard)
    INCLUDE (id) WHERE state = 'READY';

-- The lease reaper. One entry per in-flight row, ordered by deadline, so the
-- 30-second sweep is a bounded scan of the front of the index rather than a
-- predicate over the whole table.
CREATE INDEX sn_lease_expiry_ix ON notif.scheduled_notification (claim_expires_at)
    WHERE state = 'CLAIMED';

-- Not in the design's DDL sketch, and required anyway. CLAIM_SQL, MARK_DISPATCHED_SQL,
-- RECLAIM_SQL and ABANDON_SQL all address rows as `id IN (:ids)` with no bucket
-- predicate — they operate on ids handed back by Redis, which does not carry the
-- bucket. The primary key leads with due_bucket, so without this index every one
-- of those statements is a sequential scan of all 17 partitions. Unique rather
-- than plain because it costs nothing extra here and it is the only place the
-- database can say anything at all about id uniqueness: PostgreSQL requires the
-- partition key in every unique constraint, so this enforces uniqueness of
-- (id, due_bucket) and UUIDv7 has to carry the rest.
CREATE UNIQUE INDEX sn_id_uk ON notif.scheduled_notification (id, due_bucket);

-- -----------------------------------------------------------------------------
-- The immutability guard
--
-- A CHECK constraint cannot express this. `due_at AT TIME ZONE 'UTC'` calls
-- timezone(text, timestamptz), which PostgreSQL marks STABLE rather than
-- IMMUTABLE, and CHECK expressions must be immutable. A generated column cannot
-- express it either — a generated column may not be part of a partition key.
-- So it is a trigger, and it is worth the trigger: a row whose due_bucket does
-- not match its due_at is invisible to the due scan forever (the scan bounds
-- both columns, so the bucket predicate prunes away the partition the row is
-- actually in) and nothing anywhere reports it.
--
-- `UPDATE OF due_at, due_bucket` is load-bearing, not decoration. Without the
-- column list this fires on every claim, every markDispatched and every
-- reclaim — three plpgsql invocations per row on the hottest path in the
-- service. With it, the trigger is not even considered unless a statement puts
-- one of those two columns in its SET list, so the claim loop pays nothing.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION notif.scheduled_notification_bucket_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- AT TIME ZONE 'UTC', never a bare ::date. Casting a timestamptz to date
        -- resolves against the *session* TimeZone, so the same row would land in
        -- a different partition depending on which pod inserted it — and would
        -- be right in CI, where everything is UTC, and wrong everywhere else.
        IF NEW.due_bucket IS DISTINCT FROM (NEW.due_at AT TIME ZONE 'UTC')::date THEN
            RAISE EXCEPTION
                'due_bucket % does not match due_at % (UTC day %); the row would be '
                'invisible to the due scan',
                NEW.due_bucket, NEW.due_at, (NEW.due_at AT TIME ZONE 'UTC')::date
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.due_bucket IS DISTINCT FROM OLD.due_bucket
       OR NEW.due_at IS DISTINCT FROM OLD.due_at THEN
        RAISE EXCEPTION
            'scheduled_notification.% is immutable; reschedule with DELETE + INSERT',
            CASE WHEN NEW.due_bucket IS DISTINCT FROM OLD.due_bucket
                 THEN 'due_bucket' ELSE 'due_at' END
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER scheduled_notification_bucket_guard
    BEFORE INSERT OR UPDATE OF due_at, due_bucket ON notif.scheduled_notification
    FOR EACH ROW EXECUTE FUNCTION notif.scheduled_notification_bucket_guard();

-- -----------------------------------------------------------------------------
-- Partitions
-- -----------------------------------------------------------------------------

-- Wider forward window than V1's tables, and that is the point of the table:
-- every other partitioned parent here is written with created_at = now(), so
-- +7 days is generous. This one is written *for a future date* by definition —
-- a campaign scheduled for next month lands in next month's partition the
-- moment it is accepted. Two weeks is what local development and CI need;
-- production runs pg_partman with premake, and a scheduled_at beyond the
-- premake horizon is what the DEFAULT partition below is for.
DO $$
DECLARE
    d date;
BEGIN
    FOR d IN SELECT generate_series(current_date - 2, current_date + 14, interval '1 day')::date LOOP
        PERFORM notif.ensure_daily_partition('scheduled_notification', d);
    END LOOP;
END $$;

-- The safety net, on the same terms as V1's: a scheduled_at outside the
-- provisioned window must not be a hard outage. Alarm when it is non-empty —
-- a row sitting here blocks CREATE TABLE … PARTITION OF for the covering range,
-- because that has to scan the default under an ACCESS EXCLUSIVE lock and fails
-- if it finds a conflicting row.
CREATE TABLE notif.scheduled_notification_default
    PARTITION OF notif.scheduled_notification DEFAULT;

-- Known gap, stated rather than hidden: fillfactor cannot be set on a
-- partitioned parent (it has no storage), and notif.ensure_daily_partition does
-- not set it on the children. Each row here is UPDATEd twice in its life
-- (claim, then dispatch), so a lower fillfactor would keep those HOT and off
-- the four indexes above. Production sets it in the pg_partman template table;
-- local partitions get the 100 default.
