-- =============================================================================
-- V1 — baseline schema
--
-- Design notes are in docs/DATABASE.md. The short version of the four choices
-- that look unusual:
--
--   1. varchar + CHECK instead of native PG enums, because an enum value can
--      never be removed and Flyway migrations that add one then use it in the
--      same transaction fail outright.
--   2. delivery_status IS a table, because it carries rank/is_terminal — data
--      the SQL reads, not a label. Adding a status becomes an INSERT.
--   3. No foreign keys on the high-volume partitioned tables. An FK turns
--      O(1) partition DETACH into a validation scan, and retention depends on
--      that being cheap. Integrity comes from same-transaction writes plus a
--      nightly reconciliation job.
--   4. Composite primary keys leading with the partition column, because
--      PostgreSQL requires every unique constraint to include the partition
--      key. Consequence: the DB cannot enforce global uniqueness of `id`.
--      UUIDv7 makes that safe probabilistically.
-- =============================================================================

SET lock_timeout = '3s';
SET statement_timeout = '0';

CREATE SCHEMA IF NOT EXISTS notif;
SET search_path = notif, public;

-- -----------------------------------------------------------------------------
-- Reference data
-- -----------------------------------------------------------------------------

CREATE TABLE notif.tenant (
    id                bigint       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    public_id         uuid         NOT NULL DEFAULT gen_random_uuid(),
    slug              varchar(64)  NOT NULL,
    display_name      text         NOT NULL,
    status            varchar(16)  NOT NULL DEFAULT 'ACTIVE',
    daily_send_quota  bigint       NOT NULL DEFAULT 1000000,
    rate_limit_rps    integer      NOT NULL DEFAULT 100,
    created_at        timestamptz  NOT NULL DEFAULT now(),
    updated_at        timestamptz  NOT NULL DEFAULT now(),
    row_version       bigint       NOT NULL DEFAULT 0,
    CONSTRAINT tenant_public_id_uk UNIQUE (public_id),
    CONSTRAINT tenant_slug_uk      UNIQUE (slug),
    CONSTRAINT tenant_status_ck    CHECK (status IN ('ACTIVE','SUSPENDED','CLOSED')),
    CONSTRAINT tenant_quota_ck     CHECK (daily_send_quota >= 0),
    CONSTRAINT tenant_rate_ck      CHECK (rate_limit_rps > 0)
);

-- The one lookup table. `rank` is load-bearing: the monotonic guard joins
-- against it, and the ordering encodes business rules that would otherwise be
-- if-statements someone forgets to write.
CREATE TABLE notif.delivery_status (
    code        varchar(24) PRIMARY KEY,
    rank        smallint    NOT NULL,
    is_terminal boolean     NOT NULL,
    is_failure  boolean     NOT NULL,
    is_billable boolean     NOT NULL DEFAULT false,
    description text        NOT NULL,
    CONSTRAINT delivery_status_rank_uk UNIQUE (rank),
    CONSTRAINT delivery_status_rank_ck CHECK (rank BETWEEN 1 AND 999)
);

INSERT INTO notif.delivery_status (code, rank, is_terminal, is_failure, is_billable, description) VALUES
 ('PENDING',     10, false, false, false, 'recipient row created, not yet evaluated'),
 ('SCHEDULED',   20, false, false, false, 'waiting for its scheduled time'),
 ('SUPPRESSED',  25, true,  true,  false, 'blocked by preference, quiet hours, cap or suppression list'),
 -- EXPIRED and CANCELLED rank BELOW queued deliberately: a TTL can only elapse
 -- before sending, and cancelling something already queued must be impossible.
 ('EXPIRED',     27, true,  true,  false, 'ttl elapsed before send'),
 ('CANCELLED',   28, true,  true,  false, 'cancelled by the caller before dispatch'),
 ('QUEUED',      30, false, false, false, 'published to a dispatch topic'),
 ('CLAIMED',     40, false, false, false, 'leased by a worker'),
 ('SENDING',     50, false, false, false, 'in flight to the provider'),
 ('SEND_FAILED', 55, false, true,  false, 'transient failure, retry scheduled'),
 ('FAILED',      58, true,  true,  false, 'retries exhausted or permanent error'),
 ('SENT',        60, false, false, true,  'provider returned 2xx with a message id'),
 ('ACCEPTED',    70, false, false, true,  'provider accepted for delivery'),
 -- NOT terminal: a hard bounce legitimately follows an SMTP 250.
 ('DELIVERED',   80, false, false, true,  'provider confirmed delivery'),
 ('BOUNCED',     85, true,  true,  true,  'hard bounce after acceptance'),
 ('COMPLAINED',  88, true,  true,  true,  'recipient marked it as spam'),
 -- Highest rank so nothing overwrites it; non-terminal so reconciliation can resolve it.
 ('UNKNOWN',     90, false, false, false, 'provider timed out after possibly delivering');

CREATE TABLE notif.provider (
    id                        smallint    GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code                      varchar(32) NOT NULL,
    display_name              text        NOT NULL,
    channel                   varchar(16) NOT NULL,
    vendor                    varchar(32) NOT NULL,
    supports_batching         boolean     NOT NULL DEFAULT false,
    max_batch_size            integer     NOT NULL DEFAULT 1,
    supports_idempotency_key  boolean     NOT NULL DEFAULT false,
    supports_webhook          boolean     NOT NULL DEFAULT true,
    is_active                 boolean     NOT NULL DEFAULT true,
    created_at                timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_code_uk    UNIQUE (code),
    CONSTRAINT provider_channel_ck CHECK (channel IN ('SMS','EMAIL','PUSH'))
);
COMMENT ON COLUMN notif.provider.supports_idempotency_key IS
    'False for every real provider we would plausibly integrate. Drives the UNKNOWN reconciliation path.';

CREATE TABLE notif.retry_policy (
    id                 smallint     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code               varchar(32)  NOT NULL,
    max_attempts       smallint     NOT NULL,
    initial_backoff_ms integer      NOT NULL,
    max_backoff_ms     integer      NOT NULL,
    backoff_multiplier numeric(4,2) NOT NULL DEFAULT 2.00,
    jitter_strategy    varchar(16)  NOT NULL DEFAULT 'FULL',
    total_deadline_ms  integer      NOT NULL DEFAULT 86400000,
    CONSTRAINT retry_policy_code_uk     UNIQUE (code),
    CONSTRAINT retry_policy_attempts_ck CHECK (max_attempts BETWEEN 1 AND 50),
    CONSTRAINT retry_policy_backoff_ck  CHECK (initial_backoff_ms > 0 AND max_backoff_ms >= initial_backoff_ms),
    CONSTRAINT retry_policy_mult_ck     CHECK (backoff_multiplier >= 1.00),
    -- FULL jitter is the default for a reason: with EQUAL or NONE, a mass
    -- failure produces a synchronised retry wave that re-kills the provider.
    CONSTRAINT retry_policy_jitter_ck   CHECK (jitter_strategy IN ('NONE','EQUAL','FULL','DECORRELATED'))
);

INSERT INTO notif.retry_policy (code, max_attempts, initial_backoff_ms, max_backoff_ms, backoff_multiplier, jitter_strategy, total_deadline_ms) VALUES
 ('default',      5, 5000,  3600000, 4.00, 'FULL', 86400000),
 ('critical',     3, 1000,    30000, 3.00, 'FULL',    60000),
 ('bulk',         5, 30000, 3600000, 4.00, 'FULL', 259200000);

CREATE TABLE notif.provider_configuration (
    id                  bigint        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    provider_id         smallint      NOT NULL REFERENCES notif.provider(id) ON DELETE RESTRICT,
    tenant_id           bigint        REFERENCES notif.tenant(id) ON DELETE CASCADE,
    routing_priority    smallint      NOT NULL DEFAULT 100,
    weight              smallint      NOT NULL DEFAULT 100,
    rate_limit_per_sec  integer       NOT NULL DEFAULT 1000,
    daily_cap           bigint,
    credentials_ref     text          NOT NULL,
    endpoint_url        text,
    settings            jsonb         NOT NULL DEFAULT '{}'::jsonb,
    retry_policy_id     smallint      NOT NULL REFERENCES notif.retry_policy(id) ON DELETE RESTRICT,
    unit_cost_micros    bigint        NOT NULL DEFAULT 0,
    timeout_ms          integer       NOT NULL DEFAULT 5000,
    is_active           boolean       NOT NULL DEFAULT true,
    created_at          timestamptz   NOT NULL DEFAULT now(),
    updated_at          timestamptz   NOT NULL DEFAULT now(),
    row_version         bigint        NOT NULL DEFAULT 0,
    -- Structurally prevents anyone pasting an API key into the database.
    CONSTRAINT provider_configuration_secret_ck
        CHECK (credentials_ref ~ '^(arn:aws:secretsmanager:|ssm:|mock:)'),
    CONSTRAINT provider_configuration_weight_ck CHECK (weight BETWEEN 0 AND 1000),
    CONSTRAINT provider_configuration_rate_ck   CHECK (rate_limit_per_sec > 0),
    CONSTRAINT provider_configuration_cost_ck   CHECK (unit_cost_micros >= 0),
    -- timeout must stay well below the Kafka max.poll.interval, or a slow
    -- provider evicts the consumer from its group and triggers a rebalance.
    CONSTRAINT provider_configuration_timeout_ck CHECK (timeout_ms BETWEEN 100 AND 60000)
);
CREATE UNIQUE INDEX provider_configuration_active_uk
    ON notif.provider_configuration (provider_id, coalesce(tenant_id, 0))
    WHERE is_active;

-- -----------------------------------------------------------------------------
-- Idempotency — hourly partitions purely so expiry is DROP TABLE, not DELETE
-- -----------------------------------------------------------------------------

CREATE TABLE notif.idempotency_record (
    tenant_id           bigint       NOT NULL,
    idempotency_key     varchar(128) NOT NULL,
    created_at          timestamptz  NOT NULL DEFAULT now(),
    -- SHA-256 of the canonical request body. Same key + different body must be
    -- a 409, not a silent replay of an unrelated response.
    request_fingerprint bytea        NOT NULL,
    state               varchar(16)  NOT NULL DEFAULT 'IN_PROGRESS',
    request_id          uuid,
    response_status     smallint,
    response_body       jsonb,
    locked_until        timestamptz,
    expires_at          timestamptz  NOT NULL DEFAULT now() + interval '24 hours',
    PRIMARY KEY (created_at, tenant_id, idempotency_key),
    CONSTRAINT idem_state_ck CHECK (state IN ('IN_PROGRESS','COMPLETED','FAILED'))
) PARTITION BY RANGE (created_at);

-- -----------------------------------------------------------------------------
-- Request → notification → recipient
-- -----------------------------------------------------------------------------

CREATE TABLE notif.notification_request (
    id                 uuid          NOT NULL,
    created_at         timestamptz   NOT NULL DEFAULT now(),
    tenant_id          bigint        NOT NULL,
    idempotency_key    varchar(128),
    traffic_class      varchar(16)   NOT NULL,
    channels           varchar(16)[] NOT NULL,
    schedule_type      varchar(16)   NOT NULL,
    scheduled_at       timestamptz,
    recipient_source   varchar(16)   NOT NULL,
    recipient_ref      text,
    recipient_count    integer       NOT NULL DEFAULT 0,
    template_code      varchar(96),
    template_locale    varchar(16),
    payload            jsonb         NOT NULL DEFAULT '{}'::jsonb,
    status             varchar(16)   NOT NULL DEFAULT 'ACCEPTED',
    expires_at         timestamptz   NOT NULL,
    created_by         text          NOT NULL DEFAULT 'system',
    trace_id           varchar(64),
    PRIMARY KEY (created_at, id),
    CONSTRAINT nr_class_ck    CHECK (traffic_class IN ('CRITICAL','TRANSACTIONAL','BULK')),
    CONSTRAINT nr_schedule_ck CHECK (schedule_type IN ('IMMEDIATE','SCHEDULED','RECURRING')),
    -- A SCHEDULED request without a time is not schedulable. Enforce it here so
    -- no code path can create one.
    CONSTRAINT nr_sched_at_ck CHECK (schedule_type <> 'SCHEDULED' OR scheduled_at IS NOT NULL),
    CONSTRAINT nr_source_ck   CHECK (recipient_source IN ('INLINE','USER_IDS','S3_MANIFEST','AUDIENCE_REF')),
    CONSTRAINT nr_status_ck   CHECK (status IN ('ACCEPTED','EXPANDING','EXPANDED','CANCELLED','FAILED')),
    CONSTRAINT nr_channels_ck CHECK (cardinality(channels) BETWEEN 1 AND 3),
    CONSTRAINT nr_count_ck    CHECK (recipient_count >= 0)
) PARTITION BY RANGE (created_at);

CREATE TABLE notif.notification (
    id                  uuid         NOT NULL,
    created_at          timestamptz  NOT NULL DEFAULT now(),
    request_id          uuid         NOT NULL,
    tenant_id           bigint       NOT NULL,
    channel             varchar(16)  NOT NULL,
    traffic_class       varchar(16)  NOT NULL,
    priority            smallint     NOT NULL DEFAULT 5,
    template_code       varchar(96),
    status              varchar(24)  NOT NULL DEFAULT 'PENDING',
    -- Denormalised from delivery_status so the monotonic guard stays a single
    -- index probe instead of a join. A nightly job checks it hasn't drifted.
    status_rank         smallint     NOT NULL DEFAULT 10,
    status_at           timestamptz  NOT NULL DEFAULT now(),
    scheduled_at        timestamptz,
    dispatched_at       timestamptz,
    completed_at        timestamptz,
    expires_at          timestamptz  NOT NULL,
    total_recipients    integer      NOT NULL DEFAULT 0,
    delivered_count     integer      NOT NULL DEFAULT 0,
    failed_count        integer      NOT NULL DEFAULT 0,
    suppressed_count    integer      NOT NULL DEFAULT 0,
    trace_id            varchar(64),
    updated_at          timestamptz  NOT NULL DEFAULT now(),
    row_version         bigint       NOT NULL DEFAULT 0,
    PRIMARY KEY (created_at, id),
    CONSTRAINT n_channel_ck  CHECK (channel IN ('SMS','EMAIL','PUSH')),
    CONSTRAINT n_class_ck    CHECK (traffic_class IN ('CRITICAL','TRANSACTIONAL','BULK')),
    CONSTRAINT n_priority_ck CHECK (priority BETWEEN 0 AND 9),
    CONSTRAINT n_rank_ck     CHECK (status_rank BETWEEN 1 AND 999),
    CONSTRAINT n_counts_ck   CHECK (delivered_count >= 0 AND failed_count >= 0 AND suppressed_count >= 0)
    -- Deliberately NO check on `status`: validated by the app against
    -- delivery_status. A CHECK would make every new status a DDL event across
    -- every partition, for a column with 16 values.
) PARTITION BY RANGE (created_at);

-- Needed because the PK leads with created_at; this serves lookup by id.
CREATE UNIQUE INDEX n_id_uk ON notif.notification (id, created_at);
CREATE INDEX n_request_ix ON notif.notification (request_id, created_at);

CREATE TABLE notif.notification_recipient (
    id                    uuid        NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    notification_id       uuid        NOT NULL,
    notification_created_at timestamptz NOT NULL,
    tenant_id             bigint      NOT NULL,
    user_ref              varchar(128),
    channel               varchar(16) NOT NULL,
    -- AES-GCM under a per-user DEK. Erasure destroys the key, not the rows.
    address_cipher        bytea       NOT NULL,
    -- Tenant-keyed HMAC. Lets us dedup and suppression-match without decrypting.
    address_hash          bytea       NOT NULL,
    -- Safe to log: 'g***@example.com'.
    address_hint          varchar(64),
    status                varchar(24) NOT NULL DEFAULT 'PENDING',
    status_rank           smallint    NOT NULL DEFAULT 10,
    attempt_count         smallint    NOT NULL DEFAULT 0,
    next_attempt_at       timestamptz,
    current_provider_id   smallint,
    provider_message_id   varchar(128),
    dedup_key             varchar(128),
    suppression_reason    varchar(32),
    failure_type          varchar(32),
    failure_detail        text,
    sent_at               timestamptz,
    delivered_at          timestamptz,
    -- Guards against applying an event older than the one already recorded.
    last_status_at        timestamptz NOT NULL DEFAULT now(),
    row_version           bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (created_at, id),
    CONSTRAINT nrec_channel_ck CHECK (channel IN ('SMS','EMAIL','PUSH')),
    CONSTRAINT nrec_rank_ck    CHECK (status_rank BETWEEN 1 AND 999),
    CONSTRAINT nrec_attempt_ck CHECK (attempt_count >= 0 AND attempt_count <= 64)
) PARTITION BY RANGE (created_at);

CREATE INDEX nrec_notification_ix ON notif.notification_recipient (notification_id, created_at);
CREATE INDEX nrec_provider_msg_ix ON notif.notification_recipient (provider_message_id)
    WHERE provider_message_id IS NOT NULL;
-- Partial: covers ~5% of rows at a fraction of the per-row cost of a full index.
CREATE INDEX nrec_retry_ix ON notif.notification_recipient (next_attempt_at)
    WHERE status IN ('SEND_FAILED','QUEUED') AND next_attempt_at IS NOT NULL;
CREATE INDEX nrec_inflight_ix ON notif.notification_recipient (last_status_at)
    WHERE status IN ('CLAIMED','SENDING');

-- -----------------------------------------------------------------------------
-- Delivery attempts — append-only, never updated
-- -----------------------------------------------------------------------------

CREATE TABLE notif.delivery_attempt (
    id                   bigint      GENERATED ALWAYS AS IDENTITY,
    attempted_at         timestamptz NOT NULL DEFAULT now(),
    recipient_id         uuid        NOT NULL,
    tenant_id            bigint      NOT NULL,
    attempt_no           smallint    NOT NULL,
    provider_id          smallint    NOT NULL,
    -- The token we send to the provider. Unique so a redelivered message cannot
    -- create a second attempt row for the same logical send.
    idempotency_token    varchar(128) NOT NULL,
    state                varchar(16) NOT NULL DEFAULT 'PENDING',
    -- Written BEFORE the network call. A crash then leaves a visible PENDING
    -- row instead of an invisible gap, which is what makes recovery possible.
    request_started_at   timestamptz NOT NULL DEFAULT now(),
    response_at          timestamptz,
    latency_ms           integer,
    http_status          smallint,
    provider_message_id  varchar(128),
    failure_type         varchar(32),
    error_code           varchar(64),
    error_detail         text,
    cost_micros          bigint,
    PRIMARY KEY (attempted_at, id),
    CONSTRAINT da_state_ck   CHECK (state IN ('PENDING','SUCCEEDED','FAILED','UNKNOWN')),
    CONSTRAINT da_attempt_ck CHECK (attempt_no BETWEEN 1 AND 64),
    CONSTRAINT da_latency_ck CHECK (latency_ms IS NULL OR latency_ms >= 0)
) PARTITION BY RANGE (attempted_at);

CREATE INDEX da_recipient_ix ON notif.delivery_attempt (recipient_id, attempted_at DESC);
CREATE UNIQUE INDEX da_token_uk ON notif.delivery_attempt (idempotency_token, attempted_at);
CREATE INDEX da_provider_fail_ix ON notif.delivery_attempt (provider_id, attempted_at DESC)
    WHERE state <> 'SUCCEEDED';

-- -----------------------------------------------------------------------------
-- Transactional outbox — the reason nothing is ever silently lost
-- -----------------------------------------------------------------------------

CREATE TABLE notif.outbox_message (
    id             bigint       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at     timestamptz  NOT NULL DEFAULT now(),
    aggregate_type varchar(32)  NOT NULL,
    aggregate_id   text         NOT NULL,
    event_type     varchar(48)  NOT NULL,
    topic          varchar(128) NOT NULL,
    partition_key  varchar(160) NOT NULL,
    headers        jsonb        NOT NULL DEFAULT '{}'::jsonb,
    payload        jsonb        NOT NULL,
    published_at   timestamptz,
    attempts       smallint     NOT NULL DEFAULT 0,
    CONSTRAINT outbox_attempts_ck CHECK (attempts >= 0)
);
-- Partial index stays a few pages regardless of throughput, because published
-- rows leave the index. Rows are DELETEd after publish, never UPDATEd, so the
-- table never grows.
CREATE INDEX outbox_unpublished_ix ON notif.outbox_message (id)
    WHERE published_at IS NULL;
ALTER TABLE notif.outbox_message SET (fillfactor = 70, autovacuum_vacuum_threshold = 1000);

-- -----------------------------------------------------------------------------
-- Append-only event log
-- -----------------------------------------------------------------------------

CREATE TABLE notif.notification_event (
    id                uuid        NOT NULL,
    occurred_at       timestamptz NOT NULL,
    recorded_at       timestamptz NOT NULL DEFAULT now(),
    tenant_id         bigint      NOT NULL,
    notification_id   uuid,
    recipient_id      uuid,
    event_type        varchar(32) NOT NULL,
    source            varchar(16) NOT NULL,
    provider_id       smallint,
    from_status       varchar(24),
    to_status         varchar(24),
    -- false = the monotonic guard rejected it. These rows are the most valuable
    -- debugging artefact in the system: the webhooks you correctly ignored.
    applied           boolean     NOT NULL DEFAULT true,
    dedup_hash        bytea       NOT NULL,
    attributes        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (occurred_at, id),
    CONSTRAINT ne_source_ck CHECK (source IN ('API','WORKER','PROVIDER','SCHEDULER','ADMIN','SYSTEM'))
) PARTITION BY RANGE (occurred_at);

CREATE UNIQUE INDEX ne_dedup_uk ON notif.notification_event (dedup_hash, occurred_at);
CREATE INDEX ne_notification_ix ON notif.notification_event (notification_id, occurred_at DESC)
    WHERE notification_id IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Dead letter queue — not partitioned. If it needs partitioning, fix the bug.
-- -----------------------------------------------------------------------------

CREATE TABLE notif.dead_letter_message (
    id               bigint       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_seen_at    timestamptz  NOT NULL DEFAULT now(),
    last_seen_at     timestamptz  NOT NULL DEFAULT now(),
    tenant_id        bigint,
    notification_id  uuid,
    source_topic     varchar(128) NOT NULL,
    source_partition integer,
    source_offset    bigint,
    message_key      varchar(256),
    payload          jsonb        NOT NULL,
    error_class      varchar(96)  NOT NULL,
    error_message    text         NOT NULL,
    stack_digest     bytea,
    occurrence_count integer      NOT NULL DEFAULT 1,
    triage_state     varchar(16)  NOT NULL DEFAULT 'NEW',
    replayed_at      timestamptz,
    resolved_at      timestamptz,
    CONSTRAINT dlm_triage_ck CHECK (triage_state IN ('NEW','TRIAGED','REPLAYING','RESOLVED','DISCARDED')),
    CONSTRAINT dlm_count_ck  CHECK (occurrence_count > 0),
    -- With ON CONFLICT DO UPDATE SET occurrence_count = +1 this turns a
    -- four-million-row poison-pill storm into roughly thirty rows.
    CONSTRAINT dlm_dedup_uk  UNIQUE (source_topic, message_key, stack_digest)
);

-- -----------------------------------------------------------------------------
-- Partition management
--
-- Production uses pg_partman + pg_cron (see docs/DATABASE.md). This function
-- exists so local development and tests work against the stock postgres image,
-- and so the CI gate can create partitions deterministically.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION notif.ensure_daily_partition(p_table text, p_day date)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    part_name text := format('%s_%s', p_table, to_char(p_day, 'YYYYMMDD'));
BEGIN
    IF to_regclass(format('notif.%I', part_name)) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE notif.%I PARTITION OF notif.%I FOR VALUES FROM (%L) TO (%L)',
            part_name, p_table, p_day, p_day + 1);
    END IF;
END $$;

CREATE OR REPLACE FUNCTION notif.ensure_hourly_partition(p_table text, p_hour timestamptz)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    h         timestamptz := date_trunc('hour', p_hour);
    part_name text := format('%s_%s', p_table, to_char(h AT TIME ZONE 'UTC', 'YYYYMMDDHH24'));
BEGIN
    IF to_regclass(format('notif.%I', part_name)) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE notif.%I PARTITION OF notif.%I FOR VALUES FROM (%L) TO (%L)',
            part_name, p_table, h, h + interval '1 hour');
    END IF;
END $$;

-- Provision a window around today so the system works immediately after
-- migration. Production premakes 14 days via pg_partman.
DO $$
DECLARE
    d date;
    h timestamptz;
BEGIN
    FOR d IN SELECT generate_series(current_date - 2, current_date + 7, interval '1 day')::date LOOP
        PERFORM notif.ensure_daily_partition('notification_request',   d);
        PERFORM notif.ensure_daily_partition('notification',           d);
        PERFORM notif.ensure_daily_partition('notification_recipient', d);
        PERFORM notif.ensure_daily_partition('delivery_attempt',       d);
        PERFORM notif.ensure_daily_partition('notification_event',     d);
    END LOOP;

    FOR h IN SELECT generate_series(date_trunc('hour', now()) - interval '2 hours',
                                    date_trunc('hour', now()) + interval '48 hours',
                                    interval '1 hour') LOOP
        PERFORM notif.ensure_hourly_partition('idempotency_record', h);
    END LOOP;
END $$;

-- A DEFAULT partition prevents an insert outside the provisioned window from
-- being a hard outage. It is a safety net, not a destination: alarm when it is
-- non-empty, because a row landing here blocks creation of the real partition.
CREATE TABLE notif.notification_request_default   PARTITION OF notif.notification_request   DEFAULT;
CREATE TABLE notif.notification_default           PARTITION OF notif.notification           DEFAULT;
CREATE TABLE notif.notification_recipient_default PARTITION OF notif.notification_recipient DEFAULT;
CREATE TABLE notif.delivery_attempt_default       PARTITION OF notif.delivery_attempt       DEFAULT;
CREATE TABLE notif.notification_event_default     PARTITION OF notif.notification_event     DEFAULT;
CREATE TABLE notif.idempotency_record_default     PARTITION OF notif.idempotency_record     DEFAULT;
