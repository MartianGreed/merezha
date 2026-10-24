//! Schema `merezha_jobs` (AP §B.2), ledger key `jobs`.
use merezha_postgres::Migration;

/// Outbox, subscriptions, jobs, attempts, schedules and admin actions, with
/// the R3 `contains_secret` invariants and the R10 trace columns. Leasing
/// (AP-B2) and cron (AP-B3) use the tables created here.
const INITIAL: &str = r#"
CREATE SCHEMA merezha_jobs;

CREATE TABLE merezha_jobs.outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source text NOT NULL CHECK (source ~ '^[a-z][a-z0-9_]{0,31}$'),
    dedupe_key text NOT NULL CHECK (octet_length(dedupe_key) BETWEEN 1 AND 160),
    record_kind text NOT NULL CHECK (record_kind IN ('event', 'job')),
    topic text NULL CHECK (topic ~ '^[a-z][a-z0-9_]{0,31}\.[a-z][a-z0-9_]{0,63}$'),
    queue text NULL,
    job_kind text NULL,
    priority smallint NOT NULL DEFAULT 0,
    run_at timestamptz NULL,
    payload json NULL,
    contains_secret boolean NOT NULL DEFAULT false,
    correlation_id text NOT NULL,
    causation_id text NULL,
    tenant text NULL,
    trace_parent text NULL CHECK (octet_length(trace_parent) = 55),
    trace_state text NULL CHECK (octet_length(trace_state) <= 512),
    created_at timestamptz NOT NULL DEFAULT now(),
    relayed_at timestamptz NULL,
    quarantined_at timestamptz NULL,
    quarantine_code text NULL,
    UNIQUE (source, dedupe_key),
    CHECK ((quarantined_at IS NULL) = (quarantine_code IS NULL)),
    CHECK (relayed_at IS NULL OR quarantined_at IS NULL),
    -- R3: a flagged record loses its payload once relayed or quarantined as
    -- unroutable; a record isolated by its own data error keeps it, so a
    -- requeue can relay it.
    CHECK ((payload IS NULL) = (contains_secret
        AND (relayed_at IS NOT NULL OR quarantine_code IS NOT DISTINCT FROM 'unroutable'))),
    CHECK ((record_kind = 'event' AND topic IS NOT NULL AND queue IS NULL AND job_kind IS NULL)
        OR (record_kind = 'job' AND topic IS NULL AND queue IS NOT NULL AND job_kind IS NOT NULL)));
CREATE INDEX outbox_pending ON merezha_jobs.outbox (id)
    WHERE relayed_at IS NULL AND quarantined_at IS NULL;
CREATE INDEX outbox_relayed ON merezha_jobs.outbox (relayed_at) WHERE relayed_at IS NOT NULL;

CREATE TABLE merezha_jobs.subscriptions (
    topic text NOT NULL,
    job_kind text NOT NULL,
    queue text NOT NULL,
    source text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    -- Refreshed by every instance that declares it, at start and on each
    -- heartbeat; a row nobody declared for retire_after is retired.
    last_declared_at timestamptz NOT NULL DEFAULT now(),
    -- Retirement time; the relay routes only to rows where it is NULL.
    deactivated_at timestamptz NULL,
    PRIMARY KEY (topic, job_kind));

CREATE TABLE merezha_jobs.jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue text NOT NULL CHECK (queue ~ '^[a-z][a-z0-9_.-]{0,63}$'),
    kind text NOT NULL CHECK (octet_length(kind) BETWEEN 1 AND 128),
    idempotency_key text NOT NULL CHECK (octet_length(idempotency_key) BETWEEN 1 AND 200),
    payload json NULL,
    payload_digest bytea NOT NULL CHECK (octet_length(payload_digest) = 32),
    contains_secret boolean NOT NULL DEFAULT false,
    state text NOT NULL DEFAULT 'available'
        CHECK (state IN ('available', 'running', 'succeeded', 'dead', 'cancelled')),
    priority smallint NOT NULL DEFAULT 0,
    run_at timestamptz NOT NULL DEFAULT now(),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    lease_token uuid NULL,
    lease_owner text NULL,
    lease_expires_at timestamptz NULL,
    last_error_code text NULL,
    last_error_at timestamptz NULL,
    last_error_class text NULL CHECK (last_error_class IN ('permanent', 'transient', 'conflict')),
    outbox_id bigint NULL,
    schedule_name text NULL,
    scheduled_for timestamptz NULL,
    correlation_id text NOT NULL,
    causation_id text NULL,
    tenant text NULL,
    trace_parent text NULL CHECK (octet_length(trace_parent) = 55),
    trace_state text NULL CHECK (octet_length(trace_state) <= 512),
    replays integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz NULL,
    UNIQUE (queue, idempotency_key),
    CHECK ((state = 'running') = (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CHECK ((payload IS NULL) = (contains_secret AND state IN ('succeeded', 'dead', 'cancelled'))),
    CHECK (attempts <= max_attempts OR state IN ('dead', 'cancelled')));
CREATE INDEX jobs_dispatch ON merezha_jobs.jobs (queue, priority DESC, run_at, id) WHERE state = 'available';
CREATE INDEX jobs_leases ON merezha_jobs.jobs (lease_expires_at) WHERE state = 'running';
CREATE INDEX jobs_running ON merezha_jobs.jobs (queue) WHERE state = 'running';
CREATE INDEX jobs_dead ON merezha_jobs.jobs (queue, finished_at, id) WHERE state = 'dead';
CREATE INDEX jobs_finished ON merezha_jobs.jobs (finished_at) WHERE state IN ('succeeded', 'cancelled');

CREATE TABLE merezha_jobs.attempts (
    lease_token uuid PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES merezha_jobs.jobs (id) ON DELETE CASCADE,
    attempt integer NOT NULL,
    lease_owner text NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NULL,
    outcome text NULL CHECK (outcome IN ('succeeded', 'failed', 'lease_expired', 'released')),
    error_code text NULL,
    error_class text NULL);
CREATE INDEX attempts_job ON merezha_jobs.attempts (job_id, started_at);

CREATE TABLE merezha_jobs.schedules (
    name text PRIMARY KEY CHECK (name ~ '^[a-z][a-z0-9_.-]{0,63}$'),
    source text NOT NULL,
    cron text NOT NULL,
    timezone text NOT NULL,
    queue text NOT NULL,
    kind text NOT NULL,
    payload json NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    next_run_at timestamptz NOT NULL,
    last_run_at timestamptz NULL,
    last_job_id bigint NULL,
    definition bytea NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE merezha_jobs.admin_actions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    at timestamptz NOT NULL DEFAULT now(),
    principal text NULL,
    action text NOT NULL,
    target text NOT NULL,
    count integer NOT NULL);
"#;

/// AP-B2. Quarantined outbox rows are listed oldest first by the admin
/// API and purged by retention, outside the pending index; the oldest due
/// job (readiness lag) is read from `jobs_due`. A reissue claim marks a
/// flagged dead job closed for its hook (`cancelled`), so the reaper can
/// return a claim cut by a crash to `dead`.
const WORKERS: &str = "
CREATE INDEX outbox_quarantined ON merezha_jobs.outbox (quarantined_at, id)
    WHERE quarantined_at IS NOT NULL;
CREATE INDEX jobs_due ON merezha_jobs.jobs (run_at) WHERE state = 'available';
ALTER TABLE merezha_jobs.jobs
    ADD COLUMN reissue_claimed_at timestamptz NULL,
    ADD CONSTRAINT jobs_reissue_claim CHECK (reissue_claimed_at IS NULL OR state = 'cancelled');
CREATE INDEX jobs_reissuing ON merezha_jobs.jobs (reissue_claimed_at)
    WHERE reissue_claimed_at IS NOT NULL;
";

/// AP-B3. Schedules follow the subscriptions' rolling-deploy rule: every
/// declaring instance refreshes `last_declared_at`, and a schedule nobody
/// declared for `jobs.subscriptions.retire_after` gets `retired_at` (kept,
/// never fired) instead of being disabled by one instance's config.
/// `enabled` stays the admin pause flag. The tick reads due rows through
/// `schedules_due`.
const CRON: &str = "
ALTER TABLE merezha_jobs.schedules
    ADD COLUMN last_declared_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN retired_at timestamptz NULL;
CREATE INDEX schedules_due ON merezha_jobs.schedules (next_run_at)
    WHERE enabled AND retired_at IS NULL;
";

pub(crate) const MIGRATIONS: [Migration; 3] = [
    Migration::new(1, "initial", INITIAL),
    Migration::new(2, "workers", WORKERS),
    Migration::new(3, "cron", CRON),
];
