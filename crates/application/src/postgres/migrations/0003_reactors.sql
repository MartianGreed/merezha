-- Reactors (AP §A.4, §C.5, slice C3). Forward-only.
-- One row per reactor name: the checkpoint its effects committed through,
-- with row-revision CAS, and its failure bookkeeping. A reactor removed
-- from configuration keeps its row; re-adding it resumes from it.
CREATE TABLE merezha_app.reactors (
    name text PRIMARY KEY CHECK (name ~ '^[A-Za-z0-9._-]{1,48}$'),
    checkpoint bigint NOT NULL CHECK (checkpoint >= 0),
    start_position bigint NOT NULL CHECK (start_position >= 0),
    row_revision bigint NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'halted')),
    halted_position bigint NULL,
    failure_code text NULL, failure_class text NULL, failures integer NOT NULL DEFAULT 0,
    retry_at timestamptz NULL, updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'halted') = (halted_position IS NOT NULL)));

-- Events a reactor advanced past without reacting: an operator's skip of
-- the stuck event or to the head, or `on_error = "skip"`.
CREATE TABLE merezha_app.reactor_skips (
    name text NOT NULL REFERENCES merezha_app.reactors (name),
    position bigint NOT NULL,
    reason text NOT NULL CHECK (reason IN ('admin', 'on_error_skip', 'skip_to_head')),
    failure_code text NULL, principal text NULL, skipped_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, position));

-- Well-formed effects that failed validation (R4); written in the
-- checkpoint transaction, so a rejection and the checkpoint past it commit
-- together.
CREATE TABLE merezha_app.reactor_rejections (
    name text NOT NULL REFERENCES merezha_app.reactors (name),
    position bigint NOT NULL, index smallint NOT NULL CHECK (index BETWEEN 0 AND 7),
    effect text NOT NULL CHECK (effect IN ('notify', 'webhook', 'command')),
    code text NOT NULL, rejected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, position, index));
