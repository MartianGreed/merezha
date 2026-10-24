-- Merezha application core on PostgreSQL (AP §A.4). Forward-only.
-- `json`, not `jsonb`, keeps every guest document's exact text.
CREATE SCHEMA merezha_app;

CREATE TABLE merezha_app.manifest (
    id smallint PRIMARY KEY CHECK (id = 1),
    application text NOT NULL CHECK (application ~ '^[A-Za-z0-9._:/-]{1,128}$'),
    format smallint NOT NULL CHECK (format = 1),
    created_at timestamptz NOT NULL DEFAULT now());

-- The single append serialization point; position = highest committed position.
CREATE TABLE merezha_app.head (
    id smallint PRIMARY KEY CHECK (id = 1),
    position bigint NOT NULL CHECK (position >= 0));
INSERT INTO merezha_app.head (id, position) VALUES (1, 0);

CREATE TABLE merezha_app.streams (
    stream text PRIMARY KEY CHECK (octet_length(stream) BETWEEN 1 AND 128),
    tenant text NULL CHECK (tenant ~ '^[A-Za-z0-9_-]{1,40}$'),
    version bigint NOT NULL CHECK (version > 0),
    first_position bigint NOT NULL CHECK (first_position > 0),
    last_position bigint NOT NULL CHECK (last_position >= first_position),
    CHECK (tenant IS NULL OR starts_with(stream, tenant || ':')));

CREATE TABLE merezha_app.events (
    position bigint PRIMARY KEY CHECK (position > 0),
    stream text NOT NULL REFERENCES merezha_app.streams (stream),
    tenant text NULL,
    version bigint NOT NULL CHECK (version > 0),
    tag text NOT NULL CHECK (octet_length(tag) BETWEEN 1 AND 128),
    schema_version integer NOT NULL CHECK (schema_version BETWEEN 1 AND 65535),
    payload json NOT NULL,
    metadata json NOT NULL,
    accepted_unix_ms bigint NOT NULL CHECK (accepted_unix_ms >= 0),
    -- The command span's W3C trace context; host-only, never sent to guests (R10).
    trace_parent text NULL CHECK (octet_length(trace_parent) = 55),
    trace_state text NULL CHECK (octet_length(trace_state) <= 512),
    UNIQUE (stream, version));

-- '' stands for "absent": identities are never empty, so it cannot collide.
CREATE TABLE merezha_app.receipts (
    tag text NOT NULL,
    tenant text NOT NULL DEFAULT '',
    actor text NOT NULL DEFAULT '',
    key text NOT NULL,
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    stream text NOT NULL,
    version bigint NOT NULL CHECK (version >= 0),
    position bigint NOT NULL CHECK (position >= 0),
    events integer NOT NULL CHECK (events BETWEEN 0 AND 64),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tag, tenant, actor, key));

CREATE TABLE merezha_app.projections (
    name text NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    partitioned boolean NOT NULL,
    generation integer NOT NULL DEFAULT 1 CHECK (generation > 0),
    state json NULL,
    checkpoint bigint NOT NULL DEFAULT 0 CHECK (checkpoint >= 0),
    row_revision bigint NOT NULL DEFAULT 0 CHECK (row_revision >= 0),
    warm_position bigint NOT NULL CHECK (warm_position >= 0),
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'failing')),
    failure_code text NULL,
    failure_class text NULL,
    failures integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, revision),
    CHECK (partitioned = (state IS NULL)));

-- At most one rebuild per projection revision (AP-A3); deleted by the swap.
CREATE TABLE merezha_app.projection_rebuilds (
    name text NOT NULL,
    revision integer NOT NULL,
    generation integer NOT NULL,
    state json NULL,
    checkpoint bigint NOT NULL DEFAULT 0,
    target bigint NOT NULL,
    row_revision bigint NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'failed')),
    failure_code text NULL,
    requested_by text NULL,
    requested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, revision),
    FOREIGN KEY (name, revision) REFERENCES merezha_app.projections ON DELETE CASCADE);

CREATE FUNCTION merezha_app.lookup_receipt(p_tag text, p_tenant text, p_actor text, p_key text,
                                           p_digest bytea)
RETURNS TABLE (outcome text, r_version bigint, r_position bigint, r_events integer)
LANGUAGE sql SET search_path = '' AS $$
    SELECT CASE WHEN r.fingerprint = p_digest THEN 'replayed' ELSE 'mismatch' END,
           r.version, r.position, r.events
      FROM merezha_app.receipts r
     WHERE r.tag = p_tag AND r.tenant = p_tenant AND r.actor = p_actor AND r.key = p_key $$;

-- One command commit. Called as a single statement outside an explicit
-- transaction, so lock, inserts and commit take one round trip.
CREATE FUNCTION merezha_app.append(
    p_stream text, p_tenant text,
    p_expected bigint,
    p_tags text[], p_schema_versions integer[], p_payloads json[],
    p_metadata json, p_accepted_unix_ms bigint,
    p_trace_parent text, p_trace_state text,
    p_key_tag text, p_key_tenant text, p_key_actor text, p_key text,
    p_fingerprint text,
    p_lock_timeout_ms integer
) RETURNS TABLE (outcome text, r_version bigint, r_position bigint, r_events integer)
LANGUAGE plpgsql SET search_path = '' AS $$
DECLARE
    v_n integer := coalesce(cardinality(p_tags), 0);
    v_digest bytea;
    v_base bigint;
    v_current bigint;
BEGIN
    IF p_key IS NOT NULL THEN
        v_digest := sha256(convert_to(p_fingerprint, 'UTF8'));
        RETURN QUERY SELECT * FROM merezha_app.lookup_receipt(p_key_tag, p_key_tenant, p_key_actor, p_key, v_digest);
        IF FOUND THEN RETURN; END IF;
    END IF;
    -- Fail fast without the lock when the precondition is already stale.
    SELECT coalesce(max(s.version), 0) INTO v_current FROM merezha_app.streams s WHERE s.stream = p_stream;
    IF v_current <> p_expected THEN
        RETURN QUERY SELECT 'conflict'::text, v_current, 0::bigint, 0; RETURN;
    END IF;

    IF v_n > 0 THEN
        PERFORM set_config('lock_timeout', p_lock_timeout_ms::text || 'ms', true);
        -- Serialization point, held until commit. Every later statement takes a fresh
        -- READ COMMITTED snapshot and so sees every earlier committed append.
        SELECT h.position INTO v_base FROM merezha_app.head h WHERE h.id = 1 FOR UPDATE;
        IF p_key IS NOT NULL THEN
            RETURN QUERY SELECT * FROM merezha_app.lookup_receipt(p_key_tag, p_key_tenant, p_key_actor, p_key, v_digest);
            IF FOUND THEN RETURN; END IF;
        END IF;
        SELECT coalesce(max(s.version), 0) INTO v_current FROM merezha_app.streams s WHERE s.stream = p_stream;
        IF v_current <> p_expected THEN
            RETURN QUERY SELECT 'conflict'::text, v_current, v_base, 0; RETURN;
        END IF;
        UPDATE merezha_app.head SET position = v_base + v_n WHERE id = 1;
        INSERT INTO merezha_app.streams (stream, tenant, version, first_position, last_position)
             VALUES (p_stream, p_tenant, p_expected + v_n, v_base + 1, v_base + v_n)
        ON CONFLICT (stream) DO UPDATE SET version = EXCLUDED.version, last_position = EXCLUDED.last_position;
        INSERT INTO merezha_app.events (position, stream, tenant, version, tag, schema_version, payload,
                                        metadata, accepted_unix_ms, trace_parent, trace_state)
        SELECT v_base + i, p_stream, p_tenant, p_expected + i, p_tags[i], p_schema_versions[i], p_payloads[i],
               p_metadata, p_accepted_unix_ms, p_trace_parent, p_trace_state
          FROM generate_subscripts(p_tags, 1) AS i;
        PERFORM pg_notify('merezha_app_events', (v_base + v_n)::text);
    ELSE
        SELECT h.position INTO v_base FROM merezha_app.head h WHERE h.id = 1;
    END IF;

    IF p_key IS NOT NULL THEN
        INSERT INTO merezha_app.receipts (tag, tenant, actor, key, fingerprint, stream, version, position, events)
             VALUES (p_key_tag, p_key_tenant, p_key_actor, p_key, v_digest, p_stream,
                     p_expected + v_n, v_base + v_n, v_n)
        ON CONFLICT DO NOTHING;
        IF NOT FOUND THEN
            IF v_n > 0 THEN   -- a zero-event command with the same key won; undo our events
                RAISE EXCEPTION USING ERRCODE = 'MZ001', MESSAGE = 'idempotency_in_flight';
            END IF;
            RETURN QUERY SELECT * FROM merezha_app.lookup_receipt(p_key_tag, p_key_tenant, p_key_actor, p_key, v_digest);
            RETURN;
        END IF;
    END IF;
    RETURN QUERY SELECT 'appended'::text, p_expected + v_n, v_base + v_n, v_n;
END $$;
