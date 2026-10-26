-- merezha_notify migration 5 (NT-N7): outgoing webhooks (NT §2.4, §5.5).
--
-- Tenant ids are text, as migration 2 made them everywhere else; a NULL
-- tenant is the application-level registry (NT6). Registry reads and the
-- fan-out match `COALESCE(tenant_id, '')`, `''` naming the application
-- registry (a tenant id is never empty), so one statement and one
-- expression index serve both registries.
--
-- The signing secret is stored only as AES-256-GCM ciphertext and tag
-- under k_wh, with a random 96-bit nonce and the endpoint id as associated
-- data. `previous_secret_*` is the secret before the last rotation, signed
-- with as well until `previous_expires_at`. `key_version` is 0: written
-- under the `secret_key` of its time (the `retention` task re-encrypts the
-- rows that only `secret_key_previous` opens).
CREATE TABLE merezha_notify.webhook_endpoints (
  id uuid PRIMARY KEY,
  tenant_id text CHECK (length(tenant_id) BETWEEN 1 AND 128),
  url text NOT NULL CHECK (length(url) <= 2048),
  description text CHECK (length(description) <= 200),
  event_types text[] NOT NULL DEFAULT '{}',    -- empty = all
  secret_ct bytea NOT NULL CHECK (octet_length(secret_ct) <= 128),
  secret_nonce bytea NOT NULL CHECK (octet_length(secret_nonce) = 12),
  previous_secret_ct bytea CHECK (octet_length(previous_secret_ct) <= 128),
  previous_secret_nonce bytea CHECK (octet_length(previous_secret_nonce) = 12),
  previous_expires_at timestamptz,
  key_version smallint NOT NULL,
  status text NOT NULL CHECK (status IN ('active','disabled')),
  disabled_reason text CHECK (disabled_reason IN ('manual','gone','failing')),
  disabled_at timestamptz,
  failing_since timestamptz,
  last_success_at timestamptz,
  created_by text NOT NULL CHECK (length(created_by) <= 200),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  revision bigint NOT NULL DEFAULT 1,
  CHECK ((previous_secret_ct IS NULL) = (previous_secret_nonce IS NULL))
);
-- Registry listing, counting and the fan-out, for both registries.
CREATE INDEX webhook_endpoints_registry_idx
  ON merezha_notify.webhook_endpoints ((COALESCE(tenant_id, '')), created_at, id);
-- webhook_auto_disable: active endpoints failing since a given time.
CREATE INDEX webhook_endpoints_failing_idx ON merezha_notify.webhook_endpoints (failing_since)
  WHERE status = 'active' AND failing_since IS NOT NULL;
-- retention: previous secrets past their rotation window.
CREATE INDEX webhook_endpoints_previous_idx ON merezha_notify.webhook_endpoints (previous_expires_at)
  WHERE previous_expires_at IS NOT NULL;

CREATE TABLE merezha_notify.webhook_events (
  id uuid PRIMARY KEY,                          -- UUIDv5(source, source_event_id)
  source text NOT NULL CHECK (source ~ '^[a-z][a-z0-9_]{0,31}$'),
  source_event_id text NOT NULL CHECK (length(source_event_id) <= 200),
  tenant_id text CHECK (length(tenant_id) BETWEEN 1 AND 128),
  event_type text NOT NULL CHECK (event_type ~ '^[a-z][a-z0-9_]*(\.[a-z0-9_]+){1,4}$'),
  occurred_at timestamptz NOT NULL,
  body bytea,                                   -- exact signed envelope; NULL after retention
  created_at timestamptz NOT NULL,
  UNIQUE (source, source_event_id)
);
CREATE INDEX webhook_events_created_idx ON merezha_notify.webhook_events (created_at);

CREATE TABLE merezha_notify.webhook_deliveries (
  id uuid PRIMARY KEY,                          -- UUIDv5(event id, endpoint id)
  event_id uuid NOT NULL REFERENCES merezha_notify.webhook_events(id) ON DELETE CASCADE,
  endpoint_id uuid NOT NULL REFERENCES merezha_notify.webhook_endpoints(id) ON DELETE CASCADE,
  status text NOT NULL CHECK (status IN ('queued','sending','retrying','succeeded',
    'failed','cancelled')),
  attempt_count integer NOT NULL DEFAULT 0,
  generation integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz,
  last_http_status smallint,
  last_error_code text CHECK (last_error_code ~ '^[a-z0-9_]{1,64}$'),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  settled_at timestamptz,
  UNIQUE (event_id, endpoint_id)
);
CREATE INDEX webhook_deliveries_endpoint_idx
  ON merezha_notify.webhook_deliveries (endpoint_id, created_at DESC, id DESC);

CREATE TABLE merezha_notify.webhook_attempts (
  delivery_id uuid NOT NULL REFERENCES merezha_notify.webhook_deliveries(id) ON DELETE CASCADE,
  attempt integer NOT NULL CHECK (attempt >= 1),
  generation integer NOT NULL,
  started_at timestamptz NOT NULL,
  finished_at timestamptz,
  outcome text NOT NULL CHECK (outcome IN ('started','succeeded','failed','blocked','ambiguous')),
  http_status smallint,
  duration_ms integer,
  error_code text CHECK (error_code ~ '^[a-z0-9_]{1,64}$'),
  response_snippet text CHECK (length(response_snippet) <= 1024),
  PRIMARY KEY (delivery_id, attempt)
);

-- Endpoint create, update, rotate, enable, disable and delete (NT §5.5
-- "Audit"), never the secret. No foreign key: a row outlives the endpoint
-- it names. The `retention` task deletes rows after 400 days.
CREATE TABLE merezha_notify.webhook_audit (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  endpoint_id uuid NOT NULL,
  tenant_id text CHECK (length(tenant_id) BETWEEN 1 AND 128),
  action text NOT NULL CHECK (action IN ('create','update','rotate','enable','disable','delete')),
  reason text CHECK (reason IN ('manual','gone','failing')),
  actor text NOT NULL CHECK (length(actor) <= 200),
  revision bigint NOT NULL,
  occurred_at timestamptz NOT NULL
);
CREATE INDEX webhook_audit_endpoint_idx ON merezha_notify.webhook_audit (endpoint_id, id);
CREATE INDEX webhook_audit_time_idx ON merezha_notify.webhook_audit (occurred_at);
