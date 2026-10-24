-- merezha_notify migration 1 (NT-N1): the core tables of NT §2.4.
-- No foreign keys into other modules' schemas: user and tenant ids are
-- opaque identity ids.
CREATE SCHEMA IF NOT EXISTS merezha_notify;

CREATE TABLE merezha_notify.notifications (
  id uuid PRIMARY KEY,
  source text NOT NULL CHECK (source ~ '^[a-z][a-z0-9_]{0,31}$'),
  dedupe_key text NOT NULL CHECK (dedupe_key ~ '^[A-Za-z0-9_.:/-]{1,200}$'),
  topic text NOT NULL, template text NOT NULL, locale text, tenant_id uuid,
  category text NOT NULL CHECK (category IN ('security','transactional','marketing')),
  priority text NOT NULL CHECK (priority IN ('low','normal','high')),
  data jsonb,
  data_purged_at timestamptz,
  has_secret boolean NOT NULL DEFAULT false,
  channels text[] NOT NULL,
  collapse_key text CHECK (length(collapse_key) <= 64),
  not_before timestamptz, expires_at timestamptz,
  correlation_id text, causation_id text,
  status text NOT NULL CHECK (status IN ('expanded','rejected')),
  status_reason text,
  target_count integer NOT NULL,
  requested_at timestamptz NOT NULL, expanded_at timestamptz NOT NULL,
  settled_at timestamptz,
  UNIQUE (source, dedupe_key)
);
CREATE INDEX notifications_purge_idx ON merezha_notify.notifications (settled_at)
  WHERE data IS NOT NULL AND settled_at IS NOT NULL;

CREATE TABLE merezha_notify.deliveries (
  id uuid PRIMARY KEY,
  notification_id uuid NOT NULL REFERENCES merezha_notify.notifications(id) ON DELETE CASCADE,
  channel text NOT NULL CHECK (channel IN ('email','sms','push','inbox')),
  target_key text NOT NULL CHECK (length(target_key) <= 400),
  user_id uuid, push_subscription_id uuid,
  address text CHECK (length(address) <= 320),
  locale text NOT NULL,
  status text NOT NULL CHECK (status IN ('queued','sending','retrying','accepted','delivered',
    'bounced','complained','failed','skipped','expired','unknown','cancelled')),
  status_reason text CHECK (status_reason ~ '^[a-z0-9_]{1,64}$'),
  provider text CHECK (provider IN ('brevo','webpush','inbox','capture')),
  provider_message_id text CHECK (length(provider_message_id) <= 256),
  attempt_count integer NOT NULL DEFAULT 0,
  generation integer NOT NULL DEFAULT 0,
  run_at timestamptz NOT NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
  accepted_at timestamptz, settled_at timestamptz,
  UNIQUE (notification_id, channel, target_key)
);
CREATE INDEX deliveries_provider_msg_idx ON merezha_notify.deliveries (provider, provider_message_id)
  WHERE provider_message_id IS NOT NULL;
CREATE INDEX deliveries_open_idx ON merezha_notify.deliveries (status, updated_at)
  WHERE status IN ('queued','sending','retrying');
CREATE INDEX deliveries_notification_idx ON merezha_notify.deliveries (notification_id);

CREATE TABLE merezha_notify.delivery_attempts (
  delivery_id uuid NOT NULL REFERENCES merezha_notify.deliveries(id) ON DELETE CASCADE,
  attempt integer NOT NULL CHECK (attempt >= 1),
  generation integer NOT NULL,
  started_at timestamptz NOT NULL, finished_at timestamptz,
  outcome text NOT NULL CHECK (outcome IN ('started','accepted','duplicate','transient',
    'rate_limited','permanent','ambiguous')),
  http_status smallint,
  error_code text CHECK (error_code ~ '^[a-z0-9_]{1,64}$'),
  retry_after_ms integer,
  PRIMARY KEY (delivery_id, attempt)
);

CREATE TABLE merezha_notify.provider_events (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  provider text NOT NULL,
  channel text NOT NULL CHECK (channel IN ('email','sms')),
  event text NOT NULL CHECK (event ~ '^[a-z_]{1,32}$'),
  event_key text NOT NULL,
  delivery_id uuid,
  provider_message_id text,
  reason text CHECK (length(reason) <= 200),
  occurred_at timestamptz, received_at timestamptz NOT NULL,
  UNIQUE (provider, event_key)
);
CREATE INDEX provider_events_delivery_idx ON merezha_notify.provider_events (delivery_id);

CREATE TABLE merezha_notify.preferences (
  user_id uuid NOT NULL,
  topic text NOT NULL,
  channel text NOT NULL CHECK (channel IN ('email','sms','push','inbox')),
  enabled boolean NOT NULL, updated_at timestamptz NOT NULL,
  PRIMARY KEY (user_id, topic, channel)
);

CREATE TABLE merezha_notify.user_settings (
  user_id uuid PRIMARY KEY,
  timezone text,
  updated_at timestamptz NOT NULL
);

CREATE TABLE merezha_notify.suppressions (
  id uuid PRIMARY KEY,
  channel text NOT NULL CHECK (channel IN ('email','sms')),
  address text NOT NULL,
  topic text,
  reason text NOT NULL CHECK (reason IN ('unsubscribed','hard_bounce','invalid',
    'spam_complaint','blocked','manual')),
  source_delivery_id uuid, created_at timestamptz NOT NULL
);
CREATE UNIQUE INDEX suppressions_key ON merezha_notify.suppressions (channel, address, COALESCE(topic, ''));

CREATE TABLE merezha_notify.captures (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  delivery_id uuid NOT NULL, channel text NOT NULL, target text NOT NULL,
  rendered jsonb NOT NULL, captured_at timestamptz NOT NULL
);
-- One capture per delivery and idempotency key; SMS captures carry no key.
CREATE UNIQUE INDEX captures_idempotency_idx
  ON merezha_notify.captures (delivery_id, (rendered->>'idempotency_key'));
