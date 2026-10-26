-- merezha_notify migration 4 (NT-N6): Web Push subscriptions (NT §2.4, §5.3).
--
-- User and tenant ids are text, as migration 2 made them everywhere else.
-- `tenant_id` is the active tenant of the session that subscribed: like the
-- inbox (§5.4), a notification of a tenant reaches only the subscriptions
-- made in that tenant, and a notification without one reaches every
-- subscription of its user. `p256dh` and `auth` are the browser's
-- encryption keys: never logged or returned.
CREATE TABLE merezha_notify.push_subscriptions (
  id uuid PRIMARY KEY,
  user_id text NOT NULL CHECK (length(user_id) <= 128),
  tenant_id text CHECK (length(tenant_id) <= 128),
  endpoint text NOT NULL UNIQUE CHECK (length(endpoint) <= 2048),
  endpoint_origin text NOT NULL CHECK (length(endpoint_origin) <= 300),
  p256dh bytea NOT NULL CHECK (octet_length(p256dh) = 65),
  auth bytea NOT NULL CHECK (octet_length(auth) = 16),
  vapid_key_id text NOT NULL CHECK (vapid_key_id ~ '^[0-9a-f]{16}$'),  -- first 16 hex of SHA-256(public key)
  label text CHECK (length(label) <= 64),
  expires_at timestamptz,
  created_at timestamptz NOT NULL,
  last_used_at timestamptz NOT NULL,
  last_success_at timestamptz,
  consecutive_failures integer NOT NULL DEFAULT 0
);
CREATE INDEX push_subscriptions_user_idx ON merezha_notify.push_subscriptions (user_id);
-- push_expire: expired rows, rows of a rotated key, failing rows.
CREATE INDEX push_subscriptions_expiry_idx ON merezha_notify.push_subscriptions (expires_at)
  WHERE expires_at IS NOT NULL;
CREATE INDEX push_subscriptions_key_idx ON merezha_notify.push_subscriptions (vapid_key_id);
CREATE INDEX push_subscriptions_failing_idx ON merezha_notify.push_subscriptions (consecutive_failures)
  WHERE consecutive_failures >= 10;
