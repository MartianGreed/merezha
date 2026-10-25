-- merezha_notify migration 2 (NT-N2): the inbox of NT §2.4, the per-user
-- ordered counters of §5.4, and identity ids stored as text.
--
-- Identity renders its ids as prefixed text (`usr_<hex>`, R7), which the
-- Directory, principals and the jobs runtime carry as is; migration 1 typed
-- the user and tenant columns `uuid`, which those ids do not fit. Its tables
-- hold no user rows before this slice.
ALTER TABLE merezha_notify.notifications
  ALTER COLUMN tenant_id TYPE text USING tenant_id::text,
  ADD CONSTRAINT notifications_tenant_id_check CHECK (length(tenant_id) <= 128);
ALTER TABLE merezha_notify.deliveries
  ALTER COLUMN user_id TYPE text USING user_id::text,
  ADD CONSTRAINT deliveries_user_id_check CHECK (length(user_id) <= 128),
  -- The recipient's display name, for the greeting and the `To` name;
  -- nulled with the notification's data.
  ADD COLUMN name text CHECK (length(name) <= 200);
ALTER TABLE merezha_notify.preferences
  ALTER COLUMN user_id TYPE text USING user_id::text,
  ADD CONSTRAINT preferences_user_id_check CHECK (length(user_id) <= 128);
ALTER TABLE merezha_notify.user_settings
  ALTER COLUMN user_id TYPE text USING user_id::text,
  ADD CONSTRAINT user_settings_user_id_check CHECK (length(user_id) <= 128);

-- settle_notifications scans the notifications still open.
CREATE INDEX notifications_unsettled_idx ON merezha_notify.notifications (expanded_at)
  WHERE settled_at IS NULL;

-- One row per user with an inbox item. Every inbox write takes its numbers
-- from this row, locked until commit, so a user's changes commit in order.
CREATE TABLE merezha_notify.inbox_counters (
  user_id text PRIMARY KEY CHECK (length(user_id) <= 128),
  last_seq bigint NOT NULL,                     -- highest seq or change_seq handed out
  purged_through bigint NOT NULL DEFAULT 0      -- highest change_seq ever deleted
);
CREATE TABLE merezha_notify.inbox_items (
  id uuid PRIMARY KEY,                          -- = delivery id
  user_id text NOT NULL CHECK (length(user_id) <= 128),
  tenant_id text CHECK (length(tenant_id) <= 128),
  notification_id uuid NOT NULL,
  topic text NOT NULL,
  title text NOT NULL CHECK (length(title) <= 200),
  body text NOT NULL CHECK (length(body) <= 2000),
  link text CHECK (length(link) <= 2048),
  payload jsonb CHECK (octet_length(payload::text) <= 4096),
  seq bigint NOT NULL,                          -- per-user creation order, immutable
  change_seq bigint NOT NULL,                   -- next counter value on every change
  created_at timestamptz NOT NULL, read_at timestamptz, archived_at timestamptz,
  expires_at timestamptz NOT NULL
);
CREATE UNIQUE INDEX inbox_items_change_idx ON merezha_notify.inbox_items (user_id, change_seq);
CREATE INDEX inbox_items_list_idx ON merezha_notify.inbox_items (user_id, seq DESC)
  WHERE archived_at IS NULL;
CREATE INDEX inbox_items_unread_idx ON merezha_notify.inbox_items (user_id)
  WHERE read_at IS NULL AND archived_at IS NULL;
CREATE INDEX inbox_items_expiry_idx ON merezha_notify.inbox_items (expires_at);
CREATE INDEX inbox_items_archived_idx ON merezha_notify.inbox_items (archived_at)
  WHERE archived_at IS NOT NULL;
