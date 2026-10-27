-- merezha_notify migration 7 (notifications hardening, issues #63, #68,
-- #76): message ids per attempt, deleted tenants, and the indexes of
-- `reclaim_stuck`, the tenant purge and the webhook audit route. (The
-- unreadable mark of migration 5 now hashes `secret_ct || secret_nonce`;
-- marks made before no longer match and are made again by the next
-- reseal, which only costs one more decryption attempt per row.)
--
-- An accepted attempt keeps the provider's message id. A redelivery clears
-- `deliveries.provider_message_id`, so without this a late report about an
-- earlier generation could not be told apart once the current generation
-- has no message id of its own (an ambiguous SMS, a duplicate email).
ALTER TABLE merezha_notify.delivery_attempts
  ADD COLUMN provider_message_id text CHECK (length(provider_message_id) <= 256);

-- An organization identity deleted (`identity.organization_deleted`). From
-- the commit of its row on, no expansion, fan-out, endpoint or push
-- subscription of the tenant is written (writers take the tenant's shared
-- transaction lock and check this row); the purge job deletes what the
-- tenant had and sets `purged_at`. The row is kept: organization ids are
-- never reused.
CREATE TABLE merezha_notify.tenants_deleted (
  tenant_id text PRIMARY KEY CHECK (length(tenant_id) BETWEEN 1 AND 128),
  deleted_at timestamptz NOT NULL,
  source text NOT NULL CHECK (source IN ('event', 'operator')),  -- who marked it
  purged_at timestamptz
);

-- `reclaim_stuck` finds the open webhook deliveries whose job was lost
-- (`deliveries_open_idx` of migration 1 serves the notification side).
CREATE INDEX webhook_deliveries_open_idx ON merezha_notify.webhook_deliveries (updated_at)
  WHERE status IN ('queued','sending','retrying');

-- The tenant purge reads each table by tenant.
CREATE INDEX notifications_tenant_idx ON merezha_notify.notifications (tenant_id)
  WHERE tenant_id IS NOT NULL;
CREATE INDEX inbox_items_tenant_idx ON merezha_notify.inbox_items (tenant_id, user_id)
  WHERE tenant_id IS NOT NULL;
CREATE INDEX push_subscriptions_tenant_idx ON merezha_notify.push_subscriptions (tenant_id)
  WHERE tenant_id IS NOT NULL;
CREATE INDEX webhook_events_tenant_idx ON merezha_notify.webhook_events (tenant_id)
  WHERE tenant_id IS NOT NULL;

-- `GET …webhook-audit`: one registry's rows, newest first.
CREATE INDEX webhook_audit_registry_idx
  ON merezha_notify.webhook_audit ((COALESCE(tenant_id, '')), id DESC);
