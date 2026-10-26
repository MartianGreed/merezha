-- merezha_notify migration 3 (NT-N3): Brevo email events that arrive while
-- their delivery is still queued, sending or retrying are kept pending; the
-- delivery worker applies them in the transaction that records the outcome
-- (Tx B), so no event is consumed without effect.
ALTER TABLE merezha_notify.provider_events
  ADD COLUMN pending boolean NOT NULL DEFAULT false;
CREATE INDEX provider_events_pending_idx ON merezha_notify.provider_events (delivery_id, id)
  WHERE pending;
