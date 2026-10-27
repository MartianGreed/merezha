-- merezha_notify migration 6 (NT-N8): the indexes of the per-target rate
-- cap and of the `retention` task (NT §2.5, §6). It takes the number NT §11
-- reserved for N9 quiet hours, which is deferred from v1.
--
-- `max_per_target_per_hour` counts a target's deliveries of the last hour
-- at expansion: the rows of one `(target_key, channel)` newest first, each
-- joined to its notification for the topic.
CREATE INDEX deliveries_target_recent_idx
  ON merezha_notify.deliveries (target_key, channel, created_at);

-- `retention` deletes notifications settled before `retention.deliveries`
-- (their deliveries and attempts cascade) oldest first; the purge index
-- covers only rows that still hold data.
CREATE INDEX notifications_settled_idx ON merezha_notify.notifications (settled_at)
  WHERE settled_at IS NOT NULL;

-- `retention` deletes provider events received before `retention.deliveries`.
CREATE INDEX provider_events_received_idx ON merezha_notify.provider_events (received_at);
