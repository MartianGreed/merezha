//! Every statement of NT-N2 and NT-N3, registered with the TE-T4 statement registry
//! (one constant per statement; [`STATEMENTS`] lists them for the registry
//! check). Values always travel as parameters.
use merezha_postgres::Statement;

/// The `LISTEN` channel of inbox changes (NT §5.4); the payload is the user
/// id only.
pub const INBOX_CHANNEL: &str = "merezha_notify_inbox";

const fn s(name: &'static str, summary: &'static str, sql: &'static str) -> Statement {
    Statement::new(name, summary, sql)
}

// ----- expansion -----

pub const NOTIFICATION_EXISTS: Statement = s(
    "notify.notification.exists",
    "SELECT merezha_notify.notifications",
    "SELECT 1 FROM merezha_notify.notifications WHERE id = $1",
);
/// Preference rows of `$1` users for topic `$2` and `*`.
pub const PREFERENCES_FOR_USERS: Statement = s(
    "notify.preference.for_users",
    "SELECT merezha_notify.preferences",
    "SELECT user_id, topic, channel, enabled FROM merezha_notify.preferences \
     WHERE user_id = ANY($1::text[]) AND topic IN ($2, '*')",
);
/// Suppressions of `$1` addresses that may cover topic `$2`.
pub const SUPPRESSIONS_FOR_ADDRESSES: Statement = s(
    "notify.suppression.for_addresses",
    "SELECT merezha_notify.suppressions",
    "SELECT channel, address, topic, reason FROM merezha_notify.suppressions \
     WHERE address = ANY($1::text[]) AND (topic IS NULL OR topic = $2)",
);
pub const TIMEZONES_FOR_USERS: Statement = s(
    "notify.settings.for_users",
    "SELECT merezha_notify.user_settings",
    "SELECT user_id, timezone FROM merezha_notify.user_settings \
     WHERE user_id = ANY($1::text[]) AND timezone IS NOT NULL",
);
/// The notification row, unless `(source, dedupe_key)` or the id exists.
pub const NOTIFICATION_INSERT: Statement = s(
    "notify.notification.insert",
    "INSERT merezha_notify.notifications",
    "INSERT INTO merezha_notify.notifications (id, source, dedupe_key, topic, template, locale, \
     tenant_id, category, priority, data, has_secret, channels, collapse_key, not_before, \
     expires_at, correlation_id, causation_id, status, status_reason, target_count, \
     requested_at, expanded_at, settled_at) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, \
     $19, $20, $21, $22, $23) ON CONFLICT DO NOTHING RETURNING id",
);
/// The class of the rate cap's two-key advisory locks (`"ntfy"`), the
/// `$1` of [`TARGET_LOCKS`]: it keeps them apart from other modules'
/// two-key locks.
pub const TARGET_LOCK_CLASS: i32 = i32::from_be_bytes(*b"ntfy");
/// Buckets the targets of one `(topic, channel)` are hashed into for the
/// rate cap's locks: an expansion takes at most this many locks per
/// channel, whatever its target count, so capped expansions cannot
/// exhaust PostgreSQL's shared lock table (NT-N8 review).
pub const TARGET_LOCK_BUCKETS: i32 = 64;
/// Lock, for the transaction, the bucket of each `($3 channel, $4 target
/// key)` pair under topic `$2`: `hashtext(target key)` modulo `$5` buckets
/// per `(topic, channel)`, one lock per distinct bucket, taken in
/// ascending key order so two expansions never wait on each other
/// crosswise. A target always falls in the same bucket, so expansions for
/// one target count one after the other; targets sharing a bucket (or a
/// `hashtext` collision) only serialize. Returns the keys in the order
/// they were locked.
pub const TARGET_LOCKS: Statement = s(
    "notify.delivery.target_locks",
    "SELECT pg_advisory_xact_lock()",
    "SELECT k.key, pg_advisory_xact_lock($1::int4, k.key) FROM \
     (SELECT DISTINCT hashtext($2::text || chr(10) || t.channel || chr(10) || \
              (abs(hashtext(t.target_key)::bigint) % $5::int4)::text) AS key \
        FROM unnest($3::text[], $4::text[]) AS t(channel, target_key) ORDER BY 1) AS k",
);
/// Per `($1 channel, $2 target key)` pair, the deliveries of topic `$4`
/// created after `$3` that were not skipped (the rate cap, NT §6).
pub const TARGET_RECENT: Statement = s(
    "notify.delivery.target_recent",
    "SELECT merezha_notify.deliveries",
    "SELECT d.channel, d.target_key, count(*) FROM merezha_notify.deliveries d \
       JOIN merezha_notify.notifications n ON n.id = d.notification_id \
      WHERE (d.target_key, d.channel) IN \
            (SELECT t.target_key, t.channel FROM unnest($1::text[], $2::text[]) AS t(channel, target_key)) \
        AND d.created_at > $3 AND n.topic = $4 AND d.status <> 'skipped' \
      GROUP BY d.channel, d.target_key",
);
/// Deliveries of notification `$1`, one per array position; `$14` is the
/// push subscription of each push target (NT-N6).
pub const DELIVERIES_INSERT: Statement = s(
    "notify.delivery.insert",
    "INSERT merezha_notify.deliveries",
    "INSERT INTO merezha_notify.deliveries (id, notification_id, channel, target_key, user_id, \
     address, name, locale, status, status_reason, provider, push_subscription_id, run_at, \
     created_at, updated_at, settled_at) \
     SELECT d.id, $1, d.channel, d.target_key, d.user_id, d.address, d.name, d.locale, d.status, \
     d.status_reason, d.provider, d.subscription, $2::timestamptz, $3::timestamptz, \
     $3::timestamptz, CASE WHEN d.status = 'queued' THEN NULL ELSE $3::timestamptz END \
     FROM unnest($4::uuid[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[], \
     $10::text[], $11::text[], $12::text[], $13::text[], $14::uuid[]) \
     AS d(id, channel, target_key, user_id, address, name, locale, status, status_reason, provider, \
     subscription) \
     ON CONFLICT DO NOTHING",
);
/// Take `$2[i]` numbers for each user `$1[i]`, locking the counter rows in
/// ascending user order (NT §5.4), so expansions cannot deadlock.
pub const COUNTERS_TAKE: Statement = s(
    "notify.inbox.counters_take",
    "INSERT merezha_notify.inbox_counters",
    "INSERT INTO merezha_notify.inbox_counters (user_id, last_seq) \
     SELECT t.u, t.n FROM unnest($1::text[], $2::bigint[]) AS t(u, n) ORDER BY t.u \
     ON CONFLICT (user_id) DO UPDATE SET last_seq = inbox_counters.last_seq + EXCLUDED.last_seq \
     RETURNING user_id, last_seq",
);
pub const INBOX_INSERT: Statement = s(
    "notify.inbox.insert",
    "INSERT merezha_notify.inbox_items",
    "INSERT INTO merezha_notify.inbox_items (id, user_id, tenant_id, notification_id, topic, \
     title, body, link, payload, seq, change_seq, created_at, expires_at) \
     SELECT i.id, i.user_id, $1, $2, $3, i.title, i.body, i.link, i.payload::jsonb, i.seq, i.seq, \
     $4, $5 \
     FROM unnest($6::uuid[], $7::text[], $8::text[], $9::text[], $10::text[], $11::text[], \
     $12::bigint[]) AS i(id, user_id, title, body, link, payload, seq) \
     ON CONFLICT DO NOTHING",
);
/// Wake the streams of `$1` users; delivered on commit, merged when equal.
pub const INBOX_NOTIFY: Statement = s(
    "notify.inbox.notify",
    "SELECT pg_notify()",
    "SELECT pg_notify('merezha_notify_inbox', u) FROM unnest($1::text[]) AS u",
);

// ----- delivery worker -----

/// The delivery and its notification, the delivery locked (Tx A).
pub const DELIVERY_LOCK: Statement = s(
    "notify.delivery.lock",
    "SELECT merezha_notify.deliveries",
    "SELECT d.status, d.generation, d.attempt_count, d.channel, d.user_id, d.address, d.name, \
     d.locale, n.id, n.topic, n.template, n.category, n.data, n.has_secret, n.expires_at, \
     n.requested_at, n.source, n.correlation_id, n.tenant_id \
     FROM merezha_notify.deliveries d \
     JOIN merezha_notify.notifications n ON n.id = d.notification_id \
     WHERE d.id = $1 FOR UPDATE OF d",
);
/// Of delivery `$1`'s generation `$2`: when its first `ambiguous` attempt
/// started (Brevo may have processed it, so its idempotency window runs from
/// there), and when the trailing run of `provider_account` answers began.
pub const ATTEMPT_HISTORY: Statement = s(
    "notify.attempt.history",
    "SELECT merezha_notify.delivery_attempts",
    "SELECT (SELECT min(started_at) FROM merezha_notify.delivery_attempts \
       WHERE delivery_id = $1 AND generation = $2 AND outcome = 'ambiguous'), \
     (SELECT min(a.started_at) FROM merezha_notify.delivery_attempts a \
       WHERE a.delivery_id = $1 AND a.generation = $2 AND a.error_code = 'provider_account' \
       AND a.attempt > COALESCE((SELECT max(b.attempt) FROM merezha_notify.delivery_attempts b \
         WHERE b.delivery_id = $1 AND b.generation = $2 \
         AND b.error_code IS DISTINCT FROM 'provider_account'), 0))",
);
/// The last `provider_account` attempt of delivery `$1`'s generation `$2`:
/// the attempt the live job chain continues after (issue #63), `NULL`
/// while the generation's first job is the live one.
pub const ATTEMPT_CHAIN: Statement = s(
    "notify.attempt.chain",
    "SELECT merezha_notify.delivery_attempts",
    "SELECT max(attempt) FROM merezha_notify.delivery_attempts \
     WHERE delivery_id = $1 AND generation = $2 AND outcome = 'transient' \
     AND error_code = 'provider_account'",
);
/// Keep the provider message id `$3` of accepted attempt `$2` of delivery
/// `$1` (migration 7), so a late report about it is known for an earlier
/// generation's once the delivery is redelivered (issue #68).
pub const ATTEMPT_MESSAGE_ID: Statement = s(
    "notify.attempt.message_id",
    "UPDATE merezha_notify.delivery_attempts",
    "UPDATE merezha_notify.delivery_attempts SET provider_message_id = $3 \
     WHERE delivery_id = $1 AND attempt = $2",
);
pub const LAST_ATTEMPT: Statement = s(
    "notify.attempt.last",
    "SELECT merezha_notify.delivery_attempts",
    "SELECT attempt, outcome FROM merezha_notify.delivery_attempts \
     WHERE delivery_id = $1 ORDER BY attempt DESC LIMIT 1",
);
pub const ATTEMPT_START: Statement = s(
    "notify.attempt.start",
    "INSERT merezha_notify.delivery_attempts",
    "INSERT INTO merezha_notify.delivery_attempts (delivery_id, attempt, generation, started_at, \
     outcome) VALUES ($1, $2, $3, $4, 'started')",
);
pub const ATTEMPT_FINISH: Statement = s(
    "notify.attempt.finish",
    "UPDATE merezha_notify.delivery_attempts",
    "UPDATE merezha_notify.delivery_attempts SET outcome = $3, finished_at = $4, \
     error_code = $5, retry_after_ms = $6 WHERE delivery_id = $1 AND attempt = $2",
);
pub const DELIVERY_SENDING: Statement = s(
    "notify.delivery.sending",
    "UPDATE merezha_notify.deliveries",
    "UPDATE merezha_notify.deliveries SET status = 'sending', attempt_count = $2, \
     updated_at = $3 WHERE id = $1",
);
/// Move delivery `$1` of generation `$2` to `$3` / `$4`; terminal and
/// settled statuses set `settled_at`. With `$7`, only while attempt `$7` is
/// the one sending: a worker whose lease was taken over records nothing.
pub const DELIVERY_SET: Statement = s(
    "notify.delivery.set",
    "UPDATE merezha_notify.deliveries",
    "UPDATE merezha_notify.deliveries SET status = $3::text, status_reason = $4::text, \
     updated_at = $5::timestamptz, run_at = COALESCE($6::timestamptz, run_at), \
     settled_at = CASE WHEN $3::text IN ('queued', 'sending', 'retrying') THEN NULL \
       ELSE $5::timestamptz END \
     WHERE id = $1 AND generation = $2 \
     AND ($7::int IS NULL OR (status = 'sending' AND attempt_count = $7::int))",
);
/// Record at `$2` that a job still holds open delivery `$1` without an
/// attempt (a push retried for another VAPID key), so `reclaim_stuck`
/// does not take a live chain for a lost one.
pub const DELIVERY_TOUCH: Statement = s(
    "notify.delivery.touch",
    "UPDATE merezha_notify.deliveries",
    "UPDATE merezha_notify.deliveries SET updated_at = $2 WHERE id = $1",
);
pub const DELIVERY_ACCEPT: Statement = s(
    "notify.delivery.accept",
    "UPDATE merezha_notify.deliveries",
    "UPDATE merezha_notify.deliveries SET status = 'accepted', status_reason = NULL, \
     provider = $3, provider_message_id = $4, accepted_at = $5, settled_at = $5, updated_at = $5 \
     WHERE id = $1 AND generation = $2 AND status = 'sending' AND attempt_count = $6",
);
pub const PREFERENCES_FOR_USER: Statement = s(
    "notify.preference.for_user",
    "SELECT merezha_notify.preferences",
    "SELECT topic, enabled FROM merezha_notify.preferences \
     WHERE user_id = $1 AND channel = $2 AND topic IN ($3, '*')",
);
pub const SUPPRESSIONS_FOR_ADDRESS: Statement = s(
    "notify.suppression.for_address",
    "SELECT merezha_notify.suppressions",
    "SELECT topic, reason FROM merezha_notify.suppressions \
     WHERE channel = $1 AND address = $2 AND (topic IS NULL OR topic = $3)",
);
pub const TIMEZONE_FOR_USER: Statement = s(
    "notify.settings.timezone",
    "SELECT merezha_notify.user_settings",
    "SELECT timezone FROM merezha_notify.user_settings WHERE user_id = $1",
);

// ----- status and redelivery -----

pub const STATUS_NOTIFICATION: Statement = s(
    "notify.status.notification",
    "SELECT merezha_notify.notifications",
    "SELECT topic, status, status_reason, expanded_at, settled_at, source, dedupe_key \
     FROM merezha_notify.notifications WHERE id = $1",
);
pub const STATUS_DELIVERIES: Statement = s(
    "notify.status.deliveries",
    "SELECT merezha_notify.deliveries",
    "SELECT id, channel, status, status_reason, attempt_count, address, accepted_at, settled_at \
     FROM merezha_notify.deliveries WHERE notification_id = $1 \
     ORDER BY channel, target_key LIMIT $2",
);
pub const REDELIVER_LOCK: Statement = s(
    "notify.redeliver.lock",
    "SELECT merezha_notify.deliveries",
    "SELECT d.status, d.channel, n.has_secret, n.data IS NULL, n.tenant_id, n.correlation_id, \
     n.id \
     FROM merezha_notify.deliveries d \
     JOIN merezha_notify.notifications n ON n.id = d.notification_id \
     WHERE d.id = $1 FOR UPDATE OF d, n",
);
pub const REDELIVER_SET: Statement = s(
    "notify.redeliver.set",
    "UPDATE merezha_notify.deliveries",
    "UPDATE merezha_notify.deliveries SET status = 'queued', status_reason = NULL, \
     generation = generation + 1, run_at = $2, updated_at = $2, settled_at = NULL, \
     accepted_at = NULL, provider = NULL, provider_message_id = NULL \
     WHERE id = $1 RETURNING generation",
);
pub const NOTIFICATION_REOPEN: Statement = s(
    "notify.notification.reopen",
    "UPDATE merezha_notify.notifications",
    "UPDATE merezha_notify.notifications SET settled_at = NULL WHERE id = $1",
);

// ----- preferences, settings, suppressions -----

pub const PREFERENCES_LIST: Statement = s(
    "notify.preference.list",
    "SELECT merezha_notify.preferences",
    "SELECT topic, channel, enabled FROM merezha_notify.preferences \
     WHERE user_id = $1 ORDER BY topic, channel LIMIT $2",
);
pub const PREFERENCES_UPSERT: Statement = s(
    "notify.preference.upsert",
    "INSERT merezha_notify.preferences",
    "INSERT INTO merezha_notify.preferences (user_id, topic, channel, enabled, updated_at) \
     SELECT $1, p.topic, p.channel, p.enabled, $2 \
     FROM unnest($3::text[], $4::text[], $5::bool[]) AS p(topic, channel, enabled) \
     ON CONFLICT (user_id, topic, channel) \
     DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = EXCLUDED.updated_at",
);
pub const SETTINGS_GET: Statement = s(
    "notify.settings.get",
    "SELECT merezha_notify.user_settings",
    "SELECT timezone FROM merezha_notify.user_settings WHERE user_id = $1",
);
pub const SETTINGS_UPSERT: Statement = s(
    "notify.settings.upsert",
    "INSERT merezha_notify.user_settings",
    "INSERT INTO merezha_notify.user_settings (user_id, timezone, updated_at) \
     VALUES ($1, $2, $3) ON CONFLICT (user_id) \
     DO UPDATE SET timezone = EXCLUDED.timezone, updated_at = EXCLUDED.updated_at",
);
pub const SUPPRESSION_INSERT: Statement = s(
    "notify.suppression.insert",
    "INSERT merezha_notify.suppressions",
    "INSERT INTO merezha_notify.suppressions (id, channel, address, topic, reason, \
     source_delivery_id, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7) \
     ON CONFLICT DO NOTHING",
);
pub const SUPPRESSION_FIND: Statement = s(
    "notify.suppression.find",
    "SELECT merezha_notify.suppressions",
    "SELECT id, channel, address, topic, reason, created_at FROM merezha_notify.suppressions \
     WHERE channel = $1 AND address = $2 AND COALESCE(topic, '') = COALESCE($3, '')",
);
pub const SUPPRESSIONS_LIST: Statement = s(
    "notify.suppression.list",
    "SELECT merezha_notify.suppressions",
    "SELECT id, channel, address, topic, reason, created_at FROM merezha_notify.suppressions \
     WHERE ($1::text IS NULL OR channel = $1) AND ($2::text IS NULL OR address = $2) \
     AND ($3::uuid IS NULL OR id > $3) ORDER BY id LIMIT $4",
);
pub const SUPPRESSION_DELETE: Statement = s(
    "notify.suppression.delete",
    "DELETE merezha_notify.suppressions",
    "DELETE FROM merezha_notify.suppressions WHERE id = $1",
);

// ----- inbox -----

/// The `InboxItem` columns, in [`crate::inbox::item`] order.
macro_rules! item_columns {
    () => {
        "id, topic, title, body, link, payload::text, tenant_id, seq, change_seq, created_at, \
         read_at, archived_at"
    };
}
pub(crate) use item_columns;
pub const INBOX_LIST: Statement = s(
    "notify.inbox.list",
    "SELECT merezha_notify.inbox_items",
    concat!(
        "SELECT ",
        item_columns!(),
        " FROM merezha_notify.inbox_items \
         WHERE user_id = $1 AND archived_at IS NULL AND (NOT $2::bool OR read_at IS NULL) \
         AND ($3::text IS NULL OR topic = $3) \
         AND (tenant_id IS NULL OR tenant_id = $4::text) \
         AND ($5::bigint IS NULL OR seq < $5) ORDER BY seq DESC LIMIT $6"
    ),
);
pub const INBOX_UNREAD: Statement = s(
    "notify.inbox.unread",
    "SELECT merezha_notify.inbox_items",
    "SELECT count(*) FROM merezha_notify.inbox_items \
     WHERE user_id = $1 AND read_at IS NULL AND archived_at IS NULL \
     AND (tenant_id IS NULL OR tenant_id = $2::text)",
);
pub const INBOX_GET: Statement = s(
    "notify.inbox.get",
    "SELECT merezha_notify.inbox_items",
    concat!(
        "SELECT ",
        item_columns!(),
        " FROM merezha_notify.inbox_items WHERE id = $1 AND user_id = $2 \
         AND (tenant_id IS NULL OR tenant_id = $3::text)"
    ),
);
/// Hand out `$2` numbers of user `$1`, locking the row until commit.
pub const COUNTER_BUMP: Statement = s(
    "notify.inbox.counter_bump",
    "UPDATE merezha_notify.inbox_counters",
    "UPDATE merezha_notify.inbox_counters SET last_seq = last_seq + $2 \
     WHERE user_id = $1 RETURNING last_seq",
);
pub const COUNTER_LOCK: Statement = s(
    "notify.inbox.counter_lock",
    "SELECT merezha_notify.inbox_counters",
    "SELECT last_seq FROM merezha_notify.inbox_counters WHERE user_id = $1 FOR UPDATE",
);
/// Set one item's state: `$3` names it (`read`, `unread`, `archive`), `$4`
/// is the time, `$5` the new `change_seq`, `$6` the tenant in scope (NULL:
/// tenantless items only). Only a real change matches.
pub const INBOX_CHANGE: Statement = s(
    "notify.inbox.change",
    "UPDATE merezha_notify.inbox_items",
    concat!(
        "UPDATE merezha_notify.inbox_items SET \
         read_at = CASE $3::text WHEN 'read' THEN $4::timestamptz WHEN 'unread' THEN NULL \
           ELSE read_at END, \
         archived_at = CASE $3::text WHEN 'archive' THEN $4::timestamptz ELSE archived_at END, \
         change_seq = $5 \
         WHERE id = $1 AND user_id = $2 AND (tenant_id IS NULL OR tenant_id = $6::text) \
         AND CASE $3::text \
           WHEN 'read' THEN read_at IS NULL \
           WHEN 'unread' THEN read_at IS NOT NULL \
           ELSE archived_at IS NULL END \
         RETURNING ",
        item_columns!()
    ),
);
/// Mark the unread items of `$1` read, numbering them from `$3 + 1` in
/// `seq` order; at most `$6` per call.
pub const INBOX_READ_ALL: Statement = s(
    "notify.inbox.read_all",
    "UPDATE merezha_notify.inbox_items",
    "UPDATE merezha_notify.inbox_items i SET read_at = $2, change_seq = $3 + t.rn \
     FROM (SELECT id, row_number() OVER (ORDER BY seq) AS rn FROM merezha_notify.inbox_items \
       WHERE user_id = $1 AND read_at IS NULL AND archived_at IS NULL \
       AND ($4::bigint IS NULL OR seq <= $4) \
       AND (tenant_id IS NULL OR tenant_id = $5::text) \
       ORDER BY seq LIMIT $6) t \
     WHERE i.id = t.id",
);
pub const INBOX_NOTIFY_ONE: Statement = s(
    "notify.inbox.notify_one",
    "SELECT pg_notify()",
    "SELECT pg_notify('merezha_notify_inbox', $1)",
);

// ----- maintenance -----

pub const SETTLE: Statement = s(
    "notify.maintenance.settle",
    "UPDATE merezha_notify.notifications",
    "UPDATE merezha_notify.notifications n SET settled_at = $1 WHERE n.id IN \
     (SELECT m.id FROM merezha_notify.notifications m WHERE m.settled_at IS NULL \
      AND NOT EXISTS (SELECT 1 FROM merezha_notify.deliveries d WHERE d.notification_id = m.id \
        AND d.status IN ('queued', 'sending', 'retrying')) \
      ORDER BY m.expanded_at LIMIT $2) \
     AND n.settled_at IS NULL \
     AND NOT EXISTS (SELECT 1 FROM merezha_notify.deliveries d WHERE d.notification_id = n.id \
       AND d.status IN ('queued', 'sending', 'retrying'))",
);
pub const PURGE_DATA: Statement = s(
    "notify.maintenance.purge_data",
    "UPDATE merezha_notify.notifications",
    "UPDATE merezha_notify.notifications SET data = NULL, data_purged_at = $1 WHERE id IN \
     (SELECT id FROM merezha_notify.notifications WHERE data IS NOT NULL \
      AND settled_at IS NOT NULL AND settled_at < $2 ORDER BY settled_at LIMIT $3) \
     AND data IS NOT NULL AND settled_at IS NOT NULL AND settled_at < $2 \
     RETURNING id",
);
pub const PURGE_NAMES: Statement = s(
    "notify.maintenance.purge_names",
    "UPDATE merezha_notify.deliveries",
    "UPDATE merezha_notify.deliveries SET name = NULL \
     WHERE notification_id = ANY($1::uuid[]) AND name IS NOT NULL",
);
pub const INBOX_EXPIRED_USERS: Statement = s(
    "notify.maintenance.inbox_expired_users",
    "SELECT merezha_notify.inbox_items",
    "SELECT DISTINCT user_id FROM merezha_notify.inbox_items \
     WHERE expires_at < $1 OR archived_at < $2 ORDER BY user_id LIMIT $3",
);
pub const COUNTERS_LOCK: Statement = s(
    "notify.maintenance.counters_lock",
    "SELECT merezha_notify.inbox_counters",
    "SELECT user_id FROM merezha_notify.inbox_counters WHERE user_id = ANY($1::text[]) \
     ORDER BY user_id FOR UPDATE",
);
pub const INBOX_EXPIRE: Statement = s(
    "notify.maintenance.inbox_expire",
    "DELETE merezha_notify.inbox_items",
    "DELETE FROM merezha_notify.inbox_items WHERE id IN \
     (SELECT id FROM merezha_notify.inbox_items WHERE user_id = ANY($1::text[]) \
      AND (expires_at < $2 OR archived_at < $3) LIMIT $4) \
     RETURNING user_id, change_seq",
);
pub const PURGED_THROUGH: Statement = s(
    "notify.maintenance.purged_through",
    "UPDATE merezha_notify.inbox_counters",
    "UPDATE merezha_notify.inbox_counters c \
     SET purged_through = GREATEST(c.purged_through, t.m) \
     FROM unnest($1::text[], $2::bigint[]) AS t(u, m) WHERE c.user_id = t.u",
);

// ----- provider events (NT-N3) -----

/// The delivery an event names by id, locked, with its notification's
/// topic, the provider message id of its current generation, the
/// generation, and whether message id `$3` was an earlier generation's
/// (issue #68).
pub const EVENT_DELIVERY_BY_ID: Statement = s(
    "notify.event.delivery_by_id",
    "SELECT merezha_notify.deliveries",
    "SELECT d.id, d.status, d.address, n.topic, d.provider_message_id, d.generation, \
     EXISTS (SELECT 1 FROM merezha_notify.delivery_attempts a WHERE a.delivery_id = d.id \
       AND a.generation < d.generation AND a.provider_message_id = $3::text) \
     FROM merezha_notify.deliveries d \
     JOIN merezha_notify.notifications n ON n.id = d.notification_id \
     WHERE d.id = $1 AND d.channel = $2 FOR UPDATE OF d",
);
/// Resolve an event's delivery by id, without a lock.
pub const EVENT_RESOLVE_BY_ID: Statement = s(
    "notify.event.resolve_by_id",
    "SELECT merezha_notify.deliveries",
    "SELECT id FROM merezha_notify.deliveries WHERE id = $1 AND channel = $2",
);
/// Resolve the delivery that got provider message id `$2`, without a lock.
pub const EVENT_RESOLVE_BY_MESSAGE: Statement = s(
    "notify.event.resolve_by_message",
    "SELECT merezha_notify.deliveries",
    "SELECT id FROM merezha_notify.deliveries \
     WHERE provider = $1 AND provider_message_id = $2 AND channel = $3 \
     ORDER BY updated_at DESC LIMIT 1",
);
/// One provider event, unless its key was stored already.
pub const PROVIDER_EVENT_INSERT: Statement = s(
    "notify.event.insert",
    "INSERT merezha_notify.provider_events",
    "INSERT INTO merezha_notify.provider_events (provider, channel, event, event_key, \
     delivery_id, provider_message_id, reason, occurred_at, received_at, pending) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
     ON CONFLICT (provider, event_key) DO NOTHING RETURNING id",
);
/// An event's status change, only from the statuses `$5` it may leave.
pub const DELIVERY_EVENT_SET: Statement = s(
    "notify.event.delivery_set",
    "UPDATE merezha_notify.deliveries",
    "UPDATE merezha_notify.deliveries SET status = $2, status_reason = $3, updated_at = $4, \
     settled_at = COALESCE(settled_at, $4) \
     WHERE id = $1 AND status = ANY($5::text[])",
);

/// Take the events waiting for delivery `$1` (migration 3), each with
/// whether it names another message than the delivery's current one (a
/// report about an earlier generation, which moves no status). A report
/// about an earlier generation's message, or one without a message id on
/// a redelivered delivery, is judged stale when it arrives (issue #68)
/// and never stored pending.
pub const EVENTS_TAKE_PENDING: Statement = s(
    "notify.event.take_pending",
    "UPDATE merezha_notify.provider_events",
    "UPDATE merezha_notify.provider_events e SET pending = false \
     FROM merezha_notify.deliveries d \
     WHERE e.delivery_id = $1 AND e.pending AND d.id = e.delivery_id \
     RETURNING e.id, e.event, e.channel, \
     coalesce(e.provider_message_id <> d.provider_message_id, false)",
);

/// At most `$2` open deliveries whose `run_at` and `updated_at` are both
/// before `$1`, oldest first, locked: their job was lost without a
/// recorded outcome (`reclaim_stuck`, NT §2.5). A delivery a worker holds
/// is skipped.
pub const RECLAIM_CANDIDATES: Statement = s(
    "notify.delivery.reclaim_candidates",
    "SELECT merezha_notify.deliveries",
    "SELECT id FROM merezha_notify.deliveries \
     WHERE status IN ('queued', 'sending', 'retrying') AND updated_at < $1 AND run_at < $1 \
     ORDER BY updated_at LIMIT $2 FOR UPDATE SKIP LOCKED",
);
/// The `started` attempts of deliveries `$1`, whose run never recorded an
/// outcome, become `ambiguous` / `lost_outcome` at `$2`; their deliveries.
pub const RECLAIM_ATTEMPTS: Statement = s(
    "notify.attempt.reclaim",
    "UPDATE merezha_notify.delivery_attempts",
    "UPDATE merezha_notify.delivery_attempts SET outcome = 'ambiguous', finished_at = $2, \
     error_code = 'lost_outcome' WHERE delivery_id = ANY($1::uuid[]) AND outcome = 'started' \
     RETURNING delivery_id",
);
/// Settle the still open deliveries `$1` as `job_lost` at `$3`: `unknown`
/// for those in `$2` (an attempt was still `started`) and those whose
/// generation has an `ambiguous` attempt (a send may have gone out either
/// way, so a redelivery could duplicate it), `failed` for the others.
/// Each with its channel, new status and topic.
pub const RECLAIM_SET: Statement = s(
    "notify.delivery.reclaim",
    "UPDATE merezha_notify.deliveries",
    "UPDATE merezha_notify.deliveries d SET \
       status = CASE WHEN d.id = ANY($2::uuid[]) OR EXISTS (SELECT 1 \
         FROM merezha_notify.delivery_attempts a WHERE a.delivery_id = d.id \
         AND a.generation = d.generation AND a.outcome = 'ambiguous') \
         THEN 'unknown' ELSE 'failed' END, \
       status_reason = 'job_lost', updated_at = $3, settled_at = $3 \
     FROM merezha_notify.notifications n \
     WHERE d.id = ANY($1::uuid[]) AND n.id = d.notification_id \
     AND d.status IN ('queued', 'sending', 'retrying') \
     RETURNING d.id, d.channel, d.status, n.topic",
);

/// At most `$2` notifications settled before `$1`, oldest first: the
/// candidates of one `retention` batch (NT §2.5).
pub const RETENTION_CANDIDATES: Statement = s(
    "notify.notification.retention_candidates",
    "SELECT merezha_notify.notifications",
    "SELECT id FROM merezha_notify.notifications WHERE settled_at < $1 \
     ORDER BY settled_at LIMIT $2",
);
/// Lock the deliveries of notifications `$1` in id order, before their
/// notifications, as `redeliver` takes a delivery before its notification.
pub const RETENTION_LOCK_DELIVERIES: Statement = s(
    "notify.delivery.retention_lock",
    "SELECT merezha_notify.deliveries",
    "SELECT id FROM merezha_notify.deliveries WHERE notification_id = ANY($1::uuid[]) \
     ORDER BY id FOR UPDATE",
);
/// Delete notifications `$1` still settled before `$2`; their deliveries
/// and attempts cascade. A notification reopened since it was picked is
/// kept.
pub const RETENTION_NOTIFICATIONS: Statement = s(
    "notify.notification.retention",
    "DELETE merezha_notify.notifications",
    "DELETE FROM merezha_notify.notifications WHERE id = ANY($1::uuid[]) AND settled_at < $2",
);
/// Delete at most `$2` provider events received before `$1`, oldest first.
pub const RETENTION_PROVIDER_EVENTS: Statement = s(
    "notify.provider_event.retention",
    "DELETE merezha_notify.provider_events",
    "DELETE FROM merezha_notify.provider_events WHERE id IN \
     (SELECT id FROM merezha_notify.provider_events WHERE received_at < $1 \
      ORDER BY received_at LIMIT $2) AND received_at < $1",
);

// ----- deleted tenants (issue #63) -----

/// The class of the deleted-tenant gate's two-key advisory locks
/// (`"ntdl"`), the `$1` of [`TENANT_WRITE_LOCK`] and
/// [`TENANT_DELETE_LOCK`]: it keeps them apart from the rate cap's and
/// other modules' two-key locks.
pub const TENANT_LOCK_CLASS: i32 = i32::from_be_bytes(*b"ntdl");
/// The shared transaction lock of tenant `$2`, which every writer of the
/// tenant's rows takes before it checks the mark: the mark waits for the
/// writers in flight, and no writer commits after it.
pub const TENANT_WRITE_LOCK: Statement = s(
    "notify.tenant.write_lock",
    "SELECT pg_advisory_xact_lock_shared()",
    "SELECT pg_advisory_xact_lock_shared($1::int4, hashtext($2::text))",
);
/// The exclusive transaction lock of tenant `$2`, taken by the mark.
pub const TENANT_DELETE_LOCK: Statement = s(
    "notify.tenant.delete_lock",
    "SELECT pg_advisory_xact_lock()",
    "SELECT pg_advisory_xact_lock($1::int4, hashtext($2::text))",
);
/// Whether tenant `$1` was deleted.
pub const TENANT_DELETED: Statement = s(
    "notify.tenant.deleted",
    "SELECT merezha_notify.tenants_deleted",
    "SELECT 1 FROM merezha_notify.tenants_deleted WHERE tenant_id = $1",
);
/// Mark tenant `$1` deleted at `$2` by `$3` (`event`, `operator`), unless
/// it is; the row when this call marked it.
pub const TENANT_MARK: Statement = s(
    "notify.tenant.mark",
    "INSERT merezha_notify.tenants_deleted",
    "INSERT INTO merezha_notify.tenants_deleted (tenant_id, deleted_at, source) \
     VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING tenant_id",
);
/// Whether the `tenants_deleted` table (migration 7) exists.
pub const TENANTS_DELETED_EXIST: Statement = s(
    "notify.tenant.table_exists",
    "SELECT to_regclass()",
    "SELECT to_regclass('merezha_notify.tenants_deleted') IS NOT NULL",
);
/// Record that tenant `$1`'s purge finished at `$2`; the row the first
/// time.
pub const TENANT_PURGED: Statement = s(
    "notify.tenant.purged",
    "UPDATE merezha_notify.tenants_deleted",
    "UPDATE merezha_notify.tenants_deleted SET purged_at = $2 \
     WHERE tenant_id = $1 AND purged_at IS NULL RETURNING tenant_id",
);
/// Delete at most `$2` webhook events of tenant `$1`, with their
/// deliveries and attempts.
pub const TENANT_PURGE_EVENTS: Statement = s(
    "notify.tenant.purge_webhook_events",
    "DELETE merezha_notify.webhook_events",
    "DELETE FROM merezha_notify.webhook_events WHERE id IN \
     (SELECT id FROM merezha_notify.webhook_events WHERE tenant_id = $1 LIMIT $2)",
);
/// Delete at most `$2` webhook endpoints of tenant `$1` (never the
/// application's registry: `$1` is never empty), each with its revision.
pub const TENANT_PURGE_ENDPOINTS: Statement = s(
    "notify.tenant.purge_webhook_endpoints",
    "DELETE merezha_notify.webhook_endpoints",
    "DELETE FROM merezha_notify.webhook_endpoints WHERE id IN \
     (SELECT id FROM merezha_notify.webhook_endpoints WHERE COALESCE(tenant_id, '') = $1 \
      ORDER BY id LIMIT $2 FOR UPDATE) RETURNING id, revision",
);
/// Delete at most `$2` push subscriptions made in tenant `$1`.
pub const TENANT_PURGE_PUSH: Statement = s(
    "notify.tenant.purge_push",
    "DELETE merezha_notify.push_subscriptions",
    "DELETE FROM merezha_notify.push_subscriptions WHERE id IN \
     (SELECT id FROM merezha_notify.push_subscriptions WHERE tenant_id = $1 LIMIT $2)",
);
/// At most `$2` users with inbox items of tenant `$1`, in ascending order,
/// read in index order (`inbox_items_tenant_idx` on `(tenant_id, user_id)`),
/// so a step stops at its limit instead of scanning the whole tenant.
pub const TENANT_INBOX_USERS: Statement = s(
    "notify.tenant.inbox_users",
    "SELECT merezha_notify.inbox_items",
    "SELECT DISTINCT user_id FROM merezha_notify.inbox_items WHERE tenant_id = $1 \
     ORDER BY user_id LIMIT $2",
);
/// Delete at most `$3` inbox items of tenant `$1` held by users `$2`, each
/// with its user and `change_seq`.
pub const TENANT_PURGE_INBOX: Statement = s(
    "notify.tenant.purge_inbox",
    "DELETE merezha_notify.inbox_items",
    "DELETE FROM merezha_notify.inbox_items WHERE id IN \
     (SELECT id FROM merezha_notify.inbox_items WHERE tenant_id = $1 \
      AND user_id = ANY($2::text[]) LIMIT $3) RETURNING user_id, change_seq",
);
/// At most `$2` notifications of tenant `$1`.
pub const TENANT_NOTIFICATIONS: Statement = s(
    "notify.tenant.notifications",
    "SELECT merezha_notify.notifications",
    "SELECT id FROM merezha_notify.notifications WHERE tenant_id = $1 ORDER BY id LIMIT $2",
);
/// Delete the provider events correlated to deliveries `$1`.
pub const TENANT_PURGE_PROVIDER_EVENTS: Statement = s(
    "notify.tenant.purge_provider_events",
    "DELETE merezha_notify.provider_events",
    "DELETE FROM merezha_notify.provider_events WHERE delivery_id = ANY($1::uuid[])",
);
/// Delete the development captures of deliveries `$1`.
pub const TENANT_PURGE_CAPTURES: Statement = s(
    "notify.tenant.purge_captures",
    "DELETE merezha_notify.captures",
    "DELETE FROM merezha_notify.captures WHERE delivery_id = ANY($1::uuid[])",
);
/// Delete notifications `$1`; their deliveries and attempts cascade.
pub const TENANT_PURGE_NOTIFICATIONS: Statement = s(
    "notify.tenant.purge_notifications",
    "DELETE merezha_notify.notifications",
    "DELETE FROM merezha_notify.notifications WHERE id = ANY($1::uuid[])",
);

/// Every statement of this module.
pub const STATEMENTS: &[Statement] = &[
    TENANT_WRITE_LOCK,
    TENANT_DELETE_LOCK,
    TENANT_DELETED,
    TENANT_MARK,
    TENANTS_DELETED_EXIST,
    TENANT_PURGED,
    TENANT_PURGE_EVENTS,
    TENANT_PURGE_ENDPOINTS,
    TENANT_PURGE_PUSH,
    TENANT_INBOX_USERS,
    TENANT_PURGE_INBOX,
    TENANT_NOTIFICATIONS,
    TENANT_PURGE_PROVIDER_EVENTS,
    TENANT_PURGE_CAPTURES,
    TENANT_PURGE_NOTIFICATIONS,
    TARGET_LOCKS,
    TARGET_RECENT,
    RECLAIM_CANDIDATES,
    RECLAIM_ATTEMPTS,
    RECLAIM_SET,
    RETENTION_CANDIDATES,
    RETENTION_LOCK_DELIVERIES,
    RETENTION_NOTIFICATIONS,
    RETENTION_PROVIDER_EVENTS,
    NOTIFICATION_EXISTS,
    PREFERENCES_FOR_USERS,
    SUPPRESSIONS_FOR_ADDRESSES,
    TIMEZONES_FOR_USERS,
    NOTIFICATION_INSERT,
    DELIVERIES_INSERT,
    COUNTERS_TAKE,
    INBOX_INSERT,
    INBOX_NOTIFY,
    DELIVERY_LOCK,
    LAST_ATTEMPT,
    ATTEMPT_CHAIN,
    ATTEMPT_MESSAGE_ID,
    ATTEMPT_HISTORY,
    ATTEMPT_START,
    ATTEMPT_FINISH,
    DELIVERY_SENDING,
    DELIVERY_SET,
    DELIVERY_TOUCH,
    DELIVERY_ACCEPT,
    PREFERENCES_FOR_USER,
    SUPPRESSIONS_FOR_ADDRESS,
    TIMEZONE_FOR_USER,
    STATUS_NOTIFICATION,
    STATUS_DELIVERIES,
    REDELIVER_LOCK,
    REDELIVER_SET,
    NOTIFICATION_REOPEN,
    PREFERENCES_LIST,
    PREFERENCES_UPSERT,
    SETTINGS_GET,
    SETTINGS_UPSERT,
    SUPPRESSION_INSERT,
    SUPPRESSION_FIND,
    SUPPRESSIONS_LIST,
    SUPPRESSION_DELETE,
    INBOX_LIST,
    INBOX_UNREAD,
    INBOX_GET,
    COUNTER_BUMP,
    COUNTER_LOCK,
    INBOX_CHANGE,
    INBOX_READ_ALL,
    INBOX_NOTIFY_ONE,
    SETTLE,
    PURGE_DATA,
    PURGE_NAMES,
    INBOX_EXPIRED_USERS,
    COUNTERS_LOCK,
    INBOX_EXPIRE,
    PURGED_THROUGH,
    EVENT_DELIVERY_BY_ID,
    EVENT_RESOLVE_BY_ID,
    EVENT_RESOLVE_BY_MESSAGE,
    PROVIDER_EVENT_INSERT,
    DELIVERY_EVENT_SET,
    EVENTS_TAKE_PENDING,
];
