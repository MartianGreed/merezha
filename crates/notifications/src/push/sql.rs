//! Every statement of the push channel (NT-N6), registered with the TE-T4
//! statement registry; [`STATEMENTS`] lists them for the registry check.
//! Values always travel as parameters.
use merezha_postgres::Statement;

const fn s(name: &'static str, summary: &'static str, sql: &'static str) -> Statement {
    Statement::new(name, summary, sql)
}

// ----- subscription routes -----

/// Serialize the subscription writes of user `$1` until commit, so the
/// per-user limit holds under concurrent subscribes.
pub const USER_LOCK: Statement = s(
    "notify.push.user_lock",
    "SELECT pg_advisory_xact_lock()",
    "SELECT pg_advisory_xact_lock(hashtextextended('merezha_notify.push:' || $1::text, 0))",
);
/// Insert subscription `$1`, or take over the row of the same endpoint. An
/// endpoint that changes hands (another user or another tenant) gets the
/// new id `$1`, so a delivery queued for the previous owner finds no
/// subscription; the same owner keeps its id.
pub const UPSERT: Statement = s(
    "notify.push.upsert",
    "INSERT merezha_notify.push_subscriptions",
    "INSERT INTO merezha_notify.push_subscriptions AS p (id, user_id, tenant_id, endpoint, \
     endpoint_origin, p256dh, auth, vapid_key_id, label, expires_at, created_at, last_used_at) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11) \
     ON CONFLICT (endpoint) DO UPDATE SET \
       id = CASE WHEN p.user_id = EXCLUDED.user_id \
         AND p.tenant_id IS NOT DISTINCT FROM EXCLUDED.tenant_id THEN p.id ELSE EXCLUDED.id END, \
       created_at = CASE WHEN p.user_id = EXCLUDED.user_id \
         AND p.tenant_id IS NOT DISTINCT FROM EXCLUDED.tenant_id \
         THEN p.created_at ELSE EXCLUDED.created_at END, \
       user_id = EXCLUDED.user_id, tenant_id = EXCLUDED.tenant_id, \
       endpoint_origin = EXCLUDED.endpoint_origin, p256dh = EXCLUDED.p256dh, \
       auth = EXCLUDED.auth, vapid_key_id = EXCLUDED.vapid_key_id, label = EXCLUDED.label, \
       expires_at = EXCLUDED.expires_at, last_used_at = EXCLUDED.last_used_at, \
       last_success_at = CASE WHEN p.user_id = EXCLUDED.user_id THEN p.last_success_at END, \
       consecutive_failures = 0 \
     RETURNING id, created_at",
);
/// Delete the least recently used subscriptions of user `$1` beyond the
/// newest `$2` other than `$3` (the subscription just made, always kept).
pub const TRIM: Statement = s(
    "notify.push.trim",
    "DELETE merezha_notify.push_subscriptions",
    "DELETE FROM merezha_notify.push_subscriptions WHERE id IN \
     (SELECT id FROM merezha_notify.push_subscriptions WHERE user_id = $1 AND id <> $3 \
      ORDER BY last_used_at DESC, created_at DESC, id OFFSET $2) RETURNING id",
);
/// The subscriptions of user `$1` in tenant scope `$2` (NULL: those
/// without a tenant), newest first.
pub const LIST: Statement = s(
    "notify.push.list",
    "SELECT merezha_notify.push_subscriptions",
    "SELECT id, label, endpoint_origin, created_at, last_success_at \
     FROM merezha_notify.push_subscriptions \
     WHERE user_id = $1 AND (tenant_id IS NULL OR tenant_id = $2::text) \
     ORDER BY created_at DESC, id LIMIT $3",
);
/// Delete subscription `$1` of user `$2` in tenant scope `$3`.
pub const DELETE_OWN: Statement = s(
    "notify.push.delete_own",
    "DELETE merezha_notify.push_subscriptions",
    "DELETE FROM merezha_notify.push_subscriptions \
     WHERE id = $1 AND user_id = $2 AND (tenant_id IS NULL OR tenant_id = $3::text)",
);

// ----- expansion -----

/// The live subscriptions of users `$1` made with VAPID key `$2`, that a
/// notification of tenant `$3` may reach (NULL: every subscription; a
/// tenant: the subscriptions made in that tenant), at most `$5`.
pub const TARGETS: Statement = s(
    "notify.push.targets",
    "SELECT merezha_notify.push_subscriptions",
    "SELECT user_id, id FROM merezha_notify.push_subscriptions \
     WHERE user_id = ANY($1::text[]) AND vapid_key_id = $2 \
     AND ($3::text IS NULL OR tenant_id = $3::text) \
     AND (expires_at IS NULL OR expires_at > $4) \
     ORDER BY user_id, created_at, id LIMIT $5",
);

// ----- delivery worker -----

/// The subscription of push delivery `$1`, with the VAPID key it was made
/// with, and the fields of its notification the request needs, while the
/// subscription still belongs to the delivery's user, has not expired at
/// `$2` and is in the notification's tenant scope. The key is compared by
/// the worker, so a job leased by an instance on another key during a
/// rolling rotation is retried rather than skipped (issue #66).
pub const TARGET: Statement = s(
    "notify.push.target",
    "SELECT merezha_notify.push_subscriptions",
    "SELECT s.id, s.endpoint, s.p256dh, s.auth, n.collapse_key, n.priority, s.vapid_key_id \
     FROM merezha_notify.deliveries d \
     JOIN merezha_notify.notifications n ON n.id = d.notification_id \
     JOIN merezha_notify.push_subscriptions s ON s.id = d.push_subscription_id \
     WHERE d.id = $1 AND s.user_id = d.user_id \
     AND (s.expires_at IS NULL OR s.expires_at > $2) \
     AND (n.tenant_id IS NULL OR n.tenant_id = s.tenant_id)",
);
/// An accepted push.
pub const SUCCEEDED: Statement = s(
    "notify.push.succeeded",
    "UPDATE merezha_notify.push_subscriptions",
    "UPDATE merezha_notify.push_subscriptions SET last_used_at = $2, last_success_at = $2, \
     consecutive_failures = 0 WHERE id = $1",
);
/// A failed push attempt.
pub const FAILED: Statement = s(
    "notify.push.failed",
    "UPDATE merezha_notify.push_subscriptions",
    "UPDATE merezha_notify.push_subscriptions SET last_used_at = $2, \
     consecutive_failures = LEAST(consecutive_failures + 1, 1000000) WHERE id = $1",
);
/// The push service said the subscription is gone (404 or 410).
pub const GONE: Statement = s(
    "notify.push.gone",
    "DELETE merezha_notify.push_subscriptions",
    "DELETE FROM merezha_notify.push_subscriptions WHERE id = $1",
);

// ----- maintenance -----

/// One `push_expire` batch: subscriptions past `expires_at` (`$1` = now),
/// made with another VAPID key than `$2` and unused since `$6` (the
/// rotation grace: during a rolling deploy each instance keeps refreshing
/// the subscriptions of its own key, so neither key deletes the other's),
/// or with at least `$3` consecutive failures and no success since `$4`;
/// at most `$5`. Each row says why it went.
pub const EXPIRE: Statement = s(
    "notify.push.expire",
    "DELETE merezha_notify.push_subscriptions",
    "DELETE FROM merezha_notify.push_subscriptions p WHERE p.id IN \
     (SELECT id FROM merezha_notify.push_subscriptions \
      WHERE expires_at <= $1 OR (vapid_key_id <> $2 AND last_used_at < $6) \
      OR (consecutive_failures >= $3 AND COALESCE(last_success_at, created_at) < $4) \
      LIMIT $5) \
     RETURNING CASE WHEN p.vapid_key_id <> $2 AND p.last_used_at < $6 THEN 'key_changed' \
       WHEN p.expires_at <= $1 THEN 'expired' ELSE 'failures' END",
);

/// Every statement of the push channel.
pub const STATEMENTS: &[Statement] = &[
    USER_LOCK, UPSERT, TRIM, LIST, DELETE_OWN, TARGETS, TARGET, SUCCEEDED, FAILED, GONE, EXPIRE,
];
