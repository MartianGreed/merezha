//! Every statement of the outgoing webhooks (NT-N7), registered with the
//! TE-T4 statement registry; [`STATEMENTS`] lists them for the registry
//! check. Values always travel as parameters.
//!
//! A registry is named by `COALESCE(tenant_id, '')`: the tenant id, or
//! `''` for the application-level registry (NT6), which the expression
//! index `webhook_endpoints_registry_idx` serves.
use merezha_postgres::Statement;

const fn s(name: &'static str, summary: &'static str, sql: &'static str) -> Statement {
    Statement::new(name, summary, sql)
}

/// The columns of the `Endpoint` shape (NT §7.1), in this order.
macro_rules! endpoint_columns {
    () => {
        "id, url, description, event_types, status, disabled_reason, failing_since, \
         last_success_at, revision, created_at"
    };
}

// ----- fan-out -----

/// The event row, unless `(source, source_event_id)` or the id exists.
pub const EVENT_INSERT: Statement = s(
    "notify.webhook.event_insert",
    "INSERT merezha_notify.webhook_events",
    "INSERT INTO merezha_notify.webhook_events (id, source, source_event_id, tenant_id, \
     event_type, occurred_at, body, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
     ON CONFLICT DO NOTHING RETURNING id",
);
/// The active endpoints of registry `$1` whose filter matches event type
/// `$2` (empty: every type; `prefix.*`: every type under the prefix), at
/// most `$3`.
pub const FANOUT_ENDPOINTS: Statement = s(
    "notify.webhook.fanout_endpoints",
    "SELECT merezha_notify.webhook_endpoints",
    "SELECT id FROM merezha_notify.webhook_endpoints \
     WHERE COALESCE(tenant_id, '') = $1 AND status = 'active' \
     AND (event_types = '{}' OR EXISTS (SELECT 1 FROM unnest(event_types) AS f \
       WHERE f = $2 OR (f LIKE '%.*' AND starts_with($2, left(f, -1))))) \
     ORDER BY created_at, id LIMIT $3",
);
/// Deliveries of event `$1`, one per `($3[i], $4[i])` id and endpoint.
pub const DELIVERIES_INSERT: Statement = s(
    "notify.webhook.deliveries_insert",
    "INSERT merezha_notify.webhook_deliveries",
    "INSERT INTO merezha_notify.webhook_deliveries (id, event_id, endpoint_id, status, \
     created_at, updated_at) \
     SELECT d.id, $1, d.endpoint, 'queued', $2, $2 FROM unnest($3::uuid[], $4::uuid[]) \
     AS d(id, endpoint) ON CONFLICT DO NOTHING RETURNING id, endpoint_id",
);

// ----- delivery worker -----

/// Lock delivery `$1` with what an attempt needs from its endpoint and
/// event.
pub const DELIVERY_LOCK: Statement = s(
    "notify.webhook.delivery_lock",
    "SELECT merezha_notify.webhook_deliveries",
    "SELECT d.status, d.generation, e.status, e.url, e.secret_ct, e.secret_nonce, \
     e.previous_secret_ct, e.previous_secret_nonce, e.previous_expires_at, v.body, \
     d.endpoint_id, e.key_id, e.previous_key_id \
     FROM merezha_notify.webhook_deliveries d \
     JOIN merezha_notify.webhook_endpoints e ON e.id = d.endpoint_id \
     JOIN merezha_notify.webhook_events v ON v.id = d.event_id \
     WHERE d.id = $1 FOR UPDATE OF d",
);
/// The highest attempt number of delivery `$1`, how many attempts
/// generation `$2` has, and the outcome of the last attempt.
pub const ATTEMPT_STATE: Statement = s(
    "notify.webhook.attempt_state",
    "SELECT merezha_notify.webhook_attempts",
    "SELECT COALESCE(max(attempt), 0), count(*) FILTER (WHERE generation = $2), \
     (SELECT outcome FROM merezha_notify.webhook_attempts WHERE delivery_id = $1 \
      ORDER BY attempt DESC LIMIT 1) \
     FROM merezha_notify.webhook_attempts WHERE delivery_id = $1",
);
/// A `started` attempt `$2` whose run never recorded an outcome.
pub const ATTEMPT_LOST: Statement = s(
    "notify.webhook.attempt_lost",
    "UPDATE merezha_notify.webhook_attempts",
    "UPDATE merezha_notify.webhook_attempts SET outcome = 'ambiguous', finished_at = $3, \
     error_code = 'lost_outcome' WHERE delivery_id = $1 AND attempt = $2 AND outcome = 'started'",
);
pub const ATTEMPT_START: Statement = s(
    "notify.webhook.attempt_start",
    "INSERT merezha_notify.webhook_attempts",
    "INSERT INTO merezha_notify.webhook_attempts (delivery_id, attempt, generation, started_at, \
     outcome) VALUES ($1, $2, $3, $4, 'started')",
);
pub const DELIVERY_SENDING: Statement = s(
    "notify.webhook.delivery_sending",
    "UPDATE merezha_notify.webhook_deliveries",
    "UPDATE merezha_notify.webhook_deliveries SET status = 'sending', \
     attempt_count = attempt_count + 1, next_attempt_at = NULL, updated_at = $2 WHERE id = $1",
);
/// Settle open delivery `$1` of generation `$2` as `$3` with code `$4`.
pub const DELIVERY_SETTLE: Statement = s(
    "notify.webhook.delivery_settle",
    "UPDATE merezha_notify.webhook_deliveries",
    "UPDATE merezha_notify.webhook_deliveries SET status = $3, last_error_code = $4, \
     next_attempt_at = NULL, updated_at = $5, settled_at = $5 \
     WHERE id = $1 AND generation = $2 AND status IN ('queued', 'sending', 'retrying')",
);
/// The outcome of attempt `$2`, while it is still `started`: the fence of
/// Tx B, since a run that took the delivery over marked it `ambiguous`.
pub const ATTEMPT_FINISH: Statement = s(
    "notify.webhook.attempt_finish",
    "UPDATE merezha_notify.webhook_attempts",
    "UPDATE merezha_notify.webhook_attempts SET outcome = $3, finished_at = $4, \
     http_status = $5, duration_ms = $6, error_code = $7, response_snippet = $8 \
     WHERE delivery_id = $1 AND attempt = $2 AND outcome = 'started'",
);
/// Record the delivery's state after an attempt.
pub const DELIVERY_RECORD: Statement = s(
    "notify.webhook.delivery_record",
    "UPDATE merezha_notify.webhook_deliveries",
    "UPDATE merezha_notify.webhook_deliveries SET status = $3, last_http_status = $4, \
     last_error_code = $5, next_attempt_at = $6, updated_at = $7, \
     settled_at = CASE WHEN $3::text IN ('succeeded', 'failed', 'cancelled') THEN $7::timestamptz END \
     WHERE id = $1 AND generation = $2 AND status = 'sending'",
);
/// A success clears `failing_since`.
pub const ENDPOINT_SUCCEEDED: Statement = s(
    "notify.webhook.endpoint_succeeded",
    "UPDATE merezha_notify.webhook_endpoints",
    "UPDATE merezha_notify.webhook_endpoints SET last_success_at = $2, failing_since = NULL \
     WHERE id = $1",
);
/// The first failure after a success sets `failing_since`.
pub const ENDPOINT_FAILED: Statement = s(
    "notify.webhook.endpoint_failed",
    "UPDATE merezha_notify.webhook_endpoints",
    "UPDATE merezha_notify.webhook_endpoints SET failing_since = COALESCE(failing_since, $2) \
     WHERE id = $1",
);
/// Disable active endpoint `$1` for reason `$2` at `$3`; with `$4`, only
/// while it has been failing since before `$4` (auto-disable).
pub const ENDPOINT_DISABLE: Statement = s(
    "notify.webhook.endpoint_disable",
    "UPDATE merezha_notify.webhook_endpoints",
    "UPDATE merezha_notify.webhook_endpoints SET status = 'disabled', disabled_reason = $2, \
     disabled_at = $3, updated_at = $3, revision = revision + 1 \
     WHERE id = $1 AND status = 'active' \
     AND ($4::timestamptz IS NULL OR failing_since < $4) RETURNING tenant_id, revision",
);
/// Cancel the queued and retrying deliveries of endpoint `$1`.
pub const DELIVERIES_CANCEL: Statement = s(
    "notify.webhook.deliveries_cancel",
    "UPDATE merezha_notify.webhook_deliveries",
    "UPDATE merezha_notify.webhook_deliveries SET status = 'cancelled', next_attempt_at = NULL, \
     updated_at = $2, settled_at = $2 \
     WHERE endpoint_id = $1 AND status IN ('queued', 'retrying')",
);
pub const AUDIT_INSERT: Statement = s(
    "notify.webhook.audit_insert",
    "INSERT merezha_notify.webhook_audit",
    "INSERT INTO merezha_notify.webhook_audit (endpoint_id, tenant_id, action, reason, actor, \
     revision, occurred_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
);

// ----- endpoint routes -----

/// Serialize the endpoint writes of registry `$1` until commit, so the
/// per-registry limit holds under concurrent creates.
pub const REGISTRY_LOCK: Statement = s(
    "notify.webhook.registry_lock",
    "SELECT pg_advisory_xact_lock()",
    "SELECT pg_advisory_xact_lock(hashtextextended('merezha_notify.webhooks:' || $1::text, 0))",
);
pub const REGISTRY_COUNT: Statement = s(
    "notify.webhook.registry_count",
    "SELECT merezha_notify.webhook_endpoints",
    "SELECT count(*) FROM merezha_notify.webhook_endpoints WHERE COALESCE(tenant_id, '') = $1",
);
pub const ENDPOINT_INSERT: Statement = s(
    "notify.webhook.endpoint_insert",
    "INSERT merezha_notify.webhook_endpoints",
    concat!(
        "INSERT INTO merezha_notify.webhook_endpoints (id, tenant_id, url, description, \
         event_types, secret_ct, secret_nonce, key_id, status, created_by, created_at, \
         updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $10, 'active', $8, $9, $9) RETURNING ",
        endpoint_columns!()
    ),
);
pub const ENDPOINT_LIST: Statement = s(
    "notify.webhook.endpoint_list",
    "SELECT merezha_notify.webhook_endpoints",
    concat!(
        "SELECT ",
        endpoint_columns!(),
        " FROM merezha_notify.webhook_endpoints WHERE COALESCE(tenant_id, '') = $1 \
         ORDER BY created_at, id LIMIT $2"
    ),
);
pub const ENDPOINT_GET: Statement = s(
    "notify.webhook.endpoint_get",
    "SELECT merezha_notify.webhook_endpoints",
    concat!(
        "SELECT ",
        endpoint_columns!(),
        " FROM merezha_notify.webhook_endpoints WHERE id = $1 AND COALESCE(tenant_id, '') = $2"
    ),
);
/// Lock endpoint `$1` of registry `$2`: its revision, status and tenant.
pub const ENDPOINT_LOCK: Statement = s(
    "notify.webhook.endpoint_lock",
    "SELECT merezha_notify.webhook_endpoints",
    "SELECT revision, status, tenant_id FROM merezha_notify.webhook_endpoints \
     WHERE id = $1 AND COALESCE(tenant_id, '') = $2 FOR UPDATE",
);
pub const ENDPOINT_UPDATE: Statement = s(
    "notify.webhook.endpoint_update",
    "UPDATE merezha_notify.webhook_endpoints",
    concat!(
        "UPDATE merezha_notify.webhook_endpoints SET url = $2, description = $3, \
         event_types = $4, updated_at = $5, revision = revision + 1 WHERE id = $1 RETURNING ",
        endpoint_columns!()
    ),
);
/// A new secret `$2`/`$3` sealed under key `$7`; with `$4` the current one
/// becomes the previous until `$5`, else no previous secret is kept.
pub const ENDPOINT_ROTATE: Statement = s(
    "notify.webhook.endpoint_rotate",
    "UPDATE merezha_notify.webhook_endpoints",
    "UPDATE merezha_notify.webhook_endpoints SET \
       previous_secret_ct = CASE WHEN $4::boolean THEN secret_ct END, \
       previous_secret_nonce = CASE WHEN $4::boolean THEN secret_nonce END, \
       previous_expires_at = CASE WHEN $4::boolean THEN $5::timestamptz END, \
       previous_key_id = CASE WHEN $4::boolean THEN key_id END, \
       secret_ct = $2, secret_nonce = $3, key_id = $7, unreadable_under = NULL, \
       updated_at = $6, \
       revision = revision + 1 WHERE id = $1 RETURNING revision",
);
pub const ENDPOINT_ENABLE: Statement = s(
    "notify.webhook.endpoint_enable",
    "UPDATE merezha_notify.webhook_endpoints",
    "UPDATE merezha_notify.webhook_endpoints SET status = 'active', disabled_reason = NULL, \
     disabled_at = NULL, failing_since = NULL, updated_at = $2, revision = revision + 1 \
     WHERE id = $1 AND status = 'disabled' RETURNING revision",
);
pub const ENDPOINT_DELETE: Statement = s(
    "notify.webhook.endpoint_delete",
    "DELETE merezha_notify.webhook_endpoints",
    "DELETE FROM merezha_notify.webhook_endpoints WHERE id = $1 AND COALESCE(tenant_id, '') = $2 \
     RETURNING revision, tenant_id",
);
/// One page of endpoint `$1`'s deliveries, newest first, optionally of
/// status `$2`, before the cursor `($3, $4)`, at most `$5`.
pub const DELIVERY_PAGE: Statement = s(
    "notify.webhook.delivery_page",
    "SELECT merezha_notify.webhook_deliveries",
    "SELECT d.id, v.event_type, d.status, d.attempt_count, d.last_http_status, d.created_at \
     FROM merezha_notify.webhook_deliveries d \
     JOIN merezha_notify.webhook_events v ON v.id = d.event_id \
     WHERE d.endpoint_id = $1 AND ($2::text IS NULL OR d.status = $2) \
     AND ($3::timestamptz IS NULL OR (d.created_at, d.id) < ($3, $4::uuid)) \
     ORDER BY d.created_at DESC, d.id DESC LIMIT $5",
);
/// The latest `$2` attempts of each of deliveries `$1`, oldest first.
pub const ATTEMPTS_FOR: Statement = s(
    "notify.webhook.attempts_for",
    "SELECT merezha_notify.webhook_attempts",
    "SELECT delivery_id, attempt, outcome, http_status, duration_ms, started_at FROM \
     (SELECT a.*, row_number() OVER (PARTITION BY delivery_id ORDER BY attempt DESC) AS n \
      FROM merezha_notify.webhook_attempts a WHERE delivery_id = ANY($1::uuid[])) AS latest \
     WHERE n <= $2 ORDER BY delivery_id, attempt",
);
/// The `webhook.test` deliveries of endpoint `$1` and the test events of
/// registry `$2` created after `$3`.
pub const TEST_RATE: Statement = s(
    "notify.webhook.test_rate",
    "SELECT merezha_notify.webhook_events",
    "SELECT (SELECT count(*) FROM merezha_notify.webhook_deliveries d \
       JOIN merezha_notify.webhook_events v ON v.id = d.event_id \
       WHERE d.endpoint_id = $1 AND d.created_at > $3 AND v.event_type = 'webhook.test'), \
     (SELECT count(*) FROM merezha_notify.webhook_events \
       WHERE COALESCE(tenant_id, '') = $2 AND event_type = 'webhook.test' AND created_at > $3)",
);
/// Lock delivery `$1` of endpoint `$2` in registry `$3`, with whether it
/// is open and untouched since `$4` (its job is presumed dead).
pub const REDELIVER_LOCK: Statement = s(
    "notify.webhook.redeliver_lock",
    "SELECT merezha_notify.webhook_deliveries",
    "SELECT d.status, v.body IS NULL, e.status, e.tenant_id, \
     d.updated_at < $4 AND (d.next_attempt_at IS NULL OR d.next_attempt_at < $4) \
     FROM merezha_notify.webhook_deliveries d \
     JOIN merezha_notify.webhook_events v ON v.id = d.event_id \
     JOIN merezha_notify.webhook_endpoints e ON e.id = d.endpoint_id \
     WHERE d.id = $1 AND d.endpoint_id = $2 AND COALESCE(e.tenant_id, '') = $3 FOR UPDATE OF d",
);
pub const REDELIVER_SET: Statement = s(
    "notify.webhook.redeliver_set",
    "UPDATE merezha_notify.webhook_deliveries",
    "UPDATE merezha_notify.webhook_deliveries SET generation = generation + 1, status = 'queued', \
     next_attempt_at = NULL, settled_at = NULL, updated_at = $2 WHERE id = $1 RETURNING generation",
);

// ----- maintenance -----

/// Active endpoints failing since before `$1` with at least `$2` failed
/// attempts since then, oldest first, at most `$3`.
pub const AUTO_DISABLE_CANDIDATES: Statement = s(
    "notify.webhook.auto_disable_candidates",
    "SELECT merezha_notify.webhook_endpoints",
    "SELECT e.id FROM merezha_notify.webhook_endpoints e \
     WHERE e.status = 'active' AND e.failing_since < $1 \
     AND (SELECT count(*) FROM (SELECT 1 FROM merezha_notify.webhook_attempts a \
       JOIN merezha_notify.webhook_deliveries d ON d.id = a.delivery_id \
       WHERE d.endpoint_id = e.id AND a.finished_at >= e.failing_since \
       AND a.outcome IN ('failed', 'blocked', 'ambiguous') LIMIT $2) AS f) >= $2 \
     ORDER BY e.failing_since LIMIT $3",
);
/// Null the bodies of events created before `$1`, at most `$2`.
pub const RETENTION_BODIES: Statement = s(
    "notify.webhook.retention_bodies",
    "UPDATE merezha_notify.webhook_events",
    "UPDATE merezha_notify.webhook_events SET body = NULL WHERE id IN \
     (SELECT id FROM merezha_notify.webhook_events WHERE created_at < $1 AND body IS NOT NULL \
      ORDER BY created_at LIMIT $2)",
);
/// Delete events created before `$1` with no open delivery, their
/// deliveries and attempts with them, at most `$2`.
pub const RETENTION_EVENTS: Statement = s(
    "notify.webhook.retention_events",
    "DELETE merezha_notify.webhook_events",
    "DELETE FROM merezha_notify.webhook_events WHERE id IN \
     (SELECT e.id FROM merezha_notify.webhook_events e WHERE e.created_at < $1 \
      AND NOT EXISTS (SELECT 1 FROM merezha_notify.webhook_deliveries d WHERE d.event_id = e.id \
        AND d.status IN ('queued', 'sending', 'retrying')) \
      ORDER BY e.created_at LIMIT $2)",
);
pub const RETENTION_AUDIT: Statement = s(
    "notify.webhook.retention_audit",
    "DELETE merezha_notify.webhook_audit",
    "DELETE FROM merezha_notify.webhook_audit WHERE id IN \
     (SELECT id FROM merezha_notify.webhook_audit WHERE occurred_at < $1 \
      ORDER BY occurred_at LIMIT $2)",
);
/// Drop the previous secrets whose rotation window closed by `$1`, at
/// most `$2`.
pub const PREVIOUS_EXPIRE: Statement = s(
    "notify.webhook.previous_expire",
    "UPDATE merezha_notify.webhook_endpoints",
    "UPDATE merezha_notify.webhook_endpoints SET previous_secret_ct = NULL, \
     previous_secret_nonce = NULL, previous_key_id = NULL, previous_expires_at = NULL WHERE id IN \
     (SELECT id FROM merezha_notify.webhook_endpoints WHERE previous_expires_at <= $1 LIMIT $2)",
);
/// After id `$1`, the endpoints with a secret sealed under another key
/// than `$2`, at most `$3`, except those found unreadable under the key
/// pair `$4`.
pub const STALE_SECRETS: Statement = s(
    "notify.webhook.stale_secrets",
    "SELECT merezha_notify.webhook_endpoints",
    "SELECT id, secret_ct, secret_nonce, key_id, previous_secret_ct, previous_secret_nonce, \
     previous_key_id FROM merezha_notify.webhook_endpoints \
     WHERE id > $1 AND (key_id <> $2 OR previous_key_id <> $2) \
     AND unreadable_under IS DISTINCT FROM $4 ORDER BY id LIMIT $3",
);
/// How many endpoints still hold a secret sealed under another key than
/// `$1` (readable ones), and how many were found unreadable under it.
pub const SECRET_COUNTS: Statement = s(
    "notify.webhook.secret_counts",
    "SELECT merezha_notify.webhook_endpoints",
    "SELECT count(*) FILTER (WHERE (key_id <> $1 OR previous_key_id <> $1) \
       AND (unreadable_under IS NULL OR substring(unreadable_under FROM 1 FOR 8) <> $1)), \
     count(*) FILTER (WHERE substring(unreadable_under FROM 1 FOR 8) = $1) \
     FROM merezha_notify.webhook_endpoints",
);
/// Mark endpoint `$1` unreadable under key pair `$2`, unless its secret
/// changed since it was read (`$3`).
pub const MARK_UNREADABLE: Statement = s(
    "notify.webhook.mark_unreadable",
    "UPDATE merezha_notify.webhook_endpoints",
    "UPDATE merezha_notify.webhook_endpoints SET unreadable_under = $2 \
     WHERE id = $1 AND secret_ct = $3",
);
/// Store secrets re-sealed under key `$7`, unless the endpoint's secret
/// changed since it was read (`$6`).
pub const SECRETS_RESEAL: Statement = s(
    "notify.webhook.secrets_reseal",
    "UPDATE merezha_notify.webhook_endpoints",
    "UPDATE merezha_notify.webhook_endpoints SET secret_ct = $2, secret_nonce = $3, \
     previous_secret_ct = $4, previous_secret_nonce = $5, key_id = $7::bytea, \
     previous_key_id = CASE WHEN $4::bytea IS NULL THEN NULL ELSE $7::bytea END, \
     unreadable_under = NULL WHERE id = $1 AND secret_ct = $6",
);

/// Every statement of the outgoing webhooks.
pub const STATEMENTS: &[Statement] = &[
    EVENT_INSERT,
    FANOUT_ENDPOINTS,
    DELIVERIES_INSERT,
    DELIVERY_LOCK,
    ATTEMPT_STATE,
    ATTEMPT_LOST,
    ATTEMPT_START,
    DELIVERY_SENDING,
    DELIVERY_SETTLE,
    ATTEMPT_FINISH,
    DELIVERY_RECORD,
    ENDPOINT_SUCCEEDED,
    ENDPOINT_FAILED,
    ENDPOINT_DISABLE,
    DELIVERIES_CANCEL,
    AUDIT_INSERT,
    REGISTRY_LOCK,
    REGISTRY_COUNT,
    ENDPOINT_INSERT,
    ENDPOINT_LIST,
    ENDPOINT_GET,
    ENDPOINT_LOCK,
    ENDPOINT_UPDATE,
    ENDPOINT_ROTATE,
    ENDPOINT_ENABLE,
    ENDPOINT_DELETE,
    DELIVERY_PAGE,
    ATTEMPTS_FOR,
    REDELIVER_LOCK,
    REDELIVER_SET,
    AUTO_DISABLE_CANDIDATES,
    RETENTION_BODIES,
    RETENTION_EVENTS,
    RETENTION_AUDIT,
    PREVIOUS_EXPIRE,
    STALE_SECRETS,
    SECRET_COUNTS,
    MARK_UNREADABLE,
    SECRETS_RESEAL,
    TEST_RATE,
];
