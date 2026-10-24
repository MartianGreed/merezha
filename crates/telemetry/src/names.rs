//! The single catalog of span, metric, attribute and log event names for
//! every block (TE §1, §4; R9). Other crates reference these rows instead of
//! spelling names. Tests pin the experimental semantic-convention names
//! against schema 1.42.0, check every metric attribute against the
//! allowlist, and check every event against the naming rules.
//!
//! Adding a signal means adding its row here first; the catalog test
//! (`testing::Harness::assert_catalogued`) fails any suite that emits an
//! event this file does not list.
use opentelemetry::KeyValue;
use opentelemetry_semantic_conventions as semconv;

/// Semantic-convention keys used by the catalog. Stable ones come from
/// `opentelemetry-semantic-conventions` 0.33.1 (schema 1.42.0); experimental
/// ones are spelled out and pinned by a test.
pub mod key {
    use super::semconv::attribute as a;
    pub const HTTP_REQUEST_METHOD: &str = a::HTTP_REQUEST_METHOD;
    pub const HTTP_ROUTE: &str = a::HTTP_ROUTE;
    pub const HTTP_RESPONSE_STATUS_CODE: &str = a::HTTP_RESPONSE_STATUS_CODE;
    pub const URL_SCHEME: &str = a::URL_SCHEME;
    pub const SERVER_ADDRESS: &str = a::SERVER_ADDRESS;
    pub const CLIENT_ADDRESS: &str = a::CLIENT_ADDRESS;
    pub const ERROR_TYPE: &str = a::ERROR_TYPE;
    pub const DB_SYSTEM_NAME: &str = a::DB_SYSTEM_NAME;
    pub const DB_OPERATION_NAME: &str = a::DB_OPERATION_NAME;
    pub const DB_COLLECTION_NAME: &str = a::DB_COLLECTION_NAME;
    pub const DB_RESPONSE_STATUS_CODE: &str = a::DB_RESPONSE_STATUS_CODE;
    pub const DB_QUERY_SUMMARY: &str = a::DB_QUERY_SUMMARY;
    /// Never recorded (TE §1.5); listed so the allowlist can exclude it.
    pub const DB_QUERY_TEXT: &str = a::DB_QUERY_TEXT;
    // Experimental in 1.42.0, pinned by `experimental_names_match_semconv_1_42`.
    pub const USER_ID: &str = "user.id";
    pub const URL_TEMPLATE: &str = "url.template";
    pub const DB_CLIENT_CONNECTION_POOL_NAME: &str = "db.client.connection.pool.name";
    pub const DB_CLIENT_CONNECTION_STATE: &str = "db.client.connection.state";
    pub const MESSAGING_SYSTEM: &str = "messaging.system";
    pub const MESSAGING_OPERATION_TYPE: &str = "messaging.operation.type";
    pub const MESSAGING_DESTINATION_NAME: &str = "messaging.destination.name";
    pub const MESSAGING_MESSAGE_ID: &str = "messaging.message.id";
    // Resource keys.
    pub const SERVICE_NAME: &str = super::semconv::resource::SERVICE_NAME;
    pub const SERVICE_VERSION: &str = super::semconv::resource::SERVICE_VERSION;
    pub const SERVICE_NAMESPACE: &str = super::semconv::resource::SERVICE_NAMESPACE;
    pub const SERVICE_INSTANCE_ID: &str = super::semconv::resource::SERVICE_INSTANCE_ID;
    pub const DEPLOYMENT_ENVIRONMENT_NAME: &str =
        super::semconv::resource::DEPLOYMENT_ENVIRONMENT_NAME;

    // Merezha host keys.
    pub const OUTCOME: &str = "merezha.outcome";
    pub const REASON: &str = "merezha.reason";
    pub const MODULE: &str = "merezha.module";
    pub const PROVIDER: &str = "merezha.provider";
    pub const PROVIDER_OPERATION: &str = "merezha.provider.operation";
    pub const TELEMETRY_SIGNAL: &str = "merezha.telemetry.signal";
    pub const LINK_REASON: &str = "merezha.link.reason";
    pub const REQUEST_ID: &str = "merezha.request_id";
    pub const CORRELATION_ID: &str = "merezha.correlation_id";
    pub const CAUSATION_ID: &str = "merezha.causation_id";
    pub const MESSAGE_ID: &str = "merezha.message_id";
    pub const PRINCIPAL_KIND: &str = "merezha.principal.kind";
    pub const TENANT_ID: &str = "merezha.tenant.id";
    pub const CATALOG_GENERATION: &str = "merezha.catalog.generation";
    pub const COMMAND_TAG: &str = "merezha.command.tag";
    pub const QUERY_TAG: &str = "merezha.query.tag";
    pub const GUEST_SERVICE: &str = "merezha.guest.service";
    pub const GUEST_OPERATION: &str = "merezha.guest.operation";
    pub const GUEST_OUTCOME: &str = "merezha.guest.outcome";
    pub const GUEST_ERROR_CODE: &str = "merezha.guest.error_code";
    pub const PROJECTION_NAME: &str = "merezha.projection.name";
    pub const PROJECTION_TRIGGER: &str = "merezha.projection.trigger";
    pub const REACTOR_NAME: &str = "merezha.reactor.name";
    pub const REACTOR_EFFECT: &str = "merezha.reactor.effect";
    pub const STREAM_ID: &str = "merezha.stream.id";
    pub const EVENT_POSITION: &str = "merezha.event.position";
    pub const EVENTS_COUNT: &str = "merezha.events.count";
    pub const COMMAND_EXPECTED_VERSION: &str = "merezha.command.expected_version";
    pub const DB_STATEMENT: &str = "merezha.db.statement";
    pub const DB_POOL_WAIT: &str = "merezha.db.pool.wait";
}

/// Explicit histogram boundaries (TE §1.3).
pub mod buckets {
    /// Semantic-convention HTTP default, reused by every duration in seconds
    /// marked "HTTP buckets".
    pub const HTTP: &[f64] = &[
        0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
    ];
    pub const DB: &[f64] = &[
        0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0,
    ];
    /// "Job queue buckets".
    pub const JOB_QUEUE: &[f64] = &[
        0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0,
    ];
    pub const BODY_SIZE: &[f64] = &[256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0];
    pub const FUEL: &[f64] = &[1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10];
    pub const UTILIZATION: &[f64] = &[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0];
    pub const MEMORY: &[f64] = &[
        65536.0,
        262144.0,
        1048576.0,
        4194304.0,
        16777216.0,
        67108864.0,
        268435456.0,
    ];
    pub const ATTEMPTS: &[f64] = &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0];
    pub const PASSWORD_HASH: &[f64] = &[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0];
    pub const QUERY_ROWS: &[f64] = &[1.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0];
    pub const POPULATE_ENTRIES: &[f64] = &[1.0, 10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0];
    pub const UPLOAD_BYTES: &[f64] = &[
        1024.0,
        65536.0,
        1048576.0,
        10485760.0,
        52428800.0,
        536870912.0,
    ];
}

// ---------------------------------------------------------------------------
// Metrics (TE §1.3)
// ---------------------------------------------------------------------------

/// Instrument type of a catalog metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Instrument {
    /// Histogram with the row's explicit boundaries.
    Histogram,
    Counter,
    UpDownCounter,
    /// Observable gauge.
    Gauge,
}

/// One row of the metric catalog. `attributes` is the full set allowed on
/// the instrument; `budget` is its documented series budget (TE §1.4 rule 5).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Metric {
    pub name: &'static str,
    pub instrument: Instrument,
    pub unit: &'static str,
    pub buckets: &'static [f64],
    pub attributes: &'static [&'static str],
    pub budget: &'static str,
}

macro_rules! metrics {
    ($( $(#[$doc:meta])* $id:ident = $name:expr, $inst:ident, $unit:expr, $buckets:expr, [$($attr:expr),* $(,)?], $budget:expr; )*) => {
        /// Catalog metrics, one constant per row.
        pub mod metric {
            use super::*;
            $( $(#[$doc])* pub const $id: Metric = Metric {
                name: $name,
                instrument: Instrument::$inst,
                unit: $unit,
                buckets: $buckets,
                attributes: &[$($attr),*],
                budget: $budget,
            }; )*
        }
        /// Every catalog metric.
        pub const METRICS: &[Metric] = &[$(metric::$id),*];
    };
}

const NONE: &[f64] = &[];
const HTTP_ATTRS_SERVER: [&str; 5] = [
    key::HTTP_REQUEST_METHOD,
    key::HTTP_ROUTE,
    key::HTTP_RESPONSE_STATUS_CODE,
    key::ERROR_TYPE,
    key::URL_SCHEME,
];

metrics! {
    // HTTP edge
    HTTP_SERVER_REQUEST_DURATION = semconv::metric::HTTP_SERVER_REQUEST_DURATION, Histogram, "s", buckets::HTTP,
        [HTTP_ATTRS_SERVER[0], HTTP_ATTRS_SERVER[1], HTTP_ATTRS_SERVER[2], HTTP_ATTRS_SERVER[3], HTTP_ATTRS_SERVER[4]],
        "methods(10) x routes x statuses x error types x 2 schemes x buckets";
    HTTP_SERVER_ACTIVE_REQUESTS = "http.server.active_requests", UpDownCounter, "{request}", NONE,
        [key::HTTP_REQUEST_METHOD, key::URL_SCHEME], "methods(10) x 2 schemes";
    HTTP_SERVER_REQUEST_BODY_SIZE = "http.server.request.body.size", Histogram, "By", buckets::BODY_SIZE,
        [key::HTTP_REQUEST_METHOD, key::HTTP_ROUTE], "methods(10) x routes x buckets";
    HTTP_SERVER_REJECTED = "merezha.http.server.rejected", Counter, "{request}", NONE,
        [key::REASON], "6 reasons";
    // Guest invocation
    GUEST_INVOCATION_DURATION = "merezha.guest.invocation.duration", Histogram, "s", buckets::HTTP,
        [key::GUEST_SERVICE, key::GUEST_OPERATION, key::GUEST_OUTCOME], "services x 6 operations x 12 outcomes x buckets";
    GUEST_FUEL_CONSUMED = "merezha.guest.fuel.consumed", Histogram, "{fuel}", buckets::FUEL,
        [key::GUEST_SERVICE, key::GUEST_OPERATION], "services x 6 operations x buckets";
    GUEST_FUEL_UTILIZATION = "merezha.guest.fuel.utilization", Histogram, "1", buckets::UTILIZATION,
        [key::GUEST_SERVICE, key::GUEST_OPERATION], "services x 6 operations x buckets";
    GUEST_MEMORY_PEAK = "merezha.guest.memory.peak", Histogram, "By", buckets::MEMORY,
        [key::GUEST_SERVICE], "services x buckets";
    GUEST_INVOCATIONS_ACTIVE = "merezha.guest.invocations.active", UpDownCounter, "{invocation}", NONE,
        [], "1";
    CATALOG_GENERATION = "merezha.catalog.generation", Gauge, "{generation}", NONE, [], "1";
    CATALOG_RELOADS = "merezha.catalog.reloads", Counter, "{reload}", NONE, [key::OUTCOME], "2 outcomes";
    // Application dispatch, projections and reactors
    COMMAND_DURATION = "merezha.command.duration", Histogram, "s", buckets::HTTP,
        [key::COMMAND_TAG, key::OUTCOME], "(tags + 1) x 6 outcomes x buckets";
    QUERY_DURATION = "merezha.query.duration", Histogram, "s", buckets::HTTP,
        [key::QUERY_TAG, key::OUTCOME], "(tags + 1) x 6 outcomes x buckets";
    EVENTS_APPENDED = "merezha.events.appended", Counter, "{event}", NONE, [key::COMMAND_TAG], "tags + 1";
    COMMAND_RECEIPTS_REPLAYED = "merezha.command.receipts.replayed", Counter, "{command}", NONE,
        [key::COMMAND_TAG], "tags + 1";
    EVENTS_HEAD = "merezha.events.head", Gauge, "{position}", NONE, [], "1";
    PROJECTION_LAG_EVENTS = "merezha.projection.lag.events", Gauge, "{event}", NONE,
        [key::PROJECTION_NAME], "projections";
    PROJECTION_LAG_AGE = "merezha.projection.lag.age", Gauge, "s", NONE, [key::PROJECTION_NAME], "projections";
    PROJECTION_CATCH_UP_DURATION = "merezha.projection.catch_up.duration", Histogram, "s", buckets::HTTP,
        [key::PROJECTION_NAME, key::PROJECTION_TRIGGER, key::OUTCOME], "projections x 5 triggers x 6 outcomes x buckets";
    PROJECTION_EVENTS_PROCESSED = "merezha.projection.events.processed", Counter, "{event}", NONE,
        [key::PROJECTION_NAME], "projections";
    PROJECTION_LOCK_CONTENDED = "merezha.projection.lock.contended", Counter, "{attempt}", NONE,
        [key::PROJECTION_NAME], "projections";
    PROJECTION_LOCK_HELD = "merezha.projection.lock.held", Gauge, "s", NONE, [key::PROJECTION_NAME], "projections";
    PROJECTION_WARM = "merezha.projection.warm", Gauge, "1", NONE, [key::PROJECTION_NAME], "projections";
    PROJECTION_REBUILD_REMAINING = "merezha.projection.rebuild.remaining", Gauge, "{event}", NONE,
        [key::PROJECTION_NAME], "projections";
    REACTOR_LAG_EVENTS = "merezha.reactor.lag.events", Gauge, "{event}", NONE, [key::REACTOR_NAME], "reactors";
    REACTOR_LAG_AGE = "merezha.reactor.lag.age", Gauge, "s", NONE, [key::REACTOR_NAME], "reactors";
    REACTOR_LOCK_CONTENDED = "merezha.reactor.lock.contended", Counter, "{attempt}", NONE,
        [key::REACTOR_NAME], "reactors";
    REACTOR_LOCK_HELD = "merezha.reactor.lock.held", Gauge, "s", NONE, [key::REACTOR_NAME], "reactors";
    REACTOR_BATCH_DURATION = "merezha.reactor.batch.duration", Histogram, "s", buckets::HTTP,
        [key::REACTOR_NAME, key::OUTCOME], "reactors x 3 outcomes x buckets";
    REACTOR_EFFECTS = "merezha.reactor.effects", Counter, "{effect}", NONE,
        [key::REACTOR_NAME, key::REACTOR_EFFECT, key::OUTCOME], "reactors x 3 effects x 2 outcomes";
    REACTOR_HALTED = "merezha.reactor.halted", Gauge, "1", NONE, [key::REACTOR_NAME], "reactors";
    REACTOR_SKIPPED = "merezha.reactor.skipped", Counter, "{event}", NONE,
        [key::REACTOR_NAME, key::REASON], "reactors x 3 reasons";
    // PostgreSQL
    DB_CLIENT_OPERATION_DURATION = semconv::metric::DB_CLIENT_OPERATION_DURATION, Histogram, "s", buckets::DB,
        [key::DB_SYSTEM_NAME, key::DB_OPERATION_NAME, key::DB_COLLECTION_NAME, key::DB_RESPONSE_STATUS_CODE, key::ERROR_TYPE, key::MODULE],
        "registered statements x SQLSTATEs seen x modules x buckets";
    DB_CLIENT_CONNECTION_COUNT = "db.client.connection.count", Gauge, "{connection}", NONE,
        [key::DB_CLIENT_CONNECTION_POOL_NAME, key::DB_CLIENT_CONNECTION_STATE], "pools x 2 states";
    DB_CLIENT_CONNECTION_MAX = "db.client.connection.max", Gauge, "{connection}", NONE,
        [key::DB_CLIENT_CONNECTION_POOL_NAME], "pools";
    DB_CLIENT_CONNECTION_PENDING_REQUESTS = "db.client.connection.pending_requests", Gauge, "{request}", NONE,
        [key::DB_CLIENT_CONNECTION_POOL_NAME], "pools";
    DB_CLIENT_CONNECTION_WAIT_TIME = "db.client.connection.wait_time", Histogram, "s", buckets::DB,
        [key::DB_CLIENT_CONNECTION_POOL_NAME], "pools x buckets";
    DB_CLIENT_CONNECTION_TIMEOUTS = "db.client.connection.timeouts", Counter, "{timeout}", NONE,
        [key::DB_CLIENT_CONNECTION_POOL_NAME], "pools";
    DB_MIGRATIONS_PENDING = "merezha.db.migrations.pending", Gauge, "{migration}", NONE, [key::MODULE], "modules";
    DB_LISTENER_RECONNECTS = "merezha.db.listener.reconnects", Counter, "{reconnect}", NONE, [], "1";
    // Jobs and outbox
    JOB_ENQUEUED = "merezha.job.enqueued", Counter, "{job}", NONE, ["job.queue", "job.kind"], "queues x kinds";
    JOB_DURATION = "merezha.job.duration", Histogram, "s", buckets::HTTP,
        ["job.queue", "job.kind", "job.outcome", "job.classification"], "queues x kinds x 5 outcomes x 4 classifications x buckets";
    JOB_QUEUE_LATENCY = "merezha.job.queue_latency", Histogram, "s", buckets::JOB_QUEUE, ["job.queue"], "queues x buckets";
    JOB_ATTEMPTS = "merezha.job.attempts", Histogram, "{attempt}", buckets::ATTEMPTS,
        ["job.queue", "job.kind", "job.outcome"], "queues x kinds x 5 outcomes x buckets";
    JOB_DEAD_LETTERED = "merezha.job.dead_lettered", Counter, "{job}", NONE, ["job.queue", "job.kind"], "queues x kinds";
    JOB_REPLAYED = "merezha.job.replayed", Counter, "{job}", NONE, ["job.queue"], "queues";
    JOB_CANCELLED = "merezha.job.cancelled", Counter, "{job}", NONE, ["job.queue"], "queues";
    JOB_LEASE_EXPIRED = "merezha.job.lease_expired", Counter, "{job}", NONE, ["job.queue"], "queues";
    JOB_COUNT = "merezha.job.count", Gauge, "{job}", NONE, ["job.queue", "job.state"], "queues x 4 states";
    JOB_LAG = "merezha.job.lag", Gauge, "s", NONE, ["job.queue"], "queues";
    JOB_SCHEDULE_FIRED = "merezha.job.schedule.fired", Counter, "{run}", NONE, ["job.schedule"], "schedules (at most 256)";
    OUTBOX_APPENDED = "merezha.outbox.appended", Counter, "{record}", NONE, [key::MODULE, "job.record_kind"], "modules x 2 kinds";
    OUTBOX_RELAYED = "merezha.outbox.relayed", Counter, "{record}", NONE, [key::MODULE], "modules";
    OUTBOX_UNROUTED = "merezha.outbox.unrouted", Counter, "{record}", NONE, [key::MODULE], "modules";
    OUTBOX_QUARANTINED = "merezha.outbox.quarantined", Counter, "{record}", NONE, [key::MODULE, key::ERROR_TYPE],
        "modules x quarantine codes";
    OUTBOX_PENDING = "merezha.outbox.pending", Gauge, "{record}", NONE, [], "1";
    OUTBOX_LAG = "merezha.outbox.lag", Gauge, "s", NONE, [], "1";
    // Outbound HTTP
    HTTP_CLIENT_REQUEST_DURATION = semconv::metric::HTTP_CLIENT_REQUEST_DURATION, Histogram, "s", buckets::HTTP,
        [key::HTTP_REQUEST_METHOD, key::SERVER_ADDRESS, key::HTTP_RESPONSE_STATUS_CODE, key::ERROR_TYPE, key::PROVIDER, key::PROVIDER_OPERATION],
        "endpoints x statuses x error types x buckets";
    // Identity
    IDENTITY_LOGIN_ATTEMPTS = "merezha.identity.login.attempts", Counter, "{attempt}", NONE,
        ["identity.method", "identity.outcome", "identity.provider"], "4 methods x 7 outcomes x (providers + 1)";
    IDENTITY_AUTHENTICATIONS = "merezha.identity.authentications", Counter, "{request}", NONE,
        ["identity.credential", "identity.outcome"], "3 credentials x 4 outcomes";
    IDENTITY_SESSIONS_CREATED = "merezha.identity.sessions.created", Counter, "{session}", NONE,
        ["identity.method"], "4 methods";
    IDENTITY_SESSIONS_REVOKED = "merezha.identity.sessions.revoked", Counter, "{session}", NONE,
        ["identity.reason"], "4 reasons";
    IDENTITY_SESSIONS_ACTIVE = "merezha.identity.sessions.active", Gauge, "{session}", NONE, [], "1";
    IDENTITY_TOKENS_ISSUED = "merezha.identity.tokens.issued", Counter, "{token}", NONE, ["identity.purpose"], "5 purposes";
    IDENTITY_RATE_LIMITED = "merezha.identity.rate_limited", Counter, "{request}", NONE, ["identity.policy"], "policies";
    IDENTITY_PASSWORD_HASH_DURATION = "merezha.identity.password_hash.duration", Histogram, "s", buckets::PASSWORD_HASH,
        [], "buckets";
    IDENTITY_PASSWORD_HASH_QUEUE_WAIT = "merezha.identity.password_hash.queue_wait", Histogram, "s", buckets::PASSWORD_HASH,
        [], "buckets";
    IDENTITY_AUTHZ_CACHE = "merezha.identity.authz_cache", Counter, "{lookup}", NONE, ["identity.result"], "2 results";
    IDENTITY_AUDIT_WRITE_FAILURES = "merezha.identity.audit.write_failures", Counter, "{write}", NONE, [], "1";
    // Notifications
    NOTIFICATION_REQUESTS = "merezha.notification.requests", Counter, "{request}", NONE,
        ["notify.source", "notify.topic", "notify.outcome"], "sources x topics x 3 outcomes";
    NOTIFICATION_DELIVERIES = "merezha.notification.deliveries", Counter, "{delivery}", NONE,
        ["notify.channel", "notify.topic", "notify.status", "notify.reason"], "topics x 4 channels x 9 statuses x reasons";
    NOTIFICATION_ATTEMPTS = "merezha.notification.attempts", Counter, "{attempt}", NONE,
        ["notify.channel", "notify.provider", "notify.outcome"], "4 channels x 4 providers x 6 outcomes";
    NOTIFICATION_DELIVERY_DURATION = "merezha.notification.delivery.duration", Histogram, "s", buckets::HTTP,
        ["notify.channel", "notify.provider", "notify.outcome"], "4 channels x 4 providers x 6 outcomes x buckets";
    NOTIFICATION_LATENCY = "merezha.notification.latency", Histogram, "s", buckets::JOB_QUEUE,
        ["notify.channel"], "4 channels x buckets";
    NOTIFICATION_PROVIDER_EVENTS = "merezha.notification.provider_events", Counter, "{event}", NONE,
        ["notify.provider", "notify.channel", "notify.event", "notify.correlated"], "providers x channels x events x 2";
    NOTIFICATION_PROVIDER_ACCOUNT_ERRORS = "merezha.notification.provider_account_errors", Counter, "{error}", NONE,
        ["notify.provider", "notify.channel"], "providers x channels";
    NOTIFICATION_RENDER_FAILURES = "merezha.notification.render_failures", Counter, "{render}", NONE,
        ["notify.channel", "notify.topic"], "4 channels x topics";
    NOTIFICATION_SMS_SEGMENTS = "merezha.notification.sms_segments", Counter, "{segment}", NONE,
        ["notify.encoding"], "2 encodings";
    NOTIFICATION_PUSH_SUBSCRIPTIONS_REMOVED = "merezha.notification.push_subscriptions_removed", Counter, "{subscription}", NONE,
        ["notify.reason"], "5 reasons";
    NOTIFICATION_EGRESS_BLOCKED = "merezha.notification.egress_blocked", Counter, "{request}", NONE,
        ["notify.target"], "2 targets";
    NOTIFICATION_DATA_PURGED = "merezha.notification.data_purged", Counter, "{row}", NONE, [], "1";
    INBOX_SSE_CONNECTIONS = "merezha.inbox.sse.connections", UpDownCounter, "{connection}", NONE, [], "1";
    INBOX_SSE_EVENTS = "merezha.inbox.sse.events", Counter, "{event}", NONE, ["notify.event"], "4 events";
    INBOX_SSE_REJECTED = "merezha.inbox.sse.rejected", Counter, "{connection}", NONE, ["notify.reason"], "3 reasons";
    INBOX_LISTEN_RECONNECTS = "merezha.inbox.listen.reconnects", Counter, "{reconnect}", NONE, [], "1";
    WEBHOOK_DELIVERIES = "merezha.webhook.deliveries", Counter, "{delivery}", NONE, ["notify.outcome"], "3 outcomes";
    WEBHOOK_ATTEMPTS = "merezha.webhook.attempts", Counter, "{attempt}", NONE, ["notify.class"], "8 classes";
    WEBHOOK_ENDPOINTS_DISABLED = "merezha.webhook.endpoints_disabled", Counter, "{endpoint}", NONE,
        ["notify.reason"], "3 reasons";
    // Content
    CONTENT_DELIVERY_NOT_MODIFIED = "merezha.content.delivery.not_modified", Counter, "{response}", NONE,
        ["content.type"], "types";
    CONTENT_REVISION_CONFLICTS = "merezha.content.revision_conflicts", Counter, "{conflict}", NONE, ["content.type"], "types";
    CONTENT_MIGRATION_ROWS = "merezha.content.migration.rows", Counter, "{row}", NONE, ["content.type"], "types";
    CONTENT_QUERY_ROWS = "merezha.content.query.rows", Histogram, "{row}", buckets::QUERY_ROWS,
        ["content.type", "content.stage"], "types x 2 stages x buckets";
    CONTENT_POPULATE_ENTRIES = "merezha.content.populate.entries", Histogram, "{entry}", buckets::POPULATE_ENTRIES,
        ["content.type"], "types x buckets";
    CONTENT_ENTRIES_SAVED = "merezha.content.entries.saved", Counter, "{entry}", NONE, ["content.type"], "types";
    CONTENT_ENTRIES_PUBLISHED = "merezha.content.entries.published", Counter, "{entry}", NONE, ["content.type"], "types";
    CONTENT_ENTRIES_UNPUBLISHED = "merezha.content.entries.unpublished", Counter, "{entry}", NONE, ["content.type"], "types";
    CONTENT_VALIDATION_FAILURES = "merezha.content.validation.failures", Counter, "{failure}", NONE,
        ["content.type", "content.code"], "types x registered detail codes";
    CONTENT_SCHEDULE_LAG = "merezha.content.schedule.lag", Histogram, "s", buckets::JOB_QUEUE, ["content.action"], "2 actions x buckets";
    CONTENT_UPLOAD_BYTES = "merezha.content.upload.bytes", Histogram, "By", buckets::UPLOAD_BYTES, ["content.driver"], "2 drivers x buckets";
    CONTENT_SCHEDULE_FAILURES = "merezha.content.schedule.failures", Counter, "{action}", NONE, ["content.code"], "codes";
    CONTENT_UPLOAD_REJECTED = "merezha.content.upload.rejected", Counter, "{upload}", NONE, ["content.code"], "codes";
    CONTENT_BLOB_DURATION = "merezha.content.blob.duration", Histogram, "s", buckets::HTTP,
        ["content.driver", "content.operation", "content.outcome"], "2 drivers x 5 operations x 3 outcomes x buckets";
    CONTENT_BLOBS_CLEANED = "merezha.content.blobs.cleaned", Counter, "{blob}", NONE, ["content.driver"], "2 drivers";
    // Telemetry self-observation
    TELEMETRY_EXPORTED = "merezha.telemetry.exported", Counter, "{item}", NONE,
        [key::TELEMETRY_SIGNAL, key::OUTCOME], "3 signals x 2 outcomes";
    TELEMETRY_DROPPED = "merezha.telemetry.dropped", Counter, "{item}", NONE,
        [key::TELEMETRY_SIGNAL, key::REASON], "4 signals x 3 reasons";
    TELEMETRY_ATTRIBUTES_DROPPED = "merezha.telemetry.attributes.dropped", Counter, "{attribute}", NONE,
        [key::TELEMETRY_SIGNAL], "3 signals";
}

/// The catalog row named `name`.
pub fn metric(name: &str) -> Option<&'static Metric> {
    METRICS.iter().find(|m| m.name == name)
}

// ---------------------------------------------------------------------------
// Spans (TE §1.2)
// ---------------------------------------------------------------------------

/// Span kind of a catalog span.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Server,
    Client,
    Producer,
    Consumer,
    Internal,
}

/// One row of the span catalog. `{...}` placeholders take closed-set values
/// (routes, tags, names, queues) only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpanName {
    pub pattern: &'static str,
    pub kind: Kind,
}

/// Fixed span names.
pub mod span {
    /// Root of the startup sequence.
    pub const STARTUP: &str = "startup";
    pub const CATALOG_LOAD: &str = "catalog.load";
    pub const RELAY_OUTBOX: &str = "relay outbox";
    pub const NOTIFY_SSE: &str = "notify sse";
}

/// Every catalog span pattern.
pub const SPANS: &[SpanName] = &[
    SpanName {
        pattern: "{METHOD} {http.route}",
        kind: Kind::Server,
    },
    SpanName {
        pattern: "{METHOD}",
        kind: Kind::Server,
    },
    SpanName {
        pattern: span::STARTUP,
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "migrate {module}",
        kind: Kind::Internal,
    },
    SpanName {
        pattern: span::CATALOG_LOAD,
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "module.start {name}",
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "command {tag}",
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "query {tag}",
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "projection {name}",
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "react {reactor}",
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "guest {service} {operation}",
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "pipeline {id}",
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "transaction {module}",
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "{db.query.summary}",
        kind: Kind::Client,
    },
    SpanName {
        pattern: "send {queue}",
        kind: Kind::Producer,
    },
    SpanName {
        pattern: "send {topic}",
        kind: Kind::Producer,
    },
    SpanName {
        pattern: span::RELAY_OUTBOX,
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "process {queue}",
        kind: Kind::Consumer,
    },
    SpanName {
        pattern: "{METHOD} {url.template}",
        kind: Kind::Client,
    },
    SpanName {
        pattern: "identity {operation}",
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "notify {step}",
        kind: Kind::Internal,
    },
    SpanName {
        pattern: span::NOTIFY_SSE,
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "content {operation}",
        kind: Kind::Internal,
    },
    SpanName {
        pattern: "admin {operation}",
        kind: Kind::Internal,
    },
];

// ---------------------------------------------------------------------------
// Attributes (TE §1.5): the allowlist table
// ---------------------------------------------------------------------------

/// Which privacy switch gates a key (`[telemetry.privacy]`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gate {
    Always,
    TenantId,
    SubjectId,
    ClientAddress,
}

/// How a row matches keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Match {
    Exact(&'static str),
    /// Every key starting with the prefix, minus [`EXCLUDED`].
    Prefix(&'static str),
}

/// One row of the allowlist: where a key may appear.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Attribute {
    pub key: Match,
    pub span: bool,
    /// On the instruments that list it (§1.3) only.
    pub metric: bool,
    pub log: bool,
    /// Only on span links.
    pub links_only: bool,
    pub gate: Gate,
}
const fn row(key: &'static str, span: bool, metric: bool, log: bool) -> Attribute {
    Attribute {
        key: Match::Exact(key),
        span,
        metric,
        log,
        links_only: false,
        gate: Gate::Always,
    }
}
const fn family(prefix: &'static str, log: bool) -> Attribute {
    Attribute {
        key: Match::Prefix(prefix),
        span: true,
        metric: true,
        log,
        links_only: false,
        gate: Gate::Always,
    }
}
const fn gated(key: &'static str, gate: Gate) -> Attribute {
    Attribute {
        key: Match::Exact(key),
        span: true,
        metric: false,
        log: true,
        links_only: false,
        gate,
    }
}
/// Keys no row admits, whatever the prefix: SQL text and parameters.
pub const EXCLUDED: &[&str] = &[key::DB_QUERY_TEXT, "db.query.parameter."];

/// The allowlist (TE §1.5), the only source of truth for exported
/// attribute keys. A key not in the table is dropped at export.
pub const ATTRIBUTES: &[Attribute] = &[
    // semconv HTTP keys: spans and metrics, never logs.
    row(key::HTTP_REQUEST_METHOD, true, true, false),
    row(key::HTTP_ROUTE, true, true, false),
    row(key::HTTP_RESPONSE_STATUS_CODE, true, true, false),
    row(key::URL_SCHEME, true, true, false),
    row(key::SERVER_ADDRESS, true, true, false),
    row(key::URL_TEMPLATE, true, false, false),
    row(key::ERROR_TYPE, true, true, true),
    // Host keys with closed values.
    row(key::OUTCOME, true, true, true),
    row(key::REASON, true, true, true),
    row(key::MODULE, true, true, true),
    row(key::PROVIDER, true, true, true),
    row(key::PROVIDER_OPERATION, true, true, true),
    row(key::TELEMETRY_SIGNAL, true, true, true),
    row(key::COMMAND_TAG, true, true, true),
    row(key::QUERY_TAG, true, true, true),
    row(key::CATALOG_GENERATION, true, true, true),
    Attribute {
        key: Match::Exact(key::LINK_REASON),
        span: false,
        metric: false,
        log: false,
        links_only: true,
        gate: Gate::Always,
    },
    // Identifiers: spans and logs, never metrics.
    row(key::REQUEST_ID, true, false, true),
    row(key::CORRELATION_ID, true, false, true),
    row(key::CAUSATION_ID, true, false, true),
    row(key::MESSAGE_ID, true, false, true),
    row(key::STREAM_ID, true, false, true),
    row(key::EVENT_POSITION, true, false, true),
    row(key::EVENTS_COUNT, true, false, true),
    row(key::COMMAND_EXPECTED_VERSION, true, false, true),
    row(key::PRINCIPAL_KIND, true, false, true),
    gated(key::TENANT_ID, Gate::TenantId),
    gated(key::USER_ID, Gate::SubjectId),
    gated(key::CLIENT_ADDRESS, Gate::ClientAddress),
    // Runtime and application families.
    row(key::GUEST_ERROR_CODE, true, false, false),
    family("merezha.guest.", true),
    family("merezha.projection.", true),
    family("merezha.reactor.", true),
    // Statement registry: spans and listed metrics, never logs.
    family("db.", false),
    family("merezha.db.", false),
    // Jobs.
    row(key::MESSAGING_SYSTEM, true, false, true),
    row(key::MESSAGING_OPERATION_TYPE, true, false, true),
    row(key::MESSAGING_DESTINATION_NAME, true, false, true),
    row(key::MESSAGING_MESSAGE_ID, true, false, true),
    row("job.queue", true, true, true),
    row("job.kind", true, true, true),
    row("job.outcome", true, true, true),
    row("job.classification", true, true, true),
    row("job.state", true, true, true),
    row("job.schedule", true, true, true),
    row("job.record_kind", true, true, true),
    row("job.id", true, false, true),
    row("job.attempt", true, false, true),
    row("job.max_attempts", true, false, true),
    row("job.code", true, false, true),
    // Identity.
    row("identity.method", true, true, true),
    row("identity.outcome", true, true, true),
    row("identity.provider", true, true, true),
    row("identity.credential", true, true, true),
    row("identity.reason", true, true, true),
    row("identity.purpose", true, true, true),
    row("identity.policy", true, true, true),
    row("identity.result", true, true, true),
    row("identity.error_code", true, false, true),
    row("identity.permission", true, false, true),
    // Notifications.
    row("notify.source", true, true, true),
    row("notify.topic", true, true, true),
    row("notify.outcome", true, true, true),
    row("notify.channel", true, true, true),
    row("notify.status", true, true, true),
    row("notify.reason", true, true, true),
    row("notify.provider", true, true, true),
    row("notify.event", true, true, true),
    row("notify.correlated", true, true, true),
    row("notify.encoding", true, true, true),
    row("notify.target", true, true, true),
    row("notify.class", true, true, true),
    row("notify.notification_id", true, false, true),
    row("notify.delivery_id", true, false, true),
    row("notify.webhook_delivery_id", true, false, true),
    row("notify.endpoint_id", true, false, true),
    row("notify.attempt", true, false, true),
    row("notify.generation", true, false, true),
    row("notify.target_count", true, false, true),
    row("notify.locale", true, false, true),
    row("notify.resumed", true, false, true),
    // Content.
    row("content.type", true, true, true),
    row("content.stage", true, true, true),
    row("content.code", true, true, true),
    row("content.action", true, true, true),
    row("content.driver", true, true, true),
    row("content.operation", true, true, true),
    row("content.outcome", true, true, true),
    row("content.entry_id", true, false, true),
    row("content.asset_id", true, false, true),
    row("content.revision", true, false, true),
    row("content.locale", true, false, true),
    row("content.job", true, false, true),
];

/// The allowlist row for `key`, if any. Exact rows win over families;
/// [`EXCLUDED`] keys have none.
pub fn attribute(key: &str) -> Option<&'static Attribute> {
    if EXCLUDED
        .iter()
        .any(|x| key == *x || (x.ends_with('.') && key.starts_with(x)))
    {
        return None;
    }
    ATTRIBUTES
        .iter()
        .find(|a| matches!(a.key, Match::Exact(k) if k == key))
        .or_else(|| {
            ATTRIBUTES
                .iter()
                .find(|a| matches!(a.key, Match::Prefix(p) if key.starts_with(p)))
        })
}

// ---------------------------------------------------------------------------
// Typed attribute constructors (TE §1.5 wall 1)
// ---------------------------------------------------------------------------

/// `merezha.outcome` values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Accepted,
    Replayed,
    Rejected,
    Denied,
    Conflict,
    Failed,
    Halted,
    InvalidManifest,
}
impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Accepted => "accepted",
            Self::Replayed => "replayed",
            Self::Rejected => "rejected",
            Self::Denied => "denied",
            Self::Conflict => "conflict",
            Self::Failed => "failed",
            Self::Halted => "halted",
            Self::InvalidManifest => "invalid_manifest",
        }
    }
}
/// `merezha.reason` values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    Overloaded,
    BodyLimit,
    Timeout,
    Starting,
    Draining,
    Cors,
    Admin,
    OnErrorSkip,
    SkipToHead,
    QueueFull,
    ExportFailed,
    Shutdown,
}
impl Reason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Overloaded => "overloaded",
            Self::BodyLimit => "body_limit",
            Self::Timeout => "timeout",
            Self::Starting => "starting",
            Self::Draining => "draining",
            Self::Cors => "cors",
            Self::Admin => "admin",
            Self::OnErrorSkip => "on_error_skip",
            Self::SkipToHead => "skip_to_head",
            Self::QueueFull => "queue_full",
            Self::ExportFailed => "export_failed",
            Self::Shutdown => "shutdown",
        }
    }
}
/// `merezha.telemetry.signal` values: the OTLP signals plus the stderr
/// writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TelemetrySignal {
    Traces,
    Metrics,
    Logs,
    Stderr,
}
impl TelemetrySignal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Traces => "traces",
            Self::Metrics => "metrics",
            Self::Logs => "logs",
            Self::Stderr => "stderr",
        }
    }
}

/// Typed attribute constructors. No public API in this crate takes an
/// arbitrary key and value; later slices add constructors here for their
/// closed sets (`route(&MatchedPath)` lands with the edge, T2).
pub mod attr {
    use super::*;
    pub fn outcome(outcome: Outcome) -> KeyValue {
        KeyValue::new(key::OUTCOME, outcome.as_str())
    }
    pub fn reason(reason: Reason) -> KeyValue {
        KeyValue::new(key::REASON, reason.as_str())
    }
    pub fn signal(signal: TelemetrySignal) -> KeyValue {
        KeyValue::new(key::TELEMETRY_SIGNAL, signal.as_str())
    }
}

// ---------------------------------------------------------------------------
// Log events (TE §4)
// ---------------------------------------------------------------------------

/// A field of a catalog event: its short JSON name and the attribute key
/// it carries in OTLP log records (`None`: stderr only).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: &'static str,
    pub key: Option<&'static str>,
    /// The value is a JSON document written verbatim into the stderr line
    /// (`startup.failed` issues). Never bridged.
    pub json: bool,
}
const fn f(name: &'static str, key: &'static str) -> Field {
    Field {
        name,
        key: Some(key),
        json: false,
    }
}
const fn local(name: &'static str) -> Field {
    Field {
        name,
        key: None,
        json: false,
    }
}

/// One row of the event catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Event {
    pub name: &'static str,
    /// Every level the event may be emitted at.
    pub levels: &'static [tracing::Level],
    pub fields: &'static [Field],
    /// Bridged to OTLP logs. `false`: stderr only.
    pub export: bool,
}
impl Event {
    pub fn field(&self, name: &str) -> Option<&'static Field> {
        self.fields.iter().find(|f| f.name == name)
    }
}

use tracing::Level;
const ERROR: &[Level] = &[Level::ERROR];
const WARN: &[Level] = &[Level::WARN];
const INFO: &[Level] = &[Level::INFO];
const DEBUG: &[Level] = &[Level::DEBUG];
const DEBUG_WARN: &[Level] = &[Level::DEBUG, Level::WARN];
const DEBUG_WARN_ERROR: &[Level] = &[Level::DEBUG, Level::WARN, Level::ERROR];
const DEBUG_INFO_ERROR: &[Level] = &[Level::DEBUG, Level::INFO, Level::ERROR];
const WARN_ERROR: &[Level] = &[Level::WARN, Level::ERROR];
const INFO_WARN: &[Level] = &[Level::INFO, Level::WARN];

const fn ev(name: &'static str, levels: &'static [Level], fields: &'static [Field]) -> Event {
    Event {
        name,
        levels,
        fields,
        export: true,
    }
}

const CODE: Field = f("code", key::ERROR_TYPE);
const MODULE: Field = f("module", key::MODULE);
const GENERATION: Field = f("generation", key::CATALOG_GENERATION);
const REQUEST_ID: Field = f("request_id", key::REQUEST_ID);
const DURATION_MS: Field = local("duration_ms");
const VERSION: Field = local("version");
const PROJECTION: Field = f("projection", key::PROJECTION_NAME);
const REACTOR: Field = f("reactor", key::REACTOR_NAME);
const JOB_ID: Field = f("job_id", "job.id");
const QUEUE: Field = f("queue", "job.queue");
const KIND: Field = f("kind", "job.kind");
const ATTEMPT: Field = f("attempt", "job.attempt");
const TOPIC: Field = f("topic", "notify.topic");
const CHANNEL: Field = f("channel", "notify.channel");
const N_PROVIDER: Field = f("provider", "notify.provider");
const DELIVERY_ID: Field = f("delivery_id", "notify.delivery_id");
const DRAIN_MS: Field = local("drain_ms");

/// The event catalog (TE §4). Kernel events first, then block events.
pub const EVENTS: &[Event] = &[
    ev(
        "startup.failed",
        ERROR,
        &[
            CODE,
            Field {
                name: "issues",
                key: None,
                json: true,
            },
            local("usage"),
        ],
    ),
    ev("startup.completed", INFO, &[DURATION_MS, GENERATION]),
    ev(
        "listener.ready",
        INFO,
        &[local("address"), GENERATION, local("app"), local("modules")],
    ),
    ev("listener.draining", INFO, &[]),
    ev("listener.stopped", INFO, &[CODE]),
    ev("listener.drain_timeout", WARN, &[DRAIN_MS]),
    ev("registry.reloaded", INFO, &[GENERATION, CODE]),
    ev("registry.reload_failed", WARN, &[GENERATION, CODE]),
    ev(
        "invocation.completed",
        DEBUG_WARN_ERROR,
        &[
            REQUEST_ID,
            GENERATION,
            DURATION_MS,
            local("input_bytes"),
            local("output_bytes"),
            f("code", key::GUEST_OUTCOME),
        ],
    ),
    ev(
        "http.request.completed",
        DEBUG_INFO_ERROR,
        &[
            local("method"),
            local("route"),
            local("status"),
            DURATION_MS,
            REQUEST_ID,
            local("trace_id"),
        ],
    ),
    ev(
        "command.completed",
        DEBUG_WARN,
        &[
            f("tag", key::COMMAND_TAG),
            f("outcome", key::OUTCOME),
            CODE,
            DURATION_MS,
        ],
    ),
    ev(
        "query.completed",
        DEBUG_WARN,
        &[
            f("tag", key::QUERY_TAG),
            f("outcome", key::OUTCOME),
            CODE,
            DURATION_MS,
        ],
    ),
    ev(
        "projection.caught_up",
        DEBUG,
        &[PROJECTION, local("from"), local("to")],
    ),
    ev(
        "projection.failed",
        WARN,
        &[PROJECTION, local("from"), local("to"), CODE],
    ),
    ev("migration.applied", INFO, &[MODULE, VERSION, local("name")]),
    ev("migration.pending", WARN, &[MODULE, VERSION]),
    ev(
        "migration.checksum_mismatch",
        ERROR,
        &[CODE, MODULE, VERSION],
    ),
    ev(
        "migration.checked",
        INFO,
        &[local("mode"), local("applied"), local("pending")],
    ),
    ev("migration.refused", ERROR, &[CODE, MODULE, VERSION]),
    ev("migration.invalid_index_dropped", WARN, &[MODULE, VERSION]),
    ev("db.connected", INFO, &[local("pool_size")]),
    ev("db.pool_timeout", WARN, &[CODE]),
    ev("db.unavailable", ERROR, &[CODE]),
    ev("db.listener_lost", WARN, &[CODE]),
    ev("db.listener_restored", INFO, &[CODE]),
    ev(
        "outbound.failed",
        WARN,
        &[
            f("provider", key::PROVIDER),
            f("operation", key::PROVIDER_OPERATION),
            CODE,
        ],
    ),
    // Export failures never re-enter the exporter.
    Event {
        name: "telemetry.export_failed",
        levels: WARN,
        fields: &[f("signal", key::TELEMETRY_SIGNAL), CODE, local("dropped")],
        export: false,
    },
    ev("telemetry.otel_env_ignored", INFO, &[local("names")]),
    ev("health.ready", INFO, &[]),
    ev(
        "health.not_ready",
        WARN,
        &[local("check"), local("checks"), CODE],
    ),
    ev("shutdown.started", INFO, &[DRAIN_MS]),
    ev("shutdown.completed", INFO, &[DRAIN_MS]),
    ev(
        "runtime.loaded",
        INFO,
        &[local("raw_runtime"), local("application")],
    ),
    ev(
        "error.registration_refused",
        ERROR,
        &[MODULE, CODE, local("reason")],
    ),
    ev(
        "module.started",
        INFO,
        &[MODULE, local("publishes"), local("consumes")],
    ),
    ev("module.stopped", INFO, &[MODULE]),
    ev("module.start_failed", ERROR, &[MODULE, CODE]),
    ev("module.shutdown_timeout", WARN, &[MODULE]),
    ev("module.source_refused", ERROR, &[MODULE, local("sources")]),
    ev(
        "notification.captured",
        WARN,
        &[
            f("source", "notify.source"),
            local("kind"),
            TOPIC,
            local("recipients"),
            local("delivered"),
        ],
    ),
    // Application platform.
    ev(
        "projection.warmed",
        INFO,
        &[PROJECTION, local("revision"), local("checkpoint"), CODE],
    ),
    ev(
        "projection.rebuild_completed",
        INFO,
        &[PROJECTION, local("revision"), local("checkpoint"), CODE],
    ),
    ev(
        "projection.rebuild_failed",
        WARN,
        &[PROJECTION, local("revision"), local("checkpoint"), CODE],
    ),
    ev(
        "reactor.halted",
        ERROR,
        &[
            REACTOR,
            f("position", key::EVENT_POSITION),
            local("index"),
            f("reason", key::REASON),
            CODE,
        ],
    ),
    ev(
        "reactor.skipped",
        INFO,
        &[
            REACTOR,
            f("position", key::EVENT_POSITION),
            local("index"),
            f("reason", key::REASON),
            CODE,
        ],
    ),
    ev(
        "reactor.effect_rejected",
        WARN,
        &[
            REACTOR,
            f("position", key::EVENT_POSITION),
            local("index"),
            f("reason", key::REASON),
            CODE,
        ],
    ),
    // Jobs and outbox.
    ev("job.leased", DEBUG, &[JOB_ID, QUEUE, KIND, ATTEMPT]),
    ev("job.completed", DEBUG, &[JOB_ID, QUEUE, KIND, ATTEMPT]),
    // A handler gave its job back (`JobFailure::release`, content on an
    // outdated host): available again with the attempt refunded.
    ev("job.released", INFO, &[JOB_ID, QUEUE, KIND, ATTEMPT]),
    ev(
        "job.retry_scheduled",
        WARN,
        &[
            JOB_ID,
            QUEUE,
            KIND,
            ATTEMPT,
            f("code", "job.code"),
            f("classification", "job.classification"),
        ],
    ),
    ev(
        "job.dead_lettered",
        ERROR,
        &[
            JOB_ID,
            QUEUE,
            KIND,
            ATTEMPT,
            f("code", "job.code"),
            f("classification", "job.classification"),
        ],
    ),
    ev(
        "job.lease_lost",
        WARN,
        &[
            JOB_ID,
            QUEUE,
            KIND,
            ATTEMPT,
            f("code", "job.code"),
            f("classification", "job.classification"),
        ],
    ),
    ev(
        "job.replayed",
        INFO,
        &[JOB_ID, local("count"), QUEUE, local("action")],
    ),
    ev(
        "job.cancelled",
        INFO,
        &[JOB_ID, local("count"), QUEUE, local("action")],
    ),
    ev(
        "job.admin_action",
        INFO,
        &[JOB_ID, local("count"), QUEUE, local("action")],
    ),
    ev(
        "job.drain_abandoned",
        WARN,
        &[local("count"), local("job_ids")],
    ),
    // AP-B2: a queue with handlers but no configuration, a worker
    // statement failing (once per failure streak), and a reissue claim lost
    // to a concurrent settle.
    ev("job.queue_unconfigured", WARN, &[QUEUE]),
    ev(
        "job.worker_failed",
        WARN,
        &[
            local("task"),
            CODE,
            f("classification", "job.classification"),
        ],
    ),
    ev("job.reissue_claim_lost", WARN, &[JOB_ID, local("hook")]),
    ev(
        "outbox.relayed",
        DEBUG,
        &[local("relayed"), local("unrouted")],
    ),
    // Systemic failures, logged once per failing streak (AP-B1).
    ev(
        "outbox.relay_failed",
        ERROR,
        &[CODE, f("classification", "job.classification")],
    ),
    ev(
        "job.subscription_sync_failed",
        WARN,
        &[CODE, f("classification", "job.classification")],
    ),
    ev(
        "outbox.quarantined",
        WARN,
        &[local("outbox_id"), f("source", key::MODULE), CODE],
    ),
    // Identity.
    ev(
        "identity.login_failed",
        DEBUG,
        &[
            f("method", "identity.method"),
            f("outcome", "identity.outcome"),
        ],
    ),
    ev("identity.grants_truncated", WARN, &[local("count")]),
    ev("identity.audit_write_failed", ERROR, &[CODE]),
    // The one catalog event that carries a secret: stderr only, loopback
    // development mode only.
    Event {
        name: "identity.dev_link",
        levels: INFO,
        fields: &[f("purpose", "identity.purpose"), local("link")],
        export: false,
    },
    // Notifications.
    ev(
        "notification.requested",
        DEBUG,
        &[
            TOPIC,
            f("source", "notify.source"),
            f("notification_id", "notify.notification_id"),
            CODE,
        ],
    ),
    ev(
        "notification.duplicate",
        DEBUG,
        &[
            TOPIC,
            f("source", "notify.source"),
            f("notification_id", "notify.notification_id"),
            CODE,
        ],
    ),
    ev(
        "notification.invalid",
        WARN,
        &[
            TOPIC,
            f("source", "notify.source"),
            f("notification_id", "notify.notification_id"),
            CODE,
        ],
    ),
    ev(
        "notification.rejected",
        WARN,
        &[
            TOPIC,
            f("source", "notify.source"),
            f("notification_id", "notify.notification_id"),
            CODE,
        ],
    ),
    ev(
        "notification.accepted",
        DEBUG,
        &[
            CHANNEL,
            TOPIC,
            DELIVERY_ID,
            f("attempt", "notify.attempt"),
            f("reason", "notify.reason"),
            CODE,
        ],
    ),
    ev(
        "notification.skipped",
        DEBUG,
        &[
            CHANNEL,
            TOPIC,
            DELIVERY_ID,
            f("attempt", "notify.attempt"),
            f("reason", "notify.reason"),
            CODE,
        ],
    ),
    ev(
        "notification.retry_scheduled",
        WARN,
        &[
            CHANNEL,
            TOPIC,
            DELIVERY_ID,
            f("attempt", "notify.attempt"),
            f("reason", "notify.reason"),
            CODE,
        ],
    ),
    ev(
        "notification.ambiguous",
        WARN,
        &[
            CHANNEL,
            TOPIC,
            DELIVERY_ID,
            f("attempt", "notify.attempt"),
            f("reason", "notify.reason"),
            CODE,
        ],
    ),
    ev(
        "notification.expired",
        WARN,
        &[
            CHANNEL,
            TOPIC,
            DELIVERY_ID,
            f("attempt", "notify.attempt"),
            f("reason", "notify.reason"),
            CODE,
        ],
    ),
    ev(
        "notification.failed",
        WARN_ERROR,
        &[CHANNEL, TOPIC, DELIVERY_ID, CODE],
    ),
    ev(
        "notification.provider_rate_limited",
        WARN,
        &[N_PROVIDER, CHANNEL, CODE],
    ),
    ev(
        "notification.provider_account_error",
        ERROR,
        &[N_PROVIDER, CHANNEL, CODE],
    ),
    ev(
        "notification.provider_event_applied",
        DEBUG,
        &[N_PROVIDER, CHANNEL, f("event", "notify.event"), CODE],
    ),
    ev(
        "notification.provider_event_uncorrelated",
        INFO,
        &[N_PROVIDER, CHANNEL, f("event", "notify.event"), CODE],
    ),
    ev(
        "notification.provider_event_rejected",
        WARN,
        &[N_PROVIDER, CHANNEL, f("event", "notify.event"), CODE],
    ),
    ev(
        "notification.webhook_endpoint_disabled",
        WARN,
        &[
            f("endpoint_id", "notify.endpoint_id"),
            f("target", "notify.target"),
            f("reason", "notify.reason"),
        ],
    ),
    ev(
        "notification.egress_blocked",
        WARN,
        &[
            f("endpoint_id", "notify.endpoint_id"),
            f("target", "notify.target"),
            f("reason", "notify.reason"),
        ],
    ),
    ev(
        "notification.push_subscription_removed",
        INFO,
        &[
            f("endpoint_id", "notify.endpoint_id"),
            f("target", "notify.target"),
            f("reason", "notify.reason"),
        ],
    ),
    ev("notification.sse_listen_lost", WARN, &[CODE]),
    ev("notification.sse_listen_restored", INFO, &[CODE]),
    ev("notification.template_reload_failed", WARN, &[CODE]),
    // Content (CT-C2's rows).
    ev(
        "content.model_loaded",
        INFO,
        &[
            local("version"),
            local("fingerprint"),
            local("types"),
            local("locales"),
        ],
    ),
    ev(
        "content.model_activated",
        INFO,
        &[
            local("version"),
            local("previous_version"),
            local("fingerprint"),
            local("change_classes"),
        ],
    ),
    ev(
        "content.model_tightened",
        WARN,
        &[
            local("version"),
            CODE,
            f("content_type", "content.type"),
            local("counts"),
            local("truncated"),
        ],
    ),
    ev(
        "content.model_breaking",
        ERROR,
        &[local("version"), CODE, local("path"), local("change")],
    ),
    ev(
        "content.migration_batch",
        INFO,
        &[f("type", "content.type"), local("rows")],
    ),
    ev(
        "content.index_created",
        INFO,
        &[
            local("index"),
            f("content_type", "content.type"),
            local("field"),
            local("table"),
            local("purpose"),
        ],
    ),
    // `warn` when an invalid index left by a failed build is dropped.
    ev(
        "content.index_dropped",
        INFO_WARN,
        &[local("index"), local("reason")],
    ),
    ev(
        "content.index_failed",
        WARN_ERROR,
        &[
            local("index"),
            f("content_type", "content.type"),
            local("field"),
            CODE,
            local("cause"),
            local("attempts"),
            local("operation"),
        ],
    ),
    // `merezha content diff` when it cannot run.
    ev(
        "content.diff_failed",
        ERROR,
        &[
            CODE,
            local("usage"),
            Field {
                name: "issues",
                key: None,
                json: true,
            },
        ],
    ),
    ev(
        "content.schedule_failed",
        WARN,
        &[
            f("entry_id", "content.entry_id"),
            f("action", "content.action"),
            CODE,
        ],
    ),
    ev(
        "content.blob_cleanup",
        INFO,
        &[f("driver", "content.driver"), local("count")],
    ),
    ev(
        "content.spool_cleared",
        INFO,
        &[f("driver", "content.driver"), local("count")],
    ),
];

/// The catalog row named `name`.
pub fn event(name: &str) -> Option<&'static Event> {
    EVENTS.iter().find(|e| e.name == name)
}

/// `<area>.<noun>_<verb>`-shaped: lower snake segments joined by dots.
/// Distinguishes catalog names from `tracing`'s default `event file:line`.
pub fn is_event_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.contains('.')
        && name.split('.').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn experimental_names_match_semconv_1_42() {
        use semconv::{attribute as a, metric as m};
        assert_eq!(
            semconv::SCHEMA_URL,
            "https://opentelemetry.io/schemas/1.42.0"
        );
        for (ours, theirs) in [
            (key::USER_ID, a::USER_ID),
            (key::URL_TEMPLATE, a::URL_TEMPLATE),
            (
                key::DB_CLIENT_CONNECTION_POOL_NAME,
                a::DB_CLIENT_CONNECTION_POOL_NAME,
            ),
            (
                key::DB_CLIENT_CONNECTION_STATE,
                a::DB_CLIENT_CONNECTION_STATE,
            ),
            (key::MESSAGING_SYSTEM, a::MESSAGING_SYSTEM),
            (key::MESSAGING_OPERATION_TYPE, a::MESSAGING_OPERATION_TYPE),
            (
                key::MESSAGING_DESTINATION_NAME,
                a::MESSAGING_DESTINATION_NAME,
            ),
            (key::MESSAGING_MESSAGE_ID, a::MESSAGING_MESSAGE_ID),
            (
                metric::HTTP_SERVER_ACTIVE_REQUESTS.name,
                m::HTTP_SERVER_ACTIVE_REQUESTS,
            ),
            (
                metric::HTTP_SERVER_REQUEST_BODY_SIZE.name,
                m::HTTP_SERVER_REQUEST_BODY_SIZE,
            ),
            (
                metric::DB_CLIENT_CONNECTION_COUNT.name,
                m::DB_CLIENT_CONNECTION_COUNT,
            ),
            (
                metric::DB_CLIENT_CONNECTION_MAX.name,
                m::DB_CLIENT_CONNECTION_MAX,
            ),
            (
                metric::DB_CLIENT_CONNECTION_PENDING_REQUESTS.name,
                m::DB_CLIENT_CONNECTION_PENDING_REQUESTS,
            ),
            (
                metric::DB_CLIENT_CONNECTION_WAIT_TIME.name,
                m::DB_CLIENT_CONNECTION_WAIT_TIME,
            ),
            (
                metric::DB_CLIENT_CONNECTION_TIMEOUTS.name,
                m::DB_CLIENT_CONNECTION_TIMEOUTS,
            ),
        ] {
            assert_eq!(ours, theirs);
        }
    }

    #[test]
    fn metric_rows_follow_the_naming_rules() {
        let mut names = BTreeSet::new();
        for m in METRICS {
            assert!(names.insert(m.name), "duplicate metric {}", m.name);
            let standard = m.name.starts_with("http.") || m.name.starts_with("db.");
            assert!(standard || m.name.starts_with("merezha."), "{}", m.name);
            assert!(
                m.name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b"._".contains(&b))
            );
            // Durations are f64 seconds in every block (R9).
            if m.name.ends_with(".duration") || m.name.ends_with("_latency") {
                assert_eq!(m.unit, "s", "{}", m.name);
            }
            assert_eq!(
                m.instrument == Instrument::Histogram,
                !m.buckets.is_empty(),
                "{}: histograms and only histograms carry boundaries",
                m.name
            );
            assert!(m.buckets.windows(2).all(|w| w[0] < w[1]), "{}", m.name);
            assert!(!m.budget.is_empty());
        }
    }

    #[test]
    fn every_metric_attribute_is_allowlisted_for_metrics_and_carries_its_prefix() {
        for m in METRICS {
            for key in m.attributes {
                let row = attribute(key).unwrap_or_else(|| panic!("{} on {}", key, m.name));
                assert!(row.metric, "{key} is not a metric attribute");
                assert_eq!(row.gate, Gate::Always, "{key} is personal data");
                let block = key.split('.').next().unwrap();
                assert!(
                    matches!(
                        block,
                        "merezha"
                            | "http"
                            | "db"
                            | "error"
                            | "url"
                            | "server"
                            | "job"
                            | "identity"
                            | "notify"
                            | "content"
                    ),
                    "{key}"
                );
            }
        }
    }

    #[test]
    fn identifiers_and_personal_data_are_never_metric_attributes() {
        for key in [
            key::REQUEST_ID,
            key::CORRELATION_ID,
            key::TENANT_ID,
            key::USER_ID,
            key::CLIENT_ADDRESS,
            "job.id",
            "notify.delivery_id",
            "content.entry_id",
        ] {
            assert!(!attribute(key).unwrap().metric, "{key}");
        }
        assert!(attribute(key::DB_QUERY_TEXT).is_none());
        assert!(attribute("db.query.parameter.0").is_none());
        assert!(attribute("identity.session_id").is_none());
        assert!(attribute("http.request.header.authorization").is_none());
        assert!(attribute(key::LINK_REASON).unwrap().links_only);
        assert_eq!(attribute(key::USER_ID).unwrap().gate, Gate::SubjectId);
        assert!(attribute("merezha.guest.fuel.consumed").is_some());
    }

    #[test]
    fn events_follow_the_naming_rules_and_map_fields_to_log_keys() {
        let mut names = BTreeSet::new();
        for e in EVENTS {
            assert!(names.insert(e.name), "duplicate event {}", e.name);
            assert!(is_event_name(e.name), "{}", e.name);
            assert!(!e.levels.is_empty());
            let mut fields = BTreeSet::new();
            for field in e.fields {
                assert!(fields.insert(field.name), "{}: {}", e.name, field.name);
                if let Some(key) = field.key {
                    let row = attribute(key).unwrap_or_else(|| panic!("{}: {key}", e.name));
                    assert!(row.log, "{}: {key} is not a log attribute", e.name);
                    assert!(!field.json);
                }
            }
        }
        // Client-caused outcomes are never warn or error (TE §4).
        assert_eq!(event("identity.login_failed").unwrap().levels, DEBUG);
        assert_eq!(event("startup.failed").unwrap().levels, ERROR);
        assert!(!event("identity.dev_link").unwrap().export);
        assert!(!event("telemetry.export_failed").unwrap().export);
    }

    #[test]
    fn event_names_are_told_apart_from_tracing_defaults() {
        assert!(is_event_name("listener.ready"));
        assert!(is_event_name("http.request.completed"));
        assert!(!is_event_name("event crates/cli/src/main.rs:12"));
        assert!(!is_event_name("ready"));
        assert!(!is_event_name("Listener.Ready"));
    }

    #[test]
    fn span_patterns_are_unique() {
        let mut seen = BTreeSet::new();
        for s in SPANS {
            assert!(seen.insert(s.pattern), "{}", s.pattern);
        }
    }

    #[test]
    fn constructors_use_catalog_keys() {
        assert_eq!(attr::outcome(Outcome::Failed).key.as_str(), key::OUTCOME);
        assert_eq!(attr::reason(Reason::QueueFull).value.as_str(), "queue_full");
        assert_eq!(
            attr::signal(TelemetrySignal::Stderr).value.as_str(),
            "stderr"
        );
    }
}
