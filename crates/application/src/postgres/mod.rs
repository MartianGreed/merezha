//! PostgreSQL application storage (AP §A): schema `merezha_app` on the
//! host's pool, several hosts on one database.
//!
//! `Application` stays synchronous (§A.2). [`PostgresStore`] keeps the
//! runtime handle it was connected on and runs each store call with
//! `Handle::block_on` under the admitted deadline. Call the application only
//! from `spawn_blocking` or from threads outside the runtime:
//! `Handle::block_on` panics on an async worker thread, so a misuse fails
//! loudly instead of stalling the executor. No SQL runs while a guest runs.
//!
//! Positions are contiguous and commit in position order across hosts:
//! the append function locks the single `merezha_app.head` row and commits
//! as one autocommit statement (§A.4, §A.5).
//!
//! Background projection runners, the shared `Listener` and the admin API
//! come with AP-A3; [`PostgresStore`] then gains inherent methods for them.
mod sql;
#[cfg(test)]
mod tests;
mod verify;

pub use verify::{MAX_VIOLATIONS, Subject, VerifyReport, Violation, verify};

use crate::{
    application::{Key, ProjectionState},
    config::{Config, Limits, StorageConfig},
    durable::{CommitCommand, DurableStore, ProjectionRef},
};
use merezha_domain::{Event, Failure, Identity, Metadata, Receipt, StoredEvent, compatible_json};
use merezha_postgres::{
    Database, Migration, MigrationMode, ModuleMigrations, PooledClient, run_migrations,
    tokio_postgres::{self, Row, error::Severity},
};
use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::{Handle, RuntimeFlavor};

/// The migration ledger key of the application schema. It is also the
/// kernel's `application` source and route prefix.
pub const MODULE: &str = "application";
/// Forward-only migrations of `merezha_app`. Hosts apply them with the
/// kernel runner under [`MODULE`]; [`PostgresStore::connect`] only verifies.
pub const MIGRATIONS: &[Migration] = &[Migration::new(
    1,
    "application",
    include_str!("migrations/0001_application.sql"),
)];
/// The registered name of the append statement, TE's `app.events.append`.
pub const APPEND_STATEMENT: &str = sql::APPEND_NAME;
/// Below this much time left the append is not attempted (§A.4).
const APPEND_MARGIN: Duration = Duration::from_millis(50);
/// Bounds of [`PostgresOptions::lock_timeout_max`], `[application.append]`.
pub const LOCK_TIMEOUT_MAX: (Duration, Duration) =
    (Duration::from_millis(10), Duration::from_secs(30));
const METADATA_BYTES: usize = 4096;

/// A W3C trace context to store with appended events (R10). Host-only: it
/// is never part of a [`StoredEvent`] and never reaches a guest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceContext {
    parent: String,
    state: Option<String>,
}
impl TraceContext {
    /// A `traceparent` of version `00` form (55 bytes, lowercase hex, non-zero
    /// ids) and an optional `tracestate` of at most 512 printable ASCII
    /// bytes. `None` when either is malformed: telemetry never fails a
    /// command.
    pub fn new(traceparent: &str, tracestate: Option<&str>) -> Option<Self> {
        let hex = |s: &str| {
            s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        let parts: Vec<&str> = traceparent.split('-').collect();
        let valid = traceparent.len() == 55
            && matches!(parts.as_slice(), [version, trace, parent, flags]
                if version.len() == 2 && hex(version) && *version != "ff"
                    && trace.len() == 32 && hex(trace) && trace.bytes().any(|b| b != b'0')
                    && parent.len() == 16 && hex(parent) && parent.bytes().any(|b| b != b'0')
                    && flags.len() == 2 && hex(flags));
        let state_valid = tracestate
            .is_none_or(|s| s.len() <= 512 && s.bytes().all(|b| (0x20..=0x7e).contains(&b)));
        (valid && state_valid).then(|| Self {
            parent: traceparent.to_owned(),
            state: tracestate.filter(|s| !s.is_empty()).map(str::to_owned),
        })
    }
    pub fn traceparent(&self) -> &str {
        &self.parent
    }
    pub fn tracestate(&self) -> Option<&str> {
        self.state.as_deref()
    }
}
/// Where the store reads the current command span's context, on the thread
/// that commits. The telemetry block implements it from the current span
/// (TE-T3); without one, events carry no trace context.
pub trait TraceSource: Send + Sync {
    fn current(&self) -> Option<TraceContext>;
}

/// Host settings of the store (`merezha.toml` `[application.append]`).
#[derive(Clone)]
pub struct PostgresOptions {
    /// Ceiling of the append's `lock_timeout`; the effective value is
    /// `min(remaining deadline - 50 ms, lock_timeout_max)`.
    pub lock_timeout_max: Duration,
    pub trace: Option<Arc<dyn TraceSource>>,
}
impl Default for PostgresOptions {
    fn default() -> Self {
        Self {
            lock_timeout_max: Duration::from_secs(5),
            trace: None,
        }
    }
}
impl std::fmt::Debug for PostgresOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresOptions")
            .field("lock_timeout_max", &self.lock_timeout_max)
            .field("trace", &self.trace.is_some())
            .finish()
    }
}

/// The application's durable store on PostgreSQL. Cheap to share behind an
/// `Arc`; every instance on the database keeps its own pending-claim
/// registry in its `Application`.
pub struct PostgresStore {
    db: Database,
    handle: Handle,
    limits: Limits,
    /// The configuration this store was connected for, as JSON.
    configuration: String,
    application: Identity,
    projections: BTreeSet<(Identity, u32)>,
    options: PostgresOptions,
}
impl std::fmt::Debug for PostgresStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresStore")
            .field("application", &self.application)
            .finish_non_exhaustive()
    }
}

fn invalid() -> Failure {
    Failure::permanent("storage_invalid")
}
fn configuration() -> Failure {
    Failure::permanent("storage_configuration")
}
fn timeout() -> Failure {
    Failure::transient("timeout")
}
fn read_error(error: &tokio_postgres::Error) -> Failure {
    merezha_postgres::classify(error)
}
/// A failed append. Only an `ERROR` answered by the server proves the
/// statement rolled back; a lost connection or a `FATAL` (such as
/// `pg_terminate_backend`) may come after the commit.
fn append_error(error: &tokio_postgres::Error) -> Failure {
    match error.as_db_error() {
        Some(db) if db.parsed_severity() == Some(Severity::Error) => match db.code().code() {
            "55P03" => Failure::transient("storage_busy"),
            "MZ001" => Failure::conflict("idempotency_in_flight"),
            "57014" => timeout(),
            _ => merezha_postgres::classify(error),
        },
        _ => Failure::transient("commit_outcome_unknown"),
    }
}
fn unsigned(value: i64) -> Result<u64, Failure> {
    u64::try_from(value).map_err(|_| invalid())
}
fn signed(value: u64) -> Result<i64, Failure> {
    i64::try_from(value).map_err(|_| invalid())
}
fn encode(value: &impl serde::Serialize) -> Result<String, Failure> {
    serde_json::to_string(value).map_err(|_| Failure::permanent("serialization_failed"))
}
/// A stored document read back as text, bounded before parsing.
fn decode<T: serde::de::DeserializeOwned>(text: &str, max: usize) -> Result<T, Failure> {
    if text.len() > max {
        return Err(invalid());
    }
    serde_json::from_str(text).map_err(|_| invalid())
}

impl PostgresStore {
    /// Open the store for `config` on `db`, within `deadline`: check that
    /// this module's migrations are applied (`storage_schema` when pending),
    /// record or compare the manifest identity (`storage_configuration`)
    /// and create the rows of new projection revisions. It refuses a
    /// current-thread runtime, whose drivers a blocked caller could not run.
    pub async fn connect(
        db: &Database,
        config: &Config,
        options: PostgresOptions,
        deadline: Instant,
    ) -> Result<Self, Failure> {
        let handle = Handle::try_current().map_err(|_| configuration())?;
        if handle.runtime_flavor() != RuntimeFlavor::MultiThread {
            return Err(configuration());
        }
        config.validate()?;
        let StorageConfig::Postgres { application } = &config.storage else {
            return Err(configuration());
        };
        let (min, max) = LOCK_TIMEOUT_MAX;
        if !(min..=max).contains(&options.lock_timeout_max) {
            return Err(configuration());
        }
        let report = run_migrations(
            db,
            &[ModuleMigrations {
                module: MODULE,
                migrations: MIGRATIONS,
            }],
            MigrationMode::Verify,
            deadline,
        )
        .await
        .map_err(|error| error.failure)?;
        if !report.is_current() {
            return Err(Failure::permanent("storage_schema"));
        }
        let mut initial = Vec::with_capacity(config.projections.len());
        for projection in &config.projections {
            compatible_json(&projection.initial)?;
            initial.push((
                projection.name.as_str().to_owned(),
                i32::try_from(projection.revision).map_err(|_| configuration())?,
                encode(&projection.initial)?,
            ));
        }
        let application_text = application.as_str().to_owned();
        db.transaction(deadline, move |tx| {
            Box::pin(async move {
                tx.execute(sql::INSERT_MANIFEST, &[&application_text])
                    .await
                    .map_err(|e| read_error(&e))?;
                let row = tx
                    .query_one(sql::MANIFEST, &[])
                    .await
                    .map_err(|e| read_error(&e))?;
                if row.get::<_, String>(0) != application_text || row.get::<_, i16>(1) != 1 {
                    return Err(configuration());
                }
                for (name, revision, state) in &initial {
                    tx.execute(sql::INSERT_PROJECTION, &[name, revision, state])
                        .await
                        .map_err(|e| read_error(&e))?;
                    let partitioned: bool = tx
                        .query_one(sql::PROJECTION_PARTITIONED, &[name, revision])
                        .await
                        .map_err(|e| read_error(&e))?
                        .get(0);
                    // Partitioning needs a new revision; this build has none.
                    if partitioned {
                        return Err(configuration());
                    }
                }
                Ok(())
            })
        })
        .await?;
        Ok(Self {
            db: db.clone(),
            handle,
            limits: config.limits.clone(),
            configuration: encode(config)?,
            application: application.clone(),
            projections: config
                .projections
                .iter()
                .map(|p| (p.name.clone(), p.revision))
                .collect(),
            options,
        })
    }
    /// `storage_configuration` unless `config` is the one this store was
    /// connected for.
    pub(crate) fn check_config(&self, config: &Config) -> Result<(), Failure> {
        if encode(config)? != self.configuration {
            return Err(configuration());
        }
        Ok(())
    }
    /// The application identity recorded in the manifest.
    pub fn application(&self) -> &Identity {
        &self.application
    }
    /// Run `work` on the store's runtime from a blocking thread, bounded by
    /// `deadline`.
    fn block_on<T>(
        &self,
        deadline: Instant,
        work: impl Future<Output = Result<T, Failure>>,
    ) -> Result<T, Failure> {
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        self.handle.block_on(work)
    }
    /// One statement sequence on a pooled session within `deadline`. A
    /// session cut by the deadline is closed, never returned to the pool,
    /// and the call fails with `interrupted`.
    async fn session<T>(
        &self,
        deadline: Instant,
        interrupted: fn() -> Failure,
        work: impl AsyncFnOnce(&PooledClient) -> Result<T, Failure>,
    ) -> Result<T, Failure> {
        let client = self.db.acquire(deadline).await?;
        match tokio::time::timeout_at(deadline.into(), work(&client)).await {
            Ok(result) => result,
            Err(_) => {
                drop(PooledClient::take(client));
                Err(interrupted())
            }
        }
    }
    fn event(&self, row: &Row) -> Result<StoredEvent, Failure> {
        let position = unsigned(row.get(0))?;
        let payload: serde_json::Value =
            decode(row.get::<_, &str>(5), self.limits.max_payload_bytes)?;
        compatible_json(&payload).map_err(|_| invalid())?;
        let schema_version = u16::try_from(row.get::<_, i32>(4)).map_err(|_| invalid())?;
        let metadata: Metadata = decode(row.get::<_, &str>(6), METADATA_BYTES)?;
        Ok(StoredEvent {
            event_id: Identity::new(format!("event/{position}")).map_err(|_| invalid())?,
            stream: Identity::new(row.get::<_, &str>(1)).map_err(|_| invalid())?,
            version: unsigned(row.get(2))?,
            position,
            event: Event {
                tag: Identity::new(row.get::<_, &str>(3)).map_err(|_| invalid())?,
                schema_version,
                payload,
            },
            metadata,
            accepted_unix_ms: unsigned(row.get(7))?,
        })
    }
    fn events(&self, rows: &[Row]) -> Result<Vec<StoredEvent>, Failure> {
        rows.iter().map(|row| self.event(row)).collect()
    }
    fn receipt_of(stream: &Identity, row: &Row) -> Result<Receipt, Failure> {
        Ok(Receipt {
            stream: stream.clone(),
            version: unsigned(row.get(1))?,
            position: unsigned(row.get(2))?,
            events: usize::try_from(row.get::<_, i32>(3)).map_err(|_| invalid())?,
        })
    }
    fn configured(&self, p: &ProjectionRef<'_>) -> Result<(String, i32), Failure> {
        if !self.projections.contains(&(p.name.clone(), p.revision)) {
            return Err(Failure::permanent("projection_not_found"));
        }
        Ok((
            p.name.as_str().to_owned(),
            i32::try_from(p.revision).map_err(|_| configuration())?,
        ))
    }
    /// The lock wait the append may spend: what is left of the deadline
    /// minus a margin for the commit, capped by the configured maximum.
    fn lock_timeout(&self, deadline: Instant) -> Result<i32, Failure> {
        let left = deadline
            .checked_duration_since(Instant::now())
            .and_then(|left| left.checked_sub(APPEND_MARGIN))
            .filter(|left| *left >= Duration::from_millis(1))
            .ok_or_else(timeout)?;
        i32::try_from(left.min(self.options.lock_timeout_max).as_millis()).map_err(|_| timeout())
    }
}

/// Values of one append call, validated on the calling thread.
struct Append {
    stream: String,
    tenant: Option<String>,
    expected: i64,
    tags: Vec<String>,
    schema_versions: Vec<i32>,
    payloads: Vec<String>,
    metadata: String,
    accepted_unix_ms: i64,
    trace: Option<TraceContext>,
    key: Option<[String; 4]>,
    fingerprint: Option<String>,
    lock_timeout_ms: i32,
}

impl DurableStore for PostgresStore {
    fn load(&self, stream: &Identity, deadline: Instant) -> Result<Vec<StoredEvent>, Failure> {
        let limit = signed(self.limits.max_stream_events as u64 + 1)?;
        let rows = self.block_on(
            deadline,
            self.session(deadline, timeout, async |client| {
                client
                    .query(&sql::load(), &[&stream.as_str(), &limit])
                    .await
                    .map_err(|e| read_error(&e))
            }),
        )?;
        self.events(&rows)
    }
    fn head(&self, deadline: Instant) -> Result<u64, Failure> {
        let position: i64 = self.block_on(
            deadline,
            self.session(deadline, timeout, async |client| {
                Ok(client
                    .query_one(sql::HEAD, &[])
                    .await
                    .map_err(|e| read_error(&e))?
                    .get(0))
            }),
        )?;
        unsigned(position)
    }
    fn read_all(
        &self,
        after: u64,
        limit: usize,
        deadline: Instant,
    ) -> Result<Vec<StoredEvent>, Failure> {
        if limit == 0 || limit > 64 {
            return Err(Failure::permanent("invalid_batch"));
        }
        let from = signed(after)?;
        let count = limit as i64;
        let rows = self.block_on(
            deadline,
            self.session(deadline, timeout, async |client| {
                client
                    .query(&sql::read_all(), &[&from, &count])
                    .await
                    .map_err(|e| read_error(&e))
            }),
        )?;
        let events = self.events(&rows)?;
        // Commit order equals position order (§A.5), so a gap is never
        // expected; if one shows, it is transient and the reader retries.
        if events
            .iter()
            .zip(after + 1..)
            .any(|(event, expected)| event.position != expected)
        {
            return Err(Failure::transient("event_feed_gap"));
        }
        Ok(events)
    }
    fn receipt(
        &self,
        key: &Key,
        fingerprint: &str,
        deadline: Instant,
    ) -> Result<Option<Receipt>, Failure> {
        let [tag, tenant, actor, key_text] = key_parts(key);
        let rows = self.block_on(
            deadline,
            self.session(deadline, timeout, async |client| {
                client
                    .query(
                        sql::RECEIPT,
                        &[&tag, &tenant, &actor, &key_text, &fingerprint],
                    )
                    .await
                    .map_err(|e| read_error(&e))
            }),
        )?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let stream = Identity::new(row.get::<_, &str>(4)).map_err(|_| invalid())?;
        match row.get::<_, &str>(0) {
            "replayed" => Self::receipt_of(&stream, row).map(Some),
            "mismatch" => Err(Failure::conflict("idempotency_mismatch")),
            _ => Err(invalid()),
        }
    }
    fn commit_command(&self, c: CommitCommand<'_>, deadline: Instant) -> Result<Receipt, Failure> {
        // The host checks what SQLite checks before its transaction; the
        // table constraints are only a backstop.
        if c.events.len() > self.limits.batch_size {
            return Err(Failure::permanent("event_batch_limit"));
        }
        if c.tenant.is_some_and(|tenant| {
            c.stream
                .as_str()
                .strip_prefix(tenant.as_str())
                .is_none_or(|rest| !rest.starts_with(':'))
        }) {
            return Err(Failure::permanent("constraint_violation"));
        }
        let mut tags = Vec::with_capacity(c.events.len());
        let mut schema_versions = Vec::with_capacity(c.events.len());
        let mut payloads = Vec::with_capacity(c.events.len());
        for event in &c.events {
            if event.schema_version == 0 {
                return Err(Failure::permanent("invalid_event"));
            }
            compatible_json(&event.payload)?;
            let payload = encode(&event.payload)?;
            if payload.len() > self.limits.max_payload_bytes {
                return Err(Failure::permanent("payload_limit"));
            }
            tags.push(event.tag.as_str().to_owned());
            schema_versions.push(i32::from(event.schema_version));
            payloads.push(payload);
        }
        let accepted_unix_ms = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| invalid())?
                .as_millis(),
        )
        .map_err(|_| invalid())?;
        let append = Append {
            stream: c.stream.as_str().to_owned(),
            tenant: c.tenant.map(|t| t.as_str().to_owned()),
            expected: signed(c.expected)?,
            tags,
            schema_versions,
            payloads,
            metadata: encode(c.metadata)?,
            accepted_unix_ms,
            // Read on this thread: the current span is the command's.
            trace: self
                .options
                .trace
                .as_ref()
                .and_then(|source| source.current()),
            key: c.claim.map(|(key, _)| key_parts(key)),
            fingerprint: c.claim.map(|(_, fingerprint)| fingerprint.to_owned()),
            lock_timeout_ms: self.lock_timeout(deadline)?,
        };
        #[cfg(test)]
        tests::crash_cut("before_append");
        let row = self.block_on(deadline, self.append(&append, deadline))?;
        #[cfg(test)]
        tests::crash_cut("after_append_before_wake");
        // AP-A3: the in-process wake of local projection runners goes here,
        // after an `appended` outcome.
        match row.get::<_, &str>(0) {
            "appended" | "replayed" => Self::receipt_of(c.stream, &row),
            "conflict" => Err(Failure::conflict("concurrency_conflict")),
            "mismatch" => Err(Failure::conflict("idempotency_mismatch")),
            _ => Err(invalid()),
        }
    }
    fn projection(
        &self,
        p: &ProjectionRef<'_>,
        deadline: Instant,
    ) -> Result<(ProjectionState, i64), Failure> {
        let (name, revision) = self.configured(p)?;
        let row = self.block_on(
            deadline,
            self.session(deadline, timeout, async |client| {
                client
                    .query_opt(sql::PROJECTION, &[&name, &revision])
                    .await
                    .map_err(|e| read_error(&e))
            }),
        )?;
        let row = row.ok_or_else(invalid)?;
        // A partitioned row has no single state; this build has none.
        let text: Option<&str> = row.get(0);
        let state: serde_json::Value =
            decode(text.ok_or_else(invalid)?, self.limits.max_state_bytes)?;
        compatible_json(&state).map_err(|_| invalid())?;
        Ok((
            ProjectionState {
                state,
                checkpoint: unsigned(row.get(1))?,
            },
            row.get(2),
        ))
    }
    fn commit_projection(
        &self,
        p: &ProjectionRef<'_>,
        row_revision: i64,
        state: &ProjectionState,
        deadline: Instant,
    ) -> Result<i64, Failure> {
        let (name, revision) = self.configured(p)?;
        compatible_json(&state.state)?;
        let text = encode(&state.state)?;
        if text.len() > self.limits.max_state_bytes {
            return Err(Failure::permanent("payload_limit"));
        }
        let checkpoint = signed(state.checkpoint)?;
        let row = self.block_on(
            deadline,
            self.session(deadline, commit_unknown, async |client| {
                client
                    .query_opt(
                        sql::COMMIT_PROJECTION,
                        &[&name, &revision, &text, &checkpoint, &row_revision],
                    )
                    .await
                    .map_err(|e| append_error(&e))
            }),
        )?;
        row.map(|row| row.get(0))
            .ok_or_else(|| Failure::conflict("projection_conflict"))
    }
    #[cfg(feature = "test-support")]
    fn independent(
        &self,
        _: &Config,
    ) -> Result<(Arc<dyn DurableStore>, crate::sqlite::Recovered), Failure> {
        // Same pool, other sessions; nothing is recovered into memory. The
        // PostgreSQL contract fixture opens a second pool instead.
        Ok((
            Arc::new(Self {
                db: self.db.clone(),
                handle: self.handle.clone(),
                limits: self.limits.clone(),
                configuration: self.configuration.clone(),
                application: self.application.clone(),
                projections: self.projections.clone(),
                options: self.options.clone(),
            }),
            Default::default(),
        ))
    }
}
fn commit_unknown() -> Failure {
    Failure::transient("commit_outcome_unknown")
}
/// `(tag, tenant, actor, key)` with `''` for an absent part.
fn key_parts(key: &Key) -> [String; 4] {
    [
        key.tag.as_str().to_owned(),
        key.tenant.as_ref().map_or("", Identity::as_str).to_owned(),
        key.actor.as_ref().map_or("", Identity::as_str).to_owned(),
        key.key.as_str().to_owned(),
    ]
}
impl PostgresStore {
    /// The append statement on a pooled session. A deadline cut while the
    /// statement runs leaves its outcome unknown.
    async fn append(&self, a: &Append, deadline: Instant) -> Result<Row, Failure> {
        let client = self.db.acquire(deadline).await?;
        let statement = match tokio::time::timeout_at(
            deadline.into(),
            client.prepare_cached(sql::APPEND),
        )
        .await
        {
            Ok(Ok(statement)) => statement,
            Ok(Err(error)) => return Err(read_error(&error)),
            Err(_) => {
                drop(PooledClient::take(client));
                return Err(timeout());
            }
        };
        let key = a.key.as_ref();
        let part = |i: usize| key.map(|k| k[i].as_str());
        let result = tokio::time::timeout_at(
            deadline.into(),
            client.query_one(
                &statement,
                &[
                    &a.stream,
                    &a.tenant,
                    &a.expected,
                    &a.tags,
                    &a.schema_versions,
                    &a.payloads,
                    &a.metadata,
                    &a.accepted_unix_ms,
                    &a.trace.as_ref().map(TraceContext::traceparent),
                    &a.trace.as_ref().and_then(TraceContext::tracestate),
                    &part(0),
                    &part(1),
                    &part(2),
                    &part(3),
                    &a.fingerprint,
                    &a.lock_timeout_ms,
                ],
            ),
        )
        .await;
        match result {
            Ok(Ok(row)) => Ok(row),
            Ok(Err(error)) => {
                let failure = append_error(&error);
                if failure.code == "commit_outcome_unknown" {
                    drop(PooledClient::take(client));
                }
                Err(failure)
            }
            Err(_) => {
                drop(PooledClient::take(client));
                Err(commit_unknown())
            }
        }
    }
}
