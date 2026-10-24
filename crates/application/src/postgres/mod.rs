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
//! Projections advance in the background (§A.7): a [`Scheduler`] per
//! instance runs live catch-up and rebuilds under session advisory locks
//! held on connections detached from the pool, woken by local commits,
//! `LISTEN merezha_app_events` and an interval. The admin documents
//! ([`crate::status`]) read the same rows.
mod admin;
mod runner;
mod scheduler;
mod sql;
#[cfg(test)]
mod tests;
mod verify;

pub(crate) use runner::at_deadline;
pub use runner::{BackgroundOptions, CatchUp, INTERVAL, MAX_CONCURRENT, RUN_TIMEOUT};
#[doc(hidden)]
pub use runner::{Pause, RunHooks};
pub use scheduler::{EVENTS_CHANNEL, Scheduler};
pub use verify::{MAX_VIOLATIONS, Subject, VerifyReport, Violation, verify};

use crate::{
    application::{Key, ProjectionState},
    config::{Config, Limits, MAX_PAYLOAD_BYTES, MAX_STATE_BYTES, StorageConfig},
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
/// Every statement the store runs, registered with `merezha-postgres`
/// (TE §1.3): `app.*` names, the append `app.events.append`.
pub const STATEMENTS: &[merezha_postgres::Statement] = sql::STATEMENTS;
/// The registered name of the append statement, TE's `app.events.append`.
pub const APPEND_STATEMENT: &str = sql::APPEND.name;
/// The `pg_locks.classid` of every run lock (`'MZAP'`, §A.7): operators
/// find a stuck holder there and end it with `pg_terminate_backend`.
pub const RUN_LOCK_CLASS: i32 = sql::LOCK_CLASS;
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
    /// Background runners and explicit catch-ups.
    pub background: BackgroundOptions,
}
impl Default for PostgresOptions {
    fn default() -> Self {
        Self {
            lock_timeout_max: Duration::from_secs(5),
            trace: None,
            background: BackgroundOptions::default(),
        }
    }
}
impl std::fmt::Debug for PostgresOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresOptions")
            .field("lock_timeout_max", &self.lock_timeout_max)
            .field("trace", &self.trace.is_some())
            .field("background", &self.background)
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
    /// What restores the session's statement timeout after an append; a
    /// test replaces it to make the reset fail or come late.
    reset_statement: &'static str,
    /// This instance's wakes, lanes and run budgets.
    shared: Arc<runner::Shared>,
}
/// Restores the session default after an append.
const RESET_STATEMENT: &str = "RESET statement_timeout";
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
        options.background.validate()?;
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
        let store_db = db.clone();
        db.transaction(deadline, move |tx| {
            Box::pin(async move {
                store_db
                    .execute(tx, &sql::INSERT_MANIFEST, &[&application_text])
                    .await
                    .map_err(|e| read_error(&e))?;
                let row = store_db
                    .query_one(tx, &sql::MANIFEST, &[])
                    .await
                    .map_err(|e| read_error(&e))?;
                if row.get::<_, String>(0) != application_text || row.get::<_, i16>(1) != 1 {
                    return Err(configuration());
                }
                for (name, revision, state) in &initial {
                    store_db
                        .execute(tx, &sql::INSERT_PROJECTION, &[name, revision, state])
                        .await
                        .map_err(|e| read_error(&e))?;
                    let partitioned: bool = store_db
                        .query_one(tx, &sql::PROJECTION_PARTITIONED, &[name, revision])
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
            shared: Arc::new(runner::Shared::new(
                options.background.clone(),
                &config.projections,
            )),
            options,
            reset_statement: RESET_STATEMENT,
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
            // Read up to the hard ceiling: a lowered `max_payload_bytes`
            // binds new writes only, so stored history stays readable.
            decode(row.get::<_, &str>(5), MAX_PAYLOAD_BYTES)?;
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
    /// Server-side bounds of an append about to be sent. The statement gets
    /// half of [`APPEND_MARGIN`] less than what is left of the deadline,
    /// never more than the database's configured `statement_timeout` (as
    /// for kernel transactions), and its head-lock wait half the margin less
    /// again, capped by the configured maximum. The server thus gives up
    /// first, at the lock with `storage_busy` where it can, before the
    /// caller does. `None` below the margin: the append is not sent.
    fn append_bounds(&self, deadline: Instant) -> Option<(i32, u128)> {
        let left = deadline.checked_duration_since(Instant::now())?;
        left.checked_sub(APPEND_MARGIN)
            .filter(|budget| *budget >= Duration::from_millis(1))?;
        let statement = left
            .checked_sub(APPEND_MARGIN / 2)?
            .min(self.db.statement_timeout());
        let lock = statement
            .saturating_sub(APPEND_MARGIN / 2)
            .min(self.options.lock_timeout_max)
            .max(Duration::from_millis(1));
        Some((
            i32::try_from(lock.as_millis()).ok()?,
            statement.as_millis().max(1),
        ))
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
}

impl DurableStore for PostgresStore {
    fn load(&self, stream: &Identity, deadline: Instant) -> Result<Vec<StoredEvent>, Failure> {
        let limit = signed(self.limits.max_stream_events as u64 + 1)?;
        let rows = self.block_on(
            deadline,
            self.session(deadline, timeout, async |client| {
                self.db
                    .query(client, &sql::LOAD, &[&stream.as_str(), &limit])
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
                Ok(self
                    .db
                    .query_one(client, &sql::HEAD, &[])
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
                self.db
                    .query(client, &sql::READ_ALL, &[&from, &count])
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
                self.db
                    .query(
                        client,
                        &sql::RECEIPT,
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
        };
        #[cfg(test)]
        tests::crash_cut("before_append");
        let row = self.block_on(deadline, self.append(&append, deadline))?;
        #[cfg(test)]
        tests::crash_cut("after_append_before_wake");
        let outcome = row.get::<_, &str>(0);
        if outcome == "appended" && !append.tags.is_empty() {
            // The in-process wake of this instance's projection runners.
            self.shared.appended();
        }
        match outcome {
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
        let (pair, row_revision, _) =
            self.block_on(deadline, self.served_row(name, revision, deadline))?;
        Ok((pair, row_revision))
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
                self.db
                    .query_opt(
                        client,
                        &sql::COMMIT_PROJECTION,
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
        config: &Config,
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
                reset_statement: self.reset_statement,
                shared: Arc::new(runner::Shared::new(
                    self.options.background.clone(),
                    &config.projections,
                )),
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
    /// The append statement on a pooled session. Its bounds are computed
    /// after the session wait, from the time then left. A deadline cut while
    /// the statement runs leaves its outcome unknown.
    async fn append(&self, a: &Append, deadline: Instant) -> Result<Row, Failure> {
        let client = self.db.acquire(deadline).await?;
        let statement = match tokio::time::timeout_at(
            deadline.into(),
            self.db.prepare_cached(&client, &sql::APPEND),
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
        let Some((lock_timeout_ms, statement_ms)) = self.append_bounds(deadline) else {
            return Err(timeout());
        };
        let key = a.key.as_ref();
        let part = |i: usize| key.map(|k| k[i].as_str());
        let (traceparent, tracestate) = (
            a.trace.as_ref().map(TraceContext::traceparent),
            a.trace.as_ref().and_then(TraceContext::tracestate),
        );
        let (key_tag, key_tenant, key_actor, key_text) = (part(0), part(1), part(2), part(3));
        let params: [&(dyn tokio_postgres::types::ToSql + Sync); 16] = [
            &a.stream,
            &a.tenant,
            &a.expected,
            &a.tags,
            &a.schema_versions,
            &a.payloads,
            &a.metadata,
            &a.accepted_unix_ms,
            &traceparent,
            &tracestate,
            &key_tag,
            &key_tenant,
            &key_actor,
            &key_text,
            &a.fingerprint,
            &lock_timeout_ms,
        ];
        let bound = format!("SET statement_timeout = {statement_ms}");
        // Pipelined in order in one round trip: the session statement
        // timeout, the autocommit append, then the session default again.
        // The head lock is held only by the middle statement.
        // Each is bounded on its own, so an append that answered keeps its
        // known outcome even when only the reset is late.
        let at = tokio::time::Instant::from_std(deadline);
        let (bounded, result, reset) = tokio::join!(
            biased;
            tokio::time::timeout_at(
                at,
                self.db.batch_text(&client, &sql::APPEND_TIMEOUT, &bound)
            ),
            tokio::time::timeout_at(
                at,
                self.db
                    .query_one_prepared(&client, &sql::APPEND, &statement, &params)
            ),
            tokio::time::timeout_at(
                at,
                self.db
                    .batch_text(&client, &sql::RESET_TIMEOUT, self.reset_statement)
            ),
        );
        let (bounded, reset) = (
            bounded.ok().and_then(Result::ok),
            reset.ok().and_then(Result::ok),
        );
        let Ok(result) = result else {
            drop(PooledClient::take(client));
            return Err(commit_unknown());
        };
        let failure = result.as_ref().err().map(append_error);
        // A session whose statement timeout may not be the default again,
        // or whose outcome is unknown, is closed instead of pooled.
        if bounded.is_none()
            || reset.is_none()
            || failure
                .as_ref()
                .is_some_and(|f| f.code == "commit_outcome_unknown")
        {
            drop(PooledClient::take(client));
        }
        match failure {
            Some(failure) => Err(failure),
            None => result.map_err(|e| append_error(&e)),
        }
    }
}

/// Reads and writes of the projection rows, shared by queries, runs, the
/// scheduler and the admin API.
impl PostgresStore {
    pub(crate) fn shared(&self) -> &runner::Shared {
        &self.shared
    }
    /// A configured projection's `(name, revision)` key.
    fn key_of(&self, projection: &crate::config::Projection) -> Result<(String, i32), Failure> {
        self.configured(&ProjectionRef {
            name: &projection.name,
            revision: projection.revision,
        })
    }
    /// The served pair, its row revision and its `warm_position`.
    pub(crate) async fn served_row(
        &self,
        name: String,
        revision: i32,
        deadline: Instant,
    ) -> Result<(ProjectionState, i64, u64), Failure> {
        let row = self
            .session(deadline, timeout, async |client| {
                self.db
                    .query_opt(client, &sql::PROJECTION, &[&name, &revision])
                    .await
                    .map_err(|e| read_error(&e))
            })
            .await?;
        let row = row.ok_or_else(invalid)?;
        // A partitioned row has no single state; this build has none.
        let text: Option<&str> = row.get(0);
        let state: serde_json::Value = decode(text.ok_or_else(invalid)?, MAX_STATE_BYTES)?;
        compatible_json(&state).map_err(|_| invalid())?;
        Ok((
            ProjectionState {
                state,
                checkpoint: unsigned(row.get(1))?,
            },
            row.get(2),
            unsigned(row.get(3))?,
        ))
    }
    pub(crate) fn served(
        &self,
        projection: &crate::config::Projection,
        deadline: Instant,
    ) -> Result<(ProjectionState, i64, u64), Failure> {
        let (name, revision) = self.key_of(projection)?;
        self.block_on(deadline, self.served_row(name, revision, deadline))
    }
    pub(crate) async fn head_async(&self, deadline: Instant) -> Result<u64, Failure> {
        let position: i64 = self
            .session(deadline, timeout, async |client| {
                Ok(self
                    .db
                    .query_one(client, &sql::HEAD, &[])
                    .await
                    .map_err(|e| read_error(&e))?
                    .get(0))
            })
            .await?;
        unsigned(position)
    }
    pub(crate) fn head_at(&self, deadline: Instant) -> Result<u64, Failure> {
        self.block_on(deadline, self.head_async(deadline))
    }
    /// Request a rebuild (§A.7 step 1): the next generation from the
    /// configured initial state up to the current head. With `resume`, an
    /// existing rebuild (failed or running) runs again from its candidate;
    /// without it, an existing one fails `rebuild_in_progress`. Returns the
    /// target and wakes this instance's rebuild lane.
    pub(crate) fn request_rebuild(
        &self,
        projection: &crate::config::Projection,
        resume: bool,
        requested_by: Option<&str>,
        deadline: Instant,
    ) -> Result<u64, Failure> {
        let (name, revision) = self.key_of(projection)?;
        let initial = encode(&projection.initial)?;
        let by = requested_by.map(str::to_owned);
        let store_db = self.db.clone();
        let target: i64 = self.block_on(
            deadline,
            self.db.transaction(deadline, move |tx| {
                Box::pin(async move {
                    if let Some(row) = store_db
                        .query_opt(tx, &sql::INSERT_REBUILD, &[&name, &revision, &initial, &by])
                        .await
                        .map_err(|e| read_error(&e))?
                    {
                        return Ok(row.get(0));
                    }
                    if !resume {
                        return Err(Failure::conflict("rebuild_in_progress"));
                    }
                    let row = store_db
                        .query_opt(tx, &sql::RESUME_REBUILD, &[&name, &revision])
                        .await
                        .map_err(|e| read_error(&e))?;
                    // Swapped between the two statements: nothing to resume.
                    row.map(|row| row.get(0))
                        .ok_or_else(|| Failure::conflict("rebuild_in_progress"))
                })
            }),
        )?;
        self.shared.lane(projection)?.rebuild.notify_one();
        unsigned(target)
    }
    /// `Some(true)` while a rebuild runs, `Some(false)` when it failed,
    /// `None` without one.
    pub(crate) async fn rebuild_status_async(
        &self,
        name: String,
        revision: i32,
        deadline: Instant,
    ) -> Result<Option<bool>, Failure> {
        let row = self
            .session(deadline, timeout, async |client| {
                self.db
                    .query_opt(client, &sql::REBUILD_STATUS, &[&name, &revision])
                    .await
                    .map_err(|e| read_error(&e))
            })
            .await?;
        Ok(row.map(|row| row.get::<_, &str>(0) == "running"))
    }
    pub(crate) fn rebuild_status(
        &self,
        projection: &crate::config::Projection,
        deadline: Instant,
    ) -> Result<Option<bool>, Failure> {
        let (name, revision) = self.key_of(projection)?;
        self.block_on(
            deadline,
            self.rebuild_status_async(name, revision, deadline),
        )
    }
    /// Abandon a rebuild; its runner stops at its next batch. Idempotent.
    pub(crate) fn abandon_rebuild(
        &self,
        projection: &crate::config::Projection,
        deadline: Instant,
    ) -> Result<(), Failure> {
        let (name, revision) = self.key_of(projection)?;
        self.block_on(
            deadline,
            self.session(deadline, timeout, async |client| {
                self.db
                    .execute(client, &sql::DELETE_REBUILD, &[&name, &revision])
                    .await
                    .map_err(|e| read_error(&e))
            }),
        )?;
        Ok(())
    }
    /// Delete a revision row this instance does not serve (and with it its
    /// rebuild), also of a projection no longer configured.
    /// `revision_active` for the configured revision,
    /// `projection_not_found` for a row that does not exist.
    pub(crate) fn delete_revision(
        &self,
        name: &Identity,
        revision: u32,
        deadline: Instant,
    ) -> Result<(), Failure> {
        // Rows of projections no longer configured can be cleaned up too.
        if self.projections.contains(&(name.clone(), revision)) {
            return Err(Failure::conflict("revision_active"));
        }
        let text = name.as_str().to_owned();
        let revision =
            i32::try_from(revision).map_err(|_| Failure::permanent("projection_not_found"))?;
        let deleted = self.block_on(
            deadline,
            self.session(deadline, timeout, async |client| {
                self.db
                    .execute(client, &sql::DELETE_REVISION, &[&text, &revision])
                    .await
                    .map_err(|e| read_error(&e))
            }),
        )?;
        if deleted == 0 {
            return Err(Failure::permanent("projection_not_found"));
        }
        Ok(())
    }
    /// Record a failed background run on the row (`status = 'failing'`);
    /// the served pair stays.
    pub(crate) async fn record_failure(
        &self,
        name: String,
        revision: i32,
        failure: &Failure,
        deadline: Instant,
    ) -> Result<(), Failure> {
        let code = failure.code.clone();
        let class = serde_json::to_value(failure.classification)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .ok_or_else(invalid)?;
        self.session(deadline, timeout, async |client| {
            self.db
                .execute(
                    client,
                    &sql::RECORD_FAILURE,
                    &[&name, &revision, &code, &class],
                )
                .await
                .map_err(|e| read_error(&e))
        })
        .await?;
        Ok(())
    }
}
