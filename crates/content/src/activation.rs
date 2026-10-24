//! Model activation (section 2, schema changes; section 3, derived
//! indexes): one host at a time, under a session advisory lock held on a
//! connection detached from the pool, builds the indexes a pending model
//! adds, activates it in one transaction with its outbox event, then drops
//! the indexes it no longer needs.
use crate::{
    diff::{Change, ChangeClass, change_classes, diff},
    indexes::{DerivedIndex, derived_indexes, drop_statement, is_derived_name},
    model::ContentModel,
    value::{KeySource, Stage, ValidateOptions, ValueError},
};
use merezha_domain::{Classification, Failure, Identity};
use merezha_platform::{
    Database, Random,
    ports::{DedupeKey, MessageContext, NewEvent, OnDuplicate, SourceOutbox},
};
use merezha_postgres::{DetachedClient, classify, tokio_postgres};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::{Duration, Instant},
};

/// Session advisory lock of model activation: the ASCII bytes of
/// `mzconten`. Distinct from the kernel migration lock, which the
/// migration run has released before modules start.
pub const ACTIVATION_LOCK: i64 = 0x6d7a_636f_6e74_656e;
/// Bound on one activation run, index builds included.
pub const ACTIVATION_TIMEOUT: Duration = Duration::from_secs(300);
/// Attempts per index build: the first and two retries (section 3).
pub const BUILD_ATTEMPTS: u32 = 3;
/// Bound on the work after a committed activation that cannot fail it:
/// each unneeded index drop, and the tightening count.
pub const POST_ACTIVATION_BUDGET: Duration = Duration::from_secs(30);
/// Rows read per query when counting what a tightened rule rejects.
pub const TIGHTENING_BATCH: i64 = 500;
/// Most rows per type and table counted for `content.model_tightened`.
pub const TIGHTENING_SCAN_LIMIT: u64 = 100_000;
/// The event appended when a model turns active.
pub const MODEL_ACTIVATED: &str = "content.model_activated";
/// Pause between attempts to take the activation lock.
const LOCK_POLL: Duration = Duration::from_millis(50);
/// Times a lost session is reopened within one activation.
const MAX_RECONNECTS: u32 = 2;

/// The active row of `merezha_content.models`.
#[derive(Clone, Debug, PartialEq)]
pub struct ActiveModel {
    pub version: u32,
    pub fingerprint: String,
    /// The canonical form stored as `definition`.
    pub definition: Value,
}

/// What the database holds.
#[derive(Clone, Debug, PartialEq)]
pub enum StoredModel {
    /// Migration 1 is not applied yet.
    NoSchema,
    /// No model was ever activated.
    NoModel,
    Active(ActiveModel),
}
impl StoredModel {
    pub fn active(&self) -> Option<&ActiveModel> {
        match self {
            StoredModel::Active(active) => Some(active),
            _ => None,
        }
    }
}

/// The loaded model compared with the active one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Nothing is active: the model activates as the first version.
    Fresh,
    /// The loaded model is the active one.
    Current,
    /// A newer version whose changes all activate in v1.
    Pending,
    /// The active model is newer than the loaded one: this host is an old
    /// one in a rolling deploy (`content_model_outdated`).
    Outdated,
    /// Same version, another fingerprint (`content_model_version_unchanged`).
    VersionUnchanged,
    /// A newer version with refused changes (`content_model_breaking`).
    Breaking,
}

impl Status {
    /// How `merezha content diff` reports it: the status word or failure
    /// code, and what it means.
    pub fn describe(self) -> &'static str {
        match self {
            Status::Fresh => "fresh: the model activates as the first version",
            Status::Current => "current",
            Status::Pending => "pending: the changes activate",
            Status::Outdated => "content_model_outdated: the active model is newer",
            Status::VersionUnchanged => {
                "content_model_version_unchanged: the fingerprint changed, the version did not"
            }
            Status::Breaking => "content_model_breaking: refused changes",
        }
    }
}

/// The classified comparison of the loaded model with the active one.
#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    pub status: Status,
    pub previous_version: Option<u32>,
    /// Empty for [`Status::Fresh`], [`Status::Current`] and
    /// [`Status::Outdated`].
    pub changes: Vec<Change>,
}
impl Plan {
    /// Compare `model` with the `active` row.
    pub fn new(active: Option<&ActiveModel>, model: &ContentModel) -> Self {
        let Some(active) = active else {
            return Self {
                status: Status::Fresh,
                previous_version: None,
                changes: Vec::new(),
            };
        };
        let previous_version = Some(active.version);
        let current = |status| Self {
            status,
            previous_version,
            changes: Vec::new(),
        };
        if model.version() < active.version {
            return current(Status::Outdated);
        }
        if model.version() == active.version && model.fingerprint() == active.fingerprint {
            return current(Status::Current);
        }
        let new: Value = serde_json::from_str(model.canonical_json()).unwrap_or(Value::Null);
        let changes = diff(&active.definition, &new);
        let status = if model.version() == active.version {
            Status::VersionUnchanged
        } else if changes.iter().any(|c| c.class == ChangeClass::Refused) {
            Status::Breaking
        } else {
            Status::Pending
        };
        Self {
            status,
            previous_version,
            changes,
        }
    }
    /// The startup failure this plan is, if any.
    pub fn failure(&self) -> Option<Failure> {
        match self.status {
            Status::Breaking => Some(Failure::permanent("content_model_breaking")),
            Status::VersionUnchanged => Some(Failure::permanent("content_model_version_unchanged")),
            _ => None,
        }
    }
    /// The refused changes.
    pub fn refused(&self) -> impl Iterator<Item = &Change> {
        self.changes
            .iter()
            .filter(|c| c.class == ChangeClass::Refused)
    }
    /// `change_classes` of the activation event: `["additive"]` for a
    /// first activation.
    pub fn change_classes(&self) -> Vec<&'static str> {
        match self.status {
            Status::Fresh => vec![ChangeClass::Additive.as_str()],
            _ => change_classes(&self.changes),
        }
    }
}

/// How activation runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActivationOptions {
    /// Bound on the whole run.
    pub timeout: Duration,
    /// Pause before the first retry of a failed index build; doubled for
    /// the second.
    pub retry_backoff: Duration,
}
impl Default for ActivationOptions {
    fn default() -> Self {
        Self {
            timeout: ACTIVATION_TIMEOUT,
            retry_backoff: Duration::from_secs(1),
        }
    }
}

/// The index whose build stopped activation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexFailure {
    pub name: String,
    pub content_type: String,
    pub field: String,
    /// The last attempt's classified code, such as `unique_violation`.
    pub code: String,
    pub attempts: u32,
}

/// Why activation stopped. The active model and its hosts are untouched.
#[derive(Clone, Debug, PartialEq)]
pub struct ActivationError {
    /// `content_model_breaking`, `content_model_version_unchanged`,
    /// `index_build_failed`, `content_activation_timeout` or a classified
    /// database failure.
    pub failure: Failure,
    /// The classified changes, for the two model failures.
    pub changes: Vec<Change>,
    pub index: Option<Box<IndexFailure>>,
}
impl ActivationError {
    fn new(failure: Failure) -> Self {
        Self {
            failure,
            changes: Vec::new(),
            index: None,
        }
    }
}
impl std::fmt::Display for ActivationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.failure.code)?;
        if let Some(index) = &self.index {
            write!(
                f,
                " ({} on {}.{}, {} attempts, {})",
                index.name, index.content_type, index.field, index.attempts, index.code
            )?;
        }
        Ok(())
    }
}
impl std::error::Error for ActivationError {}

/// Rows a tightened rule would now reject, per top-level field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TightenedCount {
    pub content_type: String,
    pub field: String,
    /// Drafts failing save-time validation.
    pub drafts: u64,
    /// Published rows failing publish-time validation.
    pub published: u64,
    /// The scan stopped at [`TIGHTENING_SCAN_LIMIT`] rows.
    pub truncated: bool,
}

/// What an activation run did.
#[derive(Clone, Debug, PartialEq)]
pub struct ActivationReport {
    pub plan: Plan,
    /// This run turned the loaded model active.
    pub activated: bool,
    /// Derived indexes built, in order.
    pub created: Vec<String>,
    /// Derived indexes dropped after activation.
    pub dropped: Vec<String>,
    /// Unneeded derived indexes whose drop failed; logged as
    /// `content.index_failed` and retried by the next start.
    pub undropped: Vec<String>,
    pub tightened: Vec<TightenedCount>,
}

/// Read the active model.
pub async fn read_active(
    client: &impl tokio_postgres::GenericClient,
) -> Result<StoredModel, Failure> {
    let exists: bool = client
        .query_one(
            "SELECT to_regclass('merezha_content.models') IS NOT NULL",
            &[],
        )
        .await
        .map_err(|e| classify(&e))?
        .get(0);
    if !exists {
        return Ok(StoredModel::NoSchema);
    }
    let row = client
        .query_opt(
            "SELECT version, fingerprint, definition::text FROM merezha_content.models \
             WHERE state = 'active'",
            &[],
        )
        .await
        .map_err(|e| classify(&e))?;
    let Some(row) = row else {
        return Ok(StoredModel::NoModel);
    };
    let version: i32 = row.get(0);
    let definition: String = row.get(2);
    Ok(StoredModel::Active(ActiveModel {
        version: u32::try_from(version).map_err(|_| Failure::permanent("database_error"))?,
        fingerprint: row.get(1),
        definition: serde_json::from_str(&definition)
            .map_err(|_| Failure::permanent("database_error"))?,
    }))
}

/// Existing derived indexes by name, with `pg_index.indisvalid`.
pub async fn existing_indexes(
    client: &impl tokio_postgres::GenericClient,
) -> Result<BTreeMap<String, bool>, Failure> {
    let rows = client
        .query(
            "SELECT c.relname::text, i.indisvalid FROM pg_index i \
             JOIN pg_class c ON c.oid = i.indexrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'merezha_content' AND c.relname LIKE 'content\\_ix\\_%'",
            &[],
        )
        .await
        .map_err(|e| classify(&e))?;
    Ok(rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, bool>(1)))
        .filter(|(name, _)| is_derived_name(name))
        .collect())
}

/// The derived indexes of `model` that are missing or invalid.
pub fn missing_indexes<'a>(
    expected: &'a [DerivedIndex],
    existing: &BTreeMap<String, bool>,
) -> Vec<&'a DerivedIndex> {
    expected
        .iter()
        .filter(|index| existing.get(&index.name) != Some(&true))
        .collect()
}

/// Derived indexes that exist and `model` does not need.
pub fn unneeded_indexes(
    expected: &[DerivedIndex],
    existing: &BTreeMap<String, bool>,
) -> Vec<String> {
    let wanted: BTreeSet<&str> = expected.iter().map(|i| i.name.as_str()).collect();
    existing
        .keys()
        .filter(|name| !wanted.contains(name.as_str()))
        .cloned()
        .collect()
}

/// Activate `model` when it is pending and reconcile its derived indexes,
/// as one host at a time (section 2): under [`ACTIVATION_LOCK`] on a
/// detached session, build every missing or invalid index of the model
/// (each attempted up to [`BUILD_ATTEMPTS`] times for transient failures),
/// then in one transaction supersede the active row, insert the model as
/// active, sync `locales`, bump every scope's generation and append
/// `content.model_activated` through `outbox`, then drop the derived
/// indexes the model does not need and count the rows tightened rules
/// reject.
///
/// A current model only gets the index reconciliation, which repairs a
/// crashed earlier run. An outdated one (an older host in a rolling deploy)
/// changes nothing. Refused changes and a reused version fail before
/// anything is written. A cancelled or failed run closes its session, which
/// releases the lock. Rebuilding `published` fallback rows for a new locale
/// or changed fallback is C4's (it owns materialization); until C4 nothing
/// writes `published`.
pub async fn activate(
    db: &Database,
    outbox: &SourceOutbox,
    random: &dyn Random,
    model: &ContentModel,
    options: &ActivationOptions,
) -> Result<ActivationReport, ActivationError> {
    let deadline = Instant::now() + options.timeout;
    let mut run = Run {
        db,
        outbox,
        random,
        model,
        options,
        deadline,
        attempts: HashMap::new(),
        created: Vec::new(),
        committed: None,
    };
    let at = tokio::time::Instant::from_std(deadline);
    let mut reconnects = 0;
    let mut report = loop {
        match tokio::time::timeout_at(at, run.once()).await {
            // Past the commit only optional work remains: the activation
            // stands, and the next start finishes the index drops.
            Err(_) if run.committed.is_some() => {
                break ActivationReport {
                    plan: run
                        .committed
                        .take()
                        .unwrap_or_else(|| Plan::new(None, model)),
                    activated: true,
                    created: std::mem::take(&mut run.created),
                    dropped: Vec::new(),
                    undropped: Vec::new(),
                    tightened: Vec::new(),
                };
            }
            Err(_) => {
                return Err(ActivationError::new(Failure::transient(
                    "content_activation_timeout",
                )));
            }
            Ok(Ok(report)) => break report,
            Ok(Err(Stop::Failed(error))) => return Err(*error),
            Ok(Err(Stop::Lost)) if reconnects < MAX_RECONNECTS => reconnects += 1,
            Ok(Err(Stop::Lost)) => {
                return Err(ActivationError::new(Failure::transient(
                    "database_unavailable",
                )));
            }
        }
    };
    // The lock is released: the tightening count is informational, runs
    // on a pooled session with its own bound, and never fails activation.
    if report.activated
        && report
            .plan
            .changes
            .iter()
            .any(|c| c.class == ChangeClass::Tightening)
    {
        let budget = tokio::time::Instant::now() + POST_ACTIVATION_BUDGET;
        let count = run.count_tightened(&report.plan);
        match tokio::time::timeout_at(budget, count).await {
            Ok(counts) => report.tightened = counts,
            Err(_) => tracing::warn!(
                event = "content.model_tightened",
                version = model.version(),
                code = "content_activation_timeout"
            ),
        }
    }
    Ok(report)
}

/// Readiness of a host that does not activate (`database.migrate =
/// false`), or after its own run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verified {
    Ready,
    /// Not ready, with `content_model_pending`, `content_model_outdated` or
    /// `index_build_pending`.
    NotReady(&'static str),
}

/// Check `model` against the database without changing anything: a
/// pending model or a missing schema is `content_model_pending`, an older
/// one `content_model_outdated`, a current one with a missing or invalid
/// derived index `index_build_pending`. Refused changes and a reused
/// version fail with their startup codes.
pub async fn verify(
    client: &impl tokio_postgres::GenericClient,
    model: &ContentModel,
) -> Result<Verified, ActivationError> {
    let stored = read_active(client).await.map_err(ActivationError::new)?;
    let plan = Plan::new(stored.active(), model);
    if let Some(failure) = plan.failure() {
        return Err(ActivationError {
            failure,
            changes: plan.changes,
            index: None,
        });
    }
    Ok(match plan.status {
        Status::Outdated => Verified::NotReady("content_model_outdated"),
        Status::Current => {
            let existing = existing_indexes(client)
                .await
                .map_err(ActivationError::new)?;
            if missing_indexes(&derived_indexes(model), &existing).is_empty() {
                Verified::Ready
            } else {
                Verified::NotReady("index_build_pending")
            }
        }
        _ => Verified::NotReady("content_model_pending"),
    })
}

/// Log every refused change as `content.model_breaking`.
pub fn log_breaking(error: &ActivationError, version: u32) {
    for change in error
        .changes
        .iter()
        .filter(|c| c.class == ChangeClass::Refused)
    {
        tracing::error!(
            event = "content.model_breaking",
            code = error.failure.code.as_str(),
            version,
            path = change.path.as_str(),
            change = change.code
        );
    }
    if error.failure.code == "content_model_version_unchanged" {
        tracing::error!(
            event = "content.model_breaking",
            code = "content_model_version_unchanged",
            version
        );
    }
}

enum Stop {
    /// The session died, and its lock with it: reopen and plan again.
    Lost,
    Failed(Box<ActivationError>),
}
impl Stop {
    fn failed(error: ActivationError) -> Self {
        Stop::Failed(Box::new(error))
    }
}

struct Run<'a> {
    db: &'a Database,
    outbox: &'a SourceOutbox,
    random: &'a dyn Random,
    model: &'a ContentModel,
    options: &'a ActivationOptions,
    deadline: Instant,
    /// Build attempts per index, across reconnects.
    attempts: HashMap<String, u32>,
    created: Vec<String>,
    /// The plan this run committed, once the activation transaction did.
    committed: Option<Plan>,
}

impl Run<'_> {
    /// A database error: `Lost` when the session closed, else classified.
    fn stop(session: &DetachedClient, failure: Failure) -> Stop {
        // `database_unavailable` is a connection-level failure (lost
        // connection, terminated backend); the session cannot be trusted.
        if session.is_closed() || failure.code == "database_unavailable" {
            Stop::Lost
        } else {
            Stop::failed(ActivationError::new(failure))
        }
    }

    async fn once(&mut self) -> Result<ActivationReport, Stop> {
        let mut session = self
            .db
            .detach(self.deadline)
            .await
            .map_err(|failure| Stop::failed(ActivationError::new(failure)))?;
        self.statement_timeout(&session).await?;
        self.lock(&session).await?;
        let stored = read_active(&*session)
            .await
            .map_err(|f| Self::stop(&session, f))?;
        if stored == StoredModel::NoSchema {
            return Err(Stop::failed(ActivationError::new(Failure::transient(
                "content_model_pending",
            ))));
        }
        let plan = Plan::new(stored.active(), self.model);
        if let Some(failure) = plan.failure() {
            let error = ActivationError {
                failure,
                changes: plan.changes,
                index: None,
            };
            log_breaking(&error, self.model.version());
            return Err(Stop::failed(error));
        }
        if plan.status == Status::Outdated {
            return Ok(ActivationReport {
                plan,
                activated: false,
                created: Vec::new(),
                dropped: Vec::new(),
                undropped: Vec::new(),
                tightened: Vec::new(),
            });
        }
        let expected = derived_indexes(self.model);
        let existing = existing_indexes(&*session)
            .await
            .map_err(|f| Self::stop(&session, f))?;
        for index in missing_indexes(&expected, &existing) {
            self.build(&session, index).await?;
        }
        let activated = matches!(plan.status, Status::Fresh | Status::Pending);
        if activated {
            self.commit(&mut session, &plan).await?;
            self.committed = Some(plan.clone());
        }
        let (dropped, undropped) = self.drop_unneeded(&session, &expected).await;
        // Dropping the session releases the lock.
        drop(session);
        Ok(ActivationReport {
            plan,
            activated,
            created: std::mem::take(&mut self.created),
            dropped,
            undropped,
            tightened: Vec::new(),
        })
    }

    /// Drop the derived indexes `expected` does not contain, under the
    /// lock so no concurrent activation's new index is mistaken for an old
    /// one. Each drop is bounded by [`POST_ACTIVATION_BUDGET`]; a failure
    /// is logged and left to the next start, never fatal: the model is
    /// already active and an extra index only costs writes.
    async fn drop_unneeded(
        &self,
        session: &DetachedClient,
        expected: &[DerivedIndex],
    ) -> (Vec<String>, Vec<String>) {
        let (mut dropped, mut undropped) = (Vec::new(), Vec::new());
        let existing = match existing_indexes(&**session).await {
            Ok(existing) => existing,
            Err(failure) => {
                tracing::warn!(
                    event = "content.index_failed",
                    operation = "drop",
                    code = failure.code.as_str()
                );
                return (dropped, undropped);
            }
        };
        for name in unneeded_indexes(expected, &existing) {
            let left = self
                .deadline
                .saturating_duration_since(Instant::now())
                .min(POST_ACTIVATION_BUDGET);
            let set = format!("SET statement_timeout = {}", left.as_millis().max(1));
            let result = match session.batch_execute(&set).await {
                Ok(()) => session.batch_execute(&drop_statement(&name)).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => {
                    tracing::info!(event = "content.index_dropped", index = name.as_str());
                    dropped.push(name);
                }
                Err(error) => {
                    tracing::warn!(
                        event = "content.index_failed",
                        operation = "drop",
                        index = name.as_str(),
                        code = classify(&error).code.as_str()
                    );
                    undropped.push(name);
                    if session.is_closed() {
                        break;
                    }
                }
            }
        }
        (dropped, undropped)
    }

    /// Bound the session's statements by the time left.
    async fn statement_timeout(&self, session: &DetachedClient) -> Result<(), Stop> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        if left < Duration::from_millis(1) {
            return Err(Stop::failed(ActivationError::new(Failure::transient(
                "content_activation_timeout",
            ))));
        }
        session
            .batch_execute(&format!(
                "SET statement_timeout = {}",
                left.as_millis().min(i32::MAX as u128)
            ))
            .await
            .map_err(|e| Self::stop(session, classify(&e)))
    }

    /// Poll `pg_try_advisory_lock`: a blocking `pg_advisory_lock` would
    /// hold a snapshot that the holder's `CREATE INDEX CONCURRENTLY` waits
    /// for, a deadlock (as in the kernel migration runner).
    async fn lock(&self, session: &DetachedClient) -> Result<(), Stop> {
        loop {
            let locked: bool = session
                .query_one("SELECT pg_try_advisory_lock($1)", &[&ACTIVATION_LOCK])
                .await
                .map_err(|e| Self::stop(session, classify(&e)))?
                .get(0);
            if locked {
                return Ok(());
            }
            if Instant::now() + LOCK_POLL >= self.deadline {
                return Err(Stop::failed(ActivationError::new(Failure::transient(
                    "content_activation_timeout",
                ))));
            }
            tokio::time::sleep(LOCK_POLL).await;
        }
    }

    /// Drop `name` when a crashed or failed concurrent build left it
    /// invalid, so the `IF NOT EXISTS` build creates it again.
    async fn drop_invalid(session: &DetachedClient, name: &str) -> Result<(), Failure> {
        let invalid = session
            .query_opt(
                "SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'merezha_content' AND c.relname = $1 AND NOT i.indisvalid",
                &[&name],
            )
            .await
            .map_err(|e| classify(&e))?;
        if invalid.is_some() {
            session
                .batch_execute(&drop_statement(name))
                .await
                .map_err(|e| classify(&e))?;
            tracing::warn!(
                event = "content.index_dropped",
                index = name,
                reason = "invalid"
            );
        }
        Ok(())
    }

    async fn build(&mut self, session: &DetachedClient, index: &DerivedIndex) -> Result<(), Stop> {
        loop {
            let attempt = {
                let count = self.attempts.entry(index.name.clone()).or_insert(0);
                *count += 1;
                *count
            };
            self.statement_timeout(session).await?;
            let result = match Self::drop_invalid(session, &index.name).await {
                Ok(()) => session
                    .batch_execute(&index.create)
                    .await
                    .map_err(|e| classify(&e)),
                Err(failure) => Err(failure),
            };
            let failure = match result {
                Ok(()) => {
                    tracing::info!(
                        event = "content.index_created",
                        index = index.name.as_str(),
                        content_type = index.content_type.as_str(),
                        field = index.field.as_str(),
                        table = index.table.as_str(),
                        purpose = index.purpose.as_str()
                    );
                    self.created.push(index.name.clone());
                    return Ok(());
                }
                Err(failure) => failure,
            };
            let lost = session.is_closed() || failure.code == "database_unavailable";
            if !lost {
                // A failed build is dropped; the next attempt or start
                // rebuilds it.
                let _ = Self::drop_invalid(session, &index.name).await;
            }
            let retry = failure.classification == Classification::Transient;
            if !retry || attempt >= BUILD_ATTEMPTS {
                tracing::error!(
                    event = "content.index_failed",
                    code = "index_build_failed",
                    index = index.name.as_str(),
                    content_type = index.content_type.as_str(),
                    field = index.field.as_str(),
                    cause = failure.code.as_str(),
                    attempts = attempt
                );
                return Err(Stop::failed(ActivationError {
                    failure: Failure::permanent("index_build_failed"),
                    changes: Vec::new(),
                    index: Some(Box::new(IndexFailure {
                        name: index.name.clone(),
                        content_type: index.content_type.clone(),
                        field: index.field.clone(),
                        code: failure.code.clone(),
                        attempts: attempt,
                    })),
                }));
            }
            if lost {
                return Err(Stop::Lost);
            }
            let pause = self.options.retry_backoff * 2u32.pow(attempt - 1);
            let wake = (Instant::now() + pause).min(self.deadline);
            tokio::time::sleep_until(tokio::time::Instant::from_std(wake)).await;
        }
    }

    /// The activation transaction.
    async fn commit(&self, session: &mut DetachedClient, plan: &Plan) -> Result<(), Stop> {
        let version = i32::try_from(self.model.version())
            .map_err(|_| Stop::failed(ActivationError::new(Failure::permanent("invalid_input"))))?;
        let previous = plan.previous_version.map(|v| v as i32);
        let fingerprint = self.model.fingerprint().to_owned();
        let canonical = self.model.canonical_json().to_owned();
        let default_locale = self.model.default_locale().code.clone();
        let locales: Vec<(String, Option<String>)> = self
            .model
            .locales()
            .iter()
            .map(|l| (l.code.clone(), l.fallback.clone()))
            .collect();
        let record = self
            .event(plan)
            .map_err(|f| Stop::failed(ActivationError::new(f)))?;
        let outbox = self.outbox.clone();
        let result = session
            .transaction(self.deadline, move |tx| {
                Box::pin(async move {
                    let active = tx
                        .query_opt(
                            "SELECT version FROM merezha_content.models \
                         WHERE state = 'active' FOR UPDATE",
                            &[],
                        )
                        .await
                        .map_err(|e| classify(&e))?
                        .map(|row| row.get::<_, i32>(0));
                    if active != previous {
                        // Another activation committed between plan and lock;
                        // the lock makes this unreachable, so refuse to guess.
                        return Err(Failure::transient("content_model_conflict"));
                    }
                    tx.execute(
                    "UPDATE merezha_content.models SET state = 'superseded' WHERE state = 'active'",
                    &[],
                )
                .await
                .map_err(|e| classify(&e))?;
                    tx.execute(
                        "INSERT INTO merezha_content.models \
                     (version, fingerprint, definition, state, activated_at) \
                     VALUES ($1, $2, $3::text::jsonb, 'active', now())",
                        &[&version, &fingerprint, &canonical],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    // Rows are never deleted: v1 refuses locale removal, and
                    // retiring one is C10's.
                    for (code, _) in &locales {
                        let is_default = *code == default_locale;
                        tx.execute(
                        "INSERT INTO merezha_content.locales (code, is_default) VALUES ($1, $2) \
                         ON CONFLICT (code) DO NOTHING",
                        &[code, &is_default],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    }
                    for (code, fallback) in &locales {
                        tx.execute(
                            "UPDATE merezha_content.locales SET fallback = $2 \
                         WHERE code = $1 AND fallback IS DISTINCT FROM $2",
                            &[code, fallback],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    }
                    // Every scope's ETags change with the model (access rules,
                    // locales, the model endpoint).
                    tx.execute(
                        "UPDATE merezha_content.generations SET generation = generation + 1",
                        &[],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    outbox
                        .append(tx, std::slice::from_ref(&record), OnDuplicate::Fail)
                        .await?;
                    Ok(())
                })
            })
            .await;
        match result {
            Ok(()) => {
                tracing::info!(
                    event = "content.model_activated",
                    version = self.model.version(),
                    previous_version = plan.previous_version,
                    fingerprint = self.model.fingerprint(),
                    change_classes = ?plan.change_classes()
                );
                Ok(())
            }
            Err(failure) => Err(Self::stop(session, failure)),
        }
    }

    fn event(&self, plan: &Plan) -> Result<merezha_platform::ports::OutboxRecord, Failure> {
        let mut random = [0u8; 10];
        self.random.fill(&mut random)?;
        let now = jiff::Timestamp::now();
        let millis = u64::try_from(now.as_millisecond()).unwrap_or(0);
        let id = uuid::Builder::from_unix_timestamp_millis(millis, &random)
            .into_uuid()
            .to_string();
        Ok(self.outbox.event(
            Identity::new(MODEL_ACTIVATED)?,
            NewEvent {
                dedupe_key: DedupeKey::new(id.clone())?,
                version: 1,
                occurred_at: now,
                payload: json!({
                    "version": self.model.version(),
                    "fingerprint": self.model.fingerprint(),
                    "previous_version": plan.previous_version,
                    "change_classes": plan.change_classes(),
                }),
                context: MessageContext {
                    correlation_id: Identity::new(id)?,
                    causation_id: None,
                    tenant: None,
                },
            },
        ))
    }

    /// Count, per top-level field, the drafts and published rows that the
    /// newly active rules reject, and log `content.model_tightened` per
    /// type. Informational: a failed count is logged, never fatal.
    async fn count_tightened(&self, plan: &Plan) -> Vec<TightenedCount> {
        let deadline = Instant::now() + POST_ACTIVATION_BUDGET;
        let client = match self.db.acquire(deadline).await {
            Ok(client) => client,
            Err(failure) => {
                tracing::warn!(
                    event = "content.model_tightened",
                    version = self.model.version(),
                    code = failure.code.as_str()
                );
                return Vec::new();
            }
        };
        let session: &tokio_postgres::Client = &client;
        let any_component = plan
            .changes
            .iter()
            .any(|c| c.class == ChangeClass::Tightening && c.path.starts_with("components."));
        let types: BTreeSet<&str> = self
            .model
            .types()
            .keys()
            .map(String::as_str)
            .filter(|name| {
                any_component
                    || plan.changes.iter().any(|c| {
                        c.class == ChangeClass::Tightening
                            && c.path.starts_with(&format!("types.{name}."))
                    })
            })
            .collect();
        let mut out = Vec::new();
        for name in types {
            match self.count_type(session, name).await {
                Ok(counts) => {
                    let summary: Vec<(&str, u64, u64)> = counts
                        .iter()
                        .map(|c| (c.field.as_str(), c.drafts, c.published))
                        .collect();
                    tracing::warn!(
                        event = "content.model_tightened",
                        version = self.model.version(),
                        content_type = name,
                        truncated = counts.iter().any(|c| c.truncated),
                        counts = ?summary
                    );
                    out.extend(counts);
                }
                Err(failure) => tracing::warn!(
                    event = "content.model_tightened",
                    version = self.model.version(),
                    content_type = name,
                    code = failure.code.as_str()
                ),
            }
        }
        out
    }

    async fn count_type(
        &self,
        session: &tokio_postgres::Client,
        name: &str,
    ) -> Result<Vec<TightenedCount>, Failure> {
        let Some(content_type) = self.model.content_type(name) else {
            return Ok(Vec::new());
        };
        let mut counts: BTreeMap<String, (u64, u64)> = content_type
            .fields
            .iter()
            .map(|f| (f.name.clone(), (0, 0)))
            .collect();
        let mut truncated = false;
        for published in [false, true] {
            let sql = if published {
                "SELECT entry_id::text, locale, source_locale, data::text FROM merezha_content.published \
                 WHERE content_type = $1 AND (entry_id, locale) > ($2::text::uuid, $3) \
                 ORDER BY entry_id, locale LIMIT $4"
            } else {
                "SELECT entry_id::text, locale, locale, draft_data::text FROM merezha_content.variants \
                 WHERE content_type = $1 AND NOT deleted AND (entry_id, locale) > ($2::text::uuid, $3) \
                 ORDER BY entry_id, locale LIMIT $4"
            };
            let mut after = (
                "00000000-0000-0000-0000-000000000000".to_owned(),
                String::new(),
            );
            let mut scanned = 0u64;
            loop {
                if scanned >= TIGHTENING_SCAN_LIMIT {
                    truncated = true;
                    break;
                }
                let rows = session
                    .query(sql, &[&name, &after.0, &after.1, &TIGHTENING_BATCH])
                    .await
                    .map_err(|e| classify(&e))?;
                for row in &rows {
                    let source_locale: String = row.get(2);
                    let data: String = row.get(3);
                    let Ok(data) = serde_json::from_str::<Value>(&data) else {
                        continue;
                    };
                    for field in self.rejected_fields(content_type, &source_locale, data, published)
                    {
                        if let Some(entry) = counts.get_mut(&field) {
                            if published {
                                entry.1 += 1;
                            } else {
                                entry.0 += 1;
                            }
                        }
                    }
                }
                scanned += rows.len() as u64;
                match rows.last() {
                    Some(last) if rows.len() as i64 == TIGHTENING_BATCH => {
                        after = (last.get(0), last.get(1));
                    }
                    _ => break,
                }
            }
        }
        Ok(counts
            .into_iter()
            .filter(|(_, (drafts, published))| drafts + published > 0)
            .map(|(field, (drafts, published))| TightenedCount {
                content_type: name.to_owned(),
                field,
                drafts,
                published,
                truncated,
            })
            .collect())
    }

    /// Top-level fields of one stored row that fail the active rules:
    /// save-time rules for drafts, publish-time rules for published rows.
    fn rejected_fields(
        &self,
        content_type: &crate::model::ContentType,
        source_locale: &str,
        mut data: Value,
        published: bool,
    ) -> BTreeSet<String> {
        let default_variant = source_locale == self.model.default_locale().code;
        if !default_variant && let Value::Object(fields) = &mut data {
            // Other variants carry copies of the default variant's shared
            // fields, which they may not write themselves.
            fields.retain(|name, _| content_type.field(name).is_none_or(|f| f.localized));
        }
        let options = ValidateOptions {
            stage: if published {
                Stage::Publish
            } else {
                Stage::Draft
            },
            default_variant,
            new_variant: false,
        };
        let mut keys = CountingKeys(0);
        match self
            .model
            .validate_fields(&content_type.name, &data, &options, &mut keys)
        {
            Err(ValueError::Invalid(details)) => details
                .iter()
                .filter_map(|d| {
                    d.path
                        .strip_prefix("/fields/")
                        .map(|rest| rest.split('/').next().unwrap_or(rest).to_owned())
                })
                .collect(),
            _ => BTreeSet::new(),
        }
    }
}

/// Keys for validation runs whose output is discarded.
struct CountingKeys(u64);
impl KeySource for CountingKeys {
    fn next_key(&mut self) -> Result<String, Failure> {
        self.0 += 1;
        Ok(format!("k{}", self.0))
    }
}
