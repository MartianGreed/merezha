//! The content block as a platform [`Module`]: `[modules.content]`, the
//! model file, migration 1, activation at start and readiness.
use crate::store::Store;
use crate::{
    ERRORS,
    activation::{ActivationOptions, Verified, activate, log_breaking, verify},
    model::ContentModel,
    parse::{CONTENT_PERMISSIONS, MAX_MODEL_BYTES, ModelOptions},
    schema::MIGRATIONS,
    settings::{ContentSettings, KEYS},
};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    AppContext, BoxFuture, Database, ErrorCode, Migration, Module, ModuleEvents, ModuleSection, Tx,
    config::{ConfigIssue, Origin},
    ports::{JobContext, JobFailure, JobHandler, KindOptions, ScheduleDeclaration},
};
use std::{
    io::Read,
    path::{Component, Path, PathBuf},
    sync::{
        Arc, OnceLock, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// The module name: section `[modules.content]`, routes `/v1/content`,
/// ledger key and outbox source `content`.
pub const NAME: &str = "content";
/// `[modules.content] model` when the key is absent.
pub const DEFAULT_MODEL_PATH: &str = "content.toml";
/// Extra time [`Module::start`] gets beyond the activation bound.
const START_MARGIN: Duration = Duration::from_secs(10);
/// Bound on one readiness evaluation, under the host's one second.
const READY_TIMEOUT: Duration = Duration::from_millis(900);
/// Events the block publishes (section 7). C2 appends the activation one;
/// the entry and asset ones arrive with their write paths.
pub const PUBLISHES: [&str; 10] = [
    "content.entry_draft_saved",
    "content.entry_published",
    "content.entry_unpublished",
    "content.entry_deleted",
    "content.entry_undeleted",
    "content.entry_purged",
    "content.asset_uploaded",
    "content.asset_updated",
    "content.asset_deleted",
    "content.model_activated",
];

/// The loaded model and what this host knows about it, shared by the
/// module, its routes (C3 onward) and its job handlers.
pub struct ContentState {
    model: Arc<ContentModel>,
    path: PathBuf,
    settings: ContentSettings,
    database: OnceLock<Database>,
    /// The loaded model was found active with every derived index valid.
    current: AtomicBool,
    /// A newer model is active: writes are refused and readiness is false
    /// for good, so the balancer drains this host.
    outdated: AtomicBool,
}
impl std::fmt::Debug for ContentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContentState")
            .field("version", &self.model.version())
            .field("current", &self.current.load(Ordering::SeqCst))
            .field("outdated", &self.outdated.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}
impl ContentState {
    /// The model this host loaded at startup and keeps until it stops.
    pub fn model(&self) -> &Arc<ContentModel> {
        &self.model
    }
    /// The resolved model file.
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// `[modules.content]` limits, revision retention and the trash.
    pub fn settings(&self) -> &ContentSettings {
        &self.settings
    }
    /// The pool, once [`Module::start`] ran.
    pub(crate) fn database(&self) -> Option<&Database> {
        self.database.get()
    }
    /// Wrap a content job handler so this host stops leasing its kind once
    /// a newer model is active (section 2, mixed model versions): the
    /// wrapper's [`JobHandler::accepts_leases`] turns `false` with
    /// [`ContentState::is_outdated`]. A job leased just before, or one whose
    /// handler meets `content_model_superseded` mid-run, is given back with
    /// [`JobFailure::release`]: available again at once with its attempt
    /// refunded, for a current host. C4's trash purge and C7's blob cleanup
    /// register through it.
    pub fn guard_jobs(self: &Arc<Self>, handler: Arc<dyn JobHandler>) -> Arc<dyn JobHandler> {
        Arc::new(GuardedHandler {
            state: self.clone(),
            handler,
        })
    }
    /// A newer model replaced the loaded one.
    pub fn is_outdated(&self) -> bool {
        self.outdated.load(Ordering::SeqCst)
    }
    /// The model version check every content write transaction starts
    /// with (section 2, mixed model versions): it share-locks the loaded
    /// model's row while it is active. When a newer model is active the
    /// host marks itself outdated (readiness `content_model_outdated`) and
    /// the write fails transient `content_model_superseded` (503,
    /// `Retry-After: 1`), so the caller rolls back and the client retries
    /// on a current host. When the loaded model is not active yet (a new
    /// host during a rolling deploy, before the migrating one activated
    /// it) the write fails transient `content_model_pending` and the host
    /// stays as it is. C3 onward call
    /// it first in every write; content job handlers do too and stop
    /// leasing once [`ContentState::is_outdated`].
    pub async fn check_writable(&self, tx: &mut Tx<'_>) -> Result<(), Failure> {
        match lock_model_version(tx, self.model.version()).await {
            Err(failure) if failure.code == SUPERSEDED => {
                self.mark_outdated();
                Err(failure)
            }
            other => other,
        }
    }
    /// Readiness reports it from now on (`health.not_ready` logs the
    /// transition).
    fn mark_outdated(&self) {
        self.outdated.store(true, Ordering::SeqCst);
    }
    /// Readiness: `content_model_outdated`, `content_model_pending`,
    /// `index_build_pending`, or a transient database failure.
    pub async fn readiness(&self) -> Result<(), Failure> {
        if self.is_outdated() {
            return Err(Failure::transient("content_model_outdated"));
        }
        let Some(database) = self.database.get() else {
            return Err(Failure::transient("content_model_pending"));
        };
        let deadline = Instant::now() + READY_TIMEOUT;
        let at = tokio::time::Instant::from_std(deadline);
        let check = async {
            let client = database.acquire(deadline).await?;
            if self.current.load(Ordering::SeqCst) {
                // Current already: only a newer activation can change that.
                let row = client
                    .query_opt(
                        "SELECT version FROM merezha_content.models WHERE state = 'active'",
                        &[],
                    )
                    .await
                    .map_err(|e| merezha_postgres::classify(&e))?;
                return match row.map(|r| r.get::<_, i32>(0)) {
                    Some(v) if v == self.model.version() as i32 => Ok(()),
                    Some(v) if v > self.model.version() as i32 => {
                        self.mark_outdated();
                        Err(Failure::transient("content_model_outdated"))
                    }
                    _ => Err(Failure::transient("content_model_pending")),
                };
            }
            match verify(&**client, &self.model).await {
                Ok(Verified::Ready) => {
                    self.current.store(true, Ordering::SeqCst);
                    Ok(())
                }
                Ok(Verified::NotReady("content_model_outdated")) => {
                    self.mark_outdated();
                    Err(Failure::transient("content_model_outdated"))
                }
                Ok(Verified::NotReady(code)) => Err(Failure::transient(code)),
                Err(error) => Err(error.failure),
            }
        };
        match tokio::time::timeout_at(at, check).await {
            Ok(result) => result,
            Err(_) => Err(Failure::transient("database_timeout")),
        }
    }
}

/// A content job handler that refuses work on an outdated host.
struct GuardedHandler {
    state: Arc<ContentState>,
    handler: Arc<dyn JobHandler>,
}
impl JobHandler for GuardedHandler {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        payload: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            if self.state.is_outdated() {
                return Err(JobFailure::release());
            }
            match self.handler.handle(ctx, payload).await {
                // The write check found a newer model mid-run: give the job
                // back for a current host, attempt refunded.
                Err(failure) if failure.code == SUPERSEDED => Err(JobFailure::release()),
                other => other,
            }
        })
    }
    fn accepts_leases(&self) -> bool {
        !self.state.is_outdated() && self.handler.accepts_leases()
    }
}

const SUPERSEDED: &str = "content_model_superseded";

const PENDING: &str = "content_model_pending";

/// `SELECT 1 FROM merezha_content.models WHERE version = $1 AND state =
/// 'active' FOR SHARE` inside `tx`: the shared lock makes an activation's
/// update of that row wait for this write, and a write that waited on an
/// activation finds no row. When `version` is not the active model, the
/// active version decides: a newer one fails transient
/// `content_model_superseded`; an older one (or none), meaning `version`
/// is pending activation by a migrating host, fails transient
/// `content_model_pending`. Both are 503 with `Retry-After`.
pub async fn lock_model_version(tx: &mut Tx<'_>, version: u32) -> Result<(), Failure> {
    let version = i32::try_from(version).map_err(|_| Failure::permanent("invalid_input"))?;
    let row = tx
        .query_opt(
            "SELECT 1 FROM merezha_content.models WHERE version = $1 AND state = 'active' FOR SHARE",
            &[&version],
        )
        .await
        .map_err(|e| merezha_postgres::classify(&e))?;
    if row.is_some() {
        return Ok(());
    }
    let active = tx
        .query_opt(
            "SELECT version FROM merezha_content.models WHERE state = 'active'",
            &[],
        )
        .await
        .map_err(|e| merezha_postgres::classify(&e))?
        .map(|row| row.get::<_, i32>(0));
    match active {
        Some(active) if active > version => Err(Failure::transient(SUPERSEDED)),
        _ => Err(Failure::transient(PENDING)),
    }
}

/// Access to the [`ContentState`] a [`ContentModule`] builds when it is
/// configured, kept by whoever registered the module (the CLI's `merezha
/// content diff`, tests and C3's routes).
#[derive(Clone, Debug, Default)]
pub struct ContentHandle {
    state: Arc<RwLock<Option<Arc<ContentState>>>>,
    problems: Arc<RwLock<Option<crate::ModelError>>>,
}
impl ContentHandle {
    /// `None` until [`Module::configure`] accepted `[modules.content]`.
    pub fn state(&self) -> Option<Arc<ContentState>> {
        self.state.read().ok().and_then(|state| state.clone())
    }
    /// The model file's problems, with their messages, when
    /// [`Module::configure`] refused the model.
    pub fn model_error(&self) -> Option<crate::ModelError> {
        self.problems.read().ok().and_then(|error| error.clone())
    }
    fn set(&self, state: Arc<ContentState>) {
        if let Ok(mut slot) = self.state.write() {
            *slot = Some(state);
        }
    }
    fn refuse(&self, error: crate::ModelError) {
        if let Ok(mut slot) = self.problems.write() {
            *slot = Some(error);
        }
    }
}

/// The content block (`merezha-content`), enabled by `[modules.content]`.
///
/// [`Module::configure`] resolves `model` with the runtime manifest's path
/// rules and validates the model file, reporting every model problem with
/// its TOML path. [`Module::start`] activates a pending model when
/// `database.migrate = true` and otherwise verifies it; refused changes, a
/// reused version and a failed index build stop startup.
#[derive(Debug, Default)]
pub struct ContentModule {
    handle: ContentHandle,
    options: ActivationOptions,
}
impl ContentModule {
    pub fn new() -> Self {
        Self::default()
    }
    /// With other activation bounds, for tests.
    pub fn with_options(options: ActivationOptions) -> Self {
        Self {
            handle: ContentHandle::default(),
            options,
        }
    }
    pub fn handle(&self) -> ContentHandle {
        self.handle.clone()
    }
    fn state(&self) -> Result<Arc<ContentState>, Failure> {
        self.handle
            .state()
            .ok_or_else(|| Failure::permanent("content_not_configured"))
    }
    /// Register the content job kinds, each through
    /// [`ContentState::guard_jobs`], and their schedules:
    /// `content.trash_purge` on the `content` queue, declared as the cron
    /// schedule of the same name with `trash.purge_cron` in UTC (C4).
    fn register_jobs(&self, ctx: &AppContext, state: &Arc<ContentState>) -> Result<(), Failure> {
        let jobs = ctx.jobs(NAME)?;
        let store = Store::new(state.clone(), ctx.clone(), ctx.outbox(NAME)?);
        jobs.register(
            crate::trash::TRASH_PURGE,
            crate::trash::QUEUE,
            state.guard_jobs(Arc::new(crate::trash::TrashPurge { store })),
            KindOptions::default(),
        )?;
        jobs.schedule(ScheduleDeclaration {
            name: crate::trash::TRASH_PURGE.to_owned(),
            cron: state.settings().trash.purge_cron.clone(),
            timezone: crate::settings::PURGE_TIMEZONE.to_owned(),
            queue: Identity::new(crate::trash::QUEUE)?,
            kind: Identity::new(crate::trash::TRASH_PURGE)?,
            payload: serde_json::json!({}),
        })?;
        Ok(())
    }
}

/// The model options C2 can provide: the content permissions only (the
/// RBAC catalog arrives with identity), no media (C6 and C7 configure it)
/// and no organizations (C9).
fn model_options() -> ModelOptions<'static> {
    ModelOptions {
        permissions: &CONTENT_PERMISSIONS,
        media: false,
        organizations: false,
    }
}

/// Resolve `value` against `base` with the runtime manifest's rules: a
/// relative path without parent traversal, naming a regular file inside
/// the project directory.
pub fn resolve_model_path(base: &Path, value: &str) -> Result<PathBuf, &'static str> {
    let relative = Path::new(value);
    if value.is_empty()
        || value.len() > 4096
        || relative.is_absolute()
        || relative
            .components()
            .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
    {
        return Err("invalid_path");
    }
    let root = base.canonicalize().map_err(|_| "invalid_path")?;
    let resolved = root
        .join(relative)
        .canonicalize()
        .map_err(|_| "content_model_unreadable")?;
    if !resolved.starts_with(&root) {
        return Err("invalid_path");
    }
    if !std::fs::metadata(&resolved)
        .map_err(|_| "content_model_unreadable")?
        .is_file()
    {
        return Err("invalid_path");
    }
    Ok(resolved)
}

/// Read and parse the model file at `path`. Model problems become one
/// issue each, keyed `modules.content.model:<toml path>` with code
/// `content_model_invalid`; their messages are the model's, printed by
/// `merezha content diff`.
pub fn load_model(path: &Path) -> Result<ContentModel, LoadError> {
    let file = std::fs::File::open(path).map_err(|_| LoadError::Unreadable)?;
    let mut text = String::new();
    file.take(MAX_MODEL_BYTES as u64 + 1)
        .read_to_string(&mut text)
        .map_err(|_| LoadError::Unreadable)?;
    ContentModel::parse(&text, &model_options()).map_err(LoadError::Invalid)
}

/// Why the model file could not be loaded.
#[derive(Debug)]
pub enum LoadError {
    /// Missing, unreadable or not UTF-8.
    Unreadable,
    Invalid(crate::ModelError),
}

impl Module for ContentModule {
    fn name(&self) -> &'static str {
        NAME
    }
    fn configure(&mut self, section: &ModuleSection) -> Result<(), Vec<ConfigIssue>> {
        let mut issues = section.unknown_keys(&KEYS);
        let settings = match ContentSettings::parse(section) {
            Ok(settings) => settings,
            Err(found) => {
                issues.extend(found);
                ContentSettings::default()
            }
        };
        let key = section.key("model");
        let origin = section.origin("model");
        let value = match section.table.get("model") {
            None => Some(DEFAULT_MODEL_PATH.to_owned()),
            Some(toml::Value::String(value)) => Some(value.clone()),
            Some(_) => {
                issues.push(ConfigIssue::new(&key, "expected_string", origin.clone()));
                None
            }
        };
        let path = value.and_then(|value| {
            resolve_model_path(section.base_dir(), &value)
                .map_err(|code| issues.push(ConfigIssue::new(&key, code, origin.clone())))
                .ok()
        });
        if let Some(path) = path {
            match load_model(&path) {
                Ok(model) if issues.is_empty() => {
                    self.handle.set(Arc::new(ContentState {
                        model: Arc::new(model),
                        path,
                        settings,
                        database: OnceLock::new(),
                        current: AtomicBool::new(false),
                        outdated: AtomicBool::new(false),
                    }));
                }
                Ok(_) => {}
                Err(LoadError::Unreadable) => issues.push(ConfigIssue::new(
                    &key,
                    "content_model_unreadable",
                    origin.clone(),
                )),
                Err(LoadError::Invalid(error)) => {
                    for issue in &error.issues {
                        let at = if issue.path.is_empty() {
                            key.clone()
                        } else {
                            format!("{key}:{}", issue.path)
                        };
                        issues.push(ConfigIssue::new(at, crate::MODEL_INVALID, Origin::File));
                    }
                    if error.omitted > 0 {
                        issues.push(ConfigIssue::new(&key, crate::MODEL_INVALID, Origin::File));
                    }
                    self.handle.refuse(error);
                }
            }
        }
        if issues.is_empty() {
            Ok(())
        } else {
            Err(issues)
        }
    }
    fn migrations(&self) -> &[Migration] {
        MIGRATIONS
    }
    fn errors(&self) -> &[ErrorCode] {
        ERRORS
    }
    fn events(&self) -> ModuleEvents {
        ModuleEvents {
            publishes: &PUBLISHES,
            consumes: &[],
        }
    }
    fn start_timeout(&self) -> Option<Duration> {
        Some(self.options.timeout + START_MARGIN)
    }
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async move {
            let state = self.state()?;
            let model = state.model.clone();
            merezha_telemetry::event!(
                INFO,
                "content.model_loaded",
                version = model.version(),
                fingerprint = model.fingerprint(),
                types = model.types().len(),
                locales = model.locales().len()
            );
            let _ = state.database.set(ctx.database().clone());
            if ctx.migrate() {
                let outbox = ctx.outbox(NAME)?;
                match activate(ctx.database(), &outbox, ctx.random(), &model, &self.options).await {
                    Ok(report) => {
                        if report.plan.status == crate::Status::Outdated {
                            state.mark_outdated();
                        } else if !report.rebuild_pending {
                            state.current.store(true, Ordering::SeqCst);
                        }
                    }
                    Err(error) => return Err(error.failure),
                }
                self.register_jobs(&ctx, &state)
            } else {
                let deadline = Instant::now() + Duration::from_secs(10);
                let client = match ctx.database().acquire(deadline).await {
                    Ok(client) => client,
                    // Readiness keeps checking; a database that answered the
                    // host's ping is only briefly unavailable.
                    Err(_) => return self.register_jobs(&ctx, &state),
                };
                match verify(&**client, &model).await {
                    Ok(Verified::Ready) => state.current.store(true, Ordering::SeqCst),
                    Ok(Verified::NotReady("content_model_outdated")) => state.mark_outdated(),
                    // Pending work for a host that migrates; readiness says which.
                    Ok(Verified::NotReady(_)) => {}
                    Err(error)
                        if error.failure.classification
                            == merezha_domain::Classification::Permanent =>
                    {
                        log_breaking(&error, model.version());
                        return Err(error.failure);
                    }
                    Err(_) => {}
                }
                self.register_jobs(&ctx, &state)
            }
        })
    }
    fn routes(&self, ctx: &AppContext) -> axum::Router {
        let Some(state) = self.handle.state() else {
            return axum::Router::new();
        };
        let Ok(outbox) = ctx.outbox(NAME) else {
            return axum::Router::new();
        };
        let store = Store::new(state, ctx.clone(), outbox);
        crate::http::router(store, ctx.authorizer().clone())
    }
    fn ready(&self) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async move { self.state()?.readiness().await })
    }
}
