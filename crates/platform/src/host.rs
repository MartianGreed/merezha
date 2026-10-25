//! Process lifecycle: configuration with module validation, the early
//! listener, database and migrations, module start, the composed router
//! behind the edge, serving, and the drain sequence: readiness off, a drain
//! delay for load balancers, a bounded graceful drain and module shutdown.
use crate::{
    config::{ConfigErrors, ConfigInput, ConfigIssue, Origin, ProjectConfig, valid_module_name},
    context::AppContext,
    edge::{self, EdgeSettings, HttpMetrics},
    errors::ErrorRegistry,
    health::{self, Lifecycle, Readiness},
    module::{JobsRuntime, Module, RESERVED_NAMES},
    ports::{Authenticator, Authorizer, BlobStore},
    status::{self, ServiceInfo, StatusSection, StatusState},
};
use axum::{Router, extract::Request};
use merezha_domain::Failure;
use merezha_postgres::{
    Database, MigrationError, MigrationMode, MigrationReport, ModuleMigrations, run_migrations,
};
use std::{
    future::Future,
    net::SocketAddr,
    sync::{Arc, PoisonError, RwLock},
    time::{Duration, Instant},
};

/// Ceiling on the start bound a module may declare
/// ([`Module::start_timeout`]).
pub const MAX_MODULE_START: Duration = Duration::from_secs(3600);

/// Time bounds of the lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timeouts {
    /// Applying migrations at startup.
    pub migrate: Duration,
    /// Each module's `start`.
    pub module_start: Duration,
    /// In-flight requests after the shutdown signal.
    pub drain: Duration,
    /// All module `shutdown` calls together.
    pub module_shutdown: Duration,
    /// How often readiness is re-evaluated in the background
    /// ([`health::READINESS_INTERVAL`]); probes read the cached result.
    pub readiness_interval: Duration,
}
impl Default for Timeouts {
    fn default() -> Self {
        Self {
            migrate: Duration::from_secs(300),
            module_start: Duration::from_secs(10),
            drain: Duration::from_secs(30),
            module_shutdown: Duration::from_secs(10),
            readiness_interval: health::READINESS_INTERVAL,
        }
    }
}

/// Load the project and let every registered module validate its section.
/// Kernel and module problems are returned together. A section without a
/// registered module is `unknown_module`; a registered module without a
/// section stays disabled and is dropped.
pub fn configure(
    input: &ConfigInput,
    modules: Vec<Box<dyn Module>>,
) -> Result<(ProjectConfig, Vec<Box<dyn Module>>), ConfigErrors> {
    let parts = crate::config::load_parts(input);
    let mut issues = parts.issues;
    let mut names = std::collections::BTreeSet::new();
    for module in &modules {
        let name = module.name();
        if !valid_module_name(name) || RESERVED_NAMES.contains(&name) || !names.insert(name) {
            issues.push(ConfigIssue::new(
                format!("modules.{name}"),
                "invalid_module_registration",
                Origin::Default,
            ));
        }
    }
    let mut enabled = Vec::new();
    for mut module in modules {
        if let Some(section) = parts.modules.get(module.name()) {
            match module.configure(section) {
                Ok(()) => enabled.push(module),
                Err(found) => issues.extend(found),
            }
        }
    }
    for (name, section) in &parts.modules {
        if !names.contains(name.as_str()) {
            let origin = section
                .overridden
                .values()
                .next()
                .cloned()
                .unwrap_or(Origin::File);
            issues.push(ConfigIssue::new(
                format!("modules.{name}"),
                "unknown_module",
                origin,
            ));
        }
    }
    let config = match parts.config {
        Some(config) if issues.is_empty() => config,
        _ => return Err(ConfigErrors(issues)),
    };
    Ok((config, enabled))
}

/// Why startup stopped. `migration` carries the module and version.
#[derive(Debug)]
pub struct StartupError {
    pub failure: Failure,
    pub migration: Option<MigrationError>,
    pub module: Option<&'static str>,
}
impl StartupError {
    fn new(code: &str) -> Self {
        Self {
            failure: Failure::permanent(code),
            migration: None,
            module: None,
        }
    }
}
impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.migration, self.module) {
            (Some(migration), _) => write!(f, "{}: {migration}", self.failure.code),
            (None, Some(module)) => write!(f, "{} ({module})", self.failure.code),
            _ => f.write_str(&self.failure.code),
        }
    }
}
impl std::error::Error for StartupError {}

/// What the embedding binary adds to the kernel.
pub struct HostParts {
    /// Routes outside `/v1/<module>`, such as the application and runtime.
    pub routes: Router,
    /// Policy shared with modules: [`crate::ports::LoopbackAllowAll`] without
    /// the identity block.
    pub authorizer: Arc<dyn Authorizer>,
    pub timeouts: Timeouts,
    /// The jobs runtime and its outbox; `None` keeps the refusing
    /// fallbacks (`outbox_unavailable`, `jobs_unavailable`).
    pub jobs: Option<Arc<dyn JobsRuntime>>,
}

/// The kernel's codes plus every module's, or the first refused module
/// with its problems logged as `error.registration_refused`.
pub fn error_registry(modules: &[&dyn Module]) -> Result<ErrorRegistry, StartupError> {
    let mut registry = ErrorRegistry::kernel();
    for module in modules {
        if let Err(refused) = registry.register(module.name(), module.errors()) {
            for error in &refused {
                merezha_telemetry::event!(
                    ERROR,
                    "error.registration_refused",
                    module = module.name(),
                    code = error.code.as_str(),
                    reason = error.reason
                );
            }
            return Err(StartupError {
                failure: Failure::permanent("error_code_conflict"),
                migration: None,
                module: Some(module.name()),
            });
        }
    }
    Ok(registry)
}

/// What an embedding binary adds beyond [`HostParts`]: the lifecycle the
/// early listener shares, the identity authenticator, the meter for the
/// edge's instruments and the extra status sections.
#[derive(Clone, Default)]
pub struct HostOptions {
    /// The early listener's lifecycle ([`EarlyListener::lifecycle`]); a new
    /// one otherwise, marked started when [`Host::start_with`] returns.
    pub lifecycle: Option<Arc<Lifecycle>>,
    /// The identity block's authenticator. With one, `/v1/admin/status`
    /// requires `system.status.read` at application scope.
    pub authenticator: Option<Arc<dyn Authenticator>>,
    /// The identity block's `Directory` (R7), installed in every module's
    /// context; the empty fallback without one.
    pub directory: Option<Arc<dyn crate::ports::Directory>>,
    /// The meter source of the kernel's instruments: the edge's from
    /// `meters.meter(EDGE_SCOPE)`, the PostgreSQL pool's from
    /// `meters.meter(merezha_postgres::METER_SCOPE)` (TE §1.3). The
    /// default records nothing; the binary passes `Telemetry::meters()`.
    pub meters: merezha_telemetry::Meters,
    /// Sections of `/v1/admin/status` other subsystems own.
    pub status: Vec<Arc<dyn StatusSection>>,
    /// Turns `true` on a shutdown signal during startup: the host stops
    /// before its next step, interrupts a running migration or module
    /// start, shuts down the modules already started within
    /// `Timeouts::module_shutdown`, and fails `startup_interrupted`.
    pub interrupt: Option<tokio::sync::watch::Receiver<bool>>,
}

/// Resolve once `interrupt` turns `true`; never without one, or once its
/// sender is gone without having signalled.
async fn interrupted(interrupt: Option<tokio::sync::watch::Receiver<bool>>) {
    if let Some(mut interrupt) = interrupt
        && interrupt.wait_for(|stop| *stop).await.is_ok()
    {
        return;
    }
    std::future::pending::<()>().await;
}
impl std::fmt::Debug for HostOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostOptions")
            .field("authenticator", &self.authenticator.is_some())
            .field("directory", &self.directory.is_some())
            .field(
                "status",
                &self.status.iter().map(|s| s.name()).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// The edge settings of `config` with `errors`, `options`' authenticator
/// and the edge meter of `meters`.
fn edge_settings(
    config: &ProjectConfig,
    errors: Arc<ErrorRegistry>,
    authenticator: Option<Arc<dyn Authenticator>>,
    meters: &merezha_telemetry::Meters,
) -> EdgeSettings {
    EdgeSettings::new(&config.http, errors)
        .authenticator(authenticator)
        .trust(config.telemetry.traces.trust_inbound)
        .metrics(HttpMetrics::new(&meters.meter(crate::edge::EDGE_SCOPE)))
}

/// The one blob store a module provides ([`Module::blob_store`]), asked of
/// every module in registration order. A module failing to open its store
/// stops startup naming it; a second provider stops it with
/// `blob_store_conflict`.
fn blob_store(modules: &[Arc<dyn Module>]) -> Result<Option<Arc<dyn BlobStore>>, StartupError> {
    let mut found: Option<Arc<dyn BlobStore>> = None;
    for module in modules {
        let store = module.blob_store().map_err(|failure| {
            merezha_telemetry::event!(
                ERROR,
                "module.start_failed",
                module = module.name(),
                code = failure.code.as_str()
            );
            StartupError {
                failure,
                migration: None,
                module: Some(module.name()),
            }
        })?;
        match (store, &found) {
            (Some(_), Some(_)) => {
                return Err(StartupError {
                    failure: Failure::permanent("blob_store_conflict"),
                    migration: None,
                    module: Some(module.name()),
                });
            }
            (Some(store), None) => found = Some(store),
            (None, _) => {}
        }
    }
    Ok(found)
}

/// The one notifier factory a module provides ([`Module::notifier`]), or
/// `None` for the capture fallback; two fail `notifier_conflict`.
fn notifier_factory(
    modules: &[Arc<dyn Module>],
) -> Result<Option<Arc<dyn crate::ports::NotifierFactory>>, StartupError> {
    let mut found = None;
    for module in modules {
        match (module.notifier(), &found) {
            (Some(_), Some(_)) => {
                return Err(StartupError {
                    failure: Failure::permanent("notifier_conflict"),
                    migration: None,
                    module: Some(module.name()),
                });
            }
            (Some(factory), None) => found = Some(factory),
            (None, _) => {}
        }
    }
    Ok(found)
}

/// Whether `config` describes a private listener: not public, a loopback
/// address and no trusted proxy (requests through a proxy come from
/// elsewhere even on loopback). Without identity, only a private listener
/// keeps the allow-all policy and serves `/v1/admin/status`.
pub fn private_listener(config: &ProjectConfig) -> bool {
    !config.http.public
        && config.http.listen.ip().is_loopback()
        && config.http.trusted_proxies.is_empty()
}

/// A started host, ready to serve.
pub struct Host {
    config: ProjectConfig,
    database: Database,
    modules: Vec<Arc<dyn Module>>,
    readiness: Arc<Readiness>,
    router: Router,
    timeouts: Timeouts,
    migrations: MigrationReport,
    errors: Arc<ErrorRegistry>,
}
impl std::fmt::Debug for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Host")
            .field("app", &self.config.app.id)
            .field(
                "modules",
                &self.modules.iter().map(|m| m.name()).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}
impl Host {
    /// Register error codes, open the pool, prove the database is
    /// reachable, verify or apply migrations, start modules and compose the
    /// router. The host is started when this returns.
    pub async fn start(
        config: ProjectConfig,
        modules: Vec<Box<dyn Module>>,
        parts: HostParts,
    ) -> Result<Self, StartupError> {
        Self::start_with(config, modules, parts, HostOptions::default()).await
    }
    /// [`Host::start`] with `options`. With the early listener's lifecycle
    /// the host stays `starting` until [`EarlyListener::serve`] switches to
    /// its router.
    pub async fn start_with(
        config: ProjectConfig,
        modules: Vec<Box<dyn Module>>,
        parts: HostParts,
        options: HostOptions,
    ) -> Result<Self, StartupError> {
        if options
            .interrupt
            .as_ref()
            .is_some_and(|stop| *stop.borrow())
        {
            return Err(StartupError::new("startup_interrupted"));
        }
        // An authenticator means identity: permission checks must be real.
        if options.authenticator.is_some() && parts.authorizer.allows_all() {
            return Err(StartupError::new("allow_all_with_authenticator"));
        }
        let jobs = parts.jobs.clone();
        let errors = Arc::new(error_registry(
            &modules
                .iter()
                .map(|m| m.as_ref())
                .chain(jobs.iter().map(|j| j.as_ref() as &dyn Module))
                .collect::<Vec<_>>(),
        )?);
        for module in &modules {
            if let Err(failure) =
                crate::context::check_source_grants(module.name(), module.extra_sources())
            {
                merezha_telemetry::event!(
                    ERROR,
                    "module.source_refused",
                    module = module.name(),
                    sources = ?module.extra_sources()
                );
                return Err(StartupError {
                    failure,
                    migration: None,
                    module: Some(module.name()),
                });
            }
        }
        let meter = options.meters.meter(merezha_postgres::METER_SCOPE);
        let database =
            Database::connect_with_meter(&config.database, &meter).map_err(|failure| {
                StartupError {
                    failure,
                    migration: None,
                    module: None,
                }
            })?;
        let reach =
            Instant::now() + config.database.connect_timeout + config.database.acquire_timeout;
        if let Err(failure) = database.ping(reach).await {
            merezha_telemetry::event!(ERROR, "db.unavailable", code = failure.code.as_str());
            return Err(StartupError::new("database_unavailable"));
        }
        merezha_telemetry::event!(INFO, "db.connected", pool_size = config.database.pool_size);
        // The jobs runtime is the last module: it starts after every block
        // declared its handlers and stops first.
        let ports = jobs.as_ref().map(|jobs| jobs.ports(&database));
        let modules: Vec<Arc<dyn Module>> = modules
            .into_iter()
            .map(Arc::from)
            .chain(jobs.map(|jobs| jobs as Arc<dyn Module>))
            .collect();
        let declared: Vec<ModuleMigrations<'_>> = modules
            .iter()
            .map(|m| ModuleMigrations {
                module: m.name(),
                migrations: m.migrations(),
            })
            .collect();
        let mode = if config.database.migrate {
            MigrationMode::Apply
        } else {
            MigrationMode::Verify
        };
        let deadline = Instant::now()
            + match mode {
                MigrationMode::Apply => parts.timeouts.migrate,
                MigrationMode::Verify => Duration::from_secs(30),
            };
        let run = tokio::select! {
            run = run_migrations(&database, &declared, mode, deadline) => run,
            // A migration interrupted mid-way rolls back with its session.
            () = interrupted(options.interrupt.clone()) => {
                return Err(StartupError::new("startup_interrupted"));
            }
        };
        let migrations = run.map_err(|error| {
            let (code, module) = (error.failure.code.as_str(), error.module.as_deref());
            if code == "migration_checksum_mismatch" {
                merezha_telemetry::event!(
                    ERROR,
                    "migration.checksum_mismatch",
                    code,
                    module,
                    version = error.version
                );
            } else {
                merezha_telemetry::event!(
                    ERROR,
                    "migration.refused",
                    code,
                    module,
                    version = error.version
                );
            }
            StartupError {
                failure: error.failure.clone(),
                migration: Some(error),
                module: None,
            }
        })?;
        drop(declared);
        merezha_telemetry::event!(
            INFO,
            "migration.checked",
            mode = if mode == MigrationMode::Apply {
                "apply"
            } else {
                "verify"
            },
            applied = migrations.applied.len(),
            pending = migrations.pending.len()
        );
        for pending in &migrations.pending {
            merezha_telemetry::event!(
                WARN,
                "migration.pending",
                module = pending.module.as_str(),
                version = pending.version
            );
        }
        let mut builder =
            AppContext::builder(&config.app.id, database.clone(), parts.authorizer.clone())
                .instance_id(&config.app.instance_id)
                .migrate(config.database.migrate)
                .telemetry(options.meters.clone());
        if let Some(authenticator) = options.authenticator.clone() {
            builder = builder.authenticator(authenticator);
        }
        if let Some(directory) = options.directory.clone() {
            builder = builder.directory(directory);
        }
        if let Some(ports) = ports {
            builder = builder.outbox(ports.outbox).jobs(ports.jobs);
        }
        if let Some(blobs) = blob_store(&modules)? {
            builder = builder.blobs(blobs);
        }
        if let Some(notifications) = notifier_factory(&modules)? {
            builder = builder.notifications(notifications);
        }
        let ctx = builder.build().map_err(|failure| StartupError {
            failure,
            migration: None,
            module: None,
        })?;
        let mut started: Vec<Arc<dyn Module>> = Vec::new();
        for module in &modules {
            let budget = module
                .start_timeout()
                .unwrap_or(parts.timeouts.module_start)
                .min(MAX_MODULE_START);
            let now = Instant::now();
            let at = tokio::time::Instant::from_std(
                now.checked_add(budget)
                    .unwrap_or(now + parts.timeouts.module_start),
            );
            let own = ctx
                .for_module(module.name(), module.extra_sources())
                .map_err(|failure| StartupError {
                    failure,
                    migration: None,
                    module: Some(module.name()),
                })?;
            let result = tokio::select! {
                result = tokio::time::timeout_at(at, module.start(own)) => result,
                () = interrupted(options.interrupt.clone()) => {
                    // The modules already started shut down within their
                    // budget; the one interrupted mid-start is abandoned.
                    stop_modules(&started, parts.timeouts.module_shutdown).await;
                    return Err(StartupError::new("startup_interrupted"));
                }
            };
            if !matches!(result, Ok(Ok(()))) {
                let code = match &result {
                    Ok(Err(failure)) => failure.code.as_str(),
                    _ => "timeout",
                };
                merezha_telemetry::event!(
                    ERROR,
                    "module.start_failed",
                    module = module.name(),
                    code
                );
                stop_modules(&started, parts.timeouts.module_shutdown).await;
                return Err(StartupError {
                    failure: Failure::permanent("module_start_failed"),
                    migration: None,
                    module: Some(module.name()),
                });
            }
            let events = module.events();
            merezha_telemetry::event!(
                INFO,
                "module.started",
                module = module.name(),
                publishes = ?events.publishes,
                consumes = ?events.consumes
            );
            started.push(module.clone());
        }
        let external = options.lifecycle.is_some();
        let lifecycle = options.lifecycle.clone().unwrap_or_else(Lifecycle::new);
        lifecycle.set_max_age(health::staleness_bound(parts.timeouts.readiness_interval));
        let readiness = Arc::new(Readiness::new(
            database.clone(),
            modules.clone(),
            migrations.is_current(),
            lifecycle.clone(),
        ));
        let service = merezha_telemetry::Service::resolve(
            &config.telemetry.resource,
            &config.app.id,
            config.app.version.as_deref(),
            &config.app.instance_id,
        );
        let status = Arc::new(StatusState {
            lifecycle: lifecycle.clone(),
            readiness: readiness.clone(),
            database: database.clone(),
            modules: modules
                .iter()
                .map(|m| (m.name(), m.migrations().len()))
                .collect(),
            pending: migrations.pending.clone(),
            service: ServiceInfo {
                name: service.name,
                version: service.version,
                instance_id: config.app.instance_id.clone(),
            },
            private: private_listener(&config),
            authenticated: ctx.authenticator().is_some(),
            authorizer: parts.authorizer.clone(),
            jobs: match parts.jobs {
                Some(_) => ctx.jobs("jobs").ok(),
                None => None,
            },
            sections: options.status.clone(),
            gate: tokio::sync::Semaphore::new(1),
        });
        let mut router = health::router(lifecycle.clone()).merge(status::router(status));
        for module in &modules {
            let own = ctx
                .for_module(module.name(), module.extra_sources())
                .map_err(|failure| StartupError {
                    failure,
                    migration: None,
                    module: Some(module.name()),
                })?;
            router = router.nest(&format!("/v1/{}", module.name()), module.routes(&own));
        }
        let body_limits = modules
            .iter()
            .flat_map(|module| {
                module.body_limits().into_iter().map(|limit| {
                    (
                        limit.method,
                        format!("/v1/{}{}", module.name(), limit.path),
                        limit.limit,
                    )
                })
            })
            .collect();
        let credential_free = modules
            .iter()
            .flat_map(|module| {
                module
                    .credential_free_routes()
                    .into_iter()
                    .map(|route| (route.method, format!("/v1/{}{}", module.name(), route.path)))
            })
            .collect();
        let router = edge::apply(
            router.merge(parts.routes),
            edge_settings(
                &config,
                errors.clone(),
                ctx.authenticator().cloned(),
                &options.meters,
            )
            .route_body_limits(body_limits)
            .credential_free_routes(credential_free),
        );
        // The first evaluation runs before the host is handed out, so a
        // started host reports its real readiness at once.
        readiness.check().await;
        health::spawn_refresh(&readiness, parts.timeouts.readiness_interval);
        if !external {
            lifecycle.complete();
        }
        Ok(Self {
            config,
            database,
            modules,
            readiness,
            router,
            timeouts: parts.timeouts,
            migrations,
            errors,
        })
    }
    /// The registered error codes and statuses.
    pub fn errors(&self) -> &Arc<ErrorRegistry> {
        &self.errors
    }
    /// The migration outcome at startup.
    pub fn migrations(&self) -> &MigrationReport {
        &self.migrations
    }
    pub fn readiness(&self) -> &Arc<Readiness> {
        &self.readiness
    }
    pub fn lifecycle(&self) -> &Arc<Lifecycle> {
        self.readiness.lifecycle()
    }
    /// The full router behind the edge, for in-process tests.
    pub fn router(&self) -> Router {
        self.router.clone()
    }
    /// Bind `http.listen` (or `listen` when given).
    pub async fn bind(
        &self,
        listen: Option<SocketAddr>,
    ) -> Result<tokio::net::TcpListener, StartupError> {
        tokio::net::TcpListener::bind(listen.unwrap_or(self.config.http.listen))
            .await
            .map_err(|_| StartupError::new("listener_failed"))
    }
    /// Serve on `listener` until `shutdown` resolves, then run the drain
    /// sequence: readiness off at once, `http.drain_delay_ms`, then a
    /// bounded graceful drain and module shutdown. Emits `listener.ready`
    /// first.
    pub async fn serve(
        self,
        listener: tokio::net::TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), Failure> {
        let address = listener
            .local_addr()
            .map_err(|_| Failure::permanent("listener_failed"))?;
        merezha_telemetry::event!(
            INFO,
            "listener.ready",
            address = address.to_string(),
            app = self.config.app.id.as_str(),
            modules = ?self.modules.iter().map(|m| m.name()).collect::<Vec<_>>()
        );
        let server = Server::spawn(listener, self.router.clone());
        self.drain(server, shutdown).await
    }
    /// The drain sequence (TE §5): serve until `shutdown` resolves; then
    /// readiness turns `not_ready` at once while liveness stays 200, the
    /// listener keeps serving for `http.drain_delay_ms` so load balancers
    /// stop routing, then stops accepting and drains in-flight requests for
    /// at most the drain timeout; modules shut down in reverse order (the
    /// jobs runtime first) and the pool closes. Emits `shutdown.started`,
    /// `listener.draining`, `listener.stopped` and `shutdown.completed`.
    async fn drain(
        self,
        mut server: Server,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), Failure> {
        let failed = tokio::select! {
            () = shutdown => None,
            result = &mut server.task => Some(result),
        };
        let signalled = Instant::now();
        let delay = self.config.http.drain_delay;
        let result = match failed {
            Some(_) => Err(Failure::transient("listener_failed")),
            None => {
                self.readiness.drain();
                merezha_telemetry::event!(
                    INFO,
                    "shutdown.started",
                    drain_ms = delay.as_millis() as u64
                );
                merezha_telemetry::event!(INFO, "listener.draining");
                tokio::time::sleep(delay).await;
                server.stop();
                let drain = self.timeouts.drain;
                match tokio::time::timeout(drain, &mut server.task).await {
                    Ok(Ok(Ok(()))) => Ok(()),
                    Ok(_) => Err(Failure::transient("listener_failed")),
                    Err(_) => {
                        server.task.abort();
                        merezha_telemetry::event!(
                            WARN,
                            "listener.drain_timeout",
                            drain_ms = drain.as_millis() as u64
                        );
                        Ok(())
                    }
                }
            }
        };
        stop_modules(&self.modules, self.timeouts.module_shutdown).await;
        self.database.close();
        merezha_telemetry::event!(
            INFO,
            "listener.stopped",
            code = if result.is_ok() {
                "ok"
            } else {
                "listener_failed"
            }
        );
        merezha_telemetry::event!(
            INFO,
            "shutdown.completed",
            drain_ms = signalled.elapsed().as_millis() as u64
        );
        result
    }
}

/// The router a listener serves, switched once startup completes.
type Slot = Arc<RwLock<Router>>;

/// A spawned `axum::serve` over a switchable router, stopped gracefully on
/// [`Server::stop`].
struct Server {
    slot: Slot,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Server {
    fn spawn(listener: tokio::net::TcpListener, router: Router) -> Self {
        let slot: Slot = Arc::new(RwLock::new(router));
        let current = slot.clone();
        // Each request goes to the router in the slot at the time it
        // arrives; requests already admitted finish on theirs.
        let app = Router::new().fallback_service(tower::service_fn(move |request: Request| {
            let router = current
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            tower::ServiceExt::oneshot(router, request)
        }));
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = stopped.await;
            })
            .into_future(),
        );
        Self {
            slot,
            task,
            stop: Some(stop),
        }
    }
    fn switch(&self, router: Router) {
        *self.slot.write().unwrap_or_else(PoisonError::into_inner) = router;
    }
    /// Stop accepting and drain open connections.
    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

/// The early listener (TE §5): bound right after configuration, before the
/// database and modules. Until startup completes it answers `/health/*`
/// (`/health/startup` and `/health/ready` with 503) and every other route
/// with `503 starting` and `Retry-After: 1`, so a startup probe can cover
/// long migrations. [`EarlyListener::serve`] switches it to the started
/// host's router.
pub struct EarlyListener {
    address: SocketAddr,
    lifecycle: Arc<Lifecycle>,
    server: Server,
}
impl std::fmt::Debug for EarlyListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EarlyListener")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}
impl EarlyListener {
    /// Bind `http.listen` (or `listen`) and serve health at once. Emits
    /// `listener.ready` with the bound address. Fails `listener_failed`.
    pub async fn bind(
        config: &ProjectConfig,
        listen: Option<SocketAddr>,
        options: &HostOptions,
    ) -> Result<Self, StartupError> {
        let listener = tokio::net::TcpListener::bind(listen.unwrap_or(config.http.listen))
            .await
            .map_err(|_| StartupError::new("listener_failed"))?;
        let address = listener
            .local_addr()
            .map_err(|_| StartupError::new("listener_failed"))?;
        let lifecycle = options.lifecycle.clone().unwrap_or_else(Lifecycle::new);
        let router = edge::apply(
            health::router(lifecycle.clone()),
            edge_settings(
                config,
                Arc::new(ErrorRegistry::kernel()),
                None,
                &options.meters,
            )
            .starting(),
        );
        // The modules are the configured blocks; the host adds its own
        // (`application`, `jobs`) once it starts them.
        merezha_telemetry::event!(
            INFO,
            "listener.ready",
            address = address.to_string(),
            app = config.app.id.as_str(),
            modules = ?config.modules.keys().collect::<Vec<_>>()
        );
        Ok(Self {
            address,
            lifecycle,
            server: Server::spawn(listener, router),
        })
    }
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    /// The lifecycle to pass to [`Host::start_with`] through
    /// [`HostOptions::lifecycle`].
    pub fn lifecycle(&self) -> &Arc<Lifecycle> {
        &self.lifecycle
    }
    /// Switch to `host`'s router, mark startup complete (logging
    /// `startup.completed`), then serve until `shutdown` and drain as
    /// [`Host::serve`] does.
    pub async fn serve(
        self,
        host: Host,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), Failure> {
        self.server.switch(host.router.clone());
        self.lifecycle.complete();
        if !Arc::ptr_eq(&self.lifecycle, host.lifecycle()) {
            host.lifecycle().complete();
        }
        host.drain(self.server, shutdown).await
    }
    /// Startup failed or was interrupted: stop accepting, let open
    /// requests finish within `within`, and log `listener.stopped`.
    pub async fn close(mut self, within: Duration) {
        self.lifecycle.drain();
        self.server.stop();
        if tokio::time::timeout(within, &mut self.server.task)
            .await
            .is_err()
        {
            self.server.task.abort();
        }
        merezha_telemetry::event!(INFO, "listener.stopped", code = "ok");
    }
}

async fn stop_modules(modules: &[Arc<dyn Module>], budget: Duration) {
    let deadline = Instant::now() + budget;
    for module in modules.iter().rev() {
        let at = tokio::time::Instant::from_std(deadline);
        if tokio::time::timeout_at(at, module.shutdown(deadline))
            .await
            .is_err()
        {
            merezha_telemetry::event!(WARN, "module.shutdown_timeout", module = module.name());
        } else {
            merezha_telemetry::event!(INFO, "module.stopped", module = module.name());
        }
    }
}

/// Resolve on SIGINT or SIGTERM.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
