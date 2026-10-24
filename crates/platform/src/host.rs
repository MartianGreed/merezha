//! Process lifecycle: configuration with module validation, database and
//! migrations, module start, the composed router behind the edge, serving
//! with a bounded graceful drain and module shutdown.
use crate::{
    config::{ConfigErrors, ConfigInput, ConfigIssue, Origin, ProjectConfig, valid_module_name},
    context::AppContext,
    edge,
    errors::ErrorRegistry,
    health::{self, Readiness},
    module::{Module, RESERVED_NAMES},
    ports::Authorizer,
};
use axum::Router;
use merezha_domain::Failure;
use merezha_postgres::{
    Database, MigrationError, MigrationMode, MigrationReport, ModuleMigrations, run_migrations,
};
use std::{
    future::Future,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

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
}
impl Default for Timeouts {
    fn default() -> Self {
        Self {
            migrate: Duration::from_secs(300),
            module_start: Duration::from_secs(10),
            drain: Duration::from_secs(30),
            module_shutdown: Duration::from_secs(10),
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
}

/// The kernel's codes plus every module's, or the first refused module
/// with its problems logged as `error.registration_refused`.
pub fn error_registry(modules: &[&dyn Module]) -> Result<ErrorRegistry, StartupError> {
    let mut registry = ErrorRegistry::kernel();
    for module in modules {
        if let Err(refused) = registry.register(module.name(), module.errors()) {
            for error in &refused {
                tracing::error!(
                    event = "error.registration_refused",
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
    /// router.
    pub async fn start(
        config: ProjectConfig,
        modules: Vec<Box<dyn Module>>,
        parts: HostParts,
    ) -> Result<Self, StartupError> {
        let errors = Arc::new(error_registry(
            &modules.iter().map(|m| m.as_ref()).collect::<Vec<_>>(),
        )?);
        let database = Database::connect(&config.database).map_err(|failure| StartupError {
            failure,
            migration: None,
            module: None,
        })?;
        let reach =
            Instant::now() + config.database.connect_timeout + config.database.acquire_timeout;
        if let Err(failure) = database.ping(reach).await {
            tracing::error!(event = "db.unavailable", code = failure.code.as_str());
            return Err(StartupError::new("database_unavailable"));
        }
        tracing::info!(
            event = "db.connected",
            pool_size = config.database.pool_size
        );
        let modules: Vec<Arc<dyn Module>> = modules.into_iter().map(Arc::from).collect();
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
        let migrations = run_migrations(&database, &declared, mode, deadline)
            .await
            .map_err(|error| {
                tracing::error!(
                    event = if error.failure.code == "migration_checksum_mismatch" {
                        "migration.checksum_mismatch"
                    } else {
                        "migration.refused"
                    },
                    code = error.failure.code.as_str(),
                    module = error.module.as_deref(),
                    version = error.version
                );
                StartupError {
                    failure: error.failure.clone(),
                    migration: Some(error),
                    module: None,
                }
            })?;
        drop(declared);
        tracing::info!(
            event = "migration.checked",
            mode = if mode == MigrationMode::Apply {
                "apply"
            } else {
                "verify"
            },
            applied = migrations.applied.len(),
            pending = migrations.pending.len()
        );
        for pending in &migrations.pending {
            tracing::warn!(
                event = "migration.pending",
                module = pending.module.as_str(),
                version = pending.version
            );
        }
        let ctx = AppContext::builder(&config.app.id, database.clone(), parts.authorizer)
            .instance_id(&config.app.instance_id)
            .build()
            .map_err(|failure| StartupError {
                failure,
                migration: None,
                module: None,
            })?;
        let mut started: Vec<Arc<dyn Module>> = Vec::new();
        for module in &modules {
            let at = tokio::time::Instant::from_std(Instant::now() + parts.timeouts.module_start);
            let own = ctx
                .for_module(module.name())
                .map_err(|failure| StartupError {
                    failure,
                    migration: None,
                    module: Some(module.name()),
                })?;
            let result = tokio::time::timeout_at(at, module.start(own)).await;
            if !matches!(result, Ok(Ok(()))) {
                tracing::error!(event = "module.start_failed", module = module.name());
                stop_modules(&started, parts.timeouts.module_shutdown).await;
                return Err(StartupError {
                    failure: Failure::permanent("module_start_failed"),
                    migration: None,
                    module: Some(module.name()),
                });
            }
            let events = module.events();
            tracing::info!(
                event = "module.started",
                module = module.name(),
                publishes = ?events.publishes,
                consumes = ?events.consumes
            );
            started.push(module.clone());
        }
        let readiness = Arc::new(Readiness::new(
            database.clone(),
            modules.clone(),
            migrations.is_current(),
        ));
        let mut router = health::router(readiness.clone());
        for module in &modules {
            let own = ctx
                .for_module(module.name())
                .map_err(|failure| StartupError {
                    failure,
                    migration: None,
                    module: Some(module.name()),
                })?;
            router = router.nest(&format!("/v1/{}", module.name()), module.routes(&own));
        }
        let router = edge::apply(
            router.merge(parts.routes),
            &config.http,
            ctx.authenticator().cloned(),
            errors.clone(),
        );
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
    /// Serve until `shutdown` resolves, then stop admitting connections,
    /// report not ready, drain in-flight requests for at most the drain
    /// timeout, shut modules down and close the pool. Emits
    /// `listener.ready`, `listener.draining` and `listener.stopped`.
    pub async fn serve(
        self,
        listener: tokio::net::TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), Failure> {
        let address = listener
            .local_addr()
            .map_err(|_| Failure::permanent("listener_failed"))?;
        tracing::info!(
            event = "listener.ready",
            address = address.to_string(),
            app = self.config.app.id.as_str(),
            modules = ?self.modules.iter().map(|m| m.name()).collect::<Vec<_>>()
        );
        let signalled = Arc::new(tokio::sync::Notify::new());
        let readiness = self.readiness.clone();
        let notify = signalled.clone();
        let server = axum::serve(
            listener,
            self.router
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            shutdown.await;
            readiness.drain();
            tracing::info!(event = "listener.draining");
            notify.notify_one();
        });
        let drain = self.timeouts.drain;
        let result = tokio::select! {
            result = server => result.map_err(|_| Failure::transient("listener_failed")),
            () = async { signalled.notified().await; tokio::time::sleep(drain).await } => {
                tracing::warn!(event = "listener.drain_timeout", drain_ms = drain.as_millis() as u64);
                Ok(())
            }
        };
        stop_modules(&self.modules, self.timeouts.module_shutdown).await;
        self.database.close();
        tracing::info!(
            event = "listener.stopped",
            code = if result.is_ok() {
                "ok"
            } else {
                "listener_failed"
            }
        );
        result
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
            tracing::warn!(event = "module.shutdown_timeout", module = module.name());
        } else {
            tracing::info!(event = "module.stopped", module = module.name());
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
