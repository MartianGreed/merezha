//! `merezha run --config merezha.toml`: the platform host. It loads the
//! project, opens the PostgreSQL pool, applies or verifies migrations, builds
//! the runtime and the optional application, and serves everything behind the
//! platform edge until SIGINT or SIGTERM. An application with
//! `backend = "postgres"` runs as the host module `application` on the
//! project's pool; memory and SQLite applications open before the pool.
use crate::application_module::ApplicationModule;
use merezha_application::{
    config::StorageConfig,
    postgres::{BackgroundOptions, CatchUp, CurrentSpan, MODULE, PostgresOptions},
};
use merezha_domain::Failure;
use merezha_platform::config::CatchUp as ConfiguredCatchUp;
use merezha_platform::{
    BoxFuture, ConfigInput, EarlyListener, HostOptions, HostParts, Module, StatusSection, Timeouts,
    host::{self, Host},
    ports::{Authorizer, DenyAll, LoopbackAllowAll},
};
use merezha_runtime::Runtime;
use merezha_telemetry::{Service, Telemetry, names, pipeline, spawn_blocking_in_span};
use serde_json::{Value, json};
use std::{
    future::Future,
    path::PathBuf,
    process::ExitCode,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tracing::Instrument;

pub const USAGE: &str = "merezha run --config PATH [--listen ADDRESS] [--set SECTION.KEY=VALUE]...";
/// Bound on `--set` overrides.
const MAX_OVERRIDES: usize = 64;

/// Parsed `run` arguments: the configuration path and command-line
/// overrides, the highest configuration layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunArguments {
    pub config: PathBuf,
    pub overrides: Vec<(String, String)>,
}

/// Parse the arguments after `run`.
pub fn arguments(args: impl IntoIterator<Item = String>) -> Result<RunArguments, Failure> {
    let invalid = || Failure::permanent("invalid_arguments");
    let mut args = args.into_iter();
    let mut config = None;
    let mut overrides = Vec::new();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--config" if config.is_none() => {
                config = Some(PathBuf::from(args.next().ok_or_else(invalid)?));
            }
            "--listen" => {
                overrides.push(("http.listen".to_owned(), args.next().ok_or_else(invalid)?));
            }
            "--set" => {
                let pair = args.next().ok_or_else(invalid)?;
                let (key, value) = pair.split_once('=').ok_or_else(invalid)?;
                if key.is_empty() {
                    return Err(invalid());
                }
                overrides.push((key.to_owned(), value.to_owned()));
            }
            _ => return Err(invalid()),
        }
        if overrides.len() > MAX_OVERRIDES {
            return Err(invalid());
        }
    }
    Ok(RunArguments {
        config: config.ok_or_else(invalid)?,
        overrides,
    })
}

/// What `startup.failed` carries besides its code.
enum Detail {
    None,
    Usage,
    Issues(String),
}
/// `startup.failed` before the subscriber is installed: JSON to stderr,
/// unbuffered.
fn bootstrap_failed(code: &str, detail: Detail) {
    tracing::dispatcher::with_default(&pipeline::bootstrap(), || match detail {
        Detail::Issues(issues) => {
            merezha_telemetry::event!(ERROR, "startup.failed", code, issues = issues.as_str())
        }
        Detail::Usage => merezha_telemetry::event!(ERROR, "startup.failed", code, usage = USAGE),
        Detail::None => merezha_telemetry::event!(ERROR, "startup.failed", code),
    });
}

/// Entry point for `merezha run`. `ignored` names the `OTEL_*` variables
/// `main` removed from the environment. Exit codes: 0 after a graceful
/// stop, 1 when serving failed, 2 when startup failed.
pub async fn main(args: impl IntoIterator<Item = String>, ignored: Vec<String>) -> ExitCode {
    let arguments = match arguments(args) {
        Ok(arguments) => arguments,
        Err(failure) => {
            bootstrap_failed(&failure.code, Detail::Usage);
            return ExitCode::from(2);
        }
    };
    let input = ConfigInput {
        path: arguments.config,
        // Non-UTF-8 variables cannot be configuration; skip them.
        env: std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect(),
        overrides: arguments.overrides,
    };
    let (modules, identity) = crate::host_blocks(&input);
    run_with(
        input,
        modules,
        Some(identity),
        host::shutdown_signal(),
        ignored,
    )
    .await
}

/// Run the host with `modules` until `shutdown` resolves, then flush
/// telemetry. Installs the process-wide subscriber, so call it once per
/// process. Startup failures are logged as `startup.failed` at error with a
/// stable code and never with values.
pub async fn run(
    input: ConfigInput,
    modules: Vec<Box<dyn Module>>,
    shutdown: impl Future<Output = ()> + Send + 'static,
    otel_ignored: Vec<String>,
) -> ExitCode {
    run_with(input, modules, None, shutdown, otel_ignored).await
}

/// [`run`] with the identity block's handle: when `[modules.identity]` is
/// present the host installs its authenticator (sessions and API keys),
/// its RBAC `Authorizer` on every listener and its `Directory`.
pub async fn run_with(
    input: ConfigInput,
    modules: Vec<Box<dyn Module>>,
    identity: Option<merezha_identity::IdentityHandle>,
    shutdown: impl Future<Output = ()> + Send + 'static,
    otel_ignored: Vec<String>,
) -> ExitCode {
    let (config, modules) = match host::configure(&input, modules) {
        Ok(configured) => configured,
        Err(errors) => {
            // The subscriber depends on configuration, so this one event
            // goes through the bootstrap dispatcher. Issues carry keys,
            // codes and origins only.
            let issues: Vec<_> = errors
                .0
                .iter()
                .map(|i| json!({"key": i.key, "code": i.code, "origin": i.origin.to_string()}))
                .collect();
            bootstrap_failed(
                "invalid_config",
                Detail::Issues(serde_json::Value::from(issues).to_string()),
            );
            return ExitCode::from(2);
        }
    };
    let service = Service::resolve(
        &config.telemetry.resource,
        &config.app.id,
        config.app.version.as_deref(),
        &config.app.instance_id,
    );
    let telemetry = match Telemetry::init(&config.telemetry, &service) {
        Ok(telemetry) => telemetry,
        Err(failure) => {
            bootstrap_failed(&failure.code, Detail::None);
            return ExitCode::from(2);
        }
    };
    if !otel_ignored.is_empty() {
        merezha_telemetry::event!(
            INFO,
            "telemetry.otel_env_ignored",
            names = otel_ignored.join(",").as_str()
        );
    }
    // Jobs and the outbox run whenever the host does (R12). Their
    // instruments and gauge sampler run when metrics are exported (TE §1.3).
    let meters = telemetry.meters();
    let jobs = merezha_jobs::PgJobs::new(&config.jobs)
        .with_app_version(config.app.version.clone())
        .with_telemetry(&meters, config.telemetry.metrics.sample_interval);
    let mut options = HostOptions {
        meters,
        status: vec![Arc::new(TelemetrySection(telemetry.status_reader()))],
        ..HostOptions::default()
    };
    let mut rbac = None;
    if let Some(identity) = identity
        && modules.iter().any(|m| m.name() == merezha_identity::NAME)
    {
        options.authenticator = Some(identity.authenticator());
        options.directory = Some(identity.directory());
        rbac = Some(identity.authorizer());
    }
    let code = serve(config, modules, shutdown, options, jobs, rbac).await;
    // Flush traces, logs and metrics within `shutdown_timeout_ms`, then the
    // stderr writer.
    let _ = spawn_blocking_in_span(move || telemetry.shutdown()).await;
    code
}

/// How long requests already admitted by the early listener may finish
/// when startup fails or is interrupted.
const CLOSE_WITHIN: Duration = Duration::from_secs(5);

/// The `telemetry` section of `/v1/admin/status`.
struct TelemetrySection(pipeline::StatusReader);
impl StatusSection for TelemetrySection {
    fn name(&self) -> &'static str {
        "telemetry"
    }
    fn read(&self) -> BoxFuture<'_, Result<Value, Failure>> {
        let status = self.0.read().to_json();
        Box::pin(async move { Ok(status) })
    }
}
/// The `catalog` section: the runtime's active generation.
struct CatalogSection {
    runtime: Runtime,
    loaded_at: jiff::Timestamp,
}
impl StatusSection for CatalogSection {
    fn name(&self) -> &'static str {
        "catalog"
    }
    fn read(&self) -> BoxFuture<'_, Result<Value, Failure>> {
        let catalog = self.runtime.catalog();
        let loaded_at = self.loaded_at.to_string();
        Box::pin(async move {
            let catalog = catalog.map_err(|error| Failure::transient(error.code()))?;
            Ok(json!({
                "generation": catalog.generation,
                "services": catalog.services.len(),
                "pipelines": catalog.pipelines.len(),
                "loaded_at": loaded_at,
            }))
        })
    }
}

/// The `application` section: AP's `Application::status()` as returned,
/// the body of `GET /v1/application/status` (R10).
struct ApplicationSection(Arc<OnceLock<merezha_application::Application>>);
impl StatusSection for ApplicationSection {
    fn name(&self) -> &'static str {
        "application"
    }
    fn read(&self) -> BoxFuture<'_, Result<Value, Failure>> {
        let application = self.0.get().cloned();
        Box::pin(async move {
            let application = application.ok_or_else(|| Failure::transient("unavailable"))?;
            let status =
                merezha_telemetry::spawn_blocking_in_span(move || application.status()).await;
            let status = status.map_err(|_| Failure::transient("internal"))??;
            serde_json::to_value(status).map_err(|_| Failure::transient("internal"))
        })
    }
}

/// Bind the early listener, run startup inside the `startup` span, then
/// serve until `shutdown` and drain. A shutdown signal during startup
/// interrupts it (modules already started shut down) and exits 0.
async fn serve(
    config: merezha_platform::ProjectConfig,
    modules: Vec<Box<dyn Module>>,
    shutdown: impl Future<Output = ()> + Send + 'static,
    mut options: HostOptions,
    jobs: merezha_jobs::PgJobs,
    rbac: Option<Arc<dyn Authorizer>>,
) -> ExitCode {
    let (signal, stopped) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _ = signal.send(true);
    });
    let stop = {
        let stopped = stopped.clone();
        move || {
            let mut stopped = stopped.clone();
            async move {
                let _ = stopped.wait_for(|stopped| *stopped).await;
            }
        }
    };
    let early = match EarlyListener::bind(&config, None, &options).await {
        Ok(early) => early,
        Err(error) => {
            merezha_telemetry::event!(ERROR, "startup.failed", code = error.failure.code.as_str());
            return ExitCode::from(2);
        }
    };
    options.lifecycle = Some(early.lifecycle().clone());
    // A shutdown signal during startup interrupts it: the host shuts the
    // modules it already started down and fails `startup_interrupted`.
    options.interrupt = Some(stopped);
    let started = start(config, modules, options, jobs, rbac)
        .instrument(tracing::info_span!(names::span::STARTUP))
        .await;
    let host = match started {
        Ok(host) => host,
        Err(stop) if stop.code == "startup_interrupted" => {
            merezha_telemetry::event!(INFO, "listener.draining");
            early.close(CLOSE_WITHIN).await;
            return ExitCode::SUCCESS;
        }
        Err(stop) => {
            match &stop.issues {
                Some(issues) => merezha_telemetry::event!(
                    ERROR,
                    "startup.failed",
                    code = stop.code.as_str(),
                    issues = issues.as_str()
                ),
                None => {
                    merezha_telemetry::event!(ERROR, "startup.failed", code = stop.code.as_str())
                }
            }
            early.close(CLOSE_WITHIN).await;
            return ExitCode::from(2);
        }
    };
    match early.serve(host, stop()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::from(1),
    }
}

/// Why startup stopped: the `startup.failed` code and, for the
/// application's access declarations, every issue as a JSON list of
/// `{key, code}`.
struct Stop {
    code: String,
    issues: Option<String>,
}
impl From<String> for Stop {
    fn from(code: String) -> Self {
        Self { code, issues: None }
    }
}

/// Validate the application document, listing every access, tenancy and
/// permission problem at once (AP §C.4): what the document shows on its
/// own, `unknown_permission` against this binary's catalog and, with the
/// identity block, `route_access_undeclared`. The first issue is the code;
/// any other validation failure stops startup as before.
fn check_access(
    config: &merezha_platform::ProjectConfig,
    application: &merezha_application::config::Config,
    identity: bool,
) -> Result<(), Stop> {
    let own = application.route_issues();
    match application.validate() {
        Err(failure) if own.first().is_none_or(|issue| issue.code != failure.code) => {
            return Err(failure.code.into());
        }
        _ => {}
    }
    let catalog = crate::permission_catalog(application);
    // Every user holds `builtin_grants.user` (identity §5).
    let everyone = user_grants(config, &catalog);
    let mut issues = application.host_issues(
        &|permission| catalog.contains(permission),
        &|permission| everyone.iter().any(|held| held.as_str() == permission),
        identity,
    );
    issues.extend(application.block_kind_issues(crate::BLOCK_RESOURCE_KINDS));
    let Some(first) = issues.first() else {
        return Ok(());
    };
    Err(Stop {
        code: first.code.to_owned(),
        issues: serde_json::to_string(&issues).ok(),
    })
}

/// The permissions of identity's `builtin_grants.user`, expanded against
/// `catalog`: what every active user holds at application scope. Empty
/// without the identity block.
fn user_grants(
    config: &merezha_platform::ProjectConfig,
    catalog: &merezha_identity::Catalog,
) -> merezha_platform::ports::PermissionSet {
    let user = config
        .modules
        .get(merezha_identity::NAME)
        .and_then(|section| section.table.get("rbac"))
        .and_then(|rbac| rbac.get("builtin_grants"))
        .and_then(|grants| grants.get("user"))
        .and_then(|user| user.as_array());
    catalog.expand(
        user.into_iter()
            .flatten()
            .filter_map(|grant| grant.as_str()),
    )
}

/// Identity §5: `builtin_grants.user` are application grants every user
/// holds in every organization, so a permission a route checks at `scope =
/// "tenant"` belongs in `member`. Warn once per such route
/// (`identity.user_grant_tenant_route`).
fn warn_user_grants_on_tenant_routes(
    config: &merezha_platform::ProjectConfig,
    application: &merezha_application::config::Config,
) {
    use merezha_application::config::{RouteScope, Rule};
    let granted = user_grants(config, &crate::permission_catalog(application));
    let routes = application
        .commands
        .iter()
        .map(|r| ("commands", &r.tag, r.rule(), r.route_scope()))
        .chain(
            application
                .queries
                .iter()
                .map(|r| ("queries", &r.tag, r.rule(), r.route_scope())),
        );
    for (kind, tag, rule, scope) in routes {
        if let (Rule::Permission(permission), RouteScope::Tenant) = (rule, scope)
            && granted.contains(permission)
        {
            merezha_telemetry::event!(
                WARN,
                "identity.user_grant_tenant_route",
                permission = permission.as_str(),
                route = format!("{kind}.{}", tag.as_str()).as_str()
            );
        }
    }
}

/// Everything from the runtime to the started host, inside the `startup`
/// span. Fails with the `startup.failed` code.
async fn start(
    config: merezha_platform::ProjectConfig,
    modules: Vec<Box<dyn Module>>,
    mut options: HostOptions,
    jobs: merezha_jobs::PgJobs,
    rbac: Option<Arc<dyn Authorizer>>,
) -> Result<Host, Stop> {
    let manifest = config.runtime.manifest.clone();
    let meters = options.meters.clone();
    let runtime_meters = meters.clone();
    let loaded = spawn_blocking_in_span(move || {
        Runtime::load_with_meters(manifest, &runtime_meters).map_err(|_| ())
    })
    .await;
    let runtime = match loaded {
        Ok(Ok(runtime)) => runtime,
        _ => return Err("invalid_manifest".to_owned().into()),
    };
    // Database-backed gauges (the projection lag) refresh at this interval,
    // and only while metrics are exported.
    let sample_interval = config.telemetry.metrics.sample_interval;
    options.status.push(Arc::new(CatalogSection {
        runtime: runtime.clone(),
        loaded_at: jiff::Timestamp::now(),
    }));
    // Without the identity block, only a private loopback listener keeps the
    // explicit allow-all policy and the unauthenticated raw runtime
    // endpoints. A public listener denies application messages and
    // projection administration and does not mount the raw runtime at all.
    // A trusted proxy means requests arrive from elsewhere even on a
    // loopback listener, so it counts as public too.
    let private = host::private_listener(&config);
    // With the identity block every permission check is its RBAC
    // `Authorizer` (identity section 5), on every listener, loopback
    // included; application routes are checked through it at their scope
    // (AP §C.4).
    let authorizer: Arc<dyn Authorizer> = match (rbac, LoopbackAllowAll::new(config.http.listen)) {
        (Some(rbac), _) if options.authenticator.is_some() => rbac,
        (_, Ok(allow)) if private && options.authenticator.is_none() => Arc::new(allow),
        _ => Arc::new(DenyAll),
    };
    let mut modules = modules;
    let mut application_failure = None;
    let application = match config.application.clone() {
        Some(section) => {
            let path = section.path.clone();
            let read =
                spawn_blocking_in_span(move || crate::read_application_document(&path)).await;
            let application = match read {
                Ok(Ok(application)) => application,
                Ok(Err(failure)) => return Err(failure.code.into()),
                Err(_) => return Err("invalid_application_config".to_owned().into()),
            };
            check_access(&config, &application, options.authenticator.is_some())?;
            if options.authenticator.is_some() {
                warn_user_grants_on_tenant_routes(&config, &application);
            }
            let guest = runtime.clone();
            // Route permissions are enforced through the edge's caller and
            // this policy before any guest call (AP §C.4), so the payload
            // hook adds nothing on the platform host.
            let policy: Arc<dyn merezha_application::Authorizer> =
                Arc::new(merezha_application::AllowAll);
            if let StorageConfig::Postgres { .. } = application.storage {
                // Opened by the host as a module once its migrations ran.
                let module = ApplicationModule::new(
                    application,
                    Arc::new(guest),
                    authorizer.clone(),
                    PostgresOptions {
                        lock_timeout_max: section.append.lock_timeout_max,
                        // Events carry the command span's context (TE §2.2).
                        trace: Some(Arc::new(CurrentSpan)),
                        background: BackgroundOptions {
                            catch_up: match section.projections.catch_up {
                                ConfiguredCatchUp::Auto => CatchUp::Auto,
                                ConfiguredCatchUp::Manual => CatchUp::Manual,
                            },
                            interval: section.projections.interval,
                            run_timeout: section.projections.run_timeout,
                            warm_readiness: section.projections.warm_readiness,
                            max_concurrent: section.background.max_concurrent as usize,
                            instance_id: config.app.instance_id.clone(),
                            ..BackgroundOptions::default()
                        },
                    },
                    // The jobs runtime's pool needs (AP §B.10).
                    2 + config.jobs.queues.len(),
                )
                .sample_interval(sample_interval);
                application_failure = Some(module.failure());
                options
                    .status
                    .push(Arc::new(ApplicationSection(module.application())));
                modules.insert(0, Box::new(module) as Box<dyn Module>);
                None
            } else {
                let app_meters = meters.clone();
                match spawn_blocking_in_span(move || {
                    merezha_application::Application::open(application, Arc::new(guest), policy)
                        .map(|application| application.with_meters(&app_meters))
                })
                .await
                {
                    Ok(Ok(application)) => {
                        if meters.enabled() {
                            application
                                .spawn_lag_sampler(sample_interval)
                                .map_err(|failure| Stop::from(failure.code))?;
                        }
                        options.status.push(Arc::new(ApplicationSection(Arc::new(
                            OnceLock::from(application.clone()),
                        ))));
                        Some(application)
                    }
                    Ok(Err(failure)) => return Err(failure.code.into()),
                    Err(_) => return Err("invalid_application_config".to_owned().into()),
                }
            }
        }
        None => None,
    };
    // A PostgreSQL application's routes come from its module instead.
    // The raw runtime endpoints have no authentication of their own: with
    // the identity block (an authenticator) they are never mounted.
    let raw_runtime = private && options.authenticator.is_none();
    let routes = match (raw_runtime, application) {
        (true, Some(application)) => crate::app(runtime).merge(crate::application_http::router(
            application,
            Some(authorizer.clone()),
        )),
        (true, None) => crate::app(runtime),
        (false, Some(application)) => {
            crate::application_http::router(application, Some(authorizer.clone()))
        }
        (false, None) => axum::Router::new(),
    };
    merezha_telemetry::event!(
        INFO,
        "runtime.loaded",
        raw_runtime,
        application = config.application.is_some()
    );
    // The jobs runtime stops first; the module shutdown budget adds its drain
    // (`jobs.drain_timeout_ms`, the 2 s cancel grace and the release) to
    // the default, so the other modules keep theirs (AP §B.8).
    let defaults = Timeouts::default();
    let timeouts = Timeouts {
        module_shutdown: defaults.module_shutdown
            + config.jobs.drain_timeout
            + std::time::Duration::from_secs(3),
        ..defaults
    };
    let parts = HostParts {
        routes,
        authorizer,
        timeouts,
        jobs: Some(Arc::new(jobs)),
    };
    match Host::start_with(config, modules, parts, options).await {
        Ok(host) => Ok(host),
        // The application module's own reason, not `module_start_failed`.
        Err(error) => match application_failure.as_ref().and_then(|f| f.get()) {
            Some(failure) if error.module == Some(MODULE) => Err(failure.code.clone().into()),
            _ => Err(error.failure.code.into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<RunArguments, Failure> {
        arguments(args.iter().map(|a| a.to_string()))
    }
    #[test]
    fn arguments_map_flags_to_overrides() {
        let parsed = parse(&[
            "--config",
            "merezha.toml",
            "--listen",
            "127.0.0.1:0",
            "--set",
            "database.migrate=true",
        ])
        .unwrap();
        assert_eq!(parsed.config, PathBuf::from("merezha.toml"));
        assert_eq!(
            parsed.overrides,
            vec![
                ("http.listen".into(), "127.0.0.1:0".into()),
                ("database.migrate".into(), "true".into())
            ]
        );
        for bad in [
            &[][..],
            &["--listen", "127.0.0.1:0"][..],
            &["--config"][..],
            &["--config", "a", "--config", "b"][..],
            &["--config", "a", "--set", "novalue"][..],
            &["--config", "a", "--set", "=x"][..],
            &["--config", "a", "--manifest", "m"][..],
        ] {
            assert_eq!(parse(bad).unwrap_err().code, "invalid_arguments", "{bad:?}");
        }
    }
}
