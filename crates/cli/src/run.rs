//! `merezha run --config merezha.toml`: the platform host. It loads the
//! project, opens the PostgreSQL pool, applies or verifies migrations, builds
//! the runtime and the optional application, and serves everything behind the
//! platform edge until SIGINT or SIGTERM.
use merezha_domain::Failure;
use merezha_platform::{
    ConfigInput, HostParts, Module, Timeouts,
    host::{self, Host},
    ports::{Authorizer, DenyAll, LoopbackAllowAll},
    telemetry,
};
use merezha_runtime::Runtime;
use serde_json::json;
use std::{future::Future, path::PathBuf, process::ExitCode, sync::Arc};

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

/// Entry point for `merezha run`. Exit codes: 0 after a graceful stop, 1
/// when serving failed, 2 when startup failed.
pub async fn main(args: impl IntoIterator<Item = String>) -> ExitCode {
    let arguments = match arguments(args) {
        Ok(arguments) => arguments,
        Err(failure) => {
            crate::emit(
                "startup.failed",
                json!({"code": failure.code, "usage": USAGE}),
            );
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
    let modules = crate::modules(&input);
    run(input, modules, host::shutdown_signal()).await
}

/// Run the host with `modules` until `shutdown` resolves. Startup failures
/// are logged as `startup.failed` with a stable code and never with values.
pub async fn run(
    input: ConfigInput,
    modules: Vec<Box<dyn Module>>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> ExitCode {
    let (config, modules) = match host::configure(&input, modules) {
        Ok(configured) => configured,
        Err(errors) => {
            // The subscriber depends on configuration, so this one event
            // uses the plain JSON emitter. Issues carry keys, codes and
            // origins only.
            let issues: Vec<_> = errors
                .0
                .iter()
                .map(|i| json!({"key": i.key, "code": i.code, "origin": i.origin.to_string()}))
                .collect();
            crate::emit(
                "startup.failed",
                json!({"code": "invalid_config", "issues": issues}),
            );
            return ExitCode::from(2);
        }
    };
    if telemetry::init(&config.telemetry, Vec::new()).is_err() {
        crate::emit("startup.failed", json!({"code": "telemetry_initialized"}));
        return ExitCode::from(2);
    }
    let failed = |code: &str| {
        tracing::error!(event = "startup.failed", code);
        ExitCode::from(2)
    };
    let manifest = config.runtime.manifest.clone();
    let runtime =
        match tokio::task::spawn_blocking(move || Runtime::load(manifest).map_err(|_| ())).await {
            Ok(Ok(runtime)) => runtime,
            _ => return failed("invalid_manifest"),
        };
    // Without the identity block, only a private loopback listener keeps the
    // explicit allow-all policy and the unauthenticated raw runtime
    // endpoints. A public listener denies application messages and
    // projection administration and does not mount the raw runtime at all.
    // A trusted proxy means requests arrive from elsewhere even on a
    // loopback listener, so it counts as public too.
    let private = !config.http.public
        && config.http.listen.ip().is_loopback()
        && config.http.trusted_proxies.is_empty();
    let authorizer: Arc<dyn Authorizer> = match LoopbackAllowAll::new(config.http.listen) {
        Ok(allow) if private => Arc::new(allow),
        _ => Arc::new(DenyAll),
    };
    let application = match config.application.clone() {
        Some(application) => {
            let guest = runtime.clone();
            let policy: Arc<dyn merezha_application::Authorizer> = authorizer.clone();
            match tokio::task::spawn_blocking(move || {
                crate::open_application(&application.path, guest, policy)
            })
            .await
            {
                Ok(Ok(application)) => Some(application),
                Ok(Err(failure)) => return failed(&failure.code),
                Err(_) => return failed("invalid_application_config"),
            }
        }
        None => None,
    };
    let routes = match (private, application) {
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
    tracing::info!(
        event = "runtime.loaded",
        raw_runtime = private,
        application = config.application.is_some()
    );
    // Jobs and the outbox run whenever the host does (R12).
    let parts = HostParts {
        routes,
        authorizer,
        timeouts: Timeouts::default(),
        jobs: Some(Arc::new(merezha_jobs::PgJobs::new(&config.jobs))),
    };
    let host = match Host::start(config, modules, parts).await {
        Ok(host) => host,
        Err(error) => return failed(&error.failure.code),
    };
    let listener = match host.bind(None).await {
        Ok(listener) => listener,
        Err(error) => return failed(&error.failure.code),
    };
    match host.serve(listener, shutdown).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::from(1),
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
