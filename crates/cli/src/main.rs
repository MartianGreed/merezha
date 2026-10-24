use merezha_runtime::Runtime;
use merezha_telemetry::{
    Telemetry,
    config::{LogLevel, Service, TelemetryConfig},
};
use std::{net::SocketAddr, path::PathBuf};

fn arguments() -> anyhow::Result<(PathBuf, Option<PathBuf>, SocketAddr)> {
    let mut args = std::env::args().skip(1);
    anyhow::ensure!(args.next().as_deref() == Some("serve"), "expected serve");
    let mut manifest = None;
    let mut listen = None;
    let mut application = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--application" if application.is_none() => {
                application =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        anyhow::anyhow!("missing application config")
                    })?));
            }
            "--manifest" if manifest.is_none() => {
                manifest = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("missing manifest"))?,
                ))
            }
            "--listen" if listen.is_none() => {
                listen = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("missing address"))?
                        .parse::<SocketAddr>()?,
                )
            }
            _ => anyhow::bail!("invalid argument"),
        }
    }
    let listen = listen.unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 8080)));
    anyhow::ensure!(listen.ip().is_loopback(), "listener must be loopback");
    Ok((
        manifest.ok_or_else(|| anyhow::anyhow!("missing manifest"))?,
        application,
        listen,
    ))
}
fn main() -> std::process::ExitCode {
    // SAFETY: no other thread exists yet; the runtime is built below.
    let ignored = unsafe { merezha_telemetry::pipeline::scrub_otel_environment() };
    // One process-level crypto provider, so no TLS path (database, OTLP,
    // outbound) can fail for want of one.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    if std::env::args().nth(1).as_deref() == Some("notifications") {
        return merezha_cli::notifications::main(std::env::args().skip(2));
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            startup_failed("internal");
            return std::process::ExitCode::from(2);
        }
    };
    runtime.block_on(async move {
        match std::env::args().nth(1).as_deref() {
            Some("run") => merezha_cli::run::main(std::env::args().skip(2), ignored).await,
            Some("application") => {
                merezha_cli::application_cli::main(std::env::args().skip(2)).await
            }
            Some("content") => merezha_cli::content::main(std::env::args().skip(2)).await,
            _ => serve().await,
        }
    })
}
/// `startup.failed` before any subscriber is installed.
fn startup_failed(code: &str) {
    tracing::dispatcher::with_default(&merezha_telemetry::pipeline::bootstrap(), || {
        merezha_telemetry::event!(ERROR, "startup.failed", code);
    });
}
async fn serve() -> std::process::ExitCode {
    let (manifest, application, address) = match arguments() {
        Ok(args) => args,
        Err(_) => {
            tracing::dispatcher::with_default(&merezha_telemetry::pipeline::bootstrap(), || {
                merezha_telemetry::event!(
                    ERROR,
                    "startup.failed",
                    code = "invalid_arguments",
                    usage = "merezha serve --manifest PATH [--application PATH] [--listen 127.0.0.1:PORT]"
                );
            });
            return std::process::ExitCode::from(2);
        }
    };
    // The raw runtime has no project: stderr only, every invocation logged
    // (its successes are debug events), no OTLP pipeline.
    let config = TelemetryConfig {
        log_level: LogLevel::Debug,
        signals: Vec::new(),
        ..TelemetryConfig::default()
    };
    let telemetry = match Telemetry::init(&config, &Service::raw()) {
        Ok(telemetry) => telemetry,
        Err(failure) => {
            startup_failed(&failure.code);
            return std::process::ExitCode::from(2);
        }
    };
    let code = serve_with(manifest, application, address).await;
    let _ = merezha_telemetry::spawn_blocking_in_span(move || telemetry.shutdown()).await;
    code
}
async fn serve_with(
    manifest: PathBuf,
    application: Option<PathBuf>,
    address: SocketAddr,
) -> std::process::ExitCode {
    let failed = |code: &str| {
        merezha_telemetry::event!(ERROR, "startup.failed", code);
        std::process::ExitCode::from(2)
    };
    let runtime = match merezha_telemetry::spawn_blocking_in_span(move || {
        Runtime::load(manifest).map_err(|_| ())
    })
    .await
    {
        Ok(Ok(runtime)) => runtime,
        _ => return failed("invalid_manifest"),
    };
    let router = if let Some(path) = application {
        let guest = runtime.clone();
        match merezha_telemetry::spawn_blocking_in_span(move || {
            merezha_cli::open_application(
                &path,
                guest,
                std::sync::Arc::new(merezha_application::AllowAll),
            )
        })
        .await
        {
            Ok(Ok(application)) => merezha_cli::app_with_application(runtime, application),
            result => {
                let code = match &result {
                    Ok(Err(e)) => e.code.as_str(),
                    _ => "invalid_application_config",
                };
                return failed(code);
            }
        }
    } else {
        merezha_cli::app(runtime)
    };
    let listener = match tokio::net::TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(_) => return failed("listener_failed"),
    };
    let address = listener
        .local_addr()
        .expect("bound listener has an address");
    merezha_telemetry::event!(
        INFO,
        "listener.ready",
        address = address.to_string(),
        generation = 1u64
    );
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown())
        .await;
    merezha_telemetry::event!(
        INFO,
        "listener.stopped",
        code = if result.is_ok() {
            "ok"
        } else {
            "listener_failed"
        }
    );
    if result.is_ok() {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::from(1)
    }
}
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    merezha_telemetry::event!(INFO, "listener.draining");
}
