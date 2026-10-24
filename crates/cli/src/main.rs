use merezha_runtime::Runtime;
use serde_json::json;
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
#[tokio::main]
async fn main() -> std::process::ExitCode {
    if std::env::args().nth(1).as_deref() == Some("run") {
        return merezha_cli::run::main(std::env::args().skip(2)).await;
    }
    if std::env::args().nth(1).as_deref() == Some("notifications") {
        return merezha_cli::notifications::main(std::env::args().skip(2));
    }
    let (manifest, application, address) = match arguments() {
        Ok(args) => args,
        Err(_) => {
            merezha_cli::emit(
                "startup.failed",
                json!({"code":"invalid_arguments","usage":"merezha serve --manifest PATH [--application PATH] [--listen 127.0.0.1:PORT]"}),
            );
            return std::process::ExitCode::from(2);
        }
    };
    let runtime =
        match tokio::task::spawn_blocking(move || Runtime::load(manifest).map_err(|_| ())).await {
            Ok(Ok(runtime)) => runtime,
            _ => {
                merezha_cli::emit("startup.failed", json!({"code":"invalid_manifest"}));
                return std::process::ExitCode::from(2);
            }
        };
    let router = if let Some(path) = application {
        let guest = runtime.clone();
        match tokio::task::spawn_blocking(move || {
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
                merezha_cli::emit("startup.failed", json!({"code":code}));
                return std::process::ExitCode::from(2);
            }
        }
    } else {
        merezha_cli::app(runtime)
    };
    let listener = match tokio::net::TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(_) => {
            merezha_cli::emit("startup.failed", json!({"code":"listener_failed"}));
            return std::process::ExitCode::from(2);
        }
    };
    let address = listener
        .local_addr()
        .expect("bound listener has an address");
    merezha_cli::emit(
        "listener.ready",
        json!({"address":address.to_string(),"generation":1}),
    );
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown())
        .await;
    merezha_cli::emit(
        "listener.stopped",
        json!({"code":if result.is_ok() {"ok"} else {"listener_failed"}}),
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
    merezha_cli::emit("listener.draining", json!({}));
}
