//! The guest build commands (scaffold §4.1, §4.3). None reads the project
//! configuration, starts anything or emits telemetry; each prints one JSON
//! report on stdout.
//!
//! `merezha build [--manifest runtime.toml] [--workspace Cargo.toml]` builds
//! every service of the runtime manifest from the Cargo workspace and
//! componentizes it into its `components/<id>.wasm`, the only path shape it
//! accepts: the service id names the package. It prints
//! `{"status":"built","components":[{"id","bytes"}],"lockfile":"locked"|"created"}`
//! and exits 0, or `{"status":"failed","error":{"code","service"?,"reason"}}`
//! and exits 2 before anything is built (`invalid_manifest`,
//! `unsupported_component_path`, `missing_wasm_target`, `unknown_package`,
//! `no_cdylib`) or 1 when a step fails (`cargo_failed`,
//! `component_source_missing`, `component_rejected`,
//! `component_write_failed`). Cargo's output goes to stderr. See
//! [`merezha_guest_build::build`].
//!
//! `merezha componentize CORE.wasm COMPONENT.wasm` is the componentize step
//! alone: `{"status":"componentized","bytes":N}` and exit 0, or a failed
//! report and exit 1.
//!
//! `merezha version` prints `{"version":"0.1.0","rev":"<commit>"|null}`, the
//! source revision recorded at build time (`build.rs`), and exits 0.
use merezha_guest_build::{Error, ErrorKind, Service, Toolchain};
use merezha_runtime::manifest::{MAX_MANIFEST_BYTES, Manifest};
use serde_json::{Value, json};
use std::{
    io::Read,
    path::{Component, Path, PathBuf},
    process::ExitCode,
};

pub const BUILD_USAGE: &str = "merezha build [--manifest PATH] [--workspace PATH]";
pub const COMPONENTIZE_USAGE: &str = "merezha componentize CORE.wasm COMPONENT.wasm";
pub const VERSION_USAGE: &str = "merezha version";

/// The merezha commit this binary was built from, when known (scaffold
/// §2.9): `MEREZHA_SOURCE_REV` at build time, else a clean checkout's
/// `HEAD`. `merezha new` (SC-S2) pins generated projects to it.
pub fn source_rev() -> Option<&'static str> {
    Some(env!("MEREZHA_BUILD_SOURCE_REV")).filter(|rev| !rev.is_empty())
}

/// `merezha version`'s report.
pub fn version_report() -> Value {
    json!({"version": env!("CARGO_PKG_VERSION"), "rev": source_rev()})
}

fn invalid_arguments(usage: &str) -> ExitCode {
    println!("{}", json!({"status": "invalid_arguments", "usage": usage}));
    ExitCode::from(2)
}

fn failure(error: &Error) -> ExitCode {
    let mut detail = json!({"code": error.code(), "reason": error.reason});
    if let Some(service) = &error.service {
        detail["service"] = json!(service);
    }
    println!("{}", json!({"status": "failed", "error": detail}));
    ExitCode::from(error.kind.exit_code())
}

/// Entry point for `merezha version`.
pub fn version_main(mut args: impl Iterator<Item = String>) -> ExitCode {
    if args.next().is_some() {
        return invalid_arguments(VERSION_USAGE);
    }
    println!("{}", version_report());
    ExitCode::SUCCESS
}

/// Entry point for `merezha componentize`.
pub fn componentize_main(mut args: impl Iterator<Item = String>) -> ExitCode {
    let (Some(input), Some(output), None) = (args.next(), args.next(), args.next()) else {
        return invalid_arguments(COMPONENTIZE_USAGE);
    };
    match merezha_guest_build::componentize_file(Path::new(&input), Path::new(&output)) {
        Ok(bytes) => {
            println!("{}", json!({"status": "componentized", "bytes": bytes}));
            ExitCode::SUCCESS
        }
        Err(error) => failure(&error),
    }
}

/// Entry point for `merezha build`.
pub fn build_main(mut args: impl Iterator<Item = String>) -> ExitCode {
    let mut manifest = None;
    let mut workspace = None;
    while let Some(argument) = args.next() {
        let slot = match argument.as_str() {
            "--manifest" if manifest.is_none() => &mut manifest,
            "--workspace" if workspace.is_none() => &mut workspace,
            _ => return invalid_arguments(BUILD_USAGE),
        };
        match args.next() {
            Some(value) => *slot = Some(PathBuf::from(value)),
            None => return invalid_arguments(BUILD_USAGE),
        }
    }
    let manifest = manifest.unwrap_or_else(|| PathBuf::from("runtime.toml"));
    let workspace = workspace.unwrap_or_else(|| PathBuf::from("Cargo.toml"));
    let services = match services(&manifest) {
        Ok(services) => services,
        Err(error) => return failure(&error),
    };
    match merezha_guest_build::build(&workspace, &services, &Toolchain::from_env()) {
        Ok(report) => {
            let components: Vec<Value> = report
                .components
                .iter()
                .map(|built| json!({"id": built.id, "bytes": built.bytes}))
                .collect();
            println!(
                "{}",
                json!({
                    "status": "built",
                    "components": components,
                    "lockfile": report.lockfile.as_str(),
                })
            );
            ExitCode::SUCCESS
        }
        Err(error) => failure(&error),
    }
}

/// The services of the runtime manifest at `path`, parsed by the kernel's
/// parser without loading components, each with its output path. A path
/// must be `components/<id>.wasm`, relative to the manifest.
pub fn services(path: &Path) -> Result<Vec<Service>, Error> {
    let invalid = |reason: String| Error::new(ErrorKind::InvalidManifest, reason);
    let file = std::fs::File::open(path)
        .map_err(|error| invalid(format!("cannot read {}: {error}", path.display())))?;
    let mut text = String::new();
    file.take(MAX_MANIFEST_BYTES as u64 + 1)
        .read_to_string(&mut text)
        .map_err(|error| invalid(format!("cannot read {}: {error}", path.display())))?;
    let manifest =
        Manifest::parse(&text).map_err(|error| invalid(format!("{}: {error}", path.display())))?;
    let directory = path.parent().unwrap_or(Path::new(""));
    manifest
        .services
        .into_iter()
        .map(|service| {
            let expected = format!("{}.wasm", service.id);
            let mut parts = service.path.components();
            let supported = matches!(
                (parts.next(), parts.next(), parts.next()),
                (Some(Component::Normal(dir)), Some(Component::Normal(file)), None)
                    if dir == "components" && file.to_str() == Some(expected.as_str())
            );
            if !supported {
                return Err(Error::new(
                    ErrorKind::UnsupportedComponentPath,
                    format!(
                        "service {} has path {}; merezha build writes components/{expected}",
                        service.id,
                        service.path.display()
                    ),
                )
                .for_service(&service.id));
            }
            Ok(Service {
                output: directory.join(&service.path),
                id: service.id,
            })
        })
        .collect()
}
