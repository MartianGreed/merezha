//! `merezha build`: Cargo on the project workspace, then every service's
//! core module componentized (scaffold §4.1, steps 2 to 4; the caller
//! parses the runtime manifest, step 1).

use crate::{Error, ErrorKind, Staged, stage};
use serde::Deserialize;
use std::{
    ffi::OsString,
    io::{self, Read},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

/// The target guests compile to: its standard library imports nothing.
pub const WASM_TARGET: &str = "wasm32-unknown-unknown";

/// Most bytes `cargo metadata --no-deps` may print.
const MAX_METADATA_BYTES: u64 = 64 * 1024 * 1024;

/// The programs `build` runs. Cargo picks the toolchain as it always does
/// (`rust-toolchain.toml`, `RUSTUP_TOOLCHAIN`), from the workspace's
/// directory.
#[derive(Clone, Debug)]
pub struct Toolchain {
    pub cargo: OsString,
    pub rustc: OsString,
}
impl Toolchain {
    /// `$CARGO` and `$RUSTC` when set, else `cargo` and `rustc` from `PATH`.
    pub fn from_env() -> Self {
        Self {
            cargo: std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()),
            rustc: std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()),
        }
    }
}

/// One service to build: the workspace package named `id`, componentized
/// into `output`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Service {
    pub id: String,
    pub output: PathBuf,
}

/// A component `build` wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Built {
    pub id: String,
    pub bytes: usize,
}

/// Whether the build used the workspace's lockfile or Cargo wrote one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lockfile {
    /// `Cargo.lock` existed; Cargo ran with `--locked`.
    Locked,
    /// There was no `Cargo.lock`; Cargo resolved and wrote it.
    Created,
}
impl Lockfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Locked => "locked",
            Self::Created => "created",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildReport {
    pub components: Vec<Built>,
    pub lockfile: Lockfile,
}

/// Builds every service of `services` from the Cargo workspace whose
/// manifest is `workspace`, then componentizes each into its `output`.
///
/// In order: the `wasm32-unknown-unknown` target must be installed (checked
/// before Cargo runs); `cargo metadata --no-deps` must list a package named
/// after each service with a `cdylib` library target, whose name gives the
/// artifact (`[lib] name = "task_domain"` works); `cargo build --release
/// --target wasm32-unknown-unknown -p <id>...` runs with `--locked` when
/// `Cargo.lock` exists, its output on stderr, into the metadata's target
/// directory (so `CARGO_TARGET_DIR` and Cargo configuration apply). Every
/// component is staged as `<output>.partial` before any is renamed into
/// place: a refused service leaves no new file behind.
///
/// # Errors
///
/// An [`Error`] whose [`ErrorKind`] is the report's code, naming the
/// service when one is at fault.
pub fn build(
    workspace: &Path,
    services: &[Service],
    toolchain: &Toolchain,
) -> Result<BuildReport, Error> {
    let workspace = std::path::absolute(workspace).map_err(|error| {
        Error::new(
            ErrorKind::CargoFailed,
            format!("cannot resolve {}: {error}", workspace.display()),
        )
    })?;
    let directory = workspace.parent().unwrap_or(Path::new("/")).to_owned();
    check_wasm_target(&directory, toolchain)?;
    let metadata = metadata(&workspace, &directory, toolchain)?;
    let mut artifacts = Vec::with_capacity(services.len());
    for service in services {
        let package = metadata
            .packages
            .iter()
            .find(|package| package.name == service.id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::UnknownPackage,
                    format!("the workspace has no package named {}", service.id),
                )
                .for_service(&service.id)
            })?;
        let library = package
            .targets
            .iter()
            // The library target: its `kind` lists the crate types. An
            // example or test with `crate-type = ["cdylib"]` has kind
            // `example` and is not the package's library.
            .find(|target| target.kind.iter().any(|t| t == "cdylib"))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::NoCdylib,
                    format!(
                        "package {} has no cdylib target; add crate-type = [\"cdylib\"] to its [lib]",
                        package.name
                    ),
                )
                .for_service(&service.id)
            })?;
        artifacts.push(
            metadata
                .target_directory
                .join(WASM_TARGET)
                .join("release")
                .join(format!("{}.wasm", library.name.replace('-', "_"))),
        );
    }
    let lockfile = if metadata.workspace_root.join("Cargo.lock").is_file() {
        Lockfile::Locked
    } else {
        Lockfile::Created
    };
    if services.is_empty() {
        return Ok(BuildReport {
            components: Vec::new(),
            lockfile,
        });
    }

    let mut cargo = Command::new(&toolchain.cargo);
    cargo
        .current_dir(&directory)
        .arg("build")
        .arg("--manifest-path")
        .arg(&workspace)
        .args(["--release", "--target", WASM_TARGET]);
    if lockfile == Lockfile::Locked {
        cargo.arg("--locked");
    }
    for service in services {
        cargo.arg("-p").arg(&service.id);
    }
    // Stdout carries the report; Cargo's own output goes to stderr.
    let status = cargo
        .stdin(Stdio::null())
        .stdout(Stdio::from(io::stderr()))
        .status()
        .map_err(|error| {
            Error::new(ErrorKind::CargoFailed, format!("cannot run cargo: {error}"))
        })?;
    if !status.success() {
        return Err(Error::new(
            ErrorKind::CargoFailed,
            format!("cargo build failed ({status})"),
        ));
    }

    let mut staged: Vec<Staged> = Vec::with_capacity(services.len());
    for (service, artifact) in services.iter().zip(&artifacts) {
        if let Some(parent) = service.output.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                Error::new(
                    ErrorKind::ComponentWriteFailed,
                    format!("cannot create {}: {error}", parent.display()),
                )
                .for_service(&service.id)
            })?;
        }
        // An error drops `staged`, which removes every partial file.
        staged.push(stage(artifact, &service.output).map_err(|e| e.for_service(&service.id))?);
    }
    let components = services
        .iter()
        .zip(&staged)
        .map(|(service, staged)| Built {
            id: service.id.clone(),
            bytes: staged.bytes,
        })
        .collect();
    // A failed rename drops the rest of the iterator and their partials.
    for (service, component) in services.iter().zip(staged) {
        component.commit().map_err(|e| e.for_service(&service.id))?;
    }
    Ok(BuildReport {
        components,
        lockfile,
    })
}

/// Fails [`ErrorKind::MissingWasmTarget`] when the toolchain Cargo would
/// use in `directory` has no `wasm32-unknown-unknown` standard library.
fn check_wasm_target(directory: &Path, toolchain: &Toolchain) -> Result<(), Error> {
    let output = Command::new(&toolchain.rustc)
        .current_dir(directory)
        .args(["--print", "target-libdir", "--target", WASM_TARGET])
        .stdin(Stdio::null())
        .stderr(Stdio::inherit())
        .output()
        .map_err(|error| {
            Error::new(ErrorKind::CargoFailed, format!("cannot run rustc: {error}"))
        })?;
    if !output.status.success() {
        return Err(Error::new(
            ErrorKind::CargoFailed,
            format!("rustc --print target-libdir failed ({})", output.status),
        ));
    }
    let libdir = String::from_utf8_lossy(&output.stdout);
    let libdir = Path::new(libdir.trim());
    if libdir.as_os_str().is_empty() || !libdir.is_dir() {
        return Err(Error::new(
            ErrorKind::MissingWasmTarget,
            format!(
                "the {WASM_TARGET} target is not installed; run: rustup target add {WASM_TARGET}"
            ),
        ));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    workspace_root: PathBuf,
    target_directory: PathBuf,
}
#[derive(Debug, Deserialize)]
struct Package {
    name: String,
    targets: Vec<Target>,
}
#[derive(Debug, Deserialize)]
struct Target {
    name: String,
    kind: Vec<String>,
}

/// `cargo metadata --format-version 1 --no-deps`, read up to
/// [`MAX_METADATA_BYTES`].
fn metadata(workspace: &Path, directory: &Path, toolchain: &Toolchain) -> Result<Metadata, Error> {
    let failed = |reason: String| Error::new(ErrorKind::CargoFailed, reason);
    let mut child = Command::new(&toolchain.cargo)
        .current_dir(directory)
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--manifest-path",
        ])
        .arg(workspace)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| failed(format!("cannot run cargo: {error}")))?;
    let mut text = Vec::new();
    let read = child
        .stdout
        .take()
        .map(|stdout| stdout.take(MAX_METADATA_BYTES + 1).read_to_end(&mut text));
    if text.len() as u64 > MAX_METADATA_BYTES {
        let _ = child.kill();
        let _ = child.wait();
        return Err(failed(format!(
            "cargo metadata printed more than {MAX_METADATA_BYTES} bytes"
        )));
    }
    let status = child
        .wait()
        .map_err(|error| failed(format!("cargo metadata: {error}")))?;
    if !status.success() {
        return Err(failed(format!("cargo metadata failed ({status})")));
    }
    if let Some(Err(error)) = read {
        return Err(failed(format!("cannot read cargo metadata: {error}")));
    }
    serde_json::from_slice(&text)
        .map_err(|error| failed(format!("cannot parse cargo metadata: {error}")))
}
