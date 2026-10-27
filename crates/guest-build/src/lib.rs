//! Turns Merezha guest crates into service components (scaffold §4.1).
//!
//! A guest compiles to a `wasm32-unknown-unknown` core module that carries
//! the component type `wit-bindgen` embedded for world
//! `merezha:service/service@0.1.0`. [`componentize`] encodes it as a
//! component, which must import nothing and export exactly
//! `merezha:service/handler@0.1.0` with the types of the canonical
//! `wit/service.wit`: what the host accepts. Any other world fails here
//! instead of at manifest load.
//!
//! [`componentize_file`] is `merezha componentize` (and the services
//! workspace's `merezha-componentize`); [`build`] is `merezha build`, which
//! runs Cargo on a project workspace first.
//!
//! The encoder is `wit-component` 0.258.3, the version Wasmtime 49 locks, so
//! components are written with the `wasmparser` the host reads them with.

mod build;

pub use build::{BuildReport, Built, Lockfile, Service, Toolchain, WASM_TARGET, build};

use anyhow::{Context, bail, ensure};
use std::{
    fmt,
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
};
use wit_component::{ComponentEncoder, DecodedWasm};
use wit_parser::Resolve;

/// The only interface a service component may export.
pub const HANDLER: &str = "merezha:service/handler@0.1.0";

/// Same per-component bound as the host's loader, applied to the core
/// module and to the component.
pub const MAX_COMPONENT_BYTES: usize = 16 * 1024 * 1024;

/// Longest reason an [`Error`] carries; longer ones are cut.
pub const MAX_REASON_BYTES: usize = 2048;

/// The canonical service WIT, owned by the host contract.
const CANONICAL_WIT: &str = include_str!("../../../wit/service.wit");

/// What failed, as a stable code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The runtime manifest cannot be read or is invalid.
    InvalidManifest,
    /// A service path is not `components/<id>.wasm`.
    UnsupportedComponentPath,
    /// The `wasm32-unknown-unknown` standard library is not installed.
    MissingWasmTarget,
    /// No workspace package has the service's id as its name.
    UnknownPackage,
    /// The service's package has no `cdylib` library target.
    NoCdylib,
    /// `cargo metadata`, `cargo build` or `rustc` failed or could not run.
    CargoFailed,
    /// The core module cannot be read.
    ComponentSourceMissing,
    /// The core module or the component breaks the service contract.
    ComponentRejected,
    /// The component cannot be written into place.
    ComponentWriteFailed,
}
impl ErrorKind {
    /// The code reports carry.
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidManifest => "invalid_manifest",
            Self::UnsupportedComponentPath => "unsupported_component_path",
            Self::MissingWasmTarget => "missing_wasm_target",
            Self::UnknownPackage => "unknown_package",
            Self::NoCdylib => "no_cdylib",
            Self::CargoFailed => "cargo_failed",
            Self::ComponentSourceMissing => "component_source_missing",
            Self::ComponentRejected => "component_rejected",
            Self::ComponentWriteFailed => "component_write_failed",
        }
    }
    /// The process exit code: 2 when the project is set up wrong before
    /// anything is built, 1 when a build or componentize step fails.
    pub fn exit_code(self) -> u8 {
        match self {
            Self::InvalidManifest
            | Self::UnsupportedComponentPath
            | Self::MissingWasmTarget
            | Self::UnknownPackage
            | Self::NoCdylib => 2,
            Self::CargoFailed
            | Self::ComponentSourceMissing
            | Self::ComponentRejected
            | Self::ComponentWriteFailed => 1,
        }
    }
}

/// A failed build or componentize step: its kind, the service it concerns
/// when there is one, and a reason for people. Reasons name paths, Cargo
/// packages and WIT items, never file contents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,
    pub service: Option<String>,
    pub reason: String,
}
impl Error {
    pub fn new(kind: ErrorKind, reason: impl Into<String>) -> Self {
        let mut reason = reason.into();
        if reason.len() > MAX_REASON_BYTES {
            let mut end = MAX_REASON_BYTES;
            while !reason.is_char_boundary(end) {
                end -= 1;
            }
            reason.truncate(end);
            reason.push('…');
        }
        Self {
            kind,
            service: None,
            reason,
        }
    }
    /// Names the service the error concerns.
    #[must_use]
    pub fn for_service(mut self, id: impl Into<String>) -> Self {
        self.service = Some(id.into());
        self
    }
    pub fn code(&self) -> &'static str {
        self.kind.code()
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.service {
            Some(service) => write!(f, "{service}: {}", self.reason),
            None => f.write_str(&self.reason),
        }
    }
}
impl std::error::Error for Error {}

/// Encodes `module` as a component and checks it with [`check`].
///
/// # Errors
///
/// Fails when `module` or the component exceeds [`MAX_COMPONENT_BYTES`],
/// carries no usable component type, cannot be encoded, or the component
/// breaks the service contract. The message chains the causes.
pub fn componentize(module: &[u8]) -> anyhow::Result<Vec<u8>> {
    ensure!(
        module.len() <= MAX_COMPONENT_BYTES,
        "the core module is {} bytes; at most {MAX_COMPONENT_BYTES} are accepted",
        module.len()
    );
    let component = ComponentEncoder::default()
        .validate(true)
        .module(module)
        .context("the core module has no usable component type metadata")?
        .encode()
        .context("the core module cannot be encoded as a component")?;
    check(&component).context("the component does not match the service contract")?;
    ensure!(
        component.len() <= MAX_COMPONENT_BYTES,
        "the component is {} bytes; the host accepts at most {MAX_COMPONENT_BYTES}",
        component.len()
    );
    Ok(component)
}

/// Checks that `component` imports nothing, exports only [`HANDLER`], and
/// that its export has exactly the canonical types.
///
/// # Errors
///
/// Names the offending imports or exports, or reports the type mismatch.
pub fn check(component: &[u8]) -> anyhow::Result<()> {
    let DecodedWasm::Component(resolve, world) = wit_component::decode(component)? else {
        bail!("expected a component, found a WIT package");
    };
    let world = &resolve.worlds[world];
    let imports: Vec<String> = world
        .imports
        .keys()
        .map(|key| resolve.name_world_key(key))
        .collect();
    ensure!(
        imports.is_empty(),
        "it imports {}; services must import nothing",
        imports.join(", ")
    );
    let exports: Vec<String> = world
        .exports
        .keys()
        .map(|key| resolve.name_world_key(key))
        .collect();
    ensure!(
        exports == [HANDLER],
        "it exports [{}]; services must export exactly {HANDLER}",
        exports.join(", ")
    );

    let mut canonical = Resolve::default();
    let package = canonical
        .push_source("wit/service.wit", CANONICAL_WIT)
        .context("cannot parse the canonical wit/service.wit")?;
    let service = canonical.select_world(&[package], Some("service"))?;
    wit_component::targets(&canonical, service, component)
        .with_context(|| format!("its {HANDLER} export does not have the canonical types"))
}

/// Componentizes the core module at `input` into `output` and returns the
/// component's size. The component is written to `<output>.partial` and
/// renamed, so `output` is either the previous file or the new component.
///
/// # Errors
///
/// [`ErrorKind::ComponentSourceMissing`] when `input` cannot be read,
/// [`ErrorKind::ComponentRejected`] when [`componentize`] refuses it (an
/// input over [`MAX_COMPONENT_BYTES`] is refused without being read whole),
/// [`ErrorKind::ComponentWriteFailed`] when the component cannot be written.
pub fn componentize_file(input: &Path, output: &Path) -> Result<usize, Error> {
    let staged = stage(input, output)?;
    let bytes = staged.bytes;
    staged.commit()?;
    Ok(bytes)
}

/// A component written next to its destination, not yet renamed into
/// place. Dropping it removes the partial file.
#[derive(Debug)]
struct Staged {
    partial: Option<PathBuf>,
    output: PathBuf,
    bytes: usize,
}
impl Staged {
    fn commit(mut self) -> Result<(), Error> {
        let Some(partial) = self.partial.take() else {
            return Ok(());
        };
        fs::rename(&partial, &self.output).map_err(|error| {
            let _ = fs::remove_file(&partial);
            Error::new(
                ErrorKind::ComponentWriteFailed,
                format!("cannot move {} into place: {error}", self.output.display()),
            )
        })
    }
}
impl Drop for Staged {
    fn drop(&mut self) {
        if let Some(partial) = self.partial.take() {
            let _ = fs::remove_file(partial);
        }
    }
}

/// Componentizes `input` into `<output>.partial`.
fn stage(input: &Path, output: &Path) -> Result<Staged, Error> {
    let module = read_core_module(input)?;
    let component = componentize(&module).map_err(|error| {
        Error::new(
            ErrorKind::ComponentRejected,
            format!("{}: {error:#}", input.display()),
        )
    })?;
    let mut partial = output.as_os_str().to_owned();
    partial.push(".partial");
    let partial = PathBuf::from(partial);
    let staged = Staged {
        partial: Some(partial.clone()),
        output: output.to_owned(),
        bytes: component.len(),
    };
    fs::write(&partial, &component).map_err(|error| {
        Error::new(
            ErrorKind::ComponentWriteFailed,
            format!("cannot write {}: {error}", partial.display()),
        )
    })?;
    Ok(staged)
}

/// Reads at most [`MAX_COMPONENT_BYTES`] of `path`; a longer file is
/// refused without reading the rest.
fn read_core_module(path: &Path) -> Result<Vec<u8>, Error> {
    let missing = |error: io::Error| {
        Error::new(
            ErrorKind::ComponentSourceMissing,
            format!("cannot read {}: {error}", path.display()),
        )
    };
    let file = File::open(path).map_err(missing)?;
    let mut module = Vec::new();
    file.take(MAX_COMPONENT_BYTES as u64 + 1)
        .read_to_end(&mut module)
        .map_err(missing)?;
    if module.len() > MAX_COMPONENT_BYTES {
        return Err(Error::new(
            ErrorKind::ComponentRejected,
            format!(
                "{} is over {MAX_COMPONENT_BYTES} bytes; at most {MAX_COMPONENT_BYTES} are accepted",
                path.display()
            ),
        ));
    }
    Ok(module)
}
