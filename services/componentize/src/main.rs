//! Turns a service's `wasm32-unknown-unknown` core module into a component
//! and checks it against the Merezha service contract.
//!
//! Usage: `merezha-componentize <core-module.wasm> <component.wasm>`
//!
//! The core module carries the component type that `wit-bindgen` embedded
//! for world `merezha:service/service@0.1.0`. The produced component must
//! import nothing and export exactly `merezha:service/handler@0.1.0`, which
//! is what the host accepts. Any other world fails the build here instead of
//! at manifest load.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail, ensure};
use wit_component::{ComponentEncoder, DecodedWasm};

const HANDLER: &str = "merezha:service/handler@0.1.0";

/// Same per-component bound as the host's loader.
const MAX_COMPONENT_BYTES: u64 = 16 * 1024 * 1024;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("merezha-componentize: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let (Some(input), Some(output), None) = (args.next(), args.next(), args.next()) else {
        bail!("usage: merezha-componentize <core-module.wasm> <component.wasm>");
    };
    let (input, output) = (PathBuf::from(input), PathBuf::from(output));

    let module = read_bounded(&input)?;
    let component = ComponentEncoder::default()
        .validate(true)
        .module(&module)
        .with_context(|| format!("{} has no usable component type metadata", input.display()))?
        .encode()
        .with_context(|| format!("cannot encode {} as a component", input.display()))?;
    check_world(&component)
        .with_context(|| format!("{} does not match the service contract", input.display()))?;
    ensure!(
        component.len() as u64 <= MAX_COMPONENT_BYTES,
        "component is {} bytes; the host accepts at most {MAX_COMPONENT_BYTES}",
        component.len()
    );
    write_atomically(&output, &component)
}

fn read_bounded(path: &Path) -> Result<Vec<u8>> {
    let size = fs::metadata(path)
        .with_context(|| format!("cannot read {}", path.display()))?
        .len();
    ensure!(
        size <= MAX_COMPONENT_BYTES,
        "{} is {size} bytes; at most {MAX_COMPONENT_BYTES} are accepted",
        path.display()
    );
    fs::read(path).with_context(|| format!("cannot read {}", path.display()))
}

fn check_world(component: &[u8]) -> Result<()> {
    let DecodedWasm::Component(resolve, world) = wit_component::decode(component)? else {
        bail!("encoding produced a WIT package instead of a component");
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
    Ok(())
}

fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut staging = path.as_os_str().to_owned();
    staging.push(".partial");
    let staging = PathBuf::from(staging);
    fs::write(&staging, bytes).with_context(|| format!("cannot write {}", staging.display()))?;
    fs::rename(&staging, path).with_context(|| format!("cannot move {} into place", path.display()))
}
