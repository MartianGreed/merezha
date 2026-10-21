//! Turns a service's `wasm32-unknown-unknown` core module into a component
//! and checks it against the Merezha service contract.
//!
//! The core module carries the component type that `wit-bindgen` embedded
//! for world `merezha:service/service@0.1.0`. The produced component must
//! import nothing and export exactly `merezha:service/handler@0.1.0`, with
//! the types of the canonical `wit/service.wit`, which is what the host
//! accepts. Any other world fails the build here instead of at manifest load.

use anyhow::{Context, Result, bail, ensure};
use wit_component::{ComponentEncoder, DecodedWasm};
use wit_parser::Resolve;

/// The only interface a service component may export.
pub const HANDLER: &str = "merezha:service/handler@0.1.0";

/// Same per-component bound as the host's loader, applied to input and output.
pub const MAX_COMPONENT_BYTES: usize = 16 * 1024 * 1024;

/// The canonical service WIT, owned by the host contract.
const CANONICAL_WIT: &str = include_str!("../../../wit/service.wit");

/// Encodes `module` as a component and checks it with [`check`].
///
/// # Errors
///
/// Fails when `module` or the component exceeds [`MAX_COMPONENT_BYTES`],
/// carries no usable component type, cannot be encoded, or the component
/// breaks the service contract.
pub fn componentize(module: &[u8]) -> Result<Vec<u8>> {
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
pub fn check(component: &[u8]) -> Result<()> {
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
    wit_component::targets(&canonical, service, component, false)
        .with_context(|| format!("its {HANDLER} export does not have the canonical types"))
}
