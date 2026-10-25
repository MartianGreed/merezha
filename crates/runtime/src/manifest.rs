use serde::Deserialize;
use std::{
    collections::BTreeSet,
    path::{Component, PathBuf},
};
use wasmtime::{Result, ensure};

pub const MAX_MANIFEST_BYTES: usize = 65_536;
pub const MAX_COMPONENT_BYTES: usize = 16_777_216;
pub const MAX_REGISTRY_BYTES: usize = 67_108_864;
pub const MAX_PAYLOAD_BYTES: usize = 1_048_576;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub memory_bytes: usize,
    pub fuel: u64,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub timeout_ms: u64,
    pub max_concurrent_invocations: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub limits: Limits,
    #[serde(default)]
    pub services: Vec<Service>,
    #[serde(default)]
    pub pipelines: Vec<Pipeline>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Service {
    pub id: String,
    pub path: PathBuf,
    /// The application protocol the service speaks (AP §C.1): 1 (default)
    /// or 2. The runtime only records it; the application host interprets
    /// payloads.
    #[serde(default = "default_protocol")]
    pub application_protocol: u16,
}
fn default_protocol() -> u16 {
    1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pipeline {
    pub id: String,
    pub steps: Vec<String>,
}

impl Manifest {
    pub fn parse(text: &str) -> Result<Self> {
        ensure!(text.len() <= MAX_MANIFEST_BYTES, "manifest byte limit");
        let manifest: Self = toml::from_str(text)?;
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<()> {
        ensure!(self.version == 1, "unsupported manifest version");
        let l = &self.limits;
        ensure!(
            (65_536..=268_435_456).contains(&l.memory_bytes)
                && l.memory_bytes.is_multiple_of(65_536),
            "invalid memory_bytes"
        );
        ensure!((1..=1_000_000_000).contains(&l.fuel), "invalid fuel");
        ensure!(
            (1..=MAX_PAYLOAD_BYTES).contains(&l.max_input_bytes),
            "invalid max_input_bytes"
        );
        ensure!(
            (1..=MAX_PAYLOAD_BYTES).contains(&l.max_output_bytes),
            "invalid max_output_bytes"
        );
        ensure!((1..=30_000).contains(&l.timeout_ms), "invalid timeout_ms");
        ensure!(
            (1..=64).contains(&l.max_concurrent_invocations),
            "invalid max_concurrent_invocations"
        );
        ensure!(
            self.services.len() <= 64 && self.pipelines.len() <= 64,
            "registry count limit"
        );
        let mut services = BTreeSet::new();
        for service in &self.services {
            ensure!(
                valid_id(&service.id) && services.insert(&service.id),
                "invalid or duplicate service id"
            );
            ensure!(
                matches!(service.application_protocol, 1 | 2),
                "invalid application_protocol"
            );
            ensure!(
                !service.path.as_os_str().is_empty()
                    && service
                        .path
                        .components()
                        .all(|c| matches!(c, Component::Normal(_) | Component::CurDir)),
                "component path must be relative without traversal"
            );
        }
        let mut pipelines = BTreeSet::new();
        for pipeline in &self.pipelines {
            ensure!(
                valid_id(&pipeline.id) && pipelines.insert(&pipeline.id),
                "invalid or duplicate pipeline id"
            );
            ensure!(
                (1..=16).contains(&pipeline.steps.len()),
                "invalid pipeline step count"
            );
            ensure!(
                pipeline.steps.iter().all(|s| services.contains(s)),
                "pipeline references missing service"
            );
        }
        Ok(())
    }
}

pub fn valid_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
}
