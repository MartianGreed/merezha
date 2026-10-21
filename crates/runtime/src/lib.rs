//! Component services with immutable generations and bounded execution.
//!
//! [`Runtime::load`] validates the entire catalog before accepting work.
//! Prepare an invocation before receiving an HTTP body to reserve admission.
mod bounded;
mod execution;
pub mod manifest;

use execution::{StoreState, Ticker, create_store, handle};
use manifest::{Limits, MAX_COMPONENT_BYTES, MAX_MANIFEST_BYTES, MAX_REGISTRY_BYTES, Manifest};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use wasmparser::{CanonicalFunction, CanonicalOption, Parser, Payload};
use wasmtime::{
    Config, Engine,
    component::{Component, ComponentExportIndex, InstancePre, Linker},
};
use wasmtime::{Result, ensure, error::Context};

/// Stable failure classification. Display deliberately excludes guest data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Error {
    InvalidInput,
    NotFound,
    GuestError,
    InputLimit,
    OutputLimit,
    FuelExhausted,
    MemoryLimit,
    Timeout,
    Trap,
    Overloaded,
    Internal,
}
impl Error {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::NotFound => "not_found",
            Self::GuestError => "guest_error",
            Self::InputLimit => "input_limit",
            Self::OutputLimit => "output_limit",
            Self::FuelExhausted => "fuel_exhausted",
            Self::MemoryLimit => "memory_limit",
            Self::Timeout => "timeout",
            Self::Trap => "trap",
            Self::Overloaded => "overloaded",
            Self::Internal => "internal",
        }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for Error {}

struct Service {
    pre: InstancePre<StoreState>,
    function: ComponentExportIndex,
}
struct Registry {
    generation: u64,
    limits: Limits,
    services: BTreeMap<String, Service>,
    pipelines: BTreeMap<String, Vec<String>>,
}
struct Inner {
    engine: Engine,
    manifest: PathBuf,
    registry: RwLock<Arc<Registry>>,
    reload: Mutex<()>,
    active: AtomicUsize,
    _ticker: Ticker,
}
/// Clones share the current catalog and process-wide admission counter.
#[derive(Clone)]
pub struct Runtime(Arc<Inner>);
#[derive(Debug, Serialize)]
pub struct Catalog {
    pub generation: u64,
    pub services: Vec<String>,
    pub pipelines: Vec<String>,
}
#[derive(Debug)]
pub struct Output {
    pub generation: u64,
    pub output: String,
}

/// An admitted invocation owns a stable registry snapshot and a deadline.
/// Dropping it releases its process-wide concurrency slot.
pub struct Invocation {
    inner: Arc<Inner>,
    registry: Arc<Registry>,
    steps: Vec<String>,
    deadline: Instant,
}
impl Runtime {
    /// Compile portable components and validate imports, ABI and instantiation.
    /// Returns an error without exposing a partially initialized runtime.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().canonicalize().context("manifest path")?;
        let mut config = Config::new();
        config
            .wasm_component_model(true)
            .consume_fuel(true)
            .epoch_interruption(true)
            .wasm_memory64(false)
            .wasm_component_model_memory64(false)
            .max_wasm_stack(512 * 1024);
        let engine = Engine::new(&config)?;
        let ticker = Ticker::start(engine.clone())?;
        let registry = Arc::new(load_registry(&engine, &path, 1)?);
        Ok(Self(Arc::new(Inner {
            engine,
            manifest: path,
            registry: RwLock::new(registry),
            reload: Mutex::new(()),
            active: AtomicUsize::new(0),
            _ticker: ticker,
        })))
    }
    /// Serialize the entire reload transaction. Failure preserves the previous
    /// generation. Requests already admitted retain their original snapshot.
    pub fn reload(&self) -> Result<u64> {
        let _load = self
            .0
            .reload
            .lock()
            .map_err(|_| wasmtime::format_err!("reload lock poisoned"))?;
        let generation = self
            .snapshot()?
            .generation
            .checked_add(1)
            .context("generation overflow")?;
        let candidate = Arc::new(load_registry(&self.0.engine, &self.0.manifest, generation)?);
        *self
            .0
            .registry
            .write()
            .map_err(|_| wasmtime::format_err!("registry lock poisoned"))? = candidate;
        Ok(generation)
    }
    pub fn catalog(&self) -> Result<Catalog, Error> {
        let registry = self.snapshot()?;
        Ok(Catalog {
            generation: registry.generation,
            services: registry.services.keys().cloned().collect(),
            pipelines: registry.pipelines.keys().cloned().collect(),
        })
    }
    fn snapshot(&self) -> Result<Arc<Registry>, Error> {
        self.0
            .registry
            .read()
            .map(|r| Arc::clone(&r))
            .map_err(|_| Error::Internal)
    }
    pub fn prepare_service(&self, id: &str) -> Result<Invocation, Error> {
        let registry = self.snapshot()?;
        if !registry.services.contains_key(id) {
            return Err(Error::NotFound);
        }
        self.prepare(registry, vec![id.to_owned()])
    }
    pub fn prepare_pipeline(&self, id: &str) -> Result<Invocation, Error> {
        let registry = self.snapshot()?;
        let steps = registry.pipelines.get(id).ok_or(Error::NotFound)?.clone();
        self.prepare(registry, steps)
    }
    fn prepare(&self, registry: Arc<Registry>, steps: Vec<String>) -> Result<Invocation, Error> {
        self.0
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < registry.limits.max_concurrent_invocations).then_some(active + 1)
            })
            .map_err(|_| Error::Overloaded)?;
        let deadline = Instant::now() + Duration::from_millis(registry.limits.timeout_ms);
        Ok(Invocation {
            inner: Arc::clone(&self.0),
            registry,
            steps,
            deadline,
        })
    }
    pub fn invoke_service(&self, id: &str, input: &str) -> Result<Output, Error> {
        self.prepare_service(id)?.run(input)
    }
    pub fn invoke_pipeline(&self, id: &str, input: &str) -> Result<Output, Error> {
        self.prepare_pipeline(id)?.run(input)
    }
}
impl Invocation {
    pub fn limits(&self) -> &Limits {
        &self.registry.limits
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn generation(&self) -> u64 {
        self.registry.generation
    }
    pub fn run(self, input: &str) -> Result<Output, Error> {
        let limits = &self.registry.limits;
        if input.len() > limits.max_input_bytes {
            return Err(Error::InputLimit);
        }
        let mut value = input.to_owned();
        let mut fuel = limits.fuel;
        for id in &self.steps {
            if Instant::now() >= self.deadline {
                return Err(Error::Timeout);
            }
            if value.len() > limits.max_input_bytes {
                return Err(Error::InputLimit);
            }
            let service = &self.registry.services[id];
            let mut store = create_store(&self.inner.engine, limits, fuel, self.deadline)
                .map_err(|_| Error::Internal)?;
            value = execution::call(&mut store, service, &value, limits.max_output_bytes)?;
            fuel = store.get_fuel().map_err(|_| Error::Internal)?;
        }
        if Instant::now() >= self.deadline {
            return Err(Error::Timeout);
        }
        Ok(Output {
            generation: self.registry.generation,
            output: value,
        })
    }
}
impl Drop for Invocation {
    fn drop(&mut self) {
        self.inner.active.fetch_sub(1, Ordering::AcqRel);
    }
}
fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    // Do not intentionally open FIFOs/devices. Local administration assumes no
    // hostile concurrent filesystem path swaps between these checks.
    ensure!(
        path.metadata()?.is_file(),
        "artifact must be a regular file"
    );
    let file = File::open(path)?;
    ensure!(
        file.metadata()?.is_file() && file.metadata()?.len() <= maximum as u64,
        "artifact byte limit"
    );
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= maximum, "artifact byte limit");
    Ok(bytes)
}
fn validate_encoding(bytes: &[u8]) -> Result<()> {
    ensure!(
        bytes.starts_with(b"\0asm\x0d\0\x01\0"),
        "expected portable binary component"
    );
    // WasmStr borrows UTF-8. Refuse transcoding so guest lengths cannot cause a
    // host String allocation before the output byte limit is checked.
    for payload in Parser::new(0).parse_all(bytes) {
        if let Payload::ComponentCanonicalSection(section) = payload? {
            for function in section {
                if let CanonicalFunction::Lift { options, .. }
                | CanonicalFunction::Lower { options, .. } = function?
                {
                    ensure!(
                        !options.iter().any(|o| matches!(
                            o,
                            CanonicalOption::UTF16
                                | CanonicalOption::CompactUTF16
                                | CanonicalOption::Gc
                        )),
                        "only UTF-8 linear canonical ABI is supported"
                    );
                }
            }
        }
    }
    Ok(())
}
fn load_registry(engine: &Engine, path: &Path, generation: u64) -> Result<Registry> {
    let bytes = read_bounded(path, MAX_MANIFEST_BYTES)?;
    let manifest = Manifest::parse(std::str::from_utf8(&bytes)?)?;
    let root = path.parent().context("manifest parent")?;
    let mut services = BTreeMap::new();
    let mut total = 0usize;
    for spec in manifest.services {
        let artifact = root
            .join(&spec.path)
            .canonicalize()
            .context("component path")?;
        ensure!(
            artifact.starts_with(root),
            "component escapes manifest directory"
        );
        let bytes = read_bounded(&artifact, MAX_COMPONENT_BYTES)?;
        total = total
            .checked_add(bytes.len())
            .context("registry byte overflow")?;
        ensure!(total <= MAX_REGISTRY_BYTES, "registry byte limit");
        validate_encoding(&bytes)?;
        let component = Component::new(engine, bytes).context("component compilation")?;
        ensure!(
            component.component_type().imports(engine).next().is_none(),
            "component imports are forbidden"
        );
        let interface = component
            .get_export_index(None, "merezha:service/handler@0.1.0")
            .context("missing handler interface")?;
        let function = component
            .get_export_index(Some(&interface), "handle")
            .context("missing handle")?;
        let pre = Linker::<StoreState>::new(engine).instantiate_pre(&component)?;
        let service = Service { pre, function };
        let deadline = Instant::now() + Duration::from_millis(manifest.limits.timeout_ms);
        let mut store = create_store(engine, &manifest.limits, manifest.limits.fuel, deadline)?;
        let instance = service
            .pre
            .instantiate(&mut store)
            .context("component instantiation")?;
        handle(&mut store, instance, &service.function).context("handler ABI mismatch")?;
        ensure!(Instant::now() < deadline, "component instantiation timeout");
        services.insert(spec.id, service);
    }
    Ok(Registry {
        generation,
        limits: manifest.limits,
        services,
        pipelines: manifest
            .pipelines
            .into_iter()
            .map(|p| (p.id, p.steps))
            .collect(),
    })
}
