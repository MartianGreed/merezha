//! Component services with immutable generations and bounded execution.
//!
//! [`Runtime::load`] validates the entire catalog before accepting work.
//! Prepare an invocation before receiving an HTTP body to reserve admission.
//!
//! Guests get no telemetry API; the host observes them from outside (TE
//! §2.4). Every guest call runs in a `guest {service} {operation}` span
//! with fuel, peak memory, input and output bytes, the outcome and the
//! catalog generation, and feeds the `merezha.guest.*` instruments built
//! from the [`Meters`] given to [`Runtime::load_with_meters`].
mod bounded;
mod execution;
pub mod manifest;
mod metrics;

use merezha_telemetry::Meters;
pub use merezha_telemetry::names::dispatch::GuestOperation;
pub use metrics::SCOPE;
use metrics::{Labels, Metrics, PipelineLabels};

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
        atomic::{AtomicU64, AtomicUsize, Ordering},
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
    labels: Labels,
}
struct Pipeline {
    steps: Vec<String>,
    labels: PipelineLabels,
}
struct Registry {
    generation: u64,
    limits: Limits,
    services: BTreeMap<String, Service>,
    pipelines: BTreeMap<String, Pipeline>,
}
struct Inner {
    engine: Engine,
    manifest: PathBuf,
    registry: RwLock<Arc<Registry>>,
    reload: Mutex<()>,
    active: AtomicUsize,
    /// The least `max_concurrent_invocations` a reload may install.
    floor: AtomicUsize,
    /// The active generation, read by the `merezha.catalog.generation`
    /// gauge without touching the registry lock.
    generation: Arc<AtomicU64>,
    metrics: Metrics,
    _ticker: Ticker,
}
/// Clones share the current catalog and admission counter.
#[derive(Clone)]
pub struct Runtime(Arc<Inner>);
/// A catalog generation pinned across a multi-call application dispatch.
#[derive(Clone)]
pub struct Session {
    runtime: Runtime,
    registry: Arc<Registry>,
}
impl Session {
    /// A raw call ([`GuestOperation::Raw`]) bounded by `deadline` too.
    pub fn invoke_service_until(
        &self,
        id: &str,
        input: &str,
        deadline: Instant,
    ) -> Result<Output, Error> {
        self.invoke_operation_until(id, GuestOperation::Raw, input, deadline)
    }
    /// Call service `id` for `operation` (the span's and the metrics'
    /// `merezha.guest.operation`), bounded by the generation's timeout and
    /// by `deadline`.
    pub fn invoke_operation_until(
        &self,
        id: &str,
        operation: GuestOperation,
        input: &str,
        deadline: Instant,
    ) -> Result<Output, Error> {
        if !self.registry.services.contains_key(id) {
            self.runtime.0.metrics.refused(
                None,
                self.registry.generation,
                operation,
                Error::NotFound,
            );
            return Err(Error::NotFound);
        }
        let mut invocation = self.runtime.prepare(
            Arc::clone(&self.registry),
            vec![id.to_owned()],
            operation,
            None,
        )?;
        invocation.deadline = invocation.deadline.min(deadline);
        invocation.run(input)
    }
    pub fn invoke_service(&self, id: &str, input: &str) -> Result<Output, Error> {
        self.invoke_operation_until(id, GuestOperation::Raw, input, Instant::now() + MAX_WAIT)
    }
}
/// Longer than any generation's `timeout_ms`: the generation's own deadline
/// always comes first.
const MAX_WAIT: Duration = Duration::from_secs(24 * 3600);
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
    /// Fuel the whole invocation consumed: the limit minus what remained,
    /// summed over a pipeline's steps, which share one budget.
    pub fuel_consumed: u64,
}

/// An admitted invocation owns a stable registry snapshot and a deadline.
/// Dropping it releases its concurrency slot shared by runtime clones.
pub struct Invocation {
    inner: Arc<Inner>,
    registry: Arc<Registry>,
    steps: Vec<String>,
    deadline: Instant,
    operation: GuestOperation,
    /// The pipeline's labels, for the `pipeline {id}` span around the steps.
    pipeline: Option<PipelineLabels>,
}
impl Runtime {
    pub fn session(&self) -> Result<Session, Error> {
        Ok(Session {
            runtime: self.clone(),
            registry: self.snapshot()?,
        })
    }
    /// Compile portable components and validate imports, ABI and instantiation.
    /// Returns an error without exposing a partially initialized runtime.
    /// It records spans but no metrics; see [`Runtime::load_with_meters`].
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        Self::load_with_meters(path, &Meters::noop())
    }
    /// [`Runtime::load`] with the `merezha.guest.*` and `merezha.catalog.*`
    /// instruments built once from `meters` (scope [`SCOPE`], TE §1.3).
    /// With disabled meters no instrument is built.
    pub fn load_with_meters(path: impl AsRef<Path>, meters: &Meters) -> Result<Self> {
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
        let generation = Arc::new(AtomicU64::new(registry.generation));
        Ok(Self(Arc::new(Inner {
            engine,
            manifest: path,
            registry: RwLock::new(registry),
            reload: Mutex::new(()),
            active: AtomicUsize::new(0),
            floor: AtomicUsize::new(0),
            metrics: Metrics::new(meters, generation.clone()),
            generation,
            _ticker: ticker,
        })))
    }
    /// Serialize the entire reload transaction. Failure preserves the previous
    /// generation. Requests already admitted retain their original snapshot.
    pub fn reload(&self) -> Result<u64> {
        let result = self.reload_locked();
        self.0.metrics.reloaded(result.is_ok());
        result
    }
    fn reload_locked(&self) -> Result<u64> {
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
        ensure!(
            candidate.limits.max_concurrent_invocations >= self.0.floor.load(Ordering::SeqCst),
            "max_concurrent_invocations below the host's concurrency budget"
        );
        *self
            .0
            .registry
            .write()
            .map_err(|_| wasmtime::format_err!("registry lock poisoned"))? = candidate;
        self.0.generation.store(generation, Ordering::SeqCst);
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
    /// Refuse later reloads whose `max_concurrent_invocations` is below
    /// `minimum`, as an embedding host that budgets guest calls requires.
    /// Fails when the active generation is already below it.
    pub fn require_concurrency(&self, minimum: usize) -> Result<(), Error> {
        let _load = self.0.reload.lock().map_err(|_| Error::Internal)?;
        if self.snapshot()?.limits.max_concurrent_invocations < minimum {
            return Err(Error::Overloaded);
        }
        self.0.floor.store(minimum, Ordering::SeqCst);
        Ok(())
    }
    /// The limits of the active generation. A reload may change them.
    pub fn limits(&self) -> Result<Limits, Error> {
        Ok(self.snapshot()?.limits.clone())
    }
    fn snapshot(&self) -> Result<Arc<Registry>, Error> {
        self.0
            .registry
            .read()
            .map(|r| Arc::clone(&r))
            .map_err(|_| Error::Internal)
    }
    /// Admit a raw call of service `id`.
    pub fn prepare_service(&self, id: &str) -> Result<Invocation, Error> {
        let registry = self.snapshot()?;
        if !registry.services.contains_key(id) {
            self.0.metrics.refused(
                None,
                registry.generation,
                GuestOperation::Raw,
                Error::NotFound,
            );
            return Err(Error::NotFound);
        }
        self.prepare(registry, vec![id.to_owned()], GuestOperation::Raw, None)
    }
    /// Admit a raw pipeline: its steps run in one `pipeline {id}` span.
    pub fn prepare_pipeline(&self, id: &str) -> Result<Invocation, Error> {
        let registry = self.snapshot()?;
        let Some(pipeline) = registry.pipelines.get(id) else {
            self.0.metrics.refused(
                None,
                registry.generation,
                GuestOperation::Raw,
                Error::NotFound,
            );
            return Err(Error::NotFound);
        };
        let (steps, labels) = (pipeline.steps.clone(), pipeline.labels.clone());
        self.prepare(registry, steps, GuestOperation::Raw, Some(labels))
    }
    fn prepare(
        &self,
        registry: Arc<Registry>,
        steps: Vec<String>,
        operation: GuestOperation,
        pipeline: Option<PipelineLabels>,
    ) -> Result<Invocation, Error> {
        if self
            .0
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < registry.limits.max_concurrent_invocations).then_some(active + 1)
            })
            .is_err()
        {
            let labels = steps
                .first()
                .and_then(|id| registry.services.get(id))
                .map(|service| &service.labels);
            self.0
                .metrics
                .refused(labels, registry.generation, operation, Error::Overloaded);
            return Err(Error::Overloaded);
        }
        self.0.metrics.active(1);
        let deadline = Instant::now() + Duration::from_millis(registry.limits.timeout_ms);
        Ok(Invocation {
            inner: Arc::clone(&self.0),
            registry,
            steps,
            deadline,
            operation,
            pipeline,
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
    /// Run every step in order, each in its `guest {service} {operation}`
    /// span; a pipeline's steps share one fuel budget and one deadline.
    pub fn run(self, input: &str) -> Result<Output, Error> {
        let pipeline = self
            .pipeline
            .as_ref()
            .map(|labels| metrics::pipeline_span(labels, self.registry.generation));
        let _entered = pipeline.as_ref().map(tracing::Span::enter);
        let result = self.run_steps(input);
        if let Some(span) = &pipeline {
            metrics::settle_span(span, result.as_ref().map(|_| ()).map_err(|e| *e));
        }
        result
    }
    fn run_steps(&self, input: &str) -> Result<Output, Error> {
        let limits = &self.registry.limits;
        if self.steps.is_empty() && input.len() > limits.max_input_bytes {
            return Err(Error::InputLimit);
        }
        let mut value = input.to_owned();
        let mut fuel = limits.fuel;
        for id in &self.steps {
            let (result, remaining) = self.step(id, &value, fuel);
            value = result?;
            fuel = remaining;
        }
        if Instant::now() >= self.deadline {
            return Err(Error::Timeout);
        }
        Ok(Output {
            generation: self.registry.generation,
            output: value,
            fuel_consumed: limits.fuel - fuel,
        })
    }
    /// One guest call with `fuel` left of the budget, inside its span,
    /// instantiation included. Returns the output and the fuel left.
    fn step(&self, id: &str, input: &str, fuel: u64) -> (Result<String, Error>, u64) {
        let limits = &self.registry.limits;
        let service = &self.registry.services[id];
        let mut call = self.inner.metrics.call(
            &service.labels,
            self.operation,
            self.registry.generation,
            limits,
            fuel,
            input.len(),
        );
        let _entered = call.span().clone().entered();
        let result = if Instant::now() >= self.deadline {
            Err(Error::Timeout)
        } else if input.len() > limits.max_input_bytes {
            Err(Error::InputLimit)
        } else {
            match create_store(&self.inner.engine, limits, fuel, self.deadline) {
                Err(_) => Err(Error::Internal),
                Ok(mut store) => {
                    let output =
                        execution::call(&mut store, service, input, limits.max_output_bytes);
                    // Fuel is readable after a trap too; an exhausted
                    // budget reads 0.
                    let remaining = store.get_fuel().unwrap_or(0).min(fuel);
                    call.ran(fuel - remaining, store.data().peak_memory);
                    output
                }
            }
        };
        let remaining = fuel - call.consumed();
        call.finish(result.as_ref().map(String::len).map_err(|e| *e));
        (result, remaining)
    }
}
impl Drop for Invocation {
    fn drop(&mut self) {
        self.inner.active.fetch_sub(1, Ordering::AcqRel);
        self.inner.metrics.active(-1);
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
        let service = Service {
            pre,
            function,
            labels: Labels::service(&spec.id),
        };
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
            .map(|p| {
                let labels = PipelineLabels::new(&p.id);
                (
                    p.id,
                    Pipeline {
                        steps: p.steps,
                        labels,
                    },
                )
            })
            .collect(),
    })
}
