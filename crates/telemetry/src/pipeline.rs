//! The OTLP pipeline (TE §3, §4): providers, guarded processors, counting
//! exporters, the subscriber and the shutdown flush. Only the `merezha`
//! binary builds it.
//!
//! - Requests never wait on telemetry: span end and log emit admit items
//!   against `pending` and enqueue without blocking; metrics aggregate in
//!   memory; every exporter runs on an SDK-owned thread.
//! - Queues are bounded and drops exact: see [`crate::status`]. The inner
//!   SDK queue is `max_queue + max_batch` long, so it never drops silently.
//! - With `signals = []` no provider, exporter or SDK thread exists; the
//!   stderr writer is the only telemetry thread.
//! - The binary scrubs `OTEL_*` variables before any thread starts
//!   ([`scrub_otel_environment`]), because the 0.33 exporters merge
//!   `OTEL_EXPORTER_OTLP_HEADERS` into programmatic headers.
use crate::{
    allowlist::{Allowlist, AllowlistSpanProcessor},
    config::{
        Compression, LogFormat, LogLevel, Protocol, Service, Signal, TelemetryConfig, Temporality,
    },
    json::JsonLayer,
    names::{self, TelemetrySignal, attr, key},
    status::{Admission, SignalAccount, SignalCounts, TelemetryStatus, failure_code},
    subscriber::{BoxedLayer, LogBridge, TargetFilter, span_bridge},
};
use merezha_domain::Failure;
use opentelemetry::{
    Context, InstrumentationScope, KeyValue,
    logs::LoggerProvider as _,
    metrics::{Meter, MeterProvider as _},
    trace::TracerProvider as _,
};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    logs::{
        BatchLogProcessor, LogBatch, LogExporter, LogProcessor, SdkLogRecord, SdkLoggerProvider,
    },
    metrics::{
        PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData, ResourceMetrics},
        exporter::PushMetricExporter,
    },
    trace::{
        BatchSpanProcessor, Sampler, SdkTracerProvider, Span, SpanData, SpanExporter, SpanProcessor,
    },
};
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tracing::{Dispatch, level_filters::LevelFilter};
use tracing_appender::non_blocking::{ErrorCounter, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::{Layer, Registry, fmt::MakeWriter, layer::SubscriberExt};

/// Span limits (TE §1.5).
pub const MAX_SPAN_ATTRIBUTES: u32 = 64;
pub const MAX_SPAN_EVENTS: u32 = 32;
pub const MAX_SPAN_LINKS: u32 = 8;
/// Largest `ca_file` read.
pub const MAX_CA_BYTES: u64 = 1_048_576;
/// Instrumentation scope of the host's own signals.
pub const SCOPE: &str = "merezha";

fn level(level: LogLevel) -> LevelFilter {
    LevelFilter::from_level(level.as_tracing())
}

// ---------------------------------------------------------------------------
// Counting exporters and guarded processors
// ---------------------------------------------------------------------------

fn settle(account: &SignalAccount, items: u64, result: &OTelSdkResult, queued: bool) {
    let outcome = result.as_ref().map(|_| ()).map_err(failure_code);
    let due = if queued {
        account.settle(items, outcome)
    } else {
        account.record_export(items, outcome)
    };
    if due && let Err(code) = outcome {
        // At most once per signal per minute, stderr only (the catalog row
        // is not exported), so export errors never loop back.
        crate::event!(
            WARN,
            "telemetry.export_failed",
            signal = account.signal().as_str(),
            code = code,
            dropped = {
                let counts = account.counts();
                counts.dropped() + counts.failed
            }
        );
    }
}

/// Wraps an exporter and settles every finished batch in the signal's
/// account: exported or failed, and no longer pending. `Debug` names the
/// signal only, never the exporter (which holds header values).
pub struct Counting<E> {
    inner: E,
    account: Arc<SignalAccount>,
}
macro_rules! debug_by_signal {
    ($($ty:ident),*) => {$(
        impl<T> std::fmt::Debug for $ty<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($ty))
                    .field("signal", &self.account.signal())
                    .finish_non_exhaustive()
            }
        }
    )*};
}
debug_by_signal!(Counting, GuardedSpanProcessor, GuardedLogProcessor);

impl<E> Counting<E> {
    pub fn new(inner: E, account: Arc<SignalAccount>) -> Self {
        Self { inner, account }
    }
}
impl<E: SpanExporter> SpanExporter for Counting<E> {
    fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        let items = batch.len() as u64;
        let account = self.account.clone();
        let export = self.inner.export(batch);
        async move {
            let result = export.await;
            settle(&account, items, &result, true);
            result
        }
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}
impl<E: LogExporter> LogExporter for Counting<E> {
    fn export(&self, batch: LogBatch<'_>) -> impl Future<Output = OTelSdkResult> + Send {
        let items = batch.iter().count() as u64;
        let account = self.account.clone();
        let export = self.inner.export(batch);
        async move {
            let result = export.await;
            settle(&account, items, &result, true);
            result
        }
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

fn points<T>(data: &MetricData<T>) -> usize {
    match data {
        MetricData::Gauge(g) => g.data_points().count(),
        MetricData::Sum(s) => s.data_points().count(),
        MetricData::Histogram(h) => h.data_points().count(),
        MetricData::ExponentialHistogram(h) => h.data_points().count(),
    }
}
/// Data points in an export.
pub fn data_points(metrics: &ResourceMetrics) -> u64 {
    metrics
        .scope_metrics()
        .flat_map(|scope| scope.metrics())
        .map(|metric| match metric.data() {
            AggregatedMetrics::F64(d) => points(d),
            AggregatedMetrics::U64(d) => points(d),
            AggregatedMetrics::I64(d) => points(d),
        } as u64)
        .sum()
}
impl<E: PushMetricExporter> PushMetricExporter for Counting<E> {
    fn export(&self, metrics: &ResourceMetrics) -> impl Future<Output = OTelSdkResult> + Send {
        let items = data_points(metrics);
        let account = self.account.clone();
        let export = self.inner.export(metrics);
        async move {
            let result = export.await;
            settle(&account, items, &result, false);
            result
        }
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn temporality(&self) -> opentelemetry_sdk::metrics::Temporality {
        self.inner.temporality()
    }
}

/// Admits spans against the signal's bound before the inner (queueing)
/// processor sees them; refused spans count as `dropped{queue_full}` or,
/// after shutdown started, `dropped{shutdown}`.
pub struct GuardedSpanProcessor<P> {
    inner: P,
    account: Arc<SignalAccount>,
}
impl<P> GuardedSpanProcessor<P> {
    pub fn new(inner: P, account: Arc<SignalAccount>) -> Self {
        Self { inner, account }
    }
}
impl<P: SpanProcessor> SpanProcessor for GuardedSpanProcessor<P> {
    fn on_start(&self, span: &mut Span, cx: &Context) {
        self.inner.on_start(span, cx);
    }
    fn on_end(&self, span: SpanData) {
        if self.account.admit() == Admission::Accepted {
            self.inner.on_end(span);
        }
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

/// The log counterpart of [`GuardedSpanProcessor`].
pub struct GuardedLogProcessor<P> {
    inner: P,
    account: Arc<SignalAccount>,
}
impl<P> GuardedLogProcessor<P> {
    pub fn new(inner: P, account: Arc<SignalAccount>) -> Self {
        Self { inner, account }
    }
}
impl<P: LogProcessor> LogProcessor for GuardedLogProcessor<P> {
    fn emit(&self, record: &mut SdkLogRecord, scope: &InstrumentationScope) {
        if self.account.admit() == Admission::Accepted {
            self.inner.emit(record, scope);
        }
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

// ---------------------------------------------------------------------------
// Resource and exporters
// ---------------------------------------------------------------------------

/// The resource: `service.*` from [`Service`] (instance id from
/// `app.instance_id`), the environment and static attributes. No
/// environment detector runs.
pub fn resource(service: &Service) -> Resource {
    let mut attributes = vec![
        KeyValue::new(key::SERVICE_NAME, service.name.clone()),
        KeyValue::new(key::SERVICE_VERSION, service.version.clone()),
    ];
    if let Some(id) = &service.instance_id {
        attributes.push(KeyValue::new(key::SERVICE_INSTANCE_ID, id.clone()));
    }
    if let Some(namespace) = &service.namespace {
        attributes.push(KeyValue::new(key::SERVICE_NAMESPACE, namespace.clone()));
    }
    if let Some(environment) = &service.environment {
        attributes.push(KeyValue::new(
            key::DEPLOYMENT_ENVIRONMENT_NAME,
            environment.clone(),
        ));
    }
    for (k, v) in &service.attributes {
        attributes.push(KeyValue::new(k.clone(), v.clone()));
    }
    Resource::builder_empty()
        .with_attributes(attributes)
        .build()
}

fn read_ca(path: Option<&Path>) -> Result<Option<Vec<u8>>, Failure> {
    use std::io::Read;
    let Some(path) = path else {
        return Ok(None);
    };
    let invalid = || Failure::permanent("invalid_telemetry_ca");
    let file = std::fs::File::open(path).map_err(|_| invalid())?;
    if !file.metadata().map_err(|_| invalid())?.is_file() {
        return Err(invalid());
    }
    let mut pem = Vec::new();
    file.take(MAX_CA_BYTES + 1)
        .read_to_end(&mut pem)
        .map_err(|_| invalid())?;
    if pem.len() as u64 > MAX_CA_BYTES
        || reqwest::Certificate::from_pem_bundle(&pem).map_or(true, |c| c.is_empty())
    {
        return Err(invalid());
    }
    Ok(Some(pem))
}

/// Everything an exporter needs, resolved once.
struct Transport {
    protocol: Protocol,
    gzip: bool,
    timeout: Duration,
    headers: HashMap<String, String>,
    /// The blocking client for OTLP/HTTP, built on a helper thread so it is
    /// never created or dropped inside the async runtime.
    http: Option<reqwest::blocking::Client>,
    /// TLS for `https` gRPC endpoints: native roots plus `ca_file`.
    tls: Option<opentelemetry_otlp::tonic_types::transport::ClientTlsConfig>,
    /// The runtime the gRPC channels live on: one dedicated worker, so
    /// connection attempts to a down collector never run on the request
    /// runtime.
    grpc: Option<GrpcRuntime>,
}

/// A dedicated runtime for the gRPC exporters' channels, shut down in the
/// background when dropped (never blocks, even inside async code).
struct GrpcRuntime(Option<tokio::runtime::Runtime>);
impl GrpcRuntime {
    fn new() -> Result<Self, Failure> {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("merezha-otlp-grpc")
            .enable_all()
            .build()
            .map(|runtime| Self(Some(runtime)))
            .map_err(exporter_failed)
    }
    fn enter(&self) -> Option<tokio::runtime::EnterGuard<'_>> {
        self.0.as_ref().map(|runtime| runtime.enter())
    }
}
impl Drop for GrpcRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}
fn exporter_failed<E>(_: E) -> Failure {
    Failure::permanent("telemetry_exporter_failed")
}
impl Transport {
    fn new(config: &TelemetryConfig) -> Result<Self, Failure> {
        let ca = read_ca(config.ca_file.as_deref())?;
        let headers: HashMap<String, String> = config
            .headers
            .iter()
            .map(|(name, value)| (name.clone(), value.expose().to_owned()))
            .collect();
        let (http, tls) = match config.protocol {
            Protocol::HttpProtobuf => {
                let timeout = config.timeout;
                let pem = ca.clone();
                let client = std::thread::Builder::new()
                    .name("merezha-otlp-client".into())
                    .spawn(move || {
                        let mut builder = reqwest::blocking::Client::builder()
                            .timeout(timeout)
                            .redirect(reqwest::redirect::Policy::none());
                        if let Some(pem) = pem {
                            let certs = reqwest::Certificate::from_pem_bundle(&pem)
                                .map_err(exporter_failed)?;
                            builder = builder.tls_certs_merge(certs);
                        }
                        builder.build().map_err(exporter_failed)
                    })
                    .map_err(exporter_failed)?
                    .join()
                    .map_err(exporter_failed)??;
                (Some(client), None)
            }
            Protocol::Grpc => {
                use opentelemetry_otlp::tonic_types::transport::{Certificate, ClientTlsConfig};
                let tls = ClientTlsConfig::new().with_native_roots();
                let tls = match &ca {
                    Some(pem) => tls.ca_certificate(Certificate::from_pem(pem)),
                    None => tls,
                };
                (None, Some(tls))
            }
        };
        let grpc = match config.protocol {
            Protocol::Grpc => Some(GrpcRuntime::new()?),
            Protocol::HttpProtobuf => None,
        };
        Ok(Self {
            protocol: config.protocol,
            gzip: config.compression == Compression::Gzip,
            timeout: config.timeout,
            headers,
            http,
            tls,
            grpc,
        })
    }
    fn metadata(&self) -> Result<opentelemetry_otlp::tonic_types::metadata::MetadataMap, Failure> {
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in &self.headers {
            headers.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes())
                    .map_err(exporter_failed)?,
                reqwest::header::HeaderValue::from_str(value).map_err(exporter_failed)?,
            );
        }
        Ok(opentelemetry_otlp::tonic_types::metadata::MetadataMap::from_headers(headers))
    }
}

/// Build one signal's OTLP exporter from a `SpanExporter::builder()`-like
/// start, over HTTP or gRPC.
macro_rules! otlp_exporter {
    ($builder:expr, $transport:expr, $endpoint:expr) => {{
        let transport: &Transport = $transport;
        match transport.protocol {
            Protocol::HttpProtobuf => {
                let mut builder = $builder
                    .with_http()
                    .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                    .with_endpoint($endpoint)
                    .with_timeout(transport.timeout)
                    .with_headers(transport.headers.clone());
                if let Some(client) = &transport.http {
                    builder = builder.with_http_client(client.clone());
                }
                if transport.gzip {
                    builder = builder.with_compression(opentelemetry_otlp::Compression::Gzip);
                }
                builder.build().map_err(exporter_failed)
            }
            Protocol::Grpc => {
                let endpoint: String = $endpoint;
                let https = endpoint.starts_with("https://");
                let mut builder = $builder
                    .with_tonic()
                    .with_endpoint(endpoint)
                    .with_timeout(transport.timeout)
                    .with_metadata(transport.metadata()?);
                if let (true, Some(tls)) = (https, &transport.tls) {
                    builder = builder.with_tls_config(tls.clone());
                }
                if transport.gzip {
                    builder = builder.with_compression(opentelemetry_otlp::Compression::Gzip);
                }
                let _runtime = transport.grpc.as_ref().and_then(GrpcRuntime::enter);
                builder.build().map_err(exporter_failed)
            }
        }
    }};
}

// ---------------------------------------------------------------------------
// Assembly
// ---------------------------------------------------------------------------

/// Counters of every signal.
#[derive(Debug)]
pub struct Accounts {
    pub traces: Arc<SignalAccount>,
    pub metrics: Arc<SignalAccount>,
    pub logs: Arc<SignalAccount>,
    stderr: Option<ErrorCounter>,
}
impl Accounts {
    fn new(config: &TelemetryConfig, stderr: Option<ErrorCounter>) -> Self {
        Self {
            traces: Arc::new(SignalAccount::new(
                TelemetrySignal::Traces,
                config.traces.max_queue,
            )),
            metrics: Arc::new(SignalAccount::new(TelemetrySignal::Metrics, u32::MAX)),
            logs: Arc::new(SignalAccount::new(
                TelemetrySignal::Logs,
                config.logs.max_queue,
            )),
            stderr,
        }
    }
    pub fn signal(&self, signal: Signal) -> &Arc<SignalAccount> {
        match signal {
            Signal::Traces => &self.traces,
            Signal::Metrics => &self.metrics,
            Signal::Logs => &self.logs,
        }
    }
    /// Lines the lossy stderr writer dropped.
    pub fn stderr_dropped(&self) -> u64 {
        self.stderr.as_ref().map_or(0, |c| c.dropped_lines() as u64)
    }
}

/// Where the signals go.
pub(crate) enum Sinks {
    Otlp,
    #[cfg(feature = "testing")]
    Memory(crate::testing::MemorySinks),
}

/// The stderr layer: JSON or text, `merezha*` at `log_level`, every other
/// target at `log_level` but never more verbose than `info`.
pub(crate) fn stderr_layer<W>(config: &TelemetryConfig, service: &Service, writer: W) -> BoxedLayer
where
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    let merezha = level(config.log_level);
    let filter = TargetFilter {
        merezha,
        other: merezha.min(LevelFilter::INFO),
    };
    match config.format {
        LogFormat::Json => JsonLayer::new(writer, service).with_filter(filter).boxed(),
        LogFormat::Text => tracing_subscriber::fmt::layer()
            .compact()
            .with_ansi(false)
            .with_writer(writer)
            .with_filter(filter)
            .boxed(),
    }
}

/// The dispatcher for events logged before the configuration is known
/// (`startup.failed` with `invalid_config` or `invalid_arguments`): JSON to
/// stderr, unbuffered, `service = merezha`.
pub fn bootstrap() -> Dispatch {
    let config = TelemetryConfig {
        signals: Vec::new(),
        ..TelemetryConfig::default()
    };
    Dispatch::new(Registry::default().with(stderr_layer(&config, &Service::raw(), std::io::stderr)))
}

/// Names of `OTEL_*` variables in `vars`, sorted.
pub fn otel_variables(vars: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut names: Vec<String> = vars
        .into_iter()
        .filter(|n| n.starts_with("OTEL_"))
        .collect();
    names.sort();
    names.dedup();
    names
}

/// Remove every `OTEL_*` variable from the process environment and return
/// their names, for `telemetry.otel_env_ignored`. Merezha sets every SDK
/// option programmatically and reads no `OTEL_*` variable.
///
/// # Safety
///
/// Call before any other thread exists (first thing in `main`, before the
/// Tokio runtime is built): removing variables races with concurrent
/// environment reads on other threads.
pub unsafe fn scrub_otel_environment() -> Vec<String> {
    let names = otel_variables(std::env::vars_os().filter_map(|(k, _)| k.into_string().ok()));
    for name in &names {
        // SAFETY: the caller guarantees no other thread runs.
        unsafe { std::env::remove_var(name) };
    }
    names
}

/// Reads the `telemetry` status section from the pipeline's accounts.
#[derive(Clone)]
pub struct StatusReader {
    accounts: Arc<Accounts>,
    config: Arc<TelemetryConfig>,
}
impl std::fmt::Debug for StatusReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatusReader")
            .field("signals", &self.config.signals)
            .finish_non_exhaustive()
    }
}
impl StatusReader {
    /// Codes, counts and the endpoint origin only; never header values.
    pub fn read(&self) -> TelemetryStatus {
        let (enabled, protocol, endpoint, sampling_ratio) = TelemetryStatus::summary(&self.config);
        TelemetryStatus {
            enabled,
            protocol,
            endpoint,
            sampling_ratio,
            signals: self
                .config
                .signals
                .iter()
                .map(|s| (s.as_str(), self.accounts.signal(*s).counts()))
                .collect(),
            stderr_dropped: self.accounts.stderr_dropped(),
        }
    }
}

/// Per-signal counts at the end of [`Telemetry::shutdown`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShutdownReport {
    pub signals: BTreeMap<&'static str, SignalCounts>,
    pub elapsed: Duration,
}

/// The host's telemetry: providers, accounts and the stderr writer. No
/// OpenTelemetry globals; the `tracing` subscriber is installed by
/// [`Telemetry::init`].
pub struct Telemetry {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    accounts: Arc<Accounts>,
    stderr_guard: Mutex<Option<WorkerGuard>>,
    shutdown_timeout: Duration,
    config: TelemetryConfig,
    /// Declared last: dropped after the providers that use it.
    _grpc: Option<GrpcRuntime>,
}
impl std::fmt::Debug for Telemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Telemetry")
            .field("signals", &self.config.signals)
            .field("protocol", &self.config.protocol)
            .finish_non_exhaustive()
    }
}

impl Telemetry {
    /// Build the pipeline from `[telemetry]` and install the process-wide
    /// subscriber. Exporters connect lazily: a collector that is down or
    /// still starting never fails startup. Must run inside the Tokio runtime
    /// (the gRPC channel needs it). Fails `telemetry_initialized` when a
    /// subscriber is already installed, `invalid_telemetry_ca` for an
    /// unreadable `ca_file` and `telemetry_exporter_failed` when an
    /// exporter cannot be built.
    pub fn init(config: &TelemetryConfig, service: &Service) -> Result<Self, Failure> {
        let (writer, guard) = NonBlockingBuilder::default()
            .lossy(true)
            .buffered_lines_limit(config.log_buffer_lines as usize)
            .thread_name("merezha-stderr")
            .finish(std::io::stderr());
        let counter = writer.error_counter();
        let stderr = stderr_layer(config, service, writer);
        let (mut telemetry, dispatch) = Self::assemble(
            config,
            service,
            Sinks::Otlp,
            stderr,
            Some(counter),
            Vec::new(),
        )?;
        telemetry.stderr_guard = Mutex::new(Some(guard));
        tracing::dispatcher::set_global_default(dispatch)
            .map_err(|_| Failure::permanent("telemetry_initialized"))?;
        Ok(telemetry)
    }
    /// The pipeline [`Telemetry::init`] builds, with log lines written to
    /// `stderr` and the dispatcher returned instead of installed, for tests
    /// and embedders that scope it themselves.
    pub fn build<W>(
        config: &TelemetryConfig,
        service: &Service,
        stderr: W,
    ) -> Result<(Self, Dispatch), Failure>
    where
        W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
    {
        let stderr = stderr_layer(config, service, stderr);
        Self::assemble(config, service, Sinks::Otlp, stderr, None, Vec::new())
    }

    pub(crate) fn assemble(
        config: &TelemetryConfig,
        service: &Service,
        sinks: Sinks,
        stderr: BoxedLayer,
        stderr_counter: Option<ErrorCounter>,
        extra: Vec<BoxedLayer>,
    ) -> Result<(Self, Dispatch), Failure> {
        let accounts = Arc::new(Accounts::new(config, stderr_counter));
        let allowlist = Allowlist::new(config.privacy);
        let resource = resource(service);
        let scope = InstrumentationScope::builder(SCOPE)
            .with_version(Service::HOST_VERSION)
            .build();
        let transport = match (&sinks, config.signals.is_empty()) {
            (Sinks::Otlp, false) => Some(Transport::new(config)?),
            _ => None,
        };
        let mut layers: Vec<BoxedLayer> = vec![stderr];

        let tracer_provider = if config.exports(Signal::Traces) {
            let account = accounts.traces.clone();
            let builder = SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_sampler(sampler(config))
                .with_max_attributes_per_span(MAX_SPAN_ATTRIBUTES)
                .with_max_events_per_span(MAX_SPAN_EVENTS)
                .with_max_links_per_span(MAX_SPAN_LINKS);
            let builder = match (&sinks, &transport) {
                (Sinks::Otlp, Some(transport)) => {
                    let exporter = otlp_exporter!(
                        opentelemetry_otlp::SpanExporter::builder(),
                        transport,
                        config.signal_endpoint(Signal::Traces)
                    )?;
                    let batch =
                        BatchSpanProcessor::builder(Counting::new(exporter, account.clone()))
                            .with_batch_config(
                                opentelemetry_sdk::trace::BatchConfigBuilder::default()
                                    .with_max_queue_size(
                                        (config.traces.max_queue + config.traces.max_batch)
                                            as usize,
                                    )
                                    .with_max_export_batch_size(config.traces.max_batch as usize)
                                    .with_scheduled_delay(config.traces.schedule_delay)
                                    .build(),
                            )
                            .build();
                    builder.with_span_processor(AllowlistSpanProcessor::new(
                        GuardedSpanProcessor::new(batch, account.clone()),
                        allowlist,
                        account,
                    ))
                }
                #[cfg(feature = "testing")]
                (Sinks::Memory(memory), _) => {
                    let simple = opentelemetry_sdk::trace::SimpleSpanProcessor::new(Counting::new(
                        memory.spans.clone(),
                        account.clone(),
                    ));
                    builder.with_span_processor(AllowlistSpanProcessor::new(
                        GuardedSpanProcessor::new(simple, account.clone()),
                        allowlist,
                        account,
                    ))
                }
                _ => builder,
            };
            let provider = builder.build();
            layers.push(span_bridge(provider.tracer_with_scope(scope.clone())));
            Some(provider)
        } else {
            None
        };

        let logger_provider = if config.exports(Signal::Logs) {
            let account = accounts.logs.clone();
            let builder = SdkLoggerProvider::builder().with_resource(resource.clone());
            let builder = match (&sinks, &transport) {
                (Sinks::Otlp, Some(transport)) => {
                    let exporter = otlp_exporter!(
                        opentelemetry_otlp::LogExporter::builder(),
                        transport,
                        config.signal_endpoint(Signal::Logs)
                    )?;
                    let batch =
                        BatchLogProcessor::builder(Counting::new(exporter, account.clone()))
                            .with_batch_config(
                                opentelemetry_sdk::logs::BatchConfigBuilder::default()
                                    .with_max_queue_size(
                                        (config.logs.max_queue + config.logs.max_batch) as usize,
                                    )
                                    .with_max_export_batch_size(config.logs.max_batch as usize)
                                    .with_scheduled_delay(config.logs.schedule_delay)
                                    .build(),
                            )
                            .build();
                    builder.with_log_processor(GuardedLogProcessor::new(batch, account))
                }
                #[cfg(feature = "testing")]
                (Sinks::Memory(memory), _) => {
                    let simple = opentelemetry_sdk::logs::SimpleLogProcessor::new(Counting::new(
                        memory.logs.clone(),
                        account.clone(),
                    ));
                    builder.with_log_processor(GuardedLogProcessor::new(simple, account))
                }
                _ => builder,
            };
            let provider = builder.build();
            let bridge = LogBridge::new(provider.logger_with_scope(scope.clone()), allowlist)
                .with_filter(TargetFilter::merezha_only(level(config.logs.level)));
            layers.push(bridge.boxed());
            Some(provider)
        } else {
            None
        };

        let meter_provider = if config.exports(Signal::Metrics) {
            let account = accounts.metrics.clone();
            let builder = SdkMeterProvider::builder().with_resource(resource);
            let builder = match (&sinks, &transport) {
                (Sinks::Otlp, Some(transport)) => {
                    let exporter = otlp_exporter!(
                        opentelemetry_otlp::MetricExporter::builder()
                            .with_temporality(temporality(config.metrics.temporality)),
                        transport,
                        config.signal_endpoint(Signal::Metrics)
                    )?;
                    builder.with_reader(
                        PeriodicReader::builder(Counting::new(exporter, account))
                            .with_interval(config.metrics.interval)
                            .build(),
                    )
                }
                #[cfg(feature = "testing")]
                (Sinks::Memory(memory), _) => builder.with_reader(
                    PeriodicReader::builder(Counting::new(memory.metrics.clone(), account))
                        .with_interval(config.metrics.interval)
                        .build(),
                ),
                _ => builder,
            };
            let provider = builder.build();
            observe_self(&provider.meter_with_scope(scope), &accounts, config);
            Some(provider)
        } else {
            None
        };

        layers.extend(extra);
        let dispatch = Dispatch::new(Registry::default().with(layers));
        Ok((
            Self {
                tracer_provider,
                meter_provider,
                logger_provider,
                accounts,
                stderr_guard: Mutex::new(None),
                shutdown_timeout: config.shutdown_timeout,
                config: config.clone(),
                _grpc: transport.and_then(|t| t.grpc),
            },
            dispatch,
        ))
    }

    /// A meter for `module`'s instruments; a no-op meter when metrics are
    /// not exported. Libraries build their instruments once from it.
    pub fn meter(&self, module: &'static str) -> Meter {
        match &self.meter_provider {
            Some(provider) => provider.meter(module),
            None => opentelemetry::metrics::noop::NoopMeterProvider::new().meter(module),
        }
    }
    /// The meter source handed to libraries (`merezha-postgres` and the
    /// blocks): the SDK provider, or no-op meters when metrics are not
    /// exported.
    pub fn meters(&self) -> crate::Meters {
        match &self.meter_provider {
            Some(provider) => crate::Meters::new(provider.clone()),
            None => crate::Meters::default(),
        }
    }
    /// Whether any OTLP provider runs.
    pub fn exporting(&self) -> bool {
        self.tracer_provider.is_some()
            || self.meter_provider.is_some()
            || self.logger_provider.is_some()
    }
    pub fn accounts(&self) -> &Arc<Accounts> {
        &self.accounts
    }
    #[cfg(feature = "testing")]
    pub(crate) fn providers(
        &self,
    ) -> (
        Option<&SdkTracerProvider>,
        Option<&SdkMeterProvider>,
        Option<&SdkLoggerProvider>,
    ) {
        (
            self.tracer_provider.as_ref(),
            self.meter_provider.as_ref(),
            self.logger_provider.as_ref(),
        )
    }
    /// The `telemetry` section of `/v1/admin/status`.
    pub fn status(&self) -> TelemetryStatus {
        self.status_reader().read()
    }
    /// A cheap handle that reads [`Telemetry::status`] while the host
    /// serves; the status endpoint holds it, the binary keeps the
    /// pipeline for its shutdown flush.
    pub fn status_reader(&self) -> StatusReader {
        StatusReader {
            accounts: self.accounts.clone(),
            config: Arc::new(self.config.clone()),
        }
    }
    /// Flush traces, then logs, then metrics within `shutdown_timeout` in
    /// total; whatever is left counts as `dropped{reason=shutdown}`. Then
    /// flush the stderr writer. Blocking: call it from a blocking context.
    pub fn shutdown(self) -> ShutdownReport {
        let started = Instant::now();
        let deadline = started + self.shutdown_timeout;
        if let Some(provider) = self.tracer_provider.clone() {
            self.accounts.traces.close();
            within(deadline, move |left| provider.shutdown_with_timeout(left));
            self.accounts.traces.finalize();
        }
        if let Some(provider) = self.logger_provider.clone() {
            self.accounts.logs.close();
            within(deadline, move |left| provider.shutdown_with_timeout(left));
            self.accounts.logs.finalize();
        }
        if let Some(provider) = self.meter_provider.clone() {
            // The 0.33 meter provider ignores its timeout; `within` bounds it.
            within(deadline, move |left| provider.shutdown_with_timeout(left));
            self.accounts.metrics.finalize();
        }
        let report = ShutdownReport {
            signals: self
                .config
                .signals
                .iter()
                .map(|s| (s.as_str(), self.accounts.signal(*s).counts()))
                .collect(),
            elapsed: started.elapsed(),
        };
        // Dropping the guard flushes buffered stderr lines.
        if let Ok(mut guard) = self.stderr_guard.lock() {
            guard.take();
        }
        report
    }
}

/// Run a provider shutdown on a helper thread and stop waiting at
/// `deadline`; a shutdown still blocked on a stalled collector is
/// abandoned, and its items count as dropped.
fn within(deadline: Instant, shutdown: impl FnOnce(Duration) -> OTelSdkResult + Send + 'static) {
    let left = deadline.saturating_duration_since(Instant::now());
    let (done, wait) = std::sync::mpsc::sync_channel(1);
    let spawned = std::thread::Builder::new()
        .name("merezha-telemetry-shutdown".into())
        .spawn(move || {
            let _ = done.send(shutdown(left));
        });
    if spawned.is_ok() {
        let _ = wait.recv_timeout(deadline.saturating_duration_since(Instant::now()));
    }
}

fn sampler(config: &TelemetryConfig) -> Sampler {
    match config.traces.sampler {
        crate::config::Sampler::AlwaysOff => Sampler::AlwaysOff,
        crate::config::Sampler::ParentBasedRatio => {
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(config.traces.ratio)))
        }
    }
}
fn temporality(temporality: Temporality) -> opentelemetry_sdk::metrics::Temporality {
    match temporality {
        Temporality::Cumulative => opentelemetry_sdk::metrics::Temporality::Cumulative,
        Temporality::Delta => opentelemetry_sdk::metrics::Temporality::Delta,
    }
}

/// `merezha.telemetry.*` self-observation: observable counters reading the
/// accounts, so the hot path pays nothing.
fn observe_self(meter: &Meter, accounts: &Arc<Accounts>, config: &TelemetryConfig) {
    let signals = config.signals.clone();
    let (exported, dropped, attributes) = (
        names::metric::TELEMETRY_EXPORTED,
        names::metric::TELEMETRY_DROPPED,
        names::metric::TELEMETRY_ATTRIBUTES_DROPPED,
    );
    let a = accounts.clone();
    let s = signals.clone();
    meter
        .u64_observable_counter(exported.name)
        .with_unit(exported.unit)
        .with_callback(move |observer| {
            for signal in &s {
                let counts = a.signal(*signal).counts();
                let kind = a.signal(*signal).signal();
                observer.observe(
                    counts.exported,
                    &[attr::signal(kind), attr::outcome(names::Outcome::Ok)],
                );
                observer.observe(
                    counts.failed,
                    &[attr::signal(kind), attr::outcome(names::Outcome::Failed)],
                );
            }
        })
        .build();
    let a = accounts.clone();
    let s = signals.clone();
    meter
        .u64_observable_counter(dropped.name)
        .with_unit(dropped.unit)
        .with_callback(move |observer| {
            for signal in &s {
                let counts = a.signal(*signal).counts();
                let kind = attr::signal(a.signal(*signal).signal());
                observer.observe(
                    counts.dropped_queue_full,
                    &[kind.clone(), attr::reason(names::Reason::QueueFull)],
                );
                observer.observe(
                    counts.failed,
                    &[kind.clone(), attr::reason(names::Reason::ExportFailed)],
                );
                observer.observe(
                    counts.dropped_shutdown,
                    &[kind, attr::reason(names::Reason::Shutdown)],
                );
            }
            observer.observe(
                a.stderr_dropped(),
                &[
                    attr::signal(TelemetrySignal::Stderr),
                    attr::reason(names::Reason::QueueFull),
                ],
            );
        })
        .build();
    let a = accounts.clone();
    meter
        .u64_observable_counter(attributes.name)
        .with_unit(attributes.unit)
        .with_callback(move |observer| {
            for signal in &signals {
                let account = a.signal(*signal);
                observer.observe(
                    account.counts().attributes_dropped,
                    &[attr::signal(account.signal())],
                );
            }
        })
        .build();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An exporter whose own `Debug` would print a header value.
    struct Leaky;
    impl std::fmt::Debug for Leaky {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("authorization: Bearer s3cr3t")
        }
    }

    #[test]
    fn wrappers_never_print_what_they_wrap() {
        let account = Arc::new(SignalAccount::new(TelemetrySignal::Traces, 8));
        let printed = [
            format!("{:?}", Counting::new(Leaky, account.clone())),
            format!("{:?}", GuardedSpanProcessor::new(Leaky, account.clone())),
            format!("{:?}", GuardedLogProcessor::new(Leaky, account.clone())),
            format!(
                "{:?}",
                AllowlistSpanProcessor::new(Leaky, Allowlist::new(Default::default()), account)
            ),
        ];
        for text in printed {
            assert!(!text.contains("s3cr3t"), "{text}");
            assert!(text.contains("Traces"), "{text}");
        }
    }
}
