//! The in-memory test harness (TE §8). It runs the production pipeline —
//! allowlist, guarded processors, counting exporters, log bridge and JSON
//! formatter — with in-memory exporters and a `Vec<u8>` stderr, behind a
//! scoped dispatcher, so tests run in parallel without globals.
//!
//! ```
//! use merezha_telemetry::testing::Harness;
//! let harness = Harness::new();
//! merezha_telemetry::event!(INFO, "listener.ready", address = "127.0.0.1:1");
//! assert_eq!(harness.stderr_lines()[0]["event"], "listener.ready");
//! harness.assert_catalogued();
//! ```
use crate::{
    config::{LogLevel, Service, TelemetryConfig},
    json::{Fields, event_name, is_merezha},
    names,
    pipeline::{ShutdownReport, Sinks, Telemetry, stderr_layer},
    status::SignalCounts,
};
use opentelemetry_sdk::{
    logs::{InMemoryLogExporter, in_memory_exporter::LogDataWithResource},
    metrics::{
        InMemoryMetricExporter,
        data::{AggregatedMetrics, MetricData},
    },
    trace::{InMemorySpanExporter, SpanData},
};
use std::{
    collections::BTreeMap,
    io::Write,
    sync::{Arc, Mutex},
};
use tracing::{Dispatch, Event, Subscriber};
use tracing_subscriber::{Layer, fmt::MakeWriter, layer::Context, registry::LookupSpan};

/// The in-memory exporters the harness reads.
#[derive(Clone)]
pub(crate) struct MemorySinks {
    pub(crate) spans: InMemorySpanExporter,
    pub(crate) metrics: InMemoryMetricExporter,
    pub(crate) logs: InMemoryLogExporter,
}

/// A shared byte buffer usable as a `tracing` writer.
#[derive(Clone, Default)]
pub struct SharedBuffer(Arc<Mutex<Vec<u8>>>);
impl SharedBuffer {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().map(|b| b.clone()).unwrap_or_default()).into_owned()
    }
}
impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Ok(mut bytes) = self.0.lock() {
            bytes.extend_from_slice(buf);
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> MakeWriter<'a> for SharedBuffer {
    type Writer = SharedBuffer;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// An event as emitted, for the catalog check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recorded {
    pub target: String,
    pub name: Option<String>,
    pub level: tracing::Level,
    pub fields: Vec<&'static str>,
}

/// Records every event regardless of level filters.
#[derive(Clone, Default)]
pub struct CatalogRecorder(Arc<Mutex<Vec<Recorded>>>);
impl CatalogRecorder {
    pub fn recorded(&self) -> Vec<Recorded> {
        self.0.lock().map(|r| r.clone()).unwrap_or_default()
    }
}
impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for CatalogRecorder {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let fields = Fields::of(event);
        let record = Recorded {
            target: event.metadata().target().to_owned(),
            name: event_name(event, &fields).map(|n| n.into_owned()),
            level: *event.metadata().level(),
            fields: fields
                .values
                .iter()
                .map(|(n, _)| *n)
                .filter(|n| *n != "event")
                .collect(),
        };
        if let Ok(mut recorded) = self.0.lock() {
            recorded.push(record);
        }
    }
}

/// Check emitted `merezha*` events against the catalog: each is listed,
/// at one of its levels, with only its fields.
pub fn check_catalogued(events: &[Recorded]) -> Result<(), String> {
    let mut problems = Vec::new();
    for event in events.iter().filter(|e| is_merezha(&e.target)) {
        let Some(name) = &event.name else {
            problems.push(format!("unnamed event from {}", event.target));
            continue;
        };
        let Some(row) = names::event(name) else {
            problems.push(format!("{name} is not in names::EVENTS"));
            continue;
        };
        if !row.levels.contains(&event.level) {
            problems.push(format!("{name} at {} is not a catalog level", event.level));
        }
        for field in &event.fields {
            if row.field(field).is_none() {
                problems.push(format!("{name} has uncatalogued field {field}"));
            }
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.join("; "))
    }
}

/// One metric point: attributes and the value (sum or gauge value,
/// histogram count), plus a histogram's sum of recorded values.
#[derive(Clone, Debug, PartialEq)]
pub struct Point {
    pub attributes: BTreeMap<String, String>,
    pub value: f64,
    /// Histograms only: the sum of the recorded values.
    pub sum: Option<f64>,
}
/// The points of one metric in the latest collection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricPoints(Vec<Point>);
impl MetricPoints {
    pub fn points(&self) -> &[Point] {
        &self.0
    }
    /// The value of the point whose attributes include every `(key, value)`.
    pub fn value(&self, attributes: &[(&str, &str)]) -> Option<f64> {
        self.0
            .iter()
            .find(|p| {
                attributes
                    .iter()
                    .all(|(k, v)| p.attributes.get(*k).map(String::as_str) == Some(*v))
            })
            .map(|p| p.value)
    }
    /// The histogram sum of the point whose attributes include every
    /// `(key, value)`.
    pub fn sum(&self, attributes: &[(&str, &str)]) -> Option<f64> {
        self.0
            .iter()
            .find(|p| {
                attributes
                    .iter()
                    .all(|(k, v)| p.attributes.get(*k).map(String::as_str) == Some(*v))
            })
            .and_then(|p| p.sum)
    }
    /// The sum of every point's value (counts for histograms).
    pub fn total(&self) -> f64 {
        self.0.iter().map(|p| p.value).sum()
    }
}

fn attributes<'a>(
    kvs: impl Iterator<Item = &'a opentelemetry::KeyValue>,
) -> BTreeMap<String, String> {
    kvs.map(|kv| (kv.key.to_string(), kv.value.to_string()))
        .collect()
}
/// The value of `key` on an exported span, as text.
pub fn span_attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.to_string())
}

/// Signals, stderr and events of one test, in memory.
pub struct Harness {
    telemetry: Option<Telemetry>,
    memory: MemorySinks,
    stderr: SharedBuffer,
    recorder: CatalogRecorder,
    dispatch: Dispatch,
    _guard: tracing::dispatcher::DefaultGuard,
}
impl Default for Harness {
    fn default() -> Self {
        Self::new()
    }
}
impl Harness {
    /// Every signal on, logs at debug, the raw service identity.
    pub fn new() -> Self {
        let mut config = TelemetryConfig {
            log_level: LogLevel::Debug,
            signals: crate::config::Signal::ALL.to_vec(),
            ..TelemetryConfig::default()
        };
        config.logs.level = LogLevel::Debug;
        // Collections happen on demand.
        config.metrics.interval = std::time::Duration::from_secs(3_600);
        Self::with(config, Service::raw())
    }
    /// The harness for `config` and `service`. Endpoints, protocol and
    /// headers are ignored: signals go to memory.
    pub fn with(config: TelemetryConfig, service: Service) -> Self {
        // With a single registered dispatcher, `tracing-core` computes a new
        // callsite's interest from the *current thread's* default only, so a
        // callsite first hit on a thread without this harness (another test)
        // would be cached as never and disappear from this harness too. A
        // second, inert dispatcher for the whole process makes every
        // callsite consult all live dispatchers.
        static ANCHOR: std::sync::OnceLock<Dispatch> = std::sync::OnceLock::new();
        ANCHOR.get_or_init(|| Dispatch::new(tracing::subscriber::NoSubscriber::default()));
        let memory = MemorySinks {
            spans: InMemorySpanExporter::default(),
            metrics: InMemoryMetricExporter::default(),
            logs: InMemoryLogExporter::default(),
        };
        let stderr = SharedBuffer::default();
        let recorder = CatalogRecorder::default();
        let (telemetry, dispatch) = Telemetry::assemble(
            &config,
            &service,
            Sinks::Memory(memory.clone()),
            stderr_layer(&config, &service, stderr.clone()),
            None,
            vec![recorder.clone().boxed()],
        )
        .expect("in-memory telemetry builds");
        let guard = tracing::dispatcher::set_default(&dispatch);
        Self {
            telemetry: Some(telemetry),
            memory,
            stderr,
            recorder,
            dispatch,
            _guard: guard,
        }
    }
    /// The harness dispatcher, for other threads and futures.
    pub fn dispatch(&self) -> &Dispatch {
        &self.dispatch
    }
    pub fn telemetry(&self) -> &Telemetry {
        self.telemetry.as_ref().expect("harness not shut down")
    }
    /// Exported spans so far.
    pub fn spans(&self) -> Vec<SpanData> {
        self.memory.spans.get_finished_spans().unwrap_or_default()
    }
    /// The exported span named `name`.
    pub fn span(&self, name: &str) -> SpanData {
        self.spans()
            .into_iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("no span named {name}"))
    }
    /// Every exported span named `name`, in end order.
    pub fn spans_named(&self, name: &str) -> Vec<SpanData> {
        self.spans()
            .into_iter()
            .filter(|s| s.name == name)
            .collect()
    }
    /// Collect metrics now and return the points of `name`.
    pub fn metric(&self, name: &str) -> MetricPoints {
        if let (_, Some(meter), _) = self.telemetry().providers() {
            let _ = meter.force_flush();
        }
        let finished = self
            .memory
            .metrics
            .get_finished_metrics()
            .unwrap_or_default();
        let mut points = Vec::new();
        if let Some(latest) = finished.last() {
            for metric in latest
                .scope_metrics()
                .flat_map(|s| s.metrics())
                .filter(|m| m.name() == name)
            {
                match metric.data() {
                    AggregatedMetrics::F64(d) => collect_any(d, &mut points, |v| v),
                    AggregatedMetrics::U64(d) => collect_any(d, &mut points, |v| v as f64),
                    AggregatedMetrics::I64(d) => collect_any(d, &mut points, |v| v as f64),
                }
            }
        }
        MetricPoints(points)
    }
    /// Exported OTLP log records so far.
    pub fn logs(&self) -> Vec<LogDataWithResource> {
        self.memory.logs.get_emitted_logs().unwrap_or_default()
    }
    /// The stderr text.
    pub fn stderr(&self) -> String {
        self.stderr.text()
    }
    /// The stderr lines parsed as JSON (JSON format only).
    pub fn stderr_lines(&self) -> Vec<serde_json::Value> {
        self.stderr()
            .lines()
            .map(|l| serde_json::from_str(l).expect("stderr line is JSON"))
            .collect()
    }
    /// Every event emitted under the harness.
    pub fn recorded(&self) -> Vec<Recorded> {
        self.recorder.recorded()
    }
    /// Counts of one signal.
    pub fn counts(&self, signal: crate::config::Signal) -> SignalCounts {
        self.telemetry().accounts().signal(signal).counts()
    }
    /// Panic if an emitted `merezha*` event is missing from the catalog, at
    /// a non-catalog level, or carries an uncatalogued field.
    pub fn assert_catalogued(&self) {
        if let Err(problems) = check_catalogued(&self.recorded()) {
            panic!("event catalog: {problems}");
        }
    }
    /// Panic if `canary` appears in any exported span, metric or log, or in
    /// stderr.
    pub fn assert_no_canary(&self, canary: &str) {
        if let (_, Some(meter), _) = self.telemetry().providers() {
            let _ = meter.force_flush();
        }
        let surfaces = [
            ("spans", format!("{:?}", self.spans())),
            ("logs", format!("{:?}", self.logs())),
            (
                "metrics",
                format!(
                    "{:?}",
                    self.memory
                        .metrics
                        .get_finished_metrics()
                        .unwrap_or_default()
                ),
            ),
            ("stderr", self.stderr()),
        ];
        for (surface, text) in surfaces {
            assert!(!text.contains(canary), "canary found in {surface}");
        }
    }
    /// Shut the pipeline down and return its final counts.
    pub fn shutdown(mut self) -> ShutdownReport {
        self.telemetry
            .take()
            .expect("harness not shut down")
            .shutdown()
    }
}
fn collect_any<T: Copy>(data: &MetricData<T>, out: &mut Vec<Point>, f: impl Fn(T) -> f64) {
    match data {
        MetricData::Gauge(g) => out.extend(g.data_points().map(|p| Point {
            attributes: attributes(p.attributes()),
            value: f(p.value()),
            sum: None,
        })),
        MetricData::Sum(s) => out.extend(s.data_points().map(|p| Point {
            attributes: attributes(p.attributes()),
            value: f(p.value()),
            sum: None,
        })),
        MetricData::Histogram(h) => out.extend(h.data_points().map(|p| Point {
            attributes: attributes(p.attributes()),
            value: p.count() as f64,
            sum: Some(f(p.sum())),
        })),
        MetricData::ExponentialHistogram(h) => out.extend(h.data_points().map(|p| Point {
            attributes: attributes(p.attributes()),
            value: p.count() as f64,
            sum: Some(f(p.sum())),
        })),
    }
}
