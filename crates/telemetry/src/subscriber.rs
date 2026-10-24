//! The `tracing` registry (TE §4): one registry, three layers, each with
//! its own target filter and no regex `EnvFilter`.
//!
//! 1. stderr: the JSON formatter ([`crate::json`]) or the stock compact
//!    text formatter, at `telemetry.log_level`.
//! 2. span bridge: `tracing-opentelemetry` with context activation on, for
//!    `merezha*` spans at INFO and above, independent of the log level.
//! 3. OTLP log bridge ([`LogBridge`]): catalog events from `merezha*`
//!    targets at `telemetry.logs.level`, attributes mapped through the
//!    catalog and the allowlist. No third-party event enters it, so export
//!    errors cannot loop back into the exporter.
use crate::{
    allowlist::{Allowlist, Column, LOG_STRING_CAP, cap_value},
    json::{FieldValue, Fields, event_name, is_merezha},
    names,
};
use opentelemetry::{
    Key,
    logs::{AnyValue, LogRecord, Logger, Severity},
};
use opentelemetry_sdk::logs::SdkLogger;
use tracing::{Event, Metadata, Subscriber, level_filters::LevelFilter, subscriber::Interest};
use tracing_subscriber::{
    Layer, Registry,
    layer::{Context, Filter},
};

/// A boxed layer of the registry.
pub type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync>;

/// Crates whose events are only the exporter's own diagnostics: the SDK and
/// the OTLP exporter. They never reach stderr; with the collector down they
/// would repeat for every batch, and `telemetry.export_failed` already
/// reports, once a minute per signal.
pub const EXPORTER_TARGETS: [&str; 2] = ["opentelemetry", "tonic"];
/// HTTP transport crates the server shares with the exporter. Their
/// warnings and errors reach stderr, except on the exporter's own threads.
pub const TRANSPORT_TARGETS: [&str; 4] = ["hyper", "h2", "tower", "reqwest"];
/// Threads that only run export work: the SDK's processors and reader,
/// the blocking OTLP/HTTP client and the gRPC channels' runtime.
pub const EXPORTER_THREADS: [&str; 3] = [
    "OpenTelemetry.",
    "reqwest-internal-sync-runtime",
    "merezha-otlp-",
];

fn crate_of(target: &str) -> &str {
    target.split("::").next().unwrap_or(target)
}
/// Whether `target` belongs to one of [`EXPORTER_TARGETS`].
pub fn is_exporter(target: &str) -> bool {
    let krate = crate_of(target);
    EXPORTER_TARGETS
        .iter()
        .any(|prefix| krate.starts_with(prefix))
}
/// Whether `target` belongs to one of [`TRANSPORT_TARGETS`].
pub fn is_transport(target: &str) -> bool {
    let krate = crate_of(target);
    TRANSPORT_TARGETS
        .iter()
        .any(|prefix| krate.starts_with(prefix))
}
/// Whether `thread` is one of [`EXPORTER_THREADS`].
pub fn is_exporter_thread(thread: Option<&str>) -> bool {
    thread.is_some_and(|name| {
        EXPORTER_THREADS
            .iter()
            .any(|prefix| name.starts_with(prefix))
    })
}

/// One level for `merezha*` targets and one for every other target.
/// Exporter crates are always refused; transport crates are capped at
/// `warn` and refused on exporter threads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TargetFilter {
    pub merezha: LevelFilter,
    pub other: LevelFilter,
}
impl TargetFilter {
    /// `merezha*` targets only.
    pub fn merezha_only(level: LevelFilter) -> Self {
        Self {
            merezha: level,
            other: LevelFilter::OFF,
        }
    }
    /// The level a target is admitted at, before the thread check.
    fn level_for(&self, target: &str) -> LevelFilter {
        if is_merezha(target) {
            self.merezha
        } else if is_exporter(target) {
            LevelFilter::OFF
        } else if is_transport(target) {
            self.other.min(LevelFilter::WARN)
        } else {
            self.other
        }
    }
    /// Whether an event or span of `target` at `level` passes on `thread`.
    pub fn admits(&self, target: &str, level: &tracing::Level, thread: Option<&str>) -> bool {
        level <= &self.level_for(target) && !(is_transport(target) && is_exporter_thread(thread))
    }
}
impl<S> Filter<S> for TargetFilter {
    fn enabled(&self, meta: &Metadata<'_>, _cx: &Context<'_, S>) -> bool {
        let target = meta.target();
        if is_transport(target) {
            let thread = std::thread::current();
            self.admits(target, meta.level(), thread.name())
        } else {
            self.admits(target, meta.level(), None)
        }
    }
    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> Interest {
        if !(meta.level() <= &self.level_for(meta.target())) {
            Interest::never()
        } else if is_transport(meta.target()) {
            // Decided per event: the thread matters.
            Interest::sometimes()
        } else {
            Interest::always()
        }
    }
    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(self.merezha.max(self.other))
    }
}

/// The span bridge: no exception, location, thread, target or timing
/// attributes, so spans carry only what the code records. Error events do
/// not set the span status, and events with `otel.*` fields are refused:
/// span names and statuses come from span fields set by code only.
pub fn span_bridge(tracer: opentelemetry_sdk::trace::SdkTracer) -> BoxedLayer {
    let bridge = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_error_fields_to_exceptions(false)
        .with_error_records_to_exceptions(false)
        .with_error_events_to_exceptions(false)
        .with_error_events_to_status(false)
        .with_location(false)
        .with_threads(false)
        .with_target(false)
        .with_tracked_inactivity(false);
    NoOtelEvents(bridge)
        .with_filter(TargetFilter::merezha_only(LevelFilter::INFO))
        .boxed()
}

/// Forwards everything to the span bridge except events carrying `otel.*`
/// fields, which `tracing-opentelemetry` would turn into a span name, kind
/// or status. Span names and statuses come from span fields set by code.
struct NoOtelEvents<L>(L);
impl<S, L> Layer<S> for NoOtelEvents<L>
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    L: Layer<S>,
{
    fn on_register_dispatch(&self, subscriber: &tracing::Dispatch) {
        self.0.on_register_dispatch(subscriber);
    }
    fn on_layer(&mut self, subscriber: &mut S) {
        self.0.on_layer(subscriber);
    }
    fn register_callsite(&self, meta: &'static Metadata<'static>) -> Interest {
        self.0.register_callsite(meta)
    }
    fn enabled(&self, meta: &Metadata<'_>, ctx: Context<'_, S>) -> bool {
        self.0.enabled(meta, ctx)
    }
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        self.0.on_new_span(attrs, id, ctx);
    }
    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: Context<'_, S>,
    ) {
        self.0.on_record(id, values, ctx);
    }
    fn on_follows_from(
        &self,
        span: &tracing::span::Id,
        follows: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        self.0.on_follows_from(span, follows, ctx);
    }
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let otel = event
            .metadata()
            .fields()
            .iter()
            .any(|field| field.name().starts_with("otel."));
        if !otel {
            self.0.on_event(event, ctx);
        }
    }
    fn on_enter(&self, id: &tracing::span::Id, ctx: Context<'_, S>) {
        self.0.on_enter(id, ctx);
    }
    fn on_exit(&self, id: &tracing::span::Id, ctx: Context<'_, S>) {
        self.0.on_exit(id, ctx);
    }
    fn on_close(&self, id: tracing::span::Id, ctx: Context<'_, S>) {
        self.0.on_close(id, ctx);
    }
    fn on_id_change(&self, old: &tracing::span::Id, new: &tracing::span::Id, ctx: Context<'_, S>) {
        self.0.on_id_change(old, new, ctx);
    }
    // `tracing-opentelemetry` finds its context through `downcast_raw`
    // (`OpenTelemetrySpanExt`), so the wrapper must expose the inner layer.
    unsafe fn downcast_raw(&self, id: std::any::TypeId) -> Option<*const ()> {
        if id == std::any::TypeId::of::<Self>() {
            Some(self as *const Self as *const ())
        } else {
            // SAFETY: forwarded to the inner layer with the same contract.
            unsafe { self.0.downcast_raw(id) }
        }
    }
}

/// OTLP severity of a `tracing` level: ERROR 17, WARN 13, INFO 9, DEBUG 5,
/// TRACE 1.
pub fn severity(level: &tracing::Level) -> Severity {
    match *level {
        tracing::Level::ERROR => Severity::Error,
        tracing::Level::WARN => Severity::Warn,
        tracing::Level::INFO => Severity::Info,
        tracing::Level::DEBUG => Severity::Debug,
        _ => Severity::Trace,
    }
}

fn any_value(value: &FieldValue) -> AnyValue {
    match value {
        FieldValue::Str(text) => {
            match cap_value(opentelemetry::Value::from(text.clone()), LOG_STRING_CAP) {
                opentelemetry::Value::String(s) => AnyValue::String(s),
                _ => AnyValue::from(String::new()),
            }
        }
        FieldValue::U64(v) => AnyValue::Int(i64::try_from(*v).unwrap_or(i64::MAX)),
        FieldValue::I64(v) => AnyValue::Int(*v),
        FieldValue::F64(v) => AnyValue::Double(*v),
        FieldValue::Bool(v) => AnyValue::Boolean(*v),
    }
}

/// The OTLP log bridge. The event name is the catalog name and the body is
/// the same constant, so log lines are never empty; attributes are the
/// catalog's attribute keys for the fields it maps, filtered by the
/// allowlist and the privacy switches.
pub struct LogBridge {
    logger: SdkLogger,
    allowlist: Allowlist,
    catalog: fn(&str) -> Option<&'static names::Event>,
}
impl LogBridge {
    pub fn new(logger: SdkLogger, allowlist: Allowlist) -> Self {
        Self {
            logger,
            allowlist,
            catalog: names::event,
        }
    }
}
impl<S: Subscriber> Layer<S> for LogBridge {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        if !is_merezha(meta.target()) {
            return;
        }
        let fields = Fields::of(event);
        let Some(row) = event_name(event, &fields)
            .as_deref()
            .and_then(self.catalog)
            .filter(|row| row.export)
        else {
            return;
        };
        let mut record = self.logger.create_log_record();
        record.set_event_name(row.name);
        record.set_severity_number(severity(meta.level()));
        record.set_severity_text(meta.level().as_str());
        record.set_body(AnyValue::from(row.name));
        record.set_timestamp(std::time::SystemTime::now());
        for (name, value) in &fields.values {
            if let Some(key) = row.field(name).and_then(|f| f.key)
                && self.allowlist.allows(key, Column::Log)
            {
                record.add_attribute(Key::from_static_str(key), any_value(value));
            }
        }
        self.logger.emit(record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PrivacyConfig;
    use opentelemetry_sdk::logs::{InMemoryLogExporter, SdkLoggerProvider, SimpleLogProcessor};
    use tracing_subscriber::layer::SubscriberExt;

    const fn field(name: &'static str, key: &'static str) -> names::Field {
        names::Field {
            name,
            key: Some(key),
            json: false,
        }
    }
    /// A row mapping fields to a gated key, a log-forbidden key and two
    /// plain ones; the real catalog has no such row (its test forbids it).
    static PROBE: names::Event = names::Event {
        name: "probe.emitted",
        levels: &[tracing::Level::INFO],
        fields: &[
            field("user", "user.id"),
            field("route", "http.route"),
            field("tenant", "merezha.tenant.id"),
            field("module", "merezha.module"),
        ],
        export: true,
    };
    fn probe(name: &str) -> Option<&'static names::Event> {
        (name == PROBE.name).then_some(&PROBE)
    }
    fn keys(privacy: PrivacyConfig) -> Vec<String> {
        let exporter = InMemoryLogExporter::default();
        let provider = SdkLoggerProvider::builder()
            .with_log_processor(SimpleLogProcessor::new(exporter.clone()))
            .build();
        use opentelemetry::logs::LoggerProvider as _;
        let bridge = LogBridge {
            logger: provider.logger("probe"),
            allowlist: Allowlist::new(privacy),
            catalog: probe,
        };
        let subscriber = tracing_subscriber::registry().with(bridge);
        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                name: "probe.emitted",
                target: "merezha",
                tracing::Level::INFO,
                user = "u-1",
                route = "/r",
                tenant = "t-1",
                module = "app",
                "probe.emitted"
            );
        });
        let logs = exporter.get_emitted_logs().unwrap();
        logs[0]
            .record
            .attributes_iter()
            .map(|(k, _)| k.to_string())
            .collect()
    }

    #[test]
    fn the_log_bridge_drops_gated_and_log_forbidden_keys() {
        assert_eq!(
            keys(PrivacyConfig::default()),
            ["merezha.tenant.id", "merezha.module"]
        );
        let open = PrivacyConfig {
            record_tenant_id: false,
            record_subject_id: true,
            record_client_address: true,
        };
        assert_eq!(keys(open), ["user.id", "merezha.module"]);
    }

    #[test]
    fn exporter_crates_are_refused_and_transports_kept_at_warn_off_exporter_threads() {
        use tracing::Level;
        let stderr = TargetFilter {
            merezha: LevelFilter::DEBUG,
            other: LevelFilter::INFO,
        };
        for target in [
            "opentelemetry_sdk",
            "opentelemetry-otlp",
            "tonic::transport",
        ] {
            assert!(!stderr.admits(target, &Level::ERROR, None), "{target}");
        }
        for target in [
            "hyper_util::client",
            "h2::proto",
            "tower_http::trace",
            "reqwest::connect",
        ] {
            assert!(
                stderr.admits(target, &Level::WARN, Some("tokio-runtime-worker")),
                "{target}"
            );
            assert!(
                !stderr.admits(target, &Level::INFO, Some("main")),
                "{target} at info"
            );
            for thread in [
                "OpenTelemetry.Traces.BatchProcessor",
                "reqwest-internal-sync-runtime",
                "merezha-otlp-grpc",
            ] {
                assert!(
                    !stderr.admits(target, &Level::ERROR, Some(thread)),
                    "{target} on {thread}"
                );
            }
        }
        assert!(stderr.admits("sqlx", &Level::INFO, None));
        assert!(stderr.admits(
            "merezha",
            &Level::DEBUG,
            Some("OpenTelemetry.Logs.BatchProcessor")
        ));
        assert!(!stderr.admits("axum::serve", &Level::DEBUG, None));
    }
}
