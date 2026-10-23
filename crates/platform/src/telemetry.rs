//! Structured logs through `tracing`. Events carry a stable `event` field
//! (`listener.ready`, `http.request`, ...) and only allowlisted metadata.
//! The telemetry block adds OTLP exporters through `extra` layers.
use crate::config::{LogFormat, LogLevel, TelemetryConfig};
use merezha_domain::Failure;
use tracing_subscriber::{
    Layer, Registry, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt,
};

/// An additional subscriber layer, such as an OTLP exporter.
pub type ExtraLayer = Box<dyn Layer<Registry> + Send + Sync>;

fn level(level: LogLevel) -> LevelFilter {
    match level {
        LogLevel::Error => LevelFilter::ERROR,
        LogLevel::Warn => LevelFilter::WARN,
        LogLevel::Info => LevelFilter::INFO,
        LogLevel::Debug => LevelFilter::DEBUG,
        LogLevel::Trace => LevelFilter::TRACE,
    }
}

/// Install the process-wide subscriber writing to stderr. `extra` layers see
/// every event regardless of `log_level`, which filters the log output only.
/// Fails `telemetry_initialized` when a subscriber is already installed.
pub fn init(config: &TelemetryConfig, extra: Vec<ExtraLayer>) -> Result<(), Failure> {
    let output = match config.format {
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(false)
            .with_writer(std::io::stderr)
            .with_filter(level(config.log_level))
            .boxed(),
        LogFormat::Text => tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(std::io::stderr)
            .with_filter(level(config.log_level))
            .boxed(),
    };
    // An empty layer list reports no interest in any callsite, which would
    // silence the output layer too; omit it instead.
    let extra = (!extra.is_empty()).then_some(extra);
    tracing_subscriber::registry()
        .with(extra)
        .with(output)
        .try_init()
        .map_err(|_| Failure::permanent("telemetry_initialized"))
}
