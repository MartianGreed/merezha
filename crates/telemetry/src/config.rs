//! The `[telemetry]` section model (TE §3). The kernel's configuration
//! loader (`merezha-platform`) parses and validates every key into these
//! types; the pipeline (feature `pipeline`) consumes them. The model lives
//! here, with default features, so the kernel can build it without linking
//! any exporter.
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

/// The OTLP endpoint when none is configured.
pub const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:4318";

/// Minimum level of a log stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}
impl LogLevel {
    /// The `tracing` level this minimum admits.
    pub fn as_tracing(self) -> tracing::Level {
        match self {
            Self::Error => tracing::Level::ERROR,
            Self::Warn => tracing::Level::WARN,
            Self::Info => tracing::Level::INFO,
            Self::Debug => tracing::Level::DEBUG,
            Self::Trace => tracing::Level::TRACE,
        }
    }
}
/// Format of the stderr log stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Text,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    HttpProtobuf,
    Grpc,
}
impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HttpProtobuf => "http/protobuf",
            Self::Grpc => "grpc",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    Gzip,
    None,
}
/// An OTLP signal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Signal {
    Traces,
    Metrics,
    Logs,
}
impl Signal {
    pub const ALL: [Signal; 3] = [Signal::Traces, Signal::Metrics, Signal::Logs];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Traces => "traces",
            Self::Metrics => "metrics",
            Self::Logs => "logs",
        }
    }
    /// The OTLP/HTTP path appended to a base endpoint.
    pub fn http_path(self) -> &'static str {
        match self {
            Self::Traces => "/v1/traces",
            Self::Metrics => "/v1/metrics",
            Self::Logs => "/v1/logs",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sampler {
    ParentBasedRatio,
    AlwaysOff,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrustInbound {
    TrustedProxies,
    Always,
    Never,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Temporality {
    Cumulative,
    Delta,
}

/// An OTLP header value. Every header value is a secret: `Debug` and
/// `Display` print `<secret>`; call [`HeaderSecret::expose`] only where the
/// exporter is built.
#[derive(Clone, PartialEq, Eq)]
pub struct HeaderSecret(Arc<str>);
impl HeaderSecret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(Arc::from(value.into()))
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Debug for HeaderSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<secret>")
    }
}
impl std::fmt::Display for HeaderSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<secret>")
    }
}

/// `[telemetry.resource]`. `None` falls back as TE §3 says: the service
/// name to `app.id`, the version to `app.version`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceConfig {
    pub service_name: Option<String>,
    pub service_version: Option<String>,
    pub service_namespace: Option<String>,
    pub environment: Option<String>,
    pub attributes: BTreeMap<String, String>,
}
/// `[telemetry.traces]`.
#[derive(Clone, Debug, PartialEq)]
pub struct TracesConfig {
    pub sampler: Sampler,
    /// 0.0 to 1.0, root decisions only.
    pub ratio: f64,
    pub trust_inbound: TrustInbound,
    pub endpoint: Option<String>,
    pub max_queue: u32,
    pub max_batch: u32,
    pub schedule_delay: Duration,
}
impl Default for TracesConfig {
    fn default() -> Self {
        Self {
            sampler: Sampler::ParentBasedRatio,
            ratio: 1.0,
            trust_inbound: TrustInbound::TrustedProxies,
            endpoint: None,
            max_queue: 2_048,
            max_batch: 512,
            schedule_delay: Duration::from_millis(5_000),
        }
    }
}
/// `[telemetry.metrics]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsConfig {
    pub interval: Duration,
    pub temporality: Temporality,
    pub sample_interval: Duration,
    pub endpoint: Option<String>,
}
impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(30_000),
            temporality: Temporality::Cumulative,
            sample_interval: Duration::from_millis(15_000),
            endpoint: None,
        }
    }
}
/// `[telemetry.logs]`, the OTLP log bridge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogsConfig {
    pub level: LogLevel,
    pub endpoint: Option<String>,
    pub max_queue: u32,
    pub max_batch: u32,
    pub schedule_delay: Duration,
}
impl Default for LogsConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            endpoint: None,
            max_queue: 2_048,
            max_batch: 512,
            schedule_delay: Duration::from_millis(1_000),
        }
    }
}
/// `[telemetry.privacy]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrivacyConfig {
    pub record_tenant_id: bool,
    pub record_subject_id: bool,
    pub record_client_address: bool,
}
impl Default for PrivacyConfig {
    fn default() -> Self {
        Self {
            record_tenant_id: true,
            record_subject_id: false,
            record_client_address: false,
        }
    }
}

/// `[telemetry]`: the kernel's stderr log keys plus the telemetry block's
/// OTLP settings (TE §3). `Debug` redacts header values. `Default` is the
/// code default of every key, with no endpoint configured: no signal is
/// exported.
#[derive(Clone, Debug, PartialEq)]
pub struct TelemetryConfig {
    pub log_level: LogLevel,
    pub format: LogFormat,
    pub log_buffer_lines: u32,
    pub endpoint: String,
    pub protocol: Protocol,
    pub compression: Compression,
    pub timeout: Duration,
    pub headers: BTreeMap<String, HeaderSecret>,
    pub ca_file: Option<PathBuf>,
    pub allow_plaintext_headers: bool,
    pub shutdown_timeout: Duration,
    /// Empty: no OTLP pipeline; stderr logs, health and status remain.
    /// OTLP export is opt-in: the loader defaults this to every signal
    /// when an endpoint is configured, and to none otherwise.
    pub signals: Vec<Signal>,
    pub resource: ResourceConfig,
    pub traces: TracesConfig,
    pub metrics: MetricsConfig,
    pub logs: LogsConfig,
    pub privacy: PrivacyConfig,
    /// Exact host names that receive `traceparent` on outbound calls.
    pub propagation: Vec<String>,
}
impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_level: LogLevel::Info,
            format: LogFormat::Json,
            log_buffer_lines: 8_192,
            endpoint: DEFAULT_ENDPOINT.to_owned(),
            protocol: Protocol::HttpProtobuf,
            compression: Compression::Gzip,
            timeout: Duration::from_millis(10_000),
            headers: BTreeMap::new(),
            ca_file: None,
            allow_plaintext_headers: false,
            shutdown_timeout: Duration::from_millis(5_000),
            signals: Vec::new(),
            resource: ResourceConfig::default(),
            traces: TracesConfig::default(),
            metrics: MetricsConfig::default(),
            logs: LogsConfig::default(),
            privacy: PrivacyConfig::default(),
            propagation: Vec::new(),
        }
    }
}
impl TelemetryConfig {
    /// Whether `signal` is exported over OTLP.
    pub fn exports(&self, signal: Signal) -> bool {
        self.signals.contains(&signal)
    }
    /// The URL a signal is exported to. `http/protobuf` appends the OTLP
    /// path to the base `endpoint`; a per-signal endpoint is used verbatim;
    /// `grpc` uses the base endpoint as the channel URI.
    pub fn signal_endpoint(&self, signal: Signal) -> String {
        let own = match signal {
            Signal::Traces => self.traces.endpoint.as_ref(),
            Signal::Metrics => self.metrics.endpoint.as_ref(),
            Signal::Logs => self.logs.endpoint.as_ref(),
        };
        if let Some(own) = own {
            return own.clone();
        }
        match self.protocol {
            Protocol::Grpc => self.endpoint.clone(),
            Protocol::HttpProtobuf => {
                format!(
                    "{}{}",
                    self.endpoint.trim_end_matches('/'),
                    signal.http_path()
                )
            }
        }
    }
}

/// The identity a process reports: `service.*` resource attributes and the
/// `service`, `version` and `instance` fields of every JSON log line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Service {
    /// `service.name`: `telemetry.resource.service_name`, else `app.id`;
    /// `merezha` in raw runtime mode.
    pub name: String,
    /// `service.version`: `telemetry.resource.service_version`, else
    /// `app.version`, else `unknown`.
    pub version: String,
    /// `service.instance.id`, always `app.instance_id` (R12). `None` in raw
    /// runtime mode.
    pub instance_id: Option<String>,
    pub namespace: Option<String>,
    /// `deployment.environment.name`.
    pub environment: Option<String>,
    /// Static resource attributes from `[telemetry.resource.attributes]`.
    pub attributes: BTreeMap<String, String>,
}
impl Service {
    /// The host binary version, the `version` field of JSON log lines.
    pub const HOST_VERSION: &'static str = env!("CARGO_PKG_VERSION");

    /// Resolve the service from the project's `[app]` keys and
    /// `[telemetry.resource]`.
    pub fn resolve(
        resource: &ResourceConfig,
        app_id: &str,
        app_version: Option<&str>,
        instance_id: &str,
    ) -> Self {
        Self {
            name: resource
                .service_name
                .clone()
                .unwrap_or_else(|| app_id.to_owned()),
            version: resource
                .service_version
                .clone()
                .or_else(|| app_version.map(str::to_owned))
                .unwrap_or_else(|| "unknown".to_owned()),
            instance_id: Some(instance_id.to_owned()),
            namespace: resource.service_namespace.clone(),
            environment: resource.environment.clone(),
            attributes: resource.attributes.clone(),
        }
    }
    /// The raw runtime (`merezha serve`), which has no project.
    pub fn raw() -> Self {
        Self {
            name: "merezha".to_owned(),
            version: Self::HOST_VERSION.to_owned(),
            instance_id: None,
            namespace: None,
            environment: None,
            attributes: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_resolve_per_protocol_and_signal() {
        let mut config = TelemetryConfig::default();
        assert_eq!(
            config.signal_endpoint(Signal::Traces),
            "http://127.0.0.1:4318/v1/traces"
        );
        config.endpoint = "https://otlp.example.com/otlp/".into();
        assert_eq!(
            config.signal_endpoint(Signal::Logs),
            "https://otlp.example.com/otlp/v1/logs"
        );
        config.metrics.endpoint = Some("https://m.example.com/ingest".into());
        assert_eq!(
            config.signal_endpoint(Signal::Metrics),
            "https://m.example.com/ingest"
        );
        config.protocol = Protocol::Grpc;
        config.endpoint = "http://collector:4317".into();
        assert_eq!(
            config.signal_endpoint(Signal::Traces),
            "http://collector:4317"
        );
    }

    #[test]
    fn header_secrets_never_print() {
        let secret = HeaderSecret::new("Bearer s3cr3t");
        assert_eq!(format!("{secret:?} {secret}"), "<secret> <secret>");
        let mut config = TelemetryConfig::default();
        config.headers.insert("authorization".into(), secret);
        assert!(!format!("{config:?}").contains("s3cr3t"));
    }

    #[test]
    fn service_falls_back_to_the_app_keys() {
        let service = Service::resolve(&ResourceConfig::default(), "tasks", None, "tasks-a:1");
        assert_eq!(
            (
                service.name.as_str(),
                service.version.as_str(),
                service.instance_id.as_deref()
            ),
            ("tasks", "unknown", Some("tasks-a:1"))
        );
        let resource = ResourceConfig {
            service_name: Some("shop".into()),
            service_version: Some("2.0.0".into()),
            ..ResourceConfig::default()
        };
        let service = Service::resolve(&resource, "tasks", Some("1.4.0"), "i");
        assert_eq!(
            (service.name.as_str(), service.version.as_str()),
            ("shop", "2.0.0")
        );
        let service = Service::resolve(&ResourceConfig::default(), "tasks", Some("1.4.0"), "i");
        assert_eq!(service.version, "1.4.0");
        assert_eq!(Service::raw().name, "merezha");
    }
}
