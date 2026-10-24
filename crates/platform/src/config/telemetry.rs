//! `[telemetry]`: the kernel's stderr log keys and the telemetry block's
//! extensions (TE §3). The kernel validates every key now; the OTLP
//! pipeline that uses them arrives with the telemetry block.
use super::{
    ConfigIssue, Layers, LogFormat, LogLevel, Origin, SecretResolver, TelemetryConfig, reader,
};
use std::{collections::BTreeMap, path::Path, time::Duration};

const fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}
/// The OTLP endpoint when none is configured.
pub const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:4318";
const MAX_HEADERS: usize = 32;
const MAX_RESOURCE_ATTRIBUTES: usize = 16;
/// Resource attribute keys must start with one of these.
pub const RESOURCE_ATTRIBUTE_PREFIXES: [&str; 6] = [
    "cloud.",
    "container.",
    "deployment.",
    "host.",
    "k8s.",
    "os.",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    HttpProtobuf,
    Grpc,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    Gzip,
    None,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Signal {
    Traces,
    Metrics,
    Logs,
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
/// `[telemetry.resource]`. `None` falls back as TE §3 says: the service
/// name to `app.id`, the version to `app.version`.
#[derive(Clone, Debug, PartialEq, Eq)]
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
/// `[telemetry.metrics]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsConfig {
    pub interval: Duration,
    pub temporality: Temporality,
    pub sample_interval: Duration,
    pub endpoint: Option<String>,
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
/// `[telemetry.privacy]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrivacyConfig {
    pub record_tenant_id: bool,
    pub record_subject_id: bool,
    pub record_client_address: bool,
}

const LEVELS: [(&str, LogLevel); 5] = [
    ("error", LogLevel::Error),
    ("warn", LogLevel::Warn),
    ("info", LogLevel::Info),
    ("debug", LogLevel::Debug),
    ("trace", LogLevel::Trace),
];

/// `scheme://host[:port][/path]` with scheme `http` or `https`, no userinfo,
/// query or fragment.
fn valid_endpoint(text: &str) -> Option<reqwest::Url> {
    let url = reqwest::Url::parse(text).ok()?;
    let ok = matches!(url.scheme(), "http" | "https")
        && url.host_str().is_some_and(|h| !h.is_empty())
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
        && text.len() <= 2048;
    ok.then_some(url)
}
fn plaintext_remote(url: &reqwest::Url) -> bool {
    let host = url.host_str().unwrap_or_default();
    let loopback = host == "localhost"
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    url.scheme() == "http" && !loopback
}
/// Lowercase RFC 9110 token, except headers the exporter owns.
fn valid_header_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && !matches!(name, "content-type" | "te")
        && name.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$%&'*+-.^_`|~".contains(&b)
        })
}
fn valid_label(text: &str) -> bool {
    (1..=128).contains(&text.len()) && text.bytes().all(|b| b.is_ascii_graphic() || b == b' ')
}
fn valid_host(host: &str) -> bool {
    (1..=253).contains(&host.len())
        && host
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
}

pub(super) fn telemetry(
    layers: &mut Layers,
    base_dir: &Path,
    resolver: &SecretResolver,
    issues: &mut Vec<ConfigIssue>,
) -> TelemetryConfig {
    let (mut r, _) = reader(layers, "telemetry", issues);
    let log_level = r.choice("log_level", LogLevel::Info, &LEVELS);
    let format = r.choice(
        "format",
        LogFormat::Json,
        &[("json", LogFormat::Json), ("text", LogFormat::Text)],
    );
    let log_buffer_lines = r.integer("log_buffer_lines", 8_192, 64, 1_048_576) as u32;
    let endpoint_origin = r.origin_of("endpoint").unwrap_or(Origin::Default);
    let endpoint = match r.string("endpoint") {
        Some((value, origin)) => match valid_endpoint(&value) {
            Some(url) => Some((value, url)),
            None => {
                r.issue("endpoint", "invalid_endpoint", origin);
                None
            }
        },
        None => valid_endpoint(DEFAULT_ENDPOINT).map(|url| (DEFAULT_ENDPOINT.to_owned(), url)),
    };
    let protocol = r.choice(
        "protocol",
        Protocol::HttpProtobuf,
        &[
            ("http/protobuf", Protocol::HttpProtobuf),
            ("grpc", Protocol::Grpc),
        ],
    );
    let compression = r.choice(
        "compression",
        Compression::Gzip,
        &[("gzip", Compression::Gzip), ("none", Compression::None)],
    );
    let timeout = r.duration("timeout_ms", ms(10_000), (ms(1_000), ms(60_000)));
    let ca_file = r.path("ca_file", base_dir);
    let allow_plaintext_headers = r.boolean("allow_plaintext_headers", false);
    let shutdown_timeout = r.duration("shutdown_timeout_ms", ms(5_000), (ms(100), ms(60_000)));
    let signals_origin = r.origin_of("signals");
    let mut signals = Vec::new();
    let listed = r.list("signals", 3);
    for (name, origin) in &listed {
        let signal = match name.as_str() {
            "traces" => Signal::Traces,
            "metrics" => Signal::Metrics,
            "logs" => Signal::Logs,
            _ => {
                r.issue("signals", "invalid_signal", origin.clone());
                continue;
            }
        };
        if signals.contains(&signal) {
            r.issue("signals", "duplicate_signal", origin.clone());
        } else {
            signals.push(signal);
        }
    }
    if signals_origin.is_none() {
        signals = vec![Signal::Traces, Signal::Metrics, Signal::Logs];
    }
    r.finish();

    // [telemetry.headers]: every value is a secret.
    let (mut r, _) = reader(layers, "telemetry.headers", issues);
    let mut headers = BTreeMap::new();
    let entries = std::mem::take(&mut r.entries);
    if entries.len() > MAX_HEADERS {
        r.issues.push(ConfigIssue::new(
            "telemetry.headers",
            "too_many_items",
            Origin::File,
        ));
    }
    for (name, entry) in entries {
        // Environment names cannot hold `-`: `MEREZHA_TELEMETRY__HEADERS__X_API_KEY`
        // sets `x-api-key`.
        let name = match entry.origin {
            Origin::Env(_) => name.replace('_', "-"),
            _ => name,
        };
        if !valid_header_name(&name) {
            r.issue(&name, "invalid_header_name", entry.origin);
            continue;
        }
        match resolver.resolve(&entry.value, &entry.origin) {
            Ok(secret) => {
                headers.insert(name, secret);
            }
            Err(code) => r.issue(&name, code, entry.origin),
        }
    }
    r.finish();

    let (mut r, _) = reader(layers, "telemetry.resource", issues);
    let label = |r: &mut super::Reader<'_>, key: &str| match r.string(key) {
        Some((value, _)) if value.is_empty() => None,
        Some((value, _)) if valid_label(&value) => Some(value),
        Some((_, origin)) => {
            r.issue(key, "invalid_label", origin);
            None
        }
        None => None,
    };
    let service_name = label(&mut r, "service_name");
    let service_version = label(&mut r, "service_version");
    let service_namespace = label(&mut r, "service_namespace");
    let environment = label(&mut r, "environment");
    r.finish();
    let (mut r, _) = reader(layers, "telemetry.resource.attributes", issues);
    let entries = std::mem::take(&mut r.entries);
    if entries.len() > MAX_RESOURCE_ATTRIBUTES {
        r.issues.push(ConfigIssue::new(
            "telemetry.resource.attributes",
            "too_many_items",
            Origin::File,
        ));
    }
    let mut attributes = BTreeMap::new();
    for (key, entry) in entries {
        let allowed = key.len() <= 64
            && RESOURCE_ATTRIBUTE_PREFIXES
                .iter()
                .any(|p| key.starts_with(p))
            && key
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._".contains(&b));
        match entry.value {
            toml::Value::String(value) if allowed && valid_label(&value) => {
                attributes.insert(key, value);
            }
            toml::Value::String(_) if allowed => r.issue(&key, "invalid_label", entry.origin),
            _ if !allowed => r.issue(&key, "invalid_resource_attribute", entry.origin),
            _ => r.issue(&key, "expected_string", entry.origin),
        }
    }
    r.finish();
    let resource = ResourceConfig {
        service_name,
        service_version,
        service_namespace,
        environment,
        attributes,
    };

    let signal_endpoint = |r: &mut super::Reader<'_>| match r.string("endpoint") {
        Some((value, _)) if value.is_empty() => None,
        Some((value, origin)) => match valid_endpoint(&value) {
            Some(url) => Some((value, url, origin)),
            None => {
                r.issue("endpoint", "invalid_endpoint", origin);
                None
            }
        },
        None => None,
    };
    let queue = |r: &mut super::Reader<'_>, delay: Duration| {
        let queue_origin = r.origin_of("max_batch");
        let max_queue = r.integer("max_queue", 2_048, 1, 65_536) as u32;
        let max_batch = r.integer("max_batch", 512, 1, 65_536) as u32;
        if max_batch > max_queue {
            r.issue(
                "max_batch",
                "batch_exceeds_queue",
                queue_origin.unwrap_or(Origin::Default),
            );
        }
        let schedule_delay = r.duration("schedule_delay_ms", delay, (ms(100), ms(60_000)));
        (max_queue, max_batch, schedule_delay)
    };
    let mut endpoints = Vec::new();
    let (mut r, _) = reader(layers, "telemetry.traces", issues);
    let sampler = r.choice(
        "sampler",
        Sampler::ParentBasedRatio,
        &[
            ("parent_based_ratio", Sampler::ParentBasedRatio),
            ("always_off", Sampler::AlwaysOff),
        ],
    );
    let ratio = r.float("ratio", 1.0, 0.0, 1.0);
    let trust_inbound = r.choice(
        "trust_inbound",
        TrustInbound::TrustedProxies,
        &[
            ("trusted_proxies", TrustInbound::TrustedProxies),
            ("always", TrustInbound::Always),
            ("never", TrustInbound::Never),
        ],
    );
    let traces_endpoint = signal_endpoint(&mut r);
    let (max_queue, max_batch, schedule_delay) = queue(&mut r, ms(5_000));
    r.finish();
    endpoints.extend(traces_endpoint.clone());
    let traces = TracesConfig {
        sampler,
        ratio,
        trust_inbound,
        endpoint: traces_endpoint.map(|(text, _, _)| text),
        max_queue,
        max_batch,
        schedule_delay,
    };
    let (mut r, _) = reader(layers, "telemetry.metrics", issues);
    let interval = r.duration("interval_ms", ms(30_000), (ms(1_000), ms(3_600_000)));
    let temporality = r.choice(
        "temporality",
        Temporality::Cumulative,
        &[
            ("cumulative", Temporality::Cumulative),
            ("delta", Temporality::Delta),
        ],
    );
    let sample_interval = r.duration("sample_interval_ms", ms(15_000), (ms(1_000), ms(3_600_000)));
    let metrics_endpoint = signal_endpoint(&mut r);
    r.finish();
    endpoints.extend(metrics_endpoint.clone());
    let metrics = MetricsConfig {
        interval,
        temporality,
        sample_interval,
        endpoint: metrics_endpoint.map(|(text, _, _)| text),
    };
    let (mut r, _) = reader(layers, "telemetry.logs", issues);
    let level = r.choice("level", LogLevel::Info, &LEVELS);
    let logs_endpoint = signal_endpoint(&mut r);
    let (max_queue, max_batch, schedule_delay) = queue(&mut r, ms(1_000));
    r.finish();
    endpoints.extend(logs_endpoint.clone());
    let logs = LogsConfig {
        level,
        endpoint: logs_endpoint.map(|(text, _, _)| text),
        max_queue,
        max_batch,
        schedule_delay,
    };
    let (mut r, _) = reader(layers, "telemetry.privacy", issues);
    let privacy = PrivacyConfig {
        record_tenant_id: r.boolean("record_tenant_id", true),
        record_subject_id: r.boolean("record_subject_id", false),
        record_client_address: r.boolean("record_client_address", false),
    };
    r.finish();
    let (mut r, _) = reader(layers, "telemetry.propagation", issues);
    let mut propagation = Vec::new();
    for (host, origin) in r.list("outbound_hosts", 64) {
        if valid_host(&host) {
            propagation.push(host);
        } else {
            r.issue("outbound_hosts", "invalid_host", origin);
        }
    }
    r.finish();

    // Header secrets never travel in clear to a remote collector unless the
    // operator says so.
    if !headers.is_empty() && !allow_plaintext_headers {
        if endpoint
            .as_ref()
            .is_some_and(|(_, url)| plaintext_remote(url))
        {
            issues.push(ConfigIssue::new(
                "telemetry.headers",
                "plaintext_headers_refused",
                endpoint_origin.clone(),
            ));
        } else if let Some((_, _, origin)) =
            endpoints.iter().find(|(_, url, _)| plaintext_remote(url))
        {
            issues.push(ConfigIssue::new(
                "telemetry.headers",
                "plaintext_headers_refused",
                origin.clone(),
            ));
        }
    }
    TelemetryConfig {
        log_level,
        format,
        log_buffer_lines,
        endpoint: endpoint.map_or_else(|| DEFAULT_ENDPOINT.to_owned(), |(text, _)| text),
        protocol,
        compression,
        timeout,
        headers,
        ca_file,
        allow_plaintext_headers,
        shutdown_timeout,
        signals,
        resource,
        traces,
        metrics,
        logs,
        privacy,
        propagation,
    }
}
