//! The production pipeline through the in-memory harness: the allowlist,
//! the log bridge, the JSON formatter, the catalog check, the canary scan
//! and the self-observation metrics.
use merezha_telemetry::{
    Signal, Telemetry, TelemetryConfig,
    config::{LogFormat, Service},
    names::{self, key},
    testing::{Harness, Recorded, check_catalogued},
};
use opentelemetry::{Value, logs::AnyValue};

const CANARY: &str = "c4n4ry-7f3a9";

/// The code defaults with every signal exported, as an endpoint would.
fn all_signals() -> TelemetryConfig {
    TelemetryConfig {
        signals: Signal::ALL.to_vec(),
        ..TelemetryConfig::default()
    }
}

fn attribute<'a>(span: &'a opentelemetry_sdk::trace::SpanData, k: &str) -> Option<&'a Value> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == k)
        .map(|kv| &kv.value)
}

#[test]
fn spans_keep_only_allowlisted_attributes_and_count_the_rest() {
    let harness = Harness::new();
    {
        let span = tracing::info_span!(
            target: "merezha::test",
            "startup",
            "merezha.module" = "app",
            "merezha.request_id" = "r-1",
            "user.id" = "u-1",
            "db.query.text" = "SELECT 1",
            "merezha.reason" = "x".repeat(400).as_str(),
            password = CANARY,
        );
        let _entered = span.enter();
        merezha_telemetry::event!(INFO, "db.connected", pool_size = 4u64);
    }
    let span = harness.span("startup");
    assert_eq!(attribute(&span, key::MODULE).unwrap().as_str(), "app");
    assert_eq!(attribute(&span, key::REQUEST_ID).unwrap().as_str(), "r-1");
    for dropped in ["user.id", "db.query.text", "password"] {
        assert!(attribute(&span, dropped).is_none(), "{dropped} exported");
    }
    let reason = attribute(&span, key::REASON).unwrap().as_str();
    assert!(
        reason.len() <= 256 && reason.ends_with('…'),
        "{}",
        reason.len()
    );
    // The span event keeps the catalog name and no bridge or foreign keys.
    let event = &span.events.events[0];
    assert_eq!(event.name, "db.connected");
    assert!(event.attributes.is_empty(), "{:?}", event.attributes);
    let counts = harness.counts(Signal::Traces);
    // user.id (privacy off), db.query.text, password and the event's
    // pool_size.
    assert_eq!(counts.attributes_dropped, 4);
    assert_eq!((counts.emitted, counts.exported, counts.pending), (1, 1, 0));
    harness.assert_no_canary(CANARY);
}

#[test]
fn subject_ids_are_exported_only_when_privacy_allows() {
    let mut config = all_signals();
    config.privacy.record_subject_id = true;
    config.privacy.record_tenant_id = false;
    let harness = Harness::with(config, Service::raw());
    tracing::info_span!(target: "merezha::test", "startup", "user.id" = "u-1", "merezha.tenant.id" = "t-1").in_scope(|| {});
    let span = harness.span("startup");
    assert!(attribute(&span, "user.id").is_some());
    assert!(attribute(&span, "merezha.tenant.id").is_none());
}

#[test]
fn the_log_bridge_maps_catalog_fields_to_attribute_keys() {
    let harness = Harness::new();
    tracing::info_span!(target: "merezha::test", "startup").in_scope(|| {
        merezha_telemetry::event!(
            WARN,
            "job.retry_scheduled",
            job_id = "j-1",
            queue = "notify.email",
            kind = "send",
            attempt = 2u64,
            code = "provider_timeout",
            classification = "transient"
        );
    });
    // stderr only, uncatalogued and third-party events never reach OTLP.
    merezha_telemetry::event!(
        INFO,
        "identity.dev_link",
        purpose = "magic_link",
        link = CANARY
    );
    tracing::info!(event = "unknown.thing", secret = CANARY);
    tracing::warn!(target: "some_dependency", secret = CANARY, "{CANARY}");
    // The exporter's own crates never reach stderr.
    tracing::error!(target: "opentelemetry_sdk", "BatchSpanProcessor.ExportError");
    tracing::warn!(target: "opentelemetry-otlp", "OTLP export exhausted retries");
    tracing::info!(target: "hyper_util::client", "connecting");
    // The server's transport warnings stay (as content-free lines).
    tracing::warn!(target: "hyper::server", "connection error {CANARY}");
    let logs = harness.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    let record = &logs[0].record;
    assert_eq!(record.event_name(), Some("job.retry_scheduled"));
    assert_eq!(
        record.severity_number(),
        Some(opentelemetry::logs::Severity::Warn)
    );
    assert_eq!(record.body(), Some(&AnyValue::from("job.retry_scheduled")));
    let attributes: Vec<(String, String)> = record
        .attributes_iter()
        .map(|(k, v)| (k.to_string(), format!("{v:?}")))
        .collect();
    let keys: Vec<&str> = attributes.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(
        keys,
        [
            "job.id",
            "job.queue",
            "job.kind",
            "job.attempt",
            "job.code",
            "job.classification"
        ]
    );
    assert!(
        record.trace_context().is_some(),
        "logs inherit the active span"
    );
    // The dev link reaches stderr only.
    let lines = harness.stderr_lines();
    let dev = lines
        .iter()
        .find(|l| l["event"] == "identity.dev_link")
        .unwrap();
    assert_eq!(dev["link"], CANARY);
    for exporter in [
        "opentelemetry_sdk",
        "opentelemetry-otlp",
        "hyper_util::client",
    ] {
        assert!(
            !lines.iter().any(|l| l["target"] == exporter),
            "{exporter} reached stderr"
        );
    }
    let transport = lines
        .iter()
        .find(|l| l["target"] == "hyper::server")
        .unwrap();
    assert_eq!(
        (transport["event"].as_str(), transport["level"].as_str()),
        (Some("dependency.log"), Some("warn"))
    );
    assert!(transport.get("message").is_none());
    let third_party = lines
        .iter()
        .find(|l| l["target"] == "some_dependency")
        .unwrap();
    assert_eq!(third_party["event"], "dependency.log");
    assert!(third_party.get("message").is_none() && third_party.get("secret").is_none());
}

#[test]
fn json_lines_carry_trace_ids_inside_recording_spans_and_scope_ids() {
    let harness = Harness::new();
    merezha_telemetry::event!(INFO, "health.ready");
    tracing::info_span!(target: "merezha::test", "startup", request_id = "r-9").in_scope(|| {
        merezha_telemetry::event!(INFO, "db.connected", pool_size = 1u64);
    });
    let lines = harness.stderr_lines();
    assert!(lines[0].get("trace_id").is_none());
    let span = harness.span("startup");
    assert_eq!(
        lines[1]["trace_id"],
        span.span_context.trace_id().to_string()
    );
    assert_eq!(lines[1]["span_id"], span.span_context.span_id().to_string());
    assert_eq!(lines[1]["request_id"], "r-9");
    assert_eq!(lines[1]["pool_size"], 1);
}

#[test]
fn text_format_writes_the_stock_compact_lines() {
    let config = TelemetryConfig {
        format: LogFormat::Text,
        ..TelemetryConfig::default()
    };
    let harness = Harness::with(config, Service::raw());
    merezha_telemetry::event!(INFO, "listener.ready", address = "127.0.0.1:1");
    let text = harness.stderr();
    assert!(
        text.contains("listener.ready") && text.contains("address"),
        "{text}"
    );
}

#[test]
fn the_catalog_check_fails_unknown_events_levels_and_fields() {
    let harness = Harness::new();
    merezha_telemetry::event!(INFO, "listener.ready", address = "127.0.0.1:1");
    tracing::warn!(event = "migration.pending", module = "app", version = 1u64);
    harness.assert_catalogued();
    let bad = |name: &str, level, fields: Vec<&'static str>| Recorded {
        target: "merezha_platform::host".into(),
        name: Some(name.into()),
        level,
        fields,
    };
    for (event, problem) in [
        (
            bad("listener.readied", tracing::Level::INFO, vec![]),
            "not in names::EVENTS",
        ),
        (
            bad("startup.failed", tracing::Level::INFO, vec!["code"]),
            "not a catalog level",
        ),
        (
            bad("listener.ready", tracing::Level::INFO, vec!["path"]),
            "uncatalogued field path",
        ),
    ] {
        let error = check_catalogued(&[event]).unwrap_err();
        assert!(error.contains(problem), "{error}");
    }
    let third_party = Recorded {
        target: "hyper::proto".into(),
        name: None,
        level: tracing::Level::DEBUG,
        fields: vec!["anything"],
    };
    assert!(check_catalogued(&[third_party]).is_ok());
}

#[test]
fn self_observation_metrics_read_the_accounts() {
    let harness = Harness::new();
    for _ in 0..3 {
        tracing::info_span!(target: "merezha::test", "startup").in_scope(|| {});
    }
    let exported = harness.metric(names::metric::TELEMETRY_EXPORTED.name);
    assert_eq!(
        exported.value(&[
            ("merezha.telemetry.signal", "traces"),
            ("merezha.outcome", "ok")
        ]),
        Some(3.0)
    );
    let dropped = harness.metric(names::metric::TELEMETRY_DROPPED.name);
    assert_eq!(
        dropped.value(&[
            ("merezha.telemetry.signal", "stderr"),
            ("merezha.reason", "queue_full")
        ]),
        Some(0.0)
    );
    let report = harness.shutdown();
    assert!(report.signals.values().all(|c| c.balanced()), "{report:?}");
}

#[test]
fn the_resource_carries_the_instance_id_and_no_detected_attributes() {
    let mut resource = merezha_telemetry::config::ResourceConfig {
        environment: Some("local".into()),
        ..Default::default()
    };
    resource
        .attributes
        .insert("cloud.region".into(), "eu-west-3".into());
    let service = Service::resolve(&resource, "tasks", Some("1.4.0"), "tasks-a:1");
    let built = merezha_telemetry::pipeline::resource(&service);
    let get = |k: &'static str| {
        built
            .get(&opentelemetry::Key::from_static_str(k))
            .map(|v| v.to_string())
    };
    assert_eq!(get(key::SERVICE_NAME).as_deref(), Some("tasks"));
    assert_eq!(get(key::SERVICE_VERSION).as_deref(), Some("1.4.0"));
    assert_eq!(get(key::SERVICE_INSTANCE_ID).as_deref(), Some("tasks-a:1"));
    assert_eq!(
        get(key::DEPLOYMENT_ENVIRONMENT_NAME).as_deref(),
        Some("local")
    );
    assert_eq!(get("cloud.region").as_deref(), Some("eu-west-3"));
    assert_eq!(built.len(), 5, "no SDK or environment detector ran");
    // Logs exported through the harness carry it.
    let harness = Harness::with(all_signals(), service);
    merezha_telemetry::event!(INFO, "health.ready");
    let logs = harness.logs();
    let instance = logs[0].resource.get(&opentelemetry::Key::from_static_str(
        key::SERVICE_INSTANCE_ID,
    ));
    assert_eq!(
        instance.map(|v| v.to_string()).as_deref(),
        Some("tasks-a:1")
    );
    assert_eq!(harness.stderr_lines()[0]["instance"], "tasks-a:1");
}

#[test]
fn an_unreadable_or_invalid_ca_file_fails_the_pipeline() {
    let dir = std::env::temp_dir().join(format!("merezha-t1-ca-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let garbage = dir.join("garbage.pem");
    std::fs::write(&garbage, "not a certificate").unwrap();
    for ca in [dir.join("missing.pem"), garbage, dir.clone()] {
        let config = TelemetryConfig {
            ca_file: Some(ca.clone()),
            ..all_signals()
        };
        let error = Telemetry::build(&config, &Service::raw(), std::io::sink).unwrap_err();
        assert_eq!(error.code, "invalid_telemetry_ca", "{ca:?}");
    }
    // Without exported signals the file is never read.
    let config = TelemetryConfig {
        ca_file: Some(dir.join("missing.pem")),
        signals: Vec::new(),
        ..TelemetryConfig::default()
    };
    assert!(Telemetry::build(&config, &Service::raw(), std::io::sink).is_ok());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn event_text_never_sets_span_status_or_names() {
    let harness = Harness::new();
    // An event with `otel.*` fields is refused by the span bridge.
    tracing::info_span!(target: "merezha::test", "startup").in_scope(|| {
        tracing::event!(
            target: "merezha::test",
            tracing::Level::ERROR,
            { otel.name = CANARY, otel.status_code = "error", otel.status_description = CANARY, otel.kind = "client" },
            "x"
        );
    });
    let span = harness.span("startup");
    assert_eq!(span.status, opentelemetry::trace::Status::Unset);
    assert_eq!(span.span_kind, opentelemetry::trace::SpanKind::Internal);
    assert!(span.events.events.is_empty(), "the otel.* event is refused");
    // An error event marks the span failed without any event text.
    tracing::info_span!(target: "merezha::test", "catalog.load").in_scope(|| {
        merezha_telemetry::event!(ERROR, "db.unavailable", code = "database_unavailable");
        // A message-less event with an `error` field would set the status
        // description with `with_error_events_to_status(true)`.
        tracing::event!(target: "merezha::test", tracing::Level::ERROR, error = CANARY);
    });
    let span = harness.span("catalog.load");
    assert_eq!(span.status, opentelemetry::trace::Status::error(""));
    // (The uncatalogued event keeps its fields on stderr and fails the
    // catalog check; spans and OTLP logs never carry them.)
    assert!(!format!("{:?}{:?}", harness.spans(), harness.logs()).contains(CANARY));
}

#[test]
fn the_log_bridge_honours_telemetry_logs_level() {
    let mut config = all_signals();
    config.log_level = merezha_telemetry::config::LogLevel::Debug;
    config.logs.level = merezha_telemetry::config::LogLevel::Warn;
    let harness = Harness::with(config, Service::raw());
    merezha_telemetry::event!(INFO, "health.ready");
    merezha_telemetry::event!(
        DEBUG,
        "job.leased",
        job_id = "j-1",
        queue = "q",
        kind = "k",
        attempt = 1u64
    );
    merezha_telemetry::event!(WARN, "health.not_ready", code = "database_unavailable");
    merezha_telemetry::event!(ERROR, "db.unavailable", code = "database_unavailable");
    let logs = harness.logs();
    let exported: Vec<&str> = logs.iter().filter_map(|l| l.record.event_name()).collect();
    assert_eq!(exported, ["health.not_ready", "db.unavailable"]);
    // stderr follows log_level, independently.
    assert_eq!(harness.stderr_lines().len(), 4);
}

/// `Telemetry::meters()` is enabled exactly when metrics are exported, and
/// its meters reach the exporter.
#[test]
fn telemetry_meters_are_enabled_only_when_metrics_are_exported() {
    let harness = Harness::new();
    let meters = harness.telemetry().meters();
    assert!(meters.enabled());
    meters
        .meter("merezha-test")
        .u64_counter("merezha.db.listener.reconnects")
        .build()
        .add(1, &[]);
    assert_eq!(
        harness
            .metric("merezha.db.listener.reconnects")
            .points()
            .len(),
        1
    );
    drop(harness);
    let traces_only = TelemetryConfig {
        signals: vec![Signal::Traces],
        ..TelemetryConfig::default()
    };
    let harness = Harness::with(traces_only, Service::raw());
    assert!(!harness.telemetry().meters().enabled());
}
