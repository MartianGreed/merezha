//! Dispatch and projection telemetry (TE §1.2, §1.3, §2.2, §8): the
//! command and query outcome matrices, guest codes kept off metrics and
//! logs, the cardinality bound on unknown tags, the lag gauges with their
//! sampler, the run-lock instruments and, on PostgreSQL, the command span's
//! context stored with its events and the projection spans' links to it.
//! Runs the production pipeline in memory (`testing::Harness`); the
//! PostgreSQL tests create their own database through
//! `MEREZHA_TEST_DATABASE_URL` and skip when it is unset.
use merezha_application::{
    Application, Authorizer, Guest, GuestSession,
    config::Config,
    postgres::{
        BackgroundOptions, CatchUp, CurrentSpan, MIGRATIONS, MODULE, Pause, PostgresOptions,
        PostgresStore, RunHooks,
    },
};
use merezha_domain::{
    Command, Event, Failure, Identity, MessageKind, Metadata, Operation, Query, Reply,
};
use merezha_postgres::{Database, MigrationMode, ModuleMigrations, TestDatabase, run_migrations};
use merezha_telemetry::{
    TelemetryConfig,
    config::{LogLevel, Service, Signal},
    names::{dispatch, key, metric},
    testing::{Harness, span_attribute},
};
use opentelemetry::trace::Status;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn metadata() -> Metadata {
    Metadata {
        message_id: id("message-7"),
        correlation_id: id("correlation-7"),
        causation_id: Some(id("event/3")),
        actor: None,
    }
}
/// `{"events": n}` decides n events; `{"fail": code, "class": c}` makes
/// the guest reply with that failure.
fn command(tag: &str, payload: Value) -> Command {
    Command {
        tag: id(tag),
        stream: id("s1"),
        payload,
        metadata: metadata(),
        idempotency_key: None,
        expected_version: None,
    }
}
fn query(tag: &str, payload: Value) -> Query {
    Query {
        tag: id(tag),
        payload,
        metadata: metadata(),
        min_position: None,
    }
}
const ROUTES: &str = r#"
[[commands]]
tag="Add"
service="business"
aggregate="Counter"
[[commands]]
tag="Guarded"
service="business"
aggregate="Counter"
[[queries]]
tag="Count"
service="view"
projection="count"
[[projections]]
name="count"
service="view"
initial=0
"#;
fn memory_config() -> Config {
    Config::parse(&format!("version=1\n{ROUTES}")).unwrap()
}

#[derive(Clone)]
struct Fake;
impl Guest for Fake {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["business".into(), "view".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(Fake))
    }
}
fn reply_failure(payload: &Value) -> Option<Failure> {
    let code = payload.get("fail")?.as_str()?;
    Some(match payload["class"].as_str() {
        Some("transient") => Failure::transient(code),
        Some("conflict") => Failure::conflict(code),
        _ => Failure::permanent(code),
    })
}
impl GuestSession for Fake {
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        match operation {
            // The domain's own refusal of a payload.
            Operation::Validate { payload, .. } if payload.get("invalid").is_some() => {
                Err(Failure::permanent("validation_failed"))
            }
            Operation::Validate { payload, .. } => Ok(Reply::Validated { payload }),
            Operation::Decide { payload, .. } => {
                if let Some(failure) = reply_failure(&payload) {
                    return Err(failure);
                }
                let n = payload["events"].as_u64().unwrap_or(1) as usize;
                Ok(Reply::Decided {
                    events: (0..n)
                        .map(|_| Event {
                            tag: id("Added"),
                            schema_version: 1,
                            // `{"event": {"fail": …}}` makes projecting it fail.
                            payload: payload.get("event").cloned().unwrap_or(json!({})),
                        })
                        .collect(),
                })
            }
            Operation::Project { state, events, .. } => {
                if let Some(failure) = events.iter().find_map(|e| reply_failure(&e.event.payload)) {
                    return Err(failure);
                }
                Ok(Reply::Projected {
                    state: json!(state.as_u64().unwrap() + events.len() as u64),
                })
            }
            Operation::Query { payload, state, .. } => match reply_failure(&payload) {
                Some(failure) => Err(failure),
                None => Ok(Reply::Queried { value: state }),
            },
        }
    }
}
/// Denies `Guarded`, as a policy would.
struct Policy;
impl Authorizer for Policy {
    fn authorize(
        &self,
        _: MessageKind,
        tag: &str,
        _: Option<&str>,
        _: &Value,
        _: &Metadata,
    ) -> Result<(), Failure> {
        if tag == "Guarded" {
            Err(Failure::permanent("unauthorized"))
        } else {
            Ok(())
        }
    }
}
fn memory_app(harness: &Harness) -> Application {
    Application::in_memory(memory_config(), Arc::new(Fake), Arc::new(Policy))
        .unwrap()
        .with_meters(&harness.telemetry().meters())
}
/// `canary` appears nowhere but in `merezha.guest.error_code` span
/// attributes: not in stderr, OTLP logs, metrics or any other attribute.
fn assert_canary_only_as_guest_code(harness: &Harness, canary: &str) {
    assert!(!harness.stderr().contains(canary), "stderr");
    assert!(!format!("{:?}", harness.logs()).contains(canary), "logs");
    for name in [
        metric::COMMAND_DURATION.name,
        metric::PROJECTION_CATCH_UP_DURATION.name,
    ] {
        assert!(
            !format!("{:?}", harness.metric(name)).contains(canary),
            "{name}"
        );
    }
    for span in harness.spans() {
        for kv in &span.attributes {
            if kv.key.as_str() != key::GUEST_ERROR_CODE {
                assert!(!kv.value.to_string().contains(canary), "{}", span.name);
            }
        }
        assert!(!span.name.contains(canary));
    }
}
fn outcome_of(span: &opentelemetry_sdk::trace::SpanData) -> Option<String> {
    span_attribute(span, key::OUTCOME)
}

#[test]
fn every_command_outcome_is_recorded_and_only_failed_sets_error() {
    let harness = Harness::new();
    let app = memory_app(&harness);
    let guest_code = "insufficient_funds_canary";
    // (outcome, command, expected error code, span name, tag label)
    let mut keyed = command("Add", json!({"events": 2}));
    keyed.idempotency_key = Some(id("key-1"));
    let mut stale = command("Add", json!({"events": 1}));
    stale.expected_version = Some(41);
    let cases: Vec<(&str, Command, Option<&str>, &str)> = vec![
        ("accepted", keyed.clone(), None, "command Add"),
        ("replayed", keyed, None, "command Add"),
        (
            "rejected",
            command("Add", json!({"fail": guest_code})),
            Some(guest_code),
            "command Add",
        ),
        (
            "rejected",
            command("Missing", json!({})),
            Some("handler_not_found"),
            "command _unknown",
        ),
        (
            "denied",
            command("Guarded", json!({})),
            Some("unauthorized"),
            "command Guarded",
        ),
        (
            "conflict",
            stale,
            Some("concurrency_conflict"),
            "command Add",
        ),
        // The domain's payload refusal is a rejection with its own code.
        (
            "rejected",
            command("Add", json!({"invalid": true})),
            Some("validation_failed"),
            "command Add",
        ),
        (
            "failed",
            command("Add", json!({"fail": "trap"})),
            Some("trap"),
            "command Add",
        ),
        (
            "failed",
            command("Add", json!({"fail": "later", "class": "transient"})),
            Some("later"),
            "command Add",
        ),
    ];
    for (i, (outcome, command, code, name)) in cases.into_iter().enumerate() {
        let result = app.command(command);
        match code {
            None => assert!(result.is_ok(), "{outcome}: {result:?}"),
            // The caller still gets the guest's own code.
            Some(code) => assert_eq!(result.unwrap_err().code, code, "{outcome}"),
        }
        let spans = harness.spans_named(name);
        let span = spans.last().unwrap();
        assert_eq!(harness.spans().len(), i + 1, "one span per command");
        assert_eq!(outcome_of(span).as_deref(), Some(outcome), "{name}");
        assert_eq!(
            matches!(span.status, Status::Error { .. }),
            outcome == "failed",
            "{outcome}: {:?}",
            span.status
        );
        assert_eq!(
            span_attribute(span, key::CORRELATION_ID).as_deref(),
            Some("correlation-7")
        );
        assert_eq!(
            span_attribute(span, key::MESSAGE_ID).as_deref(),
            Some("message-7")
        );
        assert_eq!(
            span_attribute(span, key::CAUSATION_ID).as_deref(),
            Some("event/3")
        );
    }
    // The guest's own codes are a span attribute only.
    let rejected = harness
        .spans_named("command Add")
        .into_iter()
        .find(|s| span_attribute(s, key::GUEST_ERROR_CODE).is_some())
        .unwrap();
    assert_eq!(
        span_attribute(&rejected, key::GUEST_ERROR_CODE).as_deref(),
        Some(guest_code)
    );
    assert_eq!(
        span_attribute(&rejected, key::ERROR_TYPE).as_deref(),
        Some("guest_error")
    );
    // `validation_failed` comes from the guest call path: kept as the code,
    // never as a guest's own.
    let refused = harness
        .spans_named("command Add")
        .into_iter()
        .find(|s| span_attribute(s, key::ERROR_TYPE).as_deref() == Some("validation_failed"))
        .unwrap();
    assert_eq!(outcome_of(&refused).as_deref(), Some("rejected"));
    assert_eq!(span_attribute(&refused, key::GUEST_ERROR_CODE), None);
    let accepted = &harness.spans_named("command Add")[0];
    assert_eq!(
        span_attribute(accepted, key::STREAM_ID).as_deref(),
        Some("Counter/s1")
    );
    assert_eq!(
        span_attribute(accepted, key::EVENTS_COUNT).as_deref(),
        Some("2")
    );
    assert_eq!(
        span_attribute(accepted, key::EVENT_POSITION).as_deref(),
        Some("2")
    );

    let duration = harness.metric(metric::COMMAND_DURATION.name);
    let count = |tag: &str, outcome: &str| {
        duration
            .value(&[(key::COMMAND_TAG, tag), (key::OUTCOME, outcome)])
            .unwrap_or(0.0)
    };
    assert_eq!(count("Add", "accepted"), 1.0);
    assert_eq!(count("Add", "replayed"), 1.0);
    assert_eq!(count("Add", "rejected"), 2.0);
    assert_eq!(count(dispatch::UNKNOWN, "rejected"), 1.0);
    assert_eq!(count("Guarded", "denied"), 1.0);
    assert_eq!(count("Add", "conflict"), 1.0);
    assert_eq!(count("Add", "failed"), 2.0);
    assert_eq!(
        harness
            .metric(metric::EVENTS_APPENDED.name)
            .value(&[(key::COMMAND_TAG, "Add")]),
        Some(2.0)
    );
    assert_eq!(
        harness
            .metric(metric::COMMAND_RECEIPTS_REPLAYED.name)
            .value(&[(key::COMMAND_TAG, "Add")]),
        Some(1.0)
    );
    // `command.completed` at debug, warn for `failed`, never with the
    // guest's code.
    let lines: Vec<_> = harness
        .stderr_lines()
        .into_iter()
        .filter(|l| l["event"] == "command.completed")
        .collect();
    assert_eq!(lines.len(), 9);
    for line in &lines {
        let level = if line["outcome"] == "failed" {
            "warn"
        } else {
            "debug"
        };
        assert_eq!(line["level"], level, "{line}");
        assert_eq!(line["correlation_id"], "correlation-7", "{line}");
    }
    assert!(!harness.stderr().contains(guest_code));
    assert!(!format!("{:?}", harness.logs()).contains(guest_code));
    harness.assert_catalogued();
}

#[test]
fn every_query_outcome_is_recorded() {
    let harness = Harness::new();
    let app = memory_app(&harness);
    assert_eq!(
        app.query(query("Count", json!({}))).unwrap().value,
        json!(0)
    );
    assert_eq!(
        app.query(query("Nope", json!({}))).unwrap_err().code,
        "handler_not_found"
    );
    assert_eq!(
        app.query(query("Count", json!({"fail": "trap"})))
            .unwrap_err()
            .code,
        "trap"
    );
    let ok = harness.span("query Count");
    assert_eq!(outcome_of(&ok).as_deref(), Some("ok"));
    assert_eq!(ok.status, Status::Unset);
    let failed = harness.spans_named("query Count").pop().unwrap();
    assert!(matches!(failed.status, Status::Error { .. }));
    let unknown = harness.span("query _unknown");
    assert_eq!(outcome_of(&unknown).as_deref(), Some("rejected"));
    let duration = harness.metric(metric::QUERY_DURATION.name);
    for (tag, outcome) in [
        ("Count", "ok"),
        ("Count", "failed"),
        (dispatch::UNKNOWN, "rejected"),
    ] {
        assert_eq!(
            duration.value(&[(key::QUERY_TAG, tag), (key::OUTCOME, outcome)]),
            Some(1.0),
            "{tag} {outcome}"
        );
    }
    harness.assert_catalogued();
}

#[test]
fn ten_thousand_unknown_tags_and_guest_codes_stay_within_the_series_budget() {
    let harness = Harness::new();
    let app = memory_app(&harness);
    app.command(command("Add", json!({}))).unwrap();
    for i in 0..10_000 {
        let tag = format!("random_{i}");
        assert!(app.command(command(&tag, json!({}))).is_err());
        let code = format!("guest_code_{i}");
        assert!(
            app.command(command("Add", json!({ "fail": code })))
                .is_err()
        );
    }
    // One configured tag plus `_unknown`, whatever the guest replies.
    let duration = harness.metric(metric::COMMAND_DURATION.name);
    let tags: std::collections::BTreeSet<_> = duration
        .points()
        .iter()
        .map(|p| p.attributes[key::COMMAND_TAG].clone())
        .collect();
    assert_eq!(tags.len(), 2, "{tags:?}");
    assert_eq!(duration.points().len(), 3);
    assert_eq!(
        duration.value(&[(key::COMMAND_TAG, "Add"), (key::OUTCOME, "rejected")]),
        Some(10_000.0)
    );
}

/// The lag gauges after appends without a catch-up, and after it (TE §8).
#[test]
fn projection_lag_gauges_read_appends_without_catch_up_and_zero_after_it() {
    let harness = Harness::new();
    let app = memory_app(&harness);
    let lag = |name: &str| {
        harness
            .metric(name)
            .value(&[(key::PROJECTION_NAME, "count")])
    };
    assert_eq!(lag(metric::PROJECTION_LAG_EVENTS.name), Some(0.0));
    assert_eq!(lag(metric::PROJECTION_LAG_AGE.name), Some(0.0));
    for _ in 0..5 {
        app.command(command("Add", json!({"events": 1}))).unwrap();
    }
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(lag(metric::PROJECTION_LAG_EVENTS.name), Some(5.0));
    assert!(lag(metric::PROJECTION_LAG_AGE.name).unwrap() > 0.0);
    assert_eq!(
        harness.metric(metric::EVENTS_HEAD.name).value(&[]),
        Some(5.0)
    );
    // The sampler reads the store and agrees.
    app.sample_lag().unwrap();
    assert_eq!(lag(metric::PROJECTION_LAG_EVENTS.name), Some(5.0));
    assert!(lag(metric::PROJECTION_LAG_AGE.name).unwrap() >= 0.03);
    // Even inside a request's span, a catch-up span is a root (TE §1.2).
    let request = tracing::info_span!(target: "merezha", "request");
    let caught_up = request.in_scope(|| app.catch_up("count")).unwrap();
    assert_eq!(caught_up.checkpoint, 5);
    assert_eq!(lag(metric::PROJECTION_LAG_EVENTS.name), Some(0.0));
    assert_eq!(lag(metric::PROJECTION_LAG_AGE.name), Some(0.0));
    // The catch-up: one root span, its histogram and processed events.
    let span = harness.span("projection count");
    assert_eq!(span.parent_span_id, opentelemetry::trace::SpanId::INVALID);
    assert_eq!(outcome_of(&span).as_deref(), Some("accepted"));
    assert_eq!(
        span_attribute(&span, key::PROJECTION_TRIGGER).as_deref(),
        Some("admin")
    );
    assert_eq!(
        harness
            .metric(metric::PROJECTION_CATCH_UP_DURATION.name)
            .value(&[
                (key::PROJECTION_NAME, "count"),
                (key::PROJECTION_TRIGGER, "admin"),
                (key::OUTCOME, "accepted"),
            ]),
        Some(1.0)
    );
    assert_eq!(
        harness
            .metric(metric::PROJECTION_EVENTS_PROCESSED.name)
            .value(&[(key::PROJECTION_NAME, "count")]),
        Some(5.0)
    );
    // A catch-up with nothing new leaves no span.
    app.catch_up("count").unwrap();
    assert_eq!(harness.spans_named("projection count").len(), 1);
    // The next append after a catch-up starts a fresh age.
    std::thread::sleep(Duration::from_millis(300));
    app.command(command("Add", json!({"events": 1}))).unwrap();
    assert_eq!(lag(metric::PROJECTION_LAG_EVENTS.name), Some(1.0));
    let age = lag(metric::PROJECTION_LAG_AGE.name).unwrap();
    assert!((0.0..0.25).contains(&age), "{age}");
    harness.assert_catalogued();
}

/// A guest's own failure while projecting is the guest's reply: its code
/// stays on `merezha.guest.error_code`, `error.type` and the outcome
/// follow its classification (a guest `unauthorized` is no denial).
#[test]
fn a_guest_failure_while_projecting_keeps_its_code_off_logs_and_metrics() {
    let harness = Harness::new();
    let canary = "projection_canary_7";
    for (code, class, outcome) in [
        (canary, "permanent", "rejected"),
        ("unauthorized", "permanent", "rejected"),
        ("later", "transient", "failed"),
    ] {
        let app = memory_app(&harness);
        app.command(command(
            "Add",
            json!({"event": {"fail": code, "class": class}}),
        ))
        .unwrap();
        assert_eq!(app.catch_up("count").unwrap_err().code, code);
        let span = harness.spans_named("projection count").pop().unwrap();
        assert_eq!(outcome_of(&span).as_deref(), Some(outcome), "{code}");
        assert_eq!(
            span_attribute(&span, key::ERROR_TYPE).as_deref(),
            Some("guest_error")
        );
        assert_eq!(
            span_attribute(&span, key::GUEST_ERROR_CODE).as_deref(),
            Some(code)
        );
        assert_eq!(
            matches!(span.status, Status::Error { .. }),
            outcome == "failed"
        );
        assert!(
            harness
                .metric(metric::PROJECTION_CATCH_UP_DURATION.name)
                .value(&[(key::OUTCOME, outcome)])
                .is_some(),
            "{code}"
        );
    }
    assert_eq!(
        harness
            .metric(metric::PROJECTION_CATCH_UP_DURATION.name)
            .value(&[(key::OUTCOME, "denied")]),
        None
    );
    assert_canary_only_as_guest_code(&harness, canary);
    harness.assert_catalogued();
}

// ---------------------------------------------------------------------------
// PostgreSQL
// ---------------------------------------------------------------------------

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn pg_config(batch: usize) -> Config {
    Config::parse(&format!(
        "version=1\n[limits]\nbatch_size={batch}\n[storage]\nbackend=\"postgres\"\napplication=\"te-test\"\n{ROUTES}"
    ))
    .unwrap()
}
struct Pg {
    runtime: tokio::runtime::Runtime,
    admin: Database,
    test: TestDatabase,
}
impl Pg {
    fn new() -> Option<Self> {
        let test = TestDatabase::create()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let admin = Database::connect(&test.config()).unwrap();
        runtime.block_on(async {
            let modules = [ModuleMigrations {
                module: MODULE,
                migrations: MIGRATIONS,
            }];
            run_migrations(&admin, &modules, MigrationMode::Apply, deadline())
                .await
                .unwrap();
        });
        Some(Self {
            runtime,
            admin,
            test,
        })
    }
    fn open(&self, harness: &Harness, config: Config, hooks: RunHooks) -> Application {
        self.open_with(&harness.telemetry().meters(), config, hooks)
    }
    fn open_background(&self, harness: &Harness, config: Config) -> Application {
        self.open_options(
            &harness.telemetry().meters(),
            config,
            BackgroundOptions {
                catch_up: CatchUp::Auto,
                instance_id: "te".into(),
                ..BackgroundOptions::default()
            },
        )
    }
    /// An instance recording into `meters`.
    fn open_with(
        &self,
        meter: &merezha_telemetry::Meters,
        config: Config,
        hooks: RunHooks,
    ) -> Application {
        self.open_options(
            meter,
            config,
            BackgroundOptions {
                catch_up: CatchUp::Manual,
                instance_id: "te".into(),
                hooks,
                ..BackgroundOptions::default()
            },
        )
    }
    fn open_options(
        &self,
        meter: &merezha_telemetry::Meters,
        config: Config,
        background: BackgroundOptions,
    ) -> Application {
        let db = Database::connect(&self.test.config()).unwrap();
        let options = PostgresOptions {
            trace: Some(Arc::new(CurrentSpan)),
            background,
            ..PostgresOptions::default()
        };
        let store = Arc::new(
            self.runtime
                .block_on(PostgresStore::connect(&db, &config, options, deadline()))
                .unwrap(),
        );
        Application::with_postgres(config, Arc::new(Fake), Arc::new(Policy), store)
            .unwrap()
            .with_meters(meter)
    }
    fn traces(&self) -> Vec<(Option<String>, Option<String>)> {
        self.runtime.block_on(async {
            let client = self.admin.acquire(deadline()).await.unwrap();
            client
                .query(
                    "SELECT trace_parent, trace_state FROM merezha_app.events ORDER BY position",
                    &[],
                )
                .await
                .unwrap()
                .iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect()
        })
    }
}

/// TE §2.2: `append()` stores the command span's context, sampled or not;
/// projection spans are roots linked to at most 8 distinct source traces.
#[test]
fn events_carry_the_command_span_context_and_projection_spans_link_to_it() {
    let Some(pg) = Pg::new() else { return };
    let harness = Harness::new();
    let app = pg.open(&harness, pg_config(64), RunHooks::default());
    for _ in 0..10 {
        app.command(command("Add", json!({"events": 1}))).unwrap();
    }
    let commands = harness.spans_named("command Add");
    assert_eq!(commands.len(), 10);
    let stored = pg.traces();
    assert_eq!(stored.len(), 10);
    for (span, (parent, state)) in commands.iter().zip(&stored) {
        let context = &span.span_context;
        assert_eq!(
            parent.as_deref(),
            Some(format!("00-{}-{}-01", context.trace_id(), context.span_id()).as_str())
        );
        assert_eq!(state, &None);
    }
    // An explicit catch-up on PostgreSQL runs under the run lock.
    assert_eq!(app.catch_up("count").unwrap().checkpoint, 10);
    let projection = harness.span("projection count");
    assert_eq!(
        projection.parent_span_id,
        opentelemetry::trace::SpanId::INVALID
    );
    assert_eq!(projection.links.len(), dispatch::MAX_SOURCE_LINKS);
    for link in projection.links.iter() {
        assert!(
            commands
                .iter()
                .any(|c| c.span_context.trace_id() == link.span_context.trace_id())
        );
        assert_eq!(
            link.attributes
                .iter()
                .find(|kv| kv.key.as_str() == key::LINK_REASON)
                .map(|kv| kv.value.to_string())
                .as_deref(),
            Some("source_event")
        );
    }
    let mut traces: Vec<_> = projection
        .links
        .iter()
        .map(|l| l.span_context.trace_id())
        .collect();
    traces.dedup();
    assert_eq!(traces.len(), dispatch::MAX_SOURCE_LINKS);
    // Two commands of one request's trace give a span one link, not two.
    let request = tracing::info_span!(target: "merezha", "request");
    request.in_scope(|| {
        for _ in 0..2 {
            app.command(command("Add", json!({"events": 1}))).unwrap();
        }
    });
    drop(request);
    assert_eq!(app.catch_up("count").unwrap().checkpoint, 12);
    let request = harness.span("request");
    let second = harness.spans_named("projection count").pop().unwrap();
    assert_eq!(second.links.len(), 1);
    assert_eq!(
        second.links[0].span_context.trace_id(),
        request.span_context.trace_id()
    );
    harness.assert_catalogued();
}

#[test]
fn an_unsampled_command_still_stores_its_context_with_the_flag_unset() {
    let Some(pg) = Pg::new() else { return };
    let mut config = TelemetryConfig {
        log_level: LogLevel::Debug,
        signals: Signal::ALL.to_vec(),
        ..TelemetryConfig::default()
    };
    config.traces.ratio = 0.0;
    config.metrics.interval = Duration::from_secs(3_600);
    let harness = Harness::with(config, Service::raw());
    let app = pg.open(&harness, pg_config(64), RunHooks::default());
    app.command(command("Add", json!({"events": 2}))).unwrap();
    assert!(harness.spans().is_empty());
    let stored = pg.traces();
    assert_eq!(stored.len(), 2);
    for (parent, _) in &stored {
        let parent = parent.as_deref().unwrap();
        assert_eq!(parent.len(), 55);
        assert!(parent.ends_with("-00"), "{parent}");
    }
    assert_eq!(stored[0], stored[1]);
}

/// An explicit catch-up of `app` on another thread, once its paused commit
/// holds the run lock.
fn hold(
    harness: &Harness,
    app: &Application,
    pause: &Pause,
) -> std::thread::JoinHandle<Result<merezha_application::ProjectionState, Failure>> {
    let app = app.clone();
    let dispatch = harness.dispatch().clone();
    let holder = std::thread::spawn(move || {
        tracing::dispatcher::with_default(&dispatch, || app.catch_up("count"))
    });
    let started = Instant::now();
    while !pause.entered() {
        assert!(started.elapsed() < Duration::from_secs(5), "the run pauses");
        std::thread::sleep(Duration::from_millis(10));
    }
    holder
}

/// Another instance's appends count once sampled; a catch-up there brings
/// the lag to 0. The run lock's holder reports its age, the other instance
/// its contended attempts (AP-A3 sources).
#[test]
fn lag_is_sampled_across_instances_and_run_locks_are_observed() {
    let Some(pg) = Pg::new() else { return };
    let harness = Harness::new();
    let pause = Arc::new(Pause::default());
    let hooks = RunHooks {
        pause: Some(pause.clone()),
        ..RunHooks::default()
    };
    let reader = pg.open(&harness, pg_config(2), hooks);
    // Another host: its instruments are its own (a no-op meter here).
    let writer = pg.open_with(
        &merezha_telemetry::Meters::noop(),
        pg_config(2),
        RunHooks {
            pause: Some(pause.clone()),
            ..RunHooks::default()
        },
    );
    for _ in 0..6 {
        writer
            .command(command("Add", json!({"events": 1})))
            .unwrap();
    }
    reader.sample_lag().unwrap();
    let gauge = |name: &str| {
        harness
            .metric(name)
            .value(&[(key::PROJECTION_NAME, "count")])
    };
    assert_eq!(gauge(metric::PROJECTION_LAG_EVENTS.name), Some(6.0));
    assert!(gauge(metric::PROJECTION_LAG_AGE.name).unwrap() > 0.0);
    assert_eq!(gauge(metric::PROJECTION_WARM.name), Some(1.0));
    assert_eq!(gauge(metric::PROJECTION_REBUILD_REMAINING.name), None);
    // The reader holds the lock in a paused commit while the writer's
    // explicit catch-up waits for it: the holder reports the lock's age.
    pause.arm("count", Duration::from_millis(1_500));
    let holder = hold(&harness, &reader, &pause);
    let held = harness
        .metric(metric::PROJECTION_LOCK_HELD.name)
        .value(&[(key::PROJECTION_NAME, "count")]);
    assert!(held.is_some_and(|age| age > 0.0), "{held:?}");
    assert_eq!(writer.catch_up("count").unwrap().checkpoint, 6);
    assert_eq!(holder.join().unwrap().unwrap().checkpoint, 6);
    // Then the writer holds it and the reader contends.
    for _ in 0..2 {
        writer
            .command(command("Add", json!({"events": 1})))
            .unwrap();
    }
    pause.arm("count", Duration::from_millis(1_500));
    let holder = hold(&harness, &writer, &pause);
    assert_eq!(reader.catch_up("count").unwrap().checkpoint, 8);
    assert_eq!(holder.join().unwrap().unwrap().checkpoint, 8);
    let contended = harness
        .metric(metric::PROJECTION_LOCK_CONTENDED.name)
        .value(&[(key::PROJECTION_NAME, "count")]);
    assert!(contended.is_some_and(|n| n >= 1.0), "{contended:?}");
    assert_eq!(
        harness
            .metric(metric::PROJECTION_LOCK_HELD.name)
            .value(&[(key::PROJECTION_NAME, "count")]),
        None
    );
    reader.sample_lag().unwrap();
    assert_eq!(gauge(metric::PROJECTION_LAG_EVENTS.name), Some(0.0));
    assert_eq!(gauge(metric::PROJECTION_LAG_AGE.name), Some(0.0));
    // Explicit catch-ups are `admin` runs.
    assert!(
        harness
            .spans_named("projection count")
            .iter()
            .all(|s| span_attribute(s, key::PROJECTION_TRIGGER).as_deref() == Some("admin"))
    );
    harness.assert_catalogued();
}

#[test]
fn a_rebuild_reports_its_progress_and_its_own_trigger() {
    let Some(pg) = Pg::new() else { return };
    let harness = Harness::new();
    let app = pg.open(&harness, pg_config(4), RunHooks::default());
    for _ in 0..9 {
        app.command(command("Add", json!({"events": 1}))).unwrap();
    }
    assert_eq!(app.rebuild("count").unwrap().checkpoint, 9);
    let rebuild = harness.span("projection count");
    assert_eq!(
        span_attribute(&rebuild, key::PROJECTION_TRIGGER).as_deref(),
        Some("rebuild")
    );
    assert_eq!(
        span_attribute(&rebuild, key::EVENTS_COUNT).as_deref(),
        Some("9")
    );
    assert!(!rebuild.links.is_empty());
    assert_eq!(
        harness
            .metric(metric::PROJECTION_EVENTS_PROCESSED.name)
            .value(&[(key::PROJECTION_NAME, "count")]),
        Some(9.0)
    );
    app.sample_lag().unwrap();
    assert_eq!(
        harness
            .metric(metric::PROJECTION_LAG_EVENTS.name)
            .value(&[(key::PROJECTION_NAME, "count")]),
        Some(0.0)
    );
}

/// Background runs keep the scheduler's dispatcher on their blocking
/// threads (`spawn_blocking_in_span`): their root spans carry the
/// `background` trigger and link to the request that appended.
#[test]
fn background_runs_record_root_spans_linked_to_the_source_request() {
    let Some(pg) = Pg::new() else { return };
    let harness = Harness::new();
    let app = pg.open_background(&harness, pg_config(64));
    let scheduler = {
        let _runtime = pg.runtime.enter();
        merezha_application::postgres::Scheduler::start(&app).unwrap()
    };
    let request = tracing::info_span!(target: "merezha", "request");
    request.in_scope(|| {
        for _ in 0..2 {
            app.command(command("Add", json!({"events": 1}))).unwrap();
        }
    });
    drop(request);
    let started = Instant::now();
    let span = loop {
        if let Some(span) = harness
            .spans_named("projection count")
            .into_iter()
            .find(|s| span_attribute(s, key::EVENT_POSITION).as_deref() == Some("2"))
        {
            break span;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "a background run"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        span_attribute(&span, key::PROJECTION_TRIGGER).as_deref(),
        Some("background")
    );
    assert_eq!(span.parent_span_id, opentelemetry::trace::SpanId::INVALID);
    let request = harness.span("request");
    for run in harness.spans_named("projection count") {
        assert_eq!(run.parent_span_id, opentelemetry::trace::SpanId::INVALID);
        assert_eq!(run.links.len(), 1);
        assert_eq!(
            run.links[0].span_context.trace_id(),
            request.span_context.trace_id()
        );
    }
    pg.runtime.block_on(scheduler.shutdown(deadline()));
    harness.assert_catalogued();
}

/// A guest's own failure in background and rebuild runs on PostgreSQL logs
/// `projection.failed` and `projection.rebuild_failed` with `guest_error`,
/// never the guest's code.
#[test]
fn guest_failures_in_background_runs_log_guest_error() {
    let Some(pg) = Pg::new() else { return };
    let harness = Harness::new();
    let canary = "background_canary_9";
    let app = pg.open_background(&harness, pg_config(64));
    let scheduler = {
        let _runtime = pg.runtime.enter();
        merezha_application::postgres::Scheduler::start(&app).unwrap()
    };
    app.command(command("Add", json!({"event": {"fail": canary}})))
        .unwrap();
    let logged = |event: &str| {
        harness
            .stderr_lines()
            .into_iter()
            .find(|line| line["event"] == event)
    };
    let started = Instant::now();
    let failed = loop {
        if let Some(line) = logged("projection.failed") {
            break line;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "projection.failed"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(failed["code"], "guest_error", "{failed}");
    let span = harness.span("projection count");
    assert_eq!(
        span_attribute(&span, key::PROJECTION_TRIGGER).as_deref(),
        Some("background")
    );
    assert_eq!(
        span_attribute(&span, key::ERROR_TYPE).as_deref(),
        Some("guest_error")
    );
    assert_eq!(outcome_of(&span).as_deref(), Some("rejected"));
    app.admit()
        .unwrap()
        .request_rebuild("count", false, None)
        .unwrap();
    let started = Instant::now();
    let rebuild = loop {
        if let Some(line) = logged("projection.rebuild_failed") {
            break line;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "rebuild_failed"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(rebuild["code"], "guest_error", "{rebuild}");
    pg.runtime.block_on(scheduler.shutdown(deadline()));
    assert_canary_only_as_guest_code(&harness, canary);
    harness.assert_catalogued();
}
