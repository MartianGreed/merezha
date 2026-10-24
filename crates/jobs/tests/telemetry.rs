//! TE-T5 on PostgreSQL: trace context through the outbox and jobs (TE §2.3
//! and the §8 propagation test), the `send`, `relay outbox`, `process` and
//! `admin job.replay` spans, and every `merezha.job.*` and
//! `merezha.outbox.*` instrument, read through the in-memory telemetry
//! harness. Also AP-B2 test 10.
mod support;

use merezha_jobs::{PgJobs, RelayMode};
use merezha_platform::{
    JobsRuntime, Module,
    ports::{JobFailure, NewJob, OnDuplicate, Source, SourceOutbox},
};
use merezha_postgres::TestDatabase;
use merezha_telemetry::{
    Service, TelemetryConfig,
    config::{LogLevel, Signal},
    propagation::{StoredContext, parse_traceparent},
    testing::Harness,
};
use opentelemetry::trace::{SpanKind, Status, TraceContextExt, TraceId};
use opentelemetry_sdk::trace::SpanData;
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use support::*;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Request A's inbound context, sampled.
const TRACE_A: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.to_string())
}
fn named(harness: &Harness, name: &str) -> Vec<SpanData> {
    let mut spans: Vec<SpanData> = harness
        .spans()
        .into_iter()
        .filter(|span| span.name == name)
        .collect();
    spans.sort_by_key(|span| attribute(span, "job.attempt"));
    spans
}
/// Whether `span`'s parent chain reaches `ancestor`.
fn descends_from(
    harness: &Harness,
    span: &SpanData,
    ancestor: opentelemetry::trace::SpanId,
) -> bool {
    let spans = harness.spans();
    let mut parent = span.parent_span_id;
    for _ in 0..16 {
        if parent == ancestor {
            return true;
        }
        match spans.iter().find(|s| s.span_context.span_id() == parent) {
            Some(next) => parent = next.parent_span_id,
            None => return false,
        }
    }
    false
}
/// Wait until `count` spans named `name` were exported.
async fn wait_spans(harness: &Harness, name: &str, count: usize) -> Vec<SpanData> {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let spans = named(harness, name);
        if spans.len() >= count {
            return spans;
        }
        assert!(
            Instant::now() < deadline,
            "{} of {count} `{name}` spans",
            spans.len()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
/// The span of "request A": an edge span whose remote parent is
/// `traceparent`, as the trusted edge makes it (TE §2.1).
fn request(traceparent: &str) -> tracing::Span {
    let span = tracing::info_span!(
        target: "merezha_platform::edge",
        "request",
        otel.name = "POST /tasks",
        otel.kind = "server"
    );
    let remote = parse_traceparent(traceparent, None).unwrap();
    let _ = span.set_parent(opentelemetry::Context::new().with_remote_span_context(remote));
    span
}
/// The runtime with telemetry on `harness`, listening, its `tasks` module
/// owning `tasks.remind` on queue `default`.
async fn instrumented(
    harness: &Harness,
    db: &merezha_postgres::Database,
    config: &merezha_platform::config::JobsConfig,
    mode: RelayMode,
    handler: Option<Arc<dyn merezha_platform::ports::JobHandler>>,
) -> PgJobs {
    let jobs = PgJobs::with_mode(config, mode)
        .with_telemetry(&harness.telemetry().meters(), Duration::from_millis(50));
    jobs.ports(db);
    if let Some(handler) = handler {
        handle(&jobs, "tasks")
            .register("tasks.remind", "default", handler, Default::default())
            .unwrap();
    }
    jobs.start(context(db)).await.unwrap();
    wait_ready(&jobs).await;
    jobs
}
/// The runtime's own outbox port, bound to `tasks`.
fn metered(jobs: &PgJobs, db: &merezha_postgres::Database) -> SourceOutbox {
    SourceOutbox::new(Source::new("tasks").unwrap(), jobs.ports(db).outbox)
}
async fn text(db: &merezha_postgres::Database, sql: &str) -> Option<String> {
    let client = db.acquire(soon()).await.unwrap();
    client.query_one(sql, &[]).await.unwrap().get(0)
}

/// TE §8 propagation across the outbox, steps 1 to 6 (AP-B2 test 10).
///
/// 1. Request A carries a traceparent; inside it a use case appends a job
///    record in its transaction (what a reaction or a command does).
/// 2. The outbox row's `trace_parent` carries A's trace id, and the relay
///    copies it into the job.
/// 3. `process default` is a CONSUMER child of `send default`, in trace A.
/// 4. A forced failure gives a second attempt, still a child of the same
///    `send` span in trace A, hours later or not (R10).
/// 5. Exhaustion dead-letters the job; the operator replay starts a new
///    trace (`admin job.replay`) whose attempt links to trace A.
/// 6. A `notify email` span the handler opens is a child of the attempt.
///
/// Step 7 (no `traceparent` reaches a fake Brevo) needs the outbound
/// client and the email channel (TE-T6, NT-N3); see the PR.
#[tokio::test]
async fn outbox_trace_context_flows_to_every_attempt_and_replays_link_back() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 100\n\
         [jobs.queues.default]\nmax_attempts = 2\nbackoff_base_ms = 10\nbackoff_max_ms = 20",
    );
    let succeed = Arc::new(AtomicBool::new(false));
    let flag = succeed.clone();
    let handler = func(move |_, _| {
        let succeed = flag.load(Ordering::SeqCst);
        Box::pin(async move {
            // What the email channel does inside the attempt (TE §1.2).
            let notify = tracing::info_span!(
                target: "merezha_notifications",
                "notify",
                otel.name = "notify email"
            );
            async {}.instrument(notify).await;
            if succeed {
                Ok(())
            } else {
                Err(JobFailure::transient("provider_busy", None))
            }
        })
    });
    let jobs = instrumented(&harness, &db, &config, RelayMode::Listen, Some(handler)).await;
    let outbox = metered(&jobs, &db);

    // Step 1.
    let records = vec![job_record("remind-1", "default", json!({"task": 1}))];
    let appended = async { append(&db, &outbox, records, OnDuplicate::Fail).await }
        .instrument(request(TRACE_A))
        .await
        .unwrap();
    assert_eq!(appended.inserted, 1);

    // Step 2.
    let stored = text(&db, "SELECT trace_parent FROM merezha_jobs.outbox")
        .await
        .expect("the outbox row carries the send span's context");
    let trace_a = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
    let outbox_context = StoredContext::parse(&stored, None).unwrap().span_context();
    assert_eq!(outbox_context.trace_id(), trace_a);
    assert!(outbox_context.is_sampled());
    let id = wait_for_job(&db).await;
    wait_state(&db, id, "dead", PATIENCE).await;
    let job_parent = text(
        &db,
        &format!("SELECT trace_parent FROM merezha_jobs.jobs WHERE id = {id}"),
    )
    .await;
    assert_eq!(
        job_parent.as_deref(),
        Some(stored.as_str()),
        "relay copies it"
    );

    // Steps 3 and 4.
    let send = harness.span("send default");
    assert_eq!(send.span_kind, SpanKind::Producer);
    assert_eq!(send.span_context.trace_id(), trace_a);
    assert_eq!(send.span_context.span_id(), outbox_context.span_id());
    // A descendant of the request span: inside its transaction (TE-T4's
    // `transaction {module}` span sits in between).
    let edge = harness.span("POST /tasks");
    assert!(
        descends_from(&harness, &send, edge.span_context.span_id()),
        "send is inside request A"
    );
    assert_eq!(
        attribute(&send, "messaging.destination.name").as_deref(),
        Some("default")
    );
    assert_eq!(
        attribute(&send, "job.kind").as_deref(),
        Some("tasks.remind")
    );
    let relays = harness
        .spans()
        .into_iter()
        .filter(|span| span.name == "relay outbox")
        .count();
    assert!(relays >= 1, "the non-empty batch has its span");
    let attempts = wait_spans(&harness, "process default", 2).await;
    assert_eq!(attempts.len(), 2);
    for (n, attempt) in attempts.iter().enumerate() {
        assert_eq!(attempt.span_kind, SpanKind::Consumer);
        assert_eq!(
            attempt.span_context.trace_id(),
            trace_a,
            "attempt {n} in trace A"
        );
        assert_eq!(attempt.parent_span_id, send.span_context.span_id());
        assert_eq!(attribute(attempt, "job.attempt"), Some((n + 1).to_string()));
        assert_eq!(attribute(attempt, "job.max_attempts").as_deref(), Some("2"));
        assert_eq!(attribute(attempt, "job.id"), Some(id.to_string()));
        assert_eq!(
            attribute(attempt, "messaging.message.id"),
            Some(id.to_string())
        );
        assert_eq!(
            attribute(attempt, "job.kind").as_deref(),
            Some("tasks.remind")
        );
        assert_eq!(
            attribute(attempt, "job.code").as_deref(),
            Some("provider_busy")
        );
        assert_eq!(
            attribute(attempt, "job.classification").as_deref(),
            Some("transient")
        );
        assert!(
            matches!(attempt.status, Status::Error { .. }),
            "a transient failure"
        );
        assert!(attempt.links.links.is_empty());
    }
    assert_eq!(
        attribute(&attempts[0], "job.outcome").as_deref(),
        Some("retry")
    );
    assert_eq!(
        attribute(&attempts[1], "job.outcome").as_deref(),
        Some("dead_lettered")
    );

    // Step 6.
    let notified = wait_spans(&harness, "notify email", 2).await;
    let attempt_ids: Vec<_> = attempts.iter().map(|a| a.span_context.span_id()).collect();
    for notify in &notified {
        assert!(
            attempt_ids.contains(&notify.parent_span_id),
            "child of an attempt"
        );
        assert_eq!(notify.span_context.trace_id(), trace_a);
    }

    // Step 5.
    succeed.store(true, Ordering::SeqCst);
    jobs.replay(id, Some("operator")).await.unwrap();
    wait_state(&db, id, "succeeded", PATIENCE).await;
    let replay = harness.span("admin job.replay");
    assert_ne!(replay.span_context.trace_id(), trace_a, "a new trace");
    assert_eq!(attribute(&replay, "job.id"), Some(id.to_string()));
    let attempts = wait_spans(&harness, "process default", 3).await;
    let replayed = attempts
        .iter()
        .find(|a| a.span_context.trace_id() != trace_a)
        .expect("the replayed attempt");
    assert_eq!(
        replayed.span_context.trace_id(),
        replay.span_context.trace_id()
    );
    assert_eq!(replayed.parent_span_id, replay.span_context.span_id());
    assert_eq!(attribute(replayed, "job.attempt").as_deref(), Some("1"));
    assert_eq!(
        attribute(replayed, "job.outcome").as_deref(),
        Some("succeeded")
    );
    assert_eq!(replayed.status, Status::Unset);
    let link = replayed.links.links.first().expect("a replay link");
    assert_eq!(link.span_context.trace_id(), trace_a);
    assert_eq!(link.span_context.span_id(), send.span_context.span_id());
    assert_eq!(
        link.attributes
            .iter()
            .find(|kv| kv.key.as_str() == "merezha.link.reason")
            .map(|kv| kv.value.to_string())
            .as_deref(),
        Some("replay")
    );

    // The counters of the same run.
    let job = [("job.queue", "default"), ("job.kind", "tasks.remind")];
    assert_eq!(
        harness.metric("merezha.job.enqueued").value(&job),
        Some(1.0)
    );
    assert_eq!(
        harness.metric("merezha.job.dead_lettered").value(&job),
        Some(1.0)
    );
    assert_eq!(
        harness
            .metric("merezha.job.replayed")
            .value(&[("job.queue", "default")]),
        Some(1.0)
    );
    let duration = harness.metric("merezha.job.duration");
    for (outcome, class, count) in [
        ("retry", Some("transient"), 1.0),
        ("dead_lettered", Some("transient"), 1.0),
        ("succeeded", None, 1.0),
    ] {
        let point = duration
            .points()
            .iter()
            .find(|p| p.attributes.get("job.outcome").map(String::as_str) == Some(outcome))
            .unwrap_or_else(|| panic!("no {outcome} point"));
        assert_eq!(point.value, count, "{outcome}");
        assert_eq!(
            point
                .attributes
                .get("job.classification")
                .map(String::as_str),
            class
        );
    }
    let terminal = harness.metric("merezha.job.attempts");
    assert_eq!(
        terminal.value(&[("job.outcome", "dead_lettered")]),
        Some(1.0)
    );
    assert_eq!(terminal.value(&[("job.outcome", "succeeded")]), Some(1.0));
    assert_eq!(
        harness
            .metric("merezha.job.queue_latency")
            .value(&[("job.queue", "default")]),
        Some(3.0)
    );
    assert_eq!(
        harness
            .metric("merezha.outbox.appended")
            .value(&[("merezha.module", "tasks"), ("job.record_kind", "job")]),
        Some(1.0)
    );
    assert_eq!(
        harness
            .metric("merezha.outbox.relayed")
            .value(&[("merezha.module", "tasks")]),
        Some(1.0)
    );
    jobs.shutdown(Instant::now() + Duration::from_secs(10))
        .await;
    harness.assert_catalogued();
}

async fn wait_for_job(db: &merezha_postgres::Database) -> i64 {
    wait_for(
        db,
        "SELECT count(*) FROM merezha_jobs.jobs WHERE queue = 'default'",
        1,
        PATIENCE,
    )
    .await;
    scalar(
        db,
        "SELECT min(id) FROM merezha_jobs.jobs WHERE queue = 'default'",
    )
    .await
}

/// TE §8, the unsampled case: with ratio 0 at the origin, no jobs span is
/// recorded, but the unsampled flags travel through the outbox row, the
/// job, the attempt and what the attempt enqueues, all in one trace.
#[tokio::test]
async fn unsampled_origins_record_no_spans_and_keep_their_flags() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let mut telemetry = TelemetryConfig {
        log_level: LogLevel::Debug,
        signals: Signal::ALL.to_vec(),
        ..TelemetryConfig::default()
    };
    telemetry.metrics.interval = Duration::from_secs(3_600);
    telemetry.traces.ratio = 0.0;
    let harness = Harness::with(telemetry, Service::raw());
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs]\npoll_interval_ms = 100");
    let follow_up: Arc<Mutex<Option<merezha_platform::ports::JobsHandle>>> = Arc::default();
    let jobs_slot = follow_up.clone();
    let handler = func(move |ctx, _| {
        let jobs = jobs_slot.lock().unwrap().clone();
        Box::pin(async move {
            // A job enqueued from inside the attempt.
            if ctx.attempt == 1
                && let Some(jobs) = jobs
                && ctx.idempotency_key.ends_with("origin")
            {
                jobs.enqueue(NewJob {
                    queue: id("followup"),
                    kind: id("tasks.followup"),
                    idempotency_key: merezha_platform::ports::DedupeKey::new("next").unwrap(),
                    payload: json!({}),
                    contains_secret: false,
                    run_at: None,
                    priority: 0,
                    context: message(),
                })
                .await
                .map_err(JobFailure::from)?;
            }
            Ok(())
        })
    });
    let jobs = instrumented(&harness, &db, &config, RelayMode::Listen, Some(handler)).await;
    *follow_up.lock().unwrap() = Some(handle(&jobs, "tasks"));
    let outbox = metered(&jobs, &db);
    let origin = tracing::info_span!(target: "merezha", "origin");
    async {
        append(
            &db,
            &outbox,
            vec![job_record("origin", "default", json!({}))],
            OnDuplicate::Fail,
        )
        .await
    }
    .instrument(origin)
    .await
    .unwrap();
    let id = wait_for_job(&db).await;
    wait_state(&db, id, "succeeded", PATIENCE).await;
    wait_for(
        &db,
        "SELECT count(*) FROM merezha_jobs.jobs WHERE queue = 'followup'",
        1,
        PATIENCE,
    )
    .await;
    let stored = text(&db, "SELECT trace_parent FROM merezha_jobs.outbox")
        .await
        .unwrap();
    assert!(stored.ends_with("-00"), "unsampled flags stored: {stored}");
    let next = text(
        &db,
        "SELECT trace_parent FROM merezha_jobs.jobs WHERE queue = 'followup'",
    )
    .await
    .unwrap();
    assert!(next.ends_with("-00"), "{next}");
    let (origin, next) = (
        StoredContext::parse(&stored, None).unwrap().span_context(),
        StoredContext::parse(&next, None).unwrap().span_context(),
    );
    assert_eq!(origin.trace_id(), next.trace_id(), "one trace downstream");
    assert_ne!(origin.span_id(), next.span_id());
    let names: Vec<String> = harness
        .spans()
        .into_iter()
        .map(|s| s.name.into_owned())
        .collect();
    assert!(
        !names
            .iter()
            .any(|n| n.starts_with("process") || n.starts_with("send")),
        "{names:?}"
    );
    jobs.shutdown(Instant::now() + Duration::from_secs(10))
        .await;
}

/// TE §10 T5: the dead-letter counter is exact, through both paths. Three
/// jobs die on a permanent failure (handler path), one lease expires past
/// its last attempt (reaper path), and a job that retries and succeeds
/// never counts.
#[tokio::test]
async fn the_dead_letter_counter_counts_each_dead_job_exactly_once() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 100\nreaper_interval_ms = 100\n\
         [jobs.queues.default]\nmax_attempts = 3\nbackoff_base_ms = 10\nbackoff_max_ms = 20",
    );
    let handler = func(|ctx, payload| {
        Box::pin(async move {
            match payload["mode"].as_str() {
                Some("permanent") => Err(JobFailure::permanent("bad_address")),
                Some("flaky") if ctx.attempt < 2 => {
                    Err(JobFailure::transient("provider_busy", None))
                }
                _ => Ok(()),
            }
        })
    });
    let jobs = instrumented(&harness, &db, &config, RelayMode::Listen, Some(handler)).await;
    let outbox = metered(&jobs, &db);
    let mut records: Vec<_> = (0..3)
        .map(|n| job_record(&format!("p{n}"), "default", json!({"mode": "permanent"})))
        .collect();
    records.push(job_record("flaky", "default", json!({"mode": "flaky"})));
    append(&db, &outbox, records, OnDuplicate::Fail)
        .await
        .unwrap();
    // A lease that expired on its last attempt, as a killed instance leaves it.
    let reaped = insert_job(&db, "tasks.remind", "tasks:reaped", "{}", false).await;
    execute(
        &db,
        &format!(
            "UPDATE merezha_jobs.jobs SET state = 'running', attempts = max_attempts,
                    lease_token = gen_random_uuid(), lease_owner = 'gone',
                    lease_expires_at = now() - interval '1 minute'
              WHERE id = {reaped}"
        ),
    )
    .await
    .unwrap();
    wait_for(
        &db,
        "SELECT count(*) FROM merezha_jobs.jobs WHERE state = 'dead'",
        4,
        PATIENCE,
    )
    .await;
    wait_for(
        &db,
        "SELECT count(*) FROM merezha_jobs.jobs WHERE state = 'succeeded'",
        1,
        PATIENCE,
    )
    .await;
    // Settles and reaps are counted after their statements returned.
    let deadline = Instant::now() + PATIENCE;
    let dead = loop {
        let dead: f64 = harness
            .metric("merezha.job.dead_lettered")
            .points()
            .iter()
            .map(|p| p.value)
            .sum();
        if dead >= 4.0 || Instant::now() > deadline {
            break dead;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // Let a duplicate count, if one were coming, arrive.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let again: f64 = harness
        .metric("merezha.job.dead_lettered")
        .points()
        .iter()
        .map(|p| p.value)
        .sum();
    assert_eq!((dead, again), (4.0, 4.0));
    assert_eq!(
        harness
            .metric("merezha.job.lease_expired")
            .value(&[("job.queue", "default")]),
        Some(1.0)
    );
    let duration = harness.metric("merezha.job.duration");
    assert_eq!(
        duration.value(&[
            ("job.outcome", "dead_lettered"),
            ("job.classification", "permanent")
        ]),
        Some(3.0)
    );
    assert_eq!(duration.value(&[("job.outcome", "retry")]), Some(1.0));
    assert_eq!(duration.value(&[("job.outcome", "succeeded")]), Some(1.0));
    // Recorded at terminal outcomes only: the flaky job's retry is not one.
    let attempts = harness.metric("merezha.job.attempts");
    assert_eq!(
        attempts.value(&[("job.outcome", "dead_lettered")]),
        Some(4.0)
    );
    // A domain rejection is not a host failure: status Unset (TE §1.1).
    let permanent: Vec<_> = wait_spans(&harness, "process default", 5)
        .await
        .into_iter()
        .filter(|span| attribute(span, "job.code").as_deref() == Some("bad_address"))
        .collect();
    assert_eq!(permanent.len(), 3);
    for span in &permanent {
        assert_eq!(
            attribute(span, "job.outcome").as_deref(),
            Some("dead_lettered")
        );
        assert_eq!(span.status, Status::Unset);
    }
    assert_eq!(attempts.value(&[("job.outcome", "succeeded")]), Some(1.0));
    assert_eq!(attempts.value(&[("job.outcome", "retry")]), None);
    jobs.shutdown(Instant::now() + Duration::from_secs(10))
        .await;
    harness.assert_catalogued();
}

/// TE §1.3: the gauges report the sampled queue status, per queue and
/// state, and the outbox backlog; the relay counts per module, and empty
/// relay passes open no span.
#[tokio::test]
async fn gauges_report_the_sampled_queue_status_and_relay_counts_per_module() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs]\nworkers = false\n[jobs.queues.default]\n");
    let jobs = instrumented(&harness, &db, &config, RelayMode::Off, None).await;
    // Empty passes: no span.
    jobs.relay_now().await.unwrap();
    jobs.relay_now().await.unwrap();
    assert!(named(&harness, "relay outbox").is_empty());
    for (key, delay) in [
        ("r1", "- interval '5 seconds'"),
        ("r2", ""),
        ("d1", "+ interval '1 hour'"),
    ] {
        let id = insert_job(&db, "tasks.remind", &format!("tasks:{key}"), "{}", false).await;
        execute(
            &db,
            &format!("UPDATE merezha_jobs.jobs SET run_at = now() {delay} WHERE id = {id}"),
        )
        .await
        .unwrap();
    }
    let dead = insert_job(&db, "tasks.remind", "tasks:dead", "{}", false).await;
    execute(
        &db,
        &format!(
            "UPDATE merezha_jobs.jobs SET state = 'dead', finished_at = now() WHERE id = {dead}"
        ),
    )
    .await
    .unwrap();
    let outbox = metered(&jobs, &db);
    append(
        &db,
        &outbox,
        vec![
            event("tasks", "tasks.task_created", "e1", json!({})),
            job_record("j1", "default", json!({})),
        ],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    let deadline = Instant::now() + PATIENCE;
    loop {
        if harness.metric("merezha.outbox.pending").value(&[]) == Some(2.0)
            && harness
                .metric("merezha.job.count")
                .value(&[("job.queue", "default"), ("job.state", "ready")])
                == Some(2.0)
        {
            break;
        }
        assert!(Instant::now() < deadline, "the sampler never reported");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let count = harness.metric("merezha.job.count");
    for (state, expected) in [
        ("ready", 2.0),
        ("delayed", 1.0),
        ("leased", 0.0),
        ("dead", 1.0),
    ] {
        assert_eq!(
            count.value(&[("job.queue", "default"), ("job.state", state)]),
            Some(expected),
            "{state}"
        );
    }
    let lag = harness
        .metric("merezha.job.lag")
        .value(&[("job.queue", "default")])
        .unwrap();
    assert!(lag >= 5.0, "the oldest due job is 5 s late: {lag}");
    assert!(harness.metric("merezha.outbox.lag").value(&[]).unwrap() >= 0.0);
    let appended = harness.metric("merezha.outbox.appended");
    assert_eq!(
        appended.value(&[("merezha.module", "tasks"), ("job.record_kind", "event")]),
        Some(1.0)
    );
    assert_eq!(
        appended.value(&[("merezha.module", "tasks"), ("job.record_kind", "job")]),
        Some(1.0)
    );
    // Relay both: the event has no subscription.
    let report = jobs.relay_now().await.unwrap();
    assert_eq!((report.relayed, report.unrouted), (2, 1));
    assert_eq!(named(&harness, "relay outbox").len(), 1);
    let relay = harness.span("relay outbox");
    assert_eq!(relay.parent_span_id, opentelemetry::trace::SpanId::INVALID);
    assert_eq!(
        harness
            .metric("merezha.outbox.relayed")
            .value(&[("merezha.module", "tasks")]),
        Some(2.0)
    );
    assert_eq!(
        harness
            .metric("merezha.outbox.unrouted")
            .value(&[("merezha.module", "tasks")]),
        Some(1.0)
    );
    assert_eq!(
        harness
            .metric("merezha.job.enqueued")
            .value(&[("job.queue", "default"), ("job.kind", "tasks.remind")]),
        Some(1.0)
    );
    let deadline = Instant::now() + PATIENCE;
    while harness.metric("merezha.outbox.pending").value(&[]) != Some(0.0) {
        assert!(Instant::now() < deadline, "the sample never refreshed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    jobs.shutdown(Instant::now() + Duration::from_secs(10))
        .await;
    harness.assert_catalogued();
}

/// The quarantine counter carries the module and the quarantine code, and
/// cancels count per queue.
#[tokio::test]
async fn quarantines_and_cancels_are_counted() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs]\nworkers = false");
    let jobs = instrumented(&harness, &db, &config, RelayMode::Off, None).await;
    let outbox = metered(&jobs, &db);
    append(
        &db,
        &outbox,
        vec![job_record("ok", "default", json!({}))],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    // A row the relay cannot route (written around the append checks).
    execute(&db, "UPDATE merezha_jobs.outbox SET queue = 'Not A Queue'")
        .await
        .unwrap();
    let report = jobs.relay_now().await.unwrap();
    assert_eq!(report.quarantined, 1);
    assert_eq!(
        harness
            .metric("merezha.outbox.quarantined")
            .value(&[("merezha.module", "tasks"), ("error.type", "unroutable")]),
        Some(1.0)
    );
    let tasks = handle(&jobs, "tasks");
    let enqueued = tasks
        .enqueue(NewJob {
            queue: id("default"),
            kind: id("tasks.remind"),
            idempotency_key: merezha_platform::ports::DedupeKey::new("c1").unwrap(),
            payload: json!({}),
            contains_secret: false,
            run_at: None,
            priority: 0,
            context: message(),
        })
        .await
        .unwrap();
    assert!(tasks.cancel(enqueued.id).await.unwrap());
    let other = insert_job(&db, "tasks.remind", "tasks:c2", "{}", false).await;
    jobs.cancel_job(other, Some("operator")).await.unwrap();
    assert_eq!(
        harness
            .metric("merezha.job.cancelled")
            .value(&[("job.queue", "default")]),
        Some(2.0)
    );
    // The enqueue's PRODUCER span, a root here, is the job's stored parent.
    let stored = text(
        &db,
        &format!(
            "SELECT trace_parent FROM merezha_jobs.jobs WHERE id = {}",
            enqueued.id
        ),
    )
    .await
    .unwrap();
    let parent = StoredContext::parse(&stored, None).unwrap().span_context();
    let send = named(&harness, "send default")
        .into_iter()
        .find(|span| span.span_context.span_id() == parent.span_id())
        .expect("the enqueue's send span");
    assert_eq!(send.span_kind, SpanKind::Producer);
    assert_eq!(send.parent_span_id, opentelemetry::trace::SpanId::INVALID);
    jobs.shutdown(Instant::now() + Duration::from_secs(10))
        .await;
    harness.assert_catalogued();
}

/// Cron runs have no producer: a fired occurrence counts
/// `merezha.job.schedule.fired` per schedule, and its attempt is a root
/// (TE §2.3).
#[tokio::test]
async fn fired_schedules_are_counted_and_their_attempts_are_roots() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 100\n[jobs.scheduler]\ninterval_ms = 3600000\n\
         [[jobs.schedules]]\nname = \"tasks.digest\"\ncron = \"* * * * *\"\ntimezone = \"UTC\"\n\
         queue = \"default\"\nkind = \"tasks.remind\"\npayload = {}\n",
    );
    let jobs = instrumented(
        &harness,
        &db,
        &config,
        RelayMode::Listen,
        Some(func(|_, _| Box::pin(async { Ok(()) }))),
    )
    .await;
    db.transaction(soon(), |tx| {
        Box::pin(async move {
            tx.execute(
                "SELECT pg_advisory_xact_lock(1297762882, hashtext('schedules'))",
                &[],
            )
            .await
            .map_err(|e| merezha_postgres::classify(&e))?;
            tx.execute(
                "UPDATE merezha_jobs.schedules SET next_run_at = now() - interval '1 minute'",
                &[],
            )
            .await
            .map_err(|e| merezha_postgres::classify(&e))?;
            Ok(())
        })
    })
    .await
    .unwrap();
    // The start tick may fire the backdated occurrence first (review item
    // 1): tick until its job exists, whoever fires it, then count.
    let cron_jobs = "SELECT count(*) FROM merezha_jobs.jobs WHERE schedule_name = 'tasks.digest'";
    let deadline = Instant::now() + PATIENCE;
    while scalar(&db, cron_jobs).await == 0 {
        assert!(Instant::now() < deadline, "the occurrence never fired");
        let _ = jobs.schedule_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The same occurrence again is a duplicate, not a second run.
    assert_eq!(jobs.schedule_now().await.unwrap().fired, 0);
    assert_eq!(scalar(&db, cron_jobs).await, 1);
    assert_eq!(
        harness
            .metric("merezha.job.schedule.fired")
            .value(&[("job.schedule", "tasks.digest")]),
        Some(1.0)
    );
    assert_eq!(
        harness
            .metric("merezha.job.enqueued")
            .value(&[("job.queue", "default"), ("job.kind", "tasks.remind")]),
        Some(1.0)
    );
    let attempt = wait_spans(&harness, "process default", 1).await.remove(0);
    assert_eq!(
        attempt.parent_span_id,
        opentelemetry::trace::SpanId::INVALID
    );
    assert!(attempt.links.links.is_empty());
    jobs.shutdown(Instant::now() + Duration::from_secs(10))
        .await;
    harness.assert_catalogued();
}

/// Review item 4 (M3): on the handler path, a dead letter counts only once
/// its fenced settle wrote it. Attempt 1 outlives its lease (no
/// heartbeat), the reaper re-offers the job, attempt 2 succeeds, and
/// attempt 1's late permanent failure is fenced out: no dead letter, one
/// `lease_lost` attempt.
#[tokio::test]
async fn a_fenced_out_permanent_failure_is_not_a_dead_letter() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 100\nreaper_interval_ms = 100\n\
         [jobs.queues.default]\nconcurrency = 2\nlease_ms = 1000\nmax_attempts = 3\n\
         backoff_base_ms = 10\nbackoff_max_ms = 20",
    );
    let jobs = PgJobs::with_mode(&config, RelayMode::Listen)
        .without_heartbeat()
        .with_telemetry(&harness.telemetry().meters(), Duration::from_millis(50));
    jobs.ports(&db);
    handle(&jobs, "tasks")
        .register(
            "tasks.remind",
            "default",
            func(|ctx, _| {
                Box::pin(async move {
                    if ctx.attempt == 1 {
                        tokio::time::sleep(Duration::from_millis(2_500)).await;
                        return Err(JobFailure::permanent("bad_address"));
                    }
                    Ok(())
                })
            }),
            Default::default(),
        )
        .unwrap();
    jobs.start(context(&db)).await.unwrap();
    wait_ready(&jobs).await;
    let outbox = metered(&jobs, &db);
    append(
        &db,
        &outbox,
        vec![job_record("late", "default", json!({}))],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    let id = wait_for_job(&db).await;
    wait_state(&db, id, "succeeded", PATIENCE).await;
    let deadline = Instant::now() + PATIENCE;
    while harness
        .metric("merezha.job.duration")
        .value(&[("job.outcome", "lease_lost")])
        .is_none()
    {
        assert!(Instant::now() < deadline, "attempt 1 never ended");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(state(&db, id).await, "succeeded");
    assert!(
        harness
            .metric("merezha.job.dead_lettered")
            .points()
            .is_empty(),
        "a fenced settle wrote nothing"
    );
    assert_eq!(
        harness
            .metric("merezha.job.lease_expired")
            .value(&[("job.queue", "default")]),
        Some(1.0)
    );
    let lost = wait_spans(&harness, "process default", 2)
        .await
        .into_iter()
        .find(|span| attribute(span, "job.outcome").as_deref() == Some("lease_lost"))
        .expect("attempt 1's span");
    assert!(matches!(lost.status, Status::Error { .. }));
    jobs.shutdown(Instant::now() + Duration::from_secs(10))
        .await;
    harness.assert_catalogued();
}

/// Review items 4 (M4) and 8: duplicates are neither counted as appended
/// nor given a `send` span, for outbox records and for enqueues; every
/// inserted row stores its own span's context.
#[tokio::test]
async fn duplicates_are_not_counted_and_get_no_send_span() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs]\nworkers = false");
    let jobs = instrumented(&harness, &db, &config, RelayMode::Off, None).await;
    let outbox = metered(&jobs, &db);
    let record = |key: &str| job_record(key, "default", json!({}));
    append(
        &db,
        &outbox,
        vec![record("a"), record("b")],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    let again = append(
        &db,
        &outbox,
        vec![record("a"), record("b"), record("c")],
        OnDuplicate::Ignore,
    )
    .await
    .unwrap();
    assert_eq!((again.inserted, again.duplicates), (1, 2));
    assert!(
        append(&db, &outbox, vec![record("c")], OnDuplicate::Fail)
            .await
            .is_err()
    );
    assert_eq!(
        harness
            .metric("merezha.outbox.appended")
            .value(&[("merezha.module", "tasks"), ("job.record_kind", "job")]),
        Some(3.0)
    );
    let sends = named(&harness, "send default");
    assert_eq!(sends.len(), 3, "one per inserted record");
    let client = db.acquire(soon()).await.unwrap();
    let stored: Vec<String> = client
        .query(
            "SELECT trace_parent FROM merezha_jobs.outbox ORDER BY id",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(stored.len(), 3);
    for parent in &stored {
        let span_id = StoredContext::parse(parent, None)
            .unwrap()
            .span_context()
            .span_id();
        assert!(sends.iter().any(|s| s.span_context.span_id() == span_id));
    }
    let tasks = handle(&jobs, "tasks");
    let job = || NewJob {
        queue: id("other"),
        kind: id("tasks.remind"),
        idempotency_key: merezha_platform::ports::DedupeKey::new("once").unwrap(),
        payload: json!({}),
        contains_secret: false,
        run_at: None,
        priority: 0,
        context: message(),
    };
    assert!(!tasks.enqueue(job()).await.unwrap().existed);
    assert!(tasks.enqueue(job()).await.unwrap().existed);
    assert_eq!(named(&harness, "send other").len(), 1);
    assert_eq!(
        harness
            .metric("merezha.job.enqueued")
            .value(&[("job.queue", "other")]),
        Some(1.0)
    );
    jobs.shutdown(Instant::now() + Duration::from_secs(10))
        .await;
    harness.assert_catalogued();
}

/// Review item 4 (M5, M8): a sample that cannot finish within
/// `SAMPLE_DEADLINE` fails fast (well before the pool's 5 s statement
/// timeout), logs `job.worker_failed` with `task = sampler`, and the gauges
/// keep the last sample until `STALE_SAMPLES` consecutive failures.
#[tokio::test]
async fn a_failing_sample_keeps_the_last_values_within_its_deadline() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs]\nworkers = false\n[jobs.queues.default]\n");
    let jobs = instrumented(&harness, &db, &config, RelayMode::Off, None).await;
    insert_job(&db, "tasks.remind", "tasks:ready", "{}", false).await;
    let ready = [("job.queue", "default"), ("job.state", "ready")];
    let deadline = Instant::now() + PATIENCE;
    while harness.metric("merezha.job.count").value(&ready) != Some(1.0) {
        assert!(Instant::now() < deadline, "the sampler never reported");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Block every read of the jobs table.
    let locker = db.acquire(soon()).await.unwrap();
    locker
        .batch_execute("BEGIN; LOCK TABLE merezha_jobs.jobs IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let locked = Instant::now();
    let failed = |harness: &Harness| {
        harness
            .stderr_lines()
            .iter()
            .any(|line| line["event"] == "job.worker_failed" && line["task"] == "sampler")
    };
    while !failed(&harness) {
        assert!(
            Instant::now() < locked + PATIENCE,
            "the sample never failed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let took = locked.elapsed();
    assert!(
        took < merezha_jobs::SAMPLE_DEADLINE + Duration::from_secs(2),
        "the sample waited {took:?}"
    );
    assert_eq!(harness.metric("merezha.job.count").value(&ready), Some(1.0));
    // Review item 5: after STALE_SAMPLES consecutive failures the gauges
    // report nothing rather than stale values, and the failure is logged
    // once per streak.
    let deadline = Instant::now() + PATIENCE;
    while harness.metric("merezha.job.count").value(&ready).is_some() {
        assert!(Instant::now() < deadline, "stale values kept");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        locked.elapsed() >= merezha_jobs::SAMPLE_DEADLINE * (merezha_jobs::STALE_SAMPLES - 1),
        "dropped before {} failures",
        merezha_jobs::STALE_SAMPLES
    );
    assert!(harness.metric("merezha.outbox.pending").points().is_empty());
    let logged = harness
        .stderr_lines()
        .iter()
        .filter(|line| line["event"] == "job.worker_failed" && line["task"] == "sampler")
        .count();
    assert_eq!(logged, 1, "once per streak");
    locker.batch_execute("ROLLBACK").await.unwrap();
    drop(locker);
    // A successful sample reports again.
    let deadline = Instant::now() + PATIENCE;
    while harness.metric("merezha.job.count").value(&ready) != Some(1.0) {
        assert!(Instant::now() < deadline, "the gauges never came back");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    jobs.shutdown(Instant::now() + Duration::from_secs(10))
        .await;
    harness.assert_catalogued();
}

/// Review item 4 (M6): the relay's batch lock skips rows another relay
/// holds instead of waiting for them.
#[tokio::test]
async fn the_relay_skips_records_another_relay_holds() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs]\nworkers = false");
    let jobs = instrumented(&harness, &db, &config, RelayMode::Off, None).await;
    let outbox = metered(&jobs, &db);
    append(
        &db,
        &outbox,
        (0..3)
            .map(|n| job_record(&format!("k{n}"), "default", json!({})))
            .collect(),
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    // Another relay's batch: the first record, locked.
    let other = db.acquire(soon()).await.unwrap();
    other
        .batch_execute("BEGIN; SELECT id FROM merezha_jobs.outbox ORDER BY id LIMIT 1 FOR UPDATE")
        .await
        .unwrap();
    let started = Instant::now();
    let report = jobs.relay_now().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the relay waited"
    );
    assert_eq!(report.relayed, 2);
    other.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(jobs.relay_now().await.unwrap().relayed, 1);
    assert_eq!(
        harness
            .metric("merezha.outbox.relayed")
            .value(&[("merezha.module", "tasks")]),
        Some(3.0)
    );
    drop(other);
    jobs.shutdown(Instant::now() + Duration::from_secs(10))
        .await;
}

/// A reissue hook that succeeds.
struct Reissues;
impl merezha_platform::ports::Reissue for Reissues {
    fn reissue(
        &self,
        _: merezha_platform::ports::ReissueRequest,
    ) -> merezha_platform::BoxFuture<'_, Result<(), merezha_domain::Failure>> {
        Box::pin(async { Ok(()) })
    }
}

/// Review item 6: a reissued replay (a flagged dead job) links to the trace
/// the job died in, single (on `admin job.replay`) and bulk (on one child
/// `admin job.replay` per reissue).
#[tokio::test]
async fn reissued_replays_link_to_the_trace_the_job_died_in() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs]\nworkers = false");
    let jobs = PgJobs::with_mode(&config, RelayMode::Off)
        .with_telemetry(&harness.telemetry().meters(), Duration::from_millis(50));
    jobs.ports(&db);
    handle(&jobs, "tasks")
        .register(
            "tasks.remind",
            "default",
            func(|_, _| Box::pin(async { Ok(()) })),
            merezha_platform::ports::KindOptions {
                reissue: Some(Arc::new(Reissues)),
            },
        )
        .unwrap();
    jobs.start(context(&db)).await.unwrap();
    wait_ready(&jobs).await;
    let trace_a = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
    let mut dead = Vec::new();
    for key in ["tasks:s1", "tasks:s2"] {
        let id = insert_job(&db, "tasks.remind", key, "{}", true).await;
        execute(
            &db,
            &format!(
                "UPDATE merezha_jobs.jobs SET state = 'dead', finished_at = now(), payload = NULL,
                        trace_parent = '{TRACE_A}' WHERE id = {id}"
            ),
        )
        .await
        .unwrap();
        dead.push(id);
    }
    let replayed = jobs.replay(dead[0], Some("operator")).await.unwrap();
    assert_eq!(replayed.state(), "reissued");
    let bulk = jobs
        .replay_dead(
            &merezha_jobs::DeadFilter {
                queue: "default".into(),
                kind: None,
                code: None,
                dead_after: None,
                dead_before: None,
                limit: 10,
            },
            Some("operator"),
        )
        .await
        .unwrap();
    assert_eq!(bulk.reissued, 1);
    let replays = named(&harness, "admin job.replay");
    for id in &dead {
        let span = replays
            .iter()
            .find(|span| attribute(span, "job.id") == Some(id.to_string()))
            .unwrap_or_else(|| panic!("a replay span for job {id}"));
        let link = span.links.links.first().expect("a replay link");
        assert_eq!(link.span_context.trace_id(), trace_a);
        assert!(
            link.attributes
                .iter()
                .any(|kv| kv.key.as_str() == "merezha.link.reason"
                    && kv.value.to_string() == "replay")
        );
    }
    assert_eq!(
        harness
            .metric("merezha.job.replayed")
            .value(&[("job.queue", "default")]),
        Some(2.0)
    );
    jobs.shutdown(Instant::now() + Duration::from_secs(10))
        .await;
    harness.assert_catalogued();
}
