//! Guest spans and metrics (TE §1.2, §1.3, §2.4, §8 "Guests"): every
//! runtime outcome maps to its `merezha.guest.outcome` and span status,
//! fuel consumed equals the limit minus what remained, peak memory covers
//! what the guest grew, and a pipeline's steps are child spans sharing one
//! budget. Runs the production pipeline in memory (`testing::Harness`).
use merezha_runtime::{Error, GuestOperation, Runtime};
use merezha_telemetry::{
    names::{dispatch, key, metric},
    testing::{Harness, span_attribute},
};
use opentelemetry::trace::Status;
use std::time::Instant;

const HEADER: &str = r#"version = 1
[limits]
memory_bytes = 16777216
fuel = 10000000
max_input_bytes = 65536
max_output_bytes = 65536
timeout_ms = 1000
max_concurrent_invocations = 8
"#;
const ECHO: &str = "i32.const 0 i32.const 0 i32.store i32.const 4 local.get $p i32.store i32.const 8 local.get $n i32.store i32.const 0";
const PAGE: u64 = 65_536;

fn component(body: &str) -> Vec<u8> {
    wat::parse_str(format!(
        r#"(component
      (core module $m
        (memory (export "memory") 1)
        (global $next (mut i32) (i32.const 1024))
        (func (export "realloc") (param i32 i32 i32 i32) (result i32)
          (local $p i32)
          global.get $next local.tee $p
          local.get 3 i32.add global.set $next local.get $p)
        (func (export "handle") (param $p i32) (param $n i32) (result i32) {body}))
      (core instance $i (instantiate $m))
      (type $h (func (param "input" string) (result (result string (error string)))))
      (func $handle (type $h) (canon lift (core func $i "handle")
        (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
      (instance $handler (export "handle" (func $handle)))
      (export "merezha:service/handler@0.1.0" (instance $handler)))"#
    ))
    .unwrap()
}
fn guest_error() -> String {
    ECHO.replacen(
        "i32.const 0 i32.const 0 i32.store",
        "i32.const 0 i32.const 1 i32.store",
        1,
    )
}
struct Fixture {
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
}
fn fixture(body: &str, header: &str, rest: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("echo.wasm"), component(body)).unwrap();
    let path = dir.path().join("runtime.toml");
    std::fs::write(
        &path,
        format!("{header}[[services]]\nid = \"echo\"\npath = \"echo.wasm\"\n{rest}"),
    )
    .unwrap();
    Fixture { _dir: dir, path }
}
fn load(harness: &Harness, fixture: &Fixture) -> Runtime {
    Runtime::load_with_meters(&fixture.path, &harness.telemetry().meters()).unwrap()
}
fn number(span: &opentelemetry_sdk::trace::SpanData, key: &str) -> u64 {
    span_attribute(span, key)
        .unwrap_or_else(|| panic!("{} has no {key}", span.name))
        .parse()
        .unwrap()
}

#[test]
fn every_runtime_error_maps_to_its_guest_outcome_and_span_status() {
    let tight_fuel = HEADER.replace("fuel = 10000000", "fuel = 1000");
    let tight_time = HEADER
        .replace("fuel = 10000000", "fuel = 1000000000")
        .replace("timeout_ms = 1000", "timeout_ms = 5");
    let small_input = HEADER.replace("max_input_bytes = 65536", "max_input_bytes = 4");
    let small_output = HEADER.replace("max_output_bytes = 65536", "max_output_bytes = 3");
    let one_slot = HEADER.replace(
        "max_concurrent_invocations = 8",
        "max_concurrent_invocations = 1",
    );
    let forever = "(loop $forever br $forever) unreachable";
    let grow = format!("i32.const 1024 memory.grow drop {ECHO}");
    // (outcome, body, header, input, host failure)
    let cases: [(&str, &str, &str, &str, bool); 9] = [
        ("ok", ECHO, HEADER, "hi", false),
        ("guest_error", &guest_error(), HEADER, "rejected", false),
        ("trap", "unreachable", HEADER, "", true),
        ("memory_limit", &grow, HEADER, "", true),
        ("fuel_exhausted", forever, &tight_fuel, "", true),
        ("timeout", forever, &tight_time, "", true),
        ("input_limit", ECHO, &small_input, "ééé", false),
        ("output_limit", ECHO, &small_output, "éé", true),
        ("overloaded", ECHO, &one_slot, "", true),
    ];
    for (outcome, body, header, input, failure) in cases {
        let harness = Harness::new();
        let f = fixture(body, header, "");
        let rt = load(&harness, &f);
        let held = (outcome == "overloaded").then(|| rt.prepare_service("echo").unwrap());
        let result = rt.invoke_service("echo", input);
        drop(held);
        match &result {
            Ok(_) => assert_eq!(outcome, "ok"),
            Err(error) => assert_eq!(error.code(), outcome),
        }
        let spans = harness.spans_named("guest echo raw");
        assert_eq!(spans.len(), 1, "{outcome}");
        let span = &spans[0];
        assert_eq!(
            span_attribute(span, key::GUEST_OUTCOME).as_deref(),
            Some(outcome)
        );
        assert_eq!(
            span_attribute(span, key::GUEST_OPERATION).as_deref(),
            Some("raw")
        );
        assert_eq!(number(span, key::CATALOG_GENERATION), 1);
        assert_eq!(
            matches!(span.status, Status::Error { .. }),
            failure,
            "{outcome}: {:?}",
            span.status
        );
        let duration = harness.metric(metric::GUEST_INVOCATION_DURATION.name);
        assert_eq!(
            duration.value(&[
                (key::GUEST_SERVICE, "echo"),
                (key::GUEST_OPERATION, "raw"),
                (key::GUEST_OUTCOME, outcome),
            ]),
            Some(1.0),
            "{outcome}: {duration:?}"
        );
        assert_eq!(duration.points().len(), 1, "{outcome}");
        harness.assert_catalogued();
    }

    // An unknown service is `not_found` under `_unknown`: raw ids come from
    // request paths and never reach a span name or a metric.
    let harness = Harness::new();
    let f = fixture(ECHO, HEADER, "");
    let rt = load(&harness, &f);
    assert_eq!(
        rt.invoke_service("canary-service-id", "").unwrap_err(),
        Error::NotFound
    );
    assert_eq!(
        rt.prepare_pipeline("canary-service-id").err(),
        Some(Error::NotFound)
    );
    let spans = harness.spans_named("guest _unknown raw");
    assert_eq!(spans.len(), 2);
    assert!(spans.iter().all(|s| s.status == Status::Unset));
    assert_eq!(
        harness
            .metric(metric::GUEST_INVOCATION_DURATION.name)
            .value(&[
                (key::GUEST_SERVICE, dispatch::UNKNOWN),
                (key::GUEST_OUTCOME, "not_found")
            ]),
        Some(2.0)
    );
    harness.assert_no_canary("canary-service-id");
}

#[test]
fn application_operations_name_the_span_and_the_series() {
    let harness = Harness::new();
    let f = fixture(ECHO, HEADER, "");
    let rt = load(&harness, &f);
    let session = rt.session().unwrap();
    let deadline = Instant::now() + std::time::Duration::from_secs(1);
    for operation in GuestOperation::ALL {
        session
            .invoke_operation_until("echo", operation, "x", deadline)
            .unwrap();
        let name = format!("guest echo {}", operation.as_str());
        let span = &harness.spans_named(&name)[0];
        assert_eq!(
            span_attribute(span, key::GUEST_OPERATION).as_deref(),
            Some(operation.as_str())
        );
        assert_eq!(
            harness
                .metric(metric::GUEST_FUEL_CONSUMED.name)
                .value(&[(key::GUEST_OPERATION, operation.as_str())]),
            Some(1.0)
        );
    }
    // The memory series is per service only.
    let memory = harness.metric(metric::GUEST_MEMORY_PEAK.name);
    assert_eq!(memory.points().len(), 1);
    assert_eq!(memory.value(&[(key::GUEST_SERVICE, "echo")]), Some(6.0));
}

#[test]
fn fuel_consumed_is_limit_minus_remaining_and_peak_memory_covers_growth() {
    let harness = Harness::new();
    let f = fixture(ECHO, HEADER, "");
    let rt = load(&harness, &f);
    let output = rt.invoke_service("echo", "hello").unwrap();
    assert!(output.fuel_consumed > 0);
    let span = &harness.spans_named("guest echo raw")[0];
    assert_eq!(number(span, dispatch::GUEST_FUEL_LIMIT), 10_000_000);
    assert_eq!(
        number(span, dispatch::GUEST_FUEL_CONSUMED),
        output.fuel_consumed
    );
    assert_eq!(number(span, dispatch::GUEST_INPUT_BYTES), 5);
    assert_eq!(number(span, dispatch::GUEST_OUTPUT_BYTES), 5);
    assert_eq!(number(span, dispatch::GUEST_MEMORY_LIMIT), 16_777_216);
    // The initial page counts.
    assert_eq!(number(span, dispatch::GUEST_MEMORY_PEAK), PAGE);
    let fuel = harness.metric(metric::GUEST_FUEL_CONSUMED.name);
    assert_eq!(fuel.sum(&[]), Some(output.fuel_consumed as f64));
    let utilization = harness.metric(metric::GUEST_FUEL_UTILIZATION.name);
    let expected = output.fuel_consumed as f64 / 10_000_000.0;
    assert!((utilization.sum(&[]).unwrap() - expected).abs() < 1e-12);

    // A guest that grows two pages peaks at three.
    let harness = Harness::new();
    let f = fixture(&format!("i32.const 2 memory.grow drop {ECHO}"), HEADER, "");
    let rt = load(&harness, &f);
    rt.invoke_service("echo", "x").unwrap();
    let span = &harness.spans_named("guest echo raw")[0];
    assert_eq!(number(span, dispatch::GUEST_MEMORY_PEAK), 3 * PAGE);
    assert_eq!(
        harness.metric(metric::GUEST_MEMORY_PEAK.name).sum(&[]),
        Some((3 * PAGE) as f64)
    );

    // Exhaustion consumes the whole budget.
    let harness = Harness::new();
    let f = fixture(
        "(loop $forever br $forever) unreachable",
        &HEADER.replace("fuel = 10000000", "fuel = 1000"),
        "",
    );
    let rt = load(&harness, &f);
    assert_eq!(
        rt.invoke_service("echo", "").unwrap_err(),
        Error::FuelExhausted
    );
    let span = &harness.spans_named("guest echo raw")[0];
    assert_eq!(number(span, dispatch::GUEST_FUEL_CONSUMED), 1000);
    assert_eq!(
        harness.metric(metric::GUEST_FUEL_UTILIZATION.name).sum(&[]),
        Some(1.0)
    );
}

#[test]
fn pipeline_steps_are_child_spans_sharing_one_budget() {
    let harness = Harness::new();
    let f = fixture(
        ECHO,
        HEADER,
        "[[pipelines]]\nid = \"thrice\"\nsteps = [\"echo\", \"echo\", \"echo\"]\n",
    );
    let rt = load(&harness, &f);
    let output = rt.invoke_pipeline("thrice", "abc").unwrap();
    let pipeline = harness.span("pipeline thrice");
    assert_eq!(pipeline.status, Status::Unset);
    assert_eq!(
        span_attribute(&pipeline, key::GUEST_OUTCOME).as_deref(),
        Some("ok")
    );
    let mut steps = harness.spans_named("guest echo raw");
    assert_eq!(steps.len(), 3);
    steps.sort_by_key(|s| s.start_time);
    let mut budget = 10_000_000;
    let mut total = 0;
    for step in &steps {
        assert_eq!(step.parent_span_id, pipeline.span_context.span_id());
        assert_eq!(
            step.span_context.trace_id(),
            pipeline.span_context.trace_id()
        );
        assert_eq!(number(step, dispatch::GUEST_FUEL_LIMIT), budget);
        let consumed = number(step, dispatch::GUEST_FUEL_CONSUMED);
        budget -= consumed;
        total += consumed;
    }
    assert_eq!(total, output.fuel_consumed);

    // One budget across sixteen steps runs out mid-pipeline.
    let harness = Harness::new();
    let many = vec!["\"echo\""; 16].join(",");
    let f = fixture(
        ECHO,
        &HEADER.replace("fuel = 10000000", "fuel = 100"),
        &format!("[[pipelines]]\nid = \"many\"\nsteps = [{many}]\n"),
    );
    let rt = load(&harness, &f);
    assert_eq!(
        rt.invoke_pipeline("many", "a").unwrap_err(),
        Error::FuelExhausted
    );
    let pipeline = harness.span("pipeline many");
    assert!(matches!(pipeline.status, Status::Error { .. }));
    let steps = harness.spans_named("guest echo raw");
    assert!(steps.len() < 16);
    let last = steps.iter().max_by_key(|s| s.start_time).unwrap();
    assert_eq!(
        span_attribute(last, key::GUEST_OUTCOME).as_deref(),
        Some("fuel_exhausted")
    );
    let consumed: u64 = steps
        .iter()
        .map(|s| number(s, dispatch::GUEST_FUEL_CONSUMED))
        .sum();
    assert_eq!(consumed, 100);
}

#[test]
fn catalog_generation_reloads_and_active_invocations_are_observed() {
    let harness = Harness::new();
    let f = fixture(ECHO, HEADER, "");
    let rt = load(&harness, &f);
    let generation = || {
        harness
            .metric(metric::CATALOG_GENERATION.name)
            .value(&[])
            .unwrap()
    };
    assert_eq!(generation(), 1.0);
    rt.reload().unwrap();
    assert_eq!(generation(), 2.0);
    let manifest = std::fs::read_to_string(&f.path).unwrap();
    std::fs::write(&f.path, "broken").unwrap();
    assert!(rt.reload().is_err());
    std::fs::write(&f.path, manifest).unwrap();
    assert_eq!(generation(), 2.0);
    let reloads = harness.metric(metric::CATALOG_RELOADS.name);
    assert_eq!(reloads.value(&[(key::OUTCOME, "ok")]), Some(1.0));
    assert_eq!(
        reloads.value(&[(key::OUTCOME, "invalid_manifest")]),
        Some(1.0)
    );
    let active = || {
        harness
            .metric(metric::GUEST_INVOCATIONS_ACTIVE.name)
            .value(&[])
            .unwrap_or(0.0)
    };
    let held = rt.prepare_service("echo").unwrap();
    assert_eq!(active(), 1.0);
    held.run("x").unwrap();
    assert_eq!(active(), 0.0);
    let span = &harness.spans_named("guest echo raw")[0];
    assert_eq!(number(span, key::CATALOG_GENERATION), 2);
}

#[test]
fn ten_thousand_distinct_guest_errors_are_one_series_and_never_recorded() {
    let harness = Harness::new();
    let f = fixture(&guest_error(), HEADER, "");
    let rt = load(&harness, &f);
    for i in 0..10_000 {
        let input = format!("canary-{i:05}");
        assert_eq!(
            rt.invoke_service("echo", &input).unwrap_err(),
            Error::GuestError
        );
    }
    let duration = harness.metric(metric::GUEST_INVOCATION_DURATION.name);
    assert_eq!(duration.points().len(), 1);
    assert_eq!(
        duration.value(&[(key::GUEST_OUTCOME, "guest_error")]),
        Some(10_000.0)
    );
    // Neither the guest's input nor its error output reaches any signal.
    harness.assert_no_canary("canary-");
}

#[test]
fn without_exported_metrics_spans_are_still_recorded_and_no_instrument_exists() {
    let harness = Harness::new();
    let f = fixture(ECHO, HEADER, "");
    let rt = Runtime::load(&f.path).unwrap();
    rt.invoke_service("echo", "x").unwrap();
    assert_eq!(harness.spans_named("guest echo raw").len(), 1);
    assert!(
        harness
            .metric(metric::GUEST_INVOCATION_DURATION.name)
            .points()
            .is_empty()
    );
}
