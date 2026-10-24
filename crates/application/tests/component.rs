use merezha_application::{AllowAll, Application, Guest, config::Config};
use merezha_domain::{Command, Identity, MessageKind, Operation, Query, Reply};
use merezha_runtime::Runtime;
use serde_json::json;
use std::sync::Arc;

fn fixture() -> (tempfile::TempDir, Runtime) {
    let replies = [
        (
            "validate",
            json!({"kind":"validated","payload":{"amount":1}}),
        ),
        (
            "decide",
            json!({"kind":"decided","events":[{"tag":"Added","schema_version":1,"payload":{"amount":1}}]}),
        ),
        ("project", json!({"kind":"projected","state":1})),
        ("query", json!({"kind":"queried","value":1})),
    ];
    let mut data = String::new();
    let mut branches = String::new();
    let mut offset = 128;
    // Canonical SDK serialization puts the operation name immediately after
    // this prefix. The guest independently selects its protocol response.
    let prefix = r#"{"version":1,"body":{"op":""#;
    for (op, reply) in replies {
        let response = json!({"version":1,"outcome":{"status":"ok","reply":reply}}).to_string();
        let escaped: String = response.bytes().map(|b| format!("\\{b:02x}")).collect();
        data.push_str(&format!("(data (i32.const {offset}) \"{escaped}\")"));
        branches.push_str(&format!("local.get $p i32.const {} i32.add i32.load8_u i32.const {} i32.eq if i32.const 4 i32.const {offset} i32.store i32.const 8 i32.const {} i32.store end ",prefix.len(),op.as_bytes()[0],response.len()));
        offset += response.len() + 1;
    }
    let wat = format!(
        r#"(component (core module $m
      (memory (export "memory") 2) {data}
      (func (export "realloc") (param i32 i32 i32 i32) (result i32) i32.const 8192)
      (func (export "handle") (param $p i32) (param i32) (result i32)
        i32.const 0 i32.const 0 i32.store {branches} i32.const 0))
      (core instance $i (instantiate $m))
      (type $h (func (param "input" string) (result (result string (error string)))))
      (func $handle (type $h) (canon lift (core func $i "handle") (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
      (instance $handler (export "handle" (func $handle)))
      (export "merezha:service/handler@0.1.0" (instance $handler)))"#
    );
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("protocol.wasm"),
        wat::parse_str(wat).unwrap(),
    )
    .unwrap();
    let manifest = dir.path().join("runtime.toml");
    std::fs::write(
        &manifest,
        r#"version=1
[limits]
memory_bytes=16777216
fuel=1000000
max_input_bytes=65536
max_output_bytes=65536
timeout_ms=1000
max_concurrent_invocations=4
[[services]]
id="protocol"
path="protocol.wasm"
"#,
    )
    .unwrap();
    let runtime = Runtime::load(manifest).unwrap();
    (dir, runtime)
}
#[test]
fn real_no_import_component_exercises_application_protocol_and_pinned_reload_session() {
    let (dir, runtime) = fixture();
    let session = Guest::session(&runtime).unwrap();
    assert_eq!(
        session
            .invoke(
                "protocol",
                Operation::Validate {
                    kind: MessageKind::Command,
                    tag: Identity::new("Add").unwrap(),
                    payload: json!({"amount":1})
                }
            )
            .unwrap(),
        Reply::Validated {
            payload: json!({"amount":1})
        }
    );
    let manifest = dir.path().join("runtime.toml");
    let original = std::fs::read_to_string(&manifest).unwrap();
    std::fs::write(
        dir.path().join("echo.wasm"),
        wat::parse_str(include_str!("../../runtime/tests/fixtures/echo.wat")).unwrap(),
    )
    .unwrap();
    std::fs::write(
        &manifest,
        original.replace("path=\"protocol.wasm\"", "path=\"echo.wasm\""),
    )
    .unwrap();
    runtime.reload().unwrap();
    let validate = || Operation::Validate {
        kind: MessageKind::Query,
        tag: Identity::new("Total").unwrap(),
        payload: json!({}),
    };
    // The new generation echoes a request instead of a protocol response.
    assert!(
        Guest::session(&runtime)
            .unwrap()
            .invoke("protocol", validate())
            .is_err()
    );
    assert_eq!(
        session
            .invoke_until(
                "protocol",
                validate(),
                std::time::Instant::now() + std::time::Duration::from_secs(1)
            )
            .unwrap(),
        Reply::Validated {
            payload: json!({"amount":1})
        }
    );
    assert!(
        session
            .invoke(
                "protocol",
                Operation::Validate {
                    kind: MessageKind::Query,
                    tag: Identity::new("Total").unwrap(),
                    payload: json!({})
                }
            )
            .is_ok()
    );
    std::fs::write(&manifest, original).unwrap();
    runtime.reload().unwrap();
    let config = Config::parse(
        r#"version=1
[[commands]]
tag="Add"
service="protocol"
aggregate="Counter"
[[queries]]
tag="Total"
service="protocol"
projection="total"
[[projections]]
name="total"
service="protocol"
initial=0
"#,
    )
    .unwrap();
    let app = Application::in_memory(config, Arc::new(runtime), Arc::new(AllowAll)).unwrap();
    let command:Command=serde_json::from_value(json!({"tag":"Add","stream":"one","payload":{"amount":1},"metadata":{"message_id":"m1","correlation_id":"c1"}})).unwrap();
    assert_eq!(app.command(command).unwrap().version, 1);
    assert_eq!(app.catch_up("total").unwrap().checkpoint, 1);
    let query: Query = serde_json::from_value(
        json!({"tag":"Total","payload":{},"metadata":{"message_id":"m2","correlation_id":"c1"}}),
    )
    .unwrap();
    assert_eq!(app.query(query).unwrap().value, json!(1));
}

/// TE §1.2, §2.4: each guest call of a dispatch is a `guest {service}
/// {operation}` child of its `command {tag}`, `query {tag}` or
/// `projection {name}` span, on a real component.
#[test]
fn guest_calls_are_children_of_their_dispatch_and_projection_spans() {
    use merezha_telemetry::testing::Harness;
    let harness = Harness::new();
    let (_dir, runtime) = fixture();
    let config = Config::parse(
        r#"version=1
[[commands]]
tag="Add"
service="protocol"
aggregate="Counter"
[[queries]]
tag="Total"
service="protocol"
projection="total"
[[projections]]
name="total"
service="protocol"
initial=0
"#,
    )
    .unwrap();
    let app = Application::in_memory(config, Arc::new(runtime), Arc::new(AllowAll))
        .unwrap()
        .with_meters(&harness.telemetry().meters());
    let command: Command = serde_json::from_value(json!({"tag":"Add","stream":"one","payload":{"amount":1},"metadata":{"message_id":"m1","correlation_id":"c1"}})).unwrap();
    app.command(command).unwrap();
    app.catch_up("total").unwrap();
    let query: Query = serde_json::from_value(
        json!({"tag":"Total","payload":{},"metadata":{"message_id":"m2","correlation_id":"c1"}}),
    )
    .unwrap();
    app.query(query).unwrap();
    let spans = harness.spans();
    let id_of = |name: &str| harness.span(name).span_context.span_id();
    let children = |parent: &str| -> Vec<String> {
        let parent = id_of(parent);
        let mut names: Vec<_> = spans
            .iter()
            .filter(|s| s.parent_span_id == parent)
            .map(|s| s.name.to_string())
            .collect();
        names.sort();
        names
    };
    assert_eq!(
        children("command Add"),
        ["guest protocol decide", "guest protocol validate"]
    );
    assert_eq!(
        children("query Total"),
        ["guest protocol query", "guest protocol validate"]
    );
    assert_eq!(children("projection total"), ["guest protocol project"]);
    harness.assert_catalogued();
}

/// Guest output that is not a protocol response is `invalid_response`, a
/// `failed` dispatch, whether it is not JSON or JSON of another shape.
#[test]
fn guest_output_that_is_not_a_response_is_invalid_response_and_failed() {
    use merezha_telemetry::testing::{Harness, span_attribute};
    let not_json = r#"(component (core module $m
      (memory (export "memory") 1)
      (data (i32.const 64) "A")
      (func (export "realloc") (param i32 i32 i32 i32) (result i32) i32.const 1024)
      (func (export "handle") (param i32) (param i32) (result i32)
        i32.const 0 i32.const 0 i32.store i32.const 4 i32.const 64 i32.store i32.const 8 i32.const 1 i32.store i32.const 0))
      (core instance $i (instantiate $m))
      (type $h (func (param "input" string) (result (result string (error string)))))
      (func $handle (type $h) (canon lift (core func $i "handle") (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
      (instance $handler (export "handle" (func $handle)))
      (export "merezha:service/handler@0.1.0" (instance $handler)))"#;
    for wat in [
        not_json,
        include_str!("../../runtime/tests/fixtures/echo.wat"),
    ] {
        let harness = Harness::new();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("guest.wasm"), wat::parse_str(wat).unwrap()).unwrap();
        let manifest = dir.path().join("runtime.toml");
        std::fs::write(
            &manifest,
            "version=1\n[limits]\nmemory_bytes=16777216\nfuel=1000000\nmax_input_bytes=65536\nmax_output_bytes=65536\ntimeout_ms=1000\nmax_concurrent_invocations=4\n[[services]]\nid=\"protocol\"\npath=\"guest.wasm\"\n",
        )
        .unwrap();
        let runtime = Runtime::load(&manifest).unwrap();
        let config = Config::parse(
            "version=1\n[[commands]]\ntag=\"Add\"\nservice=\"protocol\"\naggregate=\"Counter\"\n",
        )
        .unwrap();
        let app = Application::in_memory(config, Arc::new(runtime), Arc::new(AllowAll))
            .unwrap()
            .with_meters(&harness.telemetry().meters());
        let command: Command = serde_json::from_value(json!({"tag":"Add","stream":"one","payload":{},"metadata":{"message_id":"m1","correlation_id":"c1"}})).unwrap();
        assert_eq!(app.command(command).unwrap_err().code, "invalid_response");
        let span = harness.span("command Add");
        assert_eq!(
            span_attribute(&span, merezha_telemetry::names::key::OUTCOME).as_deref(),
            Some("failed")
        );
        assert_eq!(
            span_attribute(&span, merezha_telemetry::names::key::ERROR_TYPE).as_deref(),
            Some("invalid_response")
        );
    }
}
