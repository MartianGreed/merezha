//! Shared fixtures for process tests.
use serde_json::json;

/// A no-import component answering the application protocol with fixed
/// replies: `Add` decides one `Added` event, projections count to 1 and the
/// `Total` query returns 1.
pub fn component() -> Vec<u8> {
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
    let prefix = r#"{"version":1,"body":{"op":""#;
    for (op, reply) in replies {
        let response = json!({"version":1,"outcome":{"status":"ok","reply":reply}}).to_string();
        let escaped: String = response.bytes().map(|b| format!("\\{b:02x}")).collect();
        data.push_str(&format!("(data (i32.const {offset}) \"{escaped}\")"));
        branches.push_str(&format!("local.get $p i32.const {} i32.add i32.load8_u i32.const {} i32.eq if i32.const 4 i32.const {offset} i32.store i32.const 8 i32.const {} i32.store end ",prefix.len(),op.as_bytes()[0],response.len()));
        offset += response.len() + 1;
    }
    wat::parse_str(format!(r#"(component (core module $m
      (memory (export "memory") 2) {data}
      (func (export "realloc") (param i32 i32 i32 i32) (result i32) i32.const 8192)
      (func (export "handle") (param $p i32) (param i32) (result i32)
        i32.const 0 i32.const 0 i32.store {branches} i32.const 0))
      (core instance $i (instantiate $m))
      (type $h (func (param "input" string) (result (result string (error string)))))
      (func $handle (type $h) (canon lift (core func $i "handle") (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
      (instance $handler (export "handle" (func $handle)))
      (export "merezha:service/handler@0.1.0" (instance $handler)))"#)).unwrap()
}
