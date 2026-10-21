use json_envelope::{fnv1a64, handle};
use serde_json::{Value, json};

fn ok(input: Value) -> Value {
    let output = handle(&input.to_string()).expect("wrapping must succeed");
    serde_json::from_str(&output).expect("output must be JSON")
}

fn err(input: &str) -> Value {
    let output = handle(input).expect_err("input must be rejected");
    serde_json::from_str::<Value>(&output).expect("error must be JSON")["error"].clone()
}

#[test]
fn fnv1a64_matches_reference_vectors() {
    assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(fnv1a64(b"foobar"), 0x8594_4171_f739_67e8);
}

#[test]
fn wraps_the_object_with_kind_version_and_meta() {
    let output = handle(r#"{"kind":"welcome","b":[1,2],"a":"x"}"#).unwrap();
    let data = r#"{"a":"x","b":[1,2]}"#;
    let expected = format!(
        r#"{{"data":{data},"kind":"welcome","meta":{{"data_bytes":{},"digest":"fnv1a64:{:016x}","fields":2}},"version":1}}"#,
        data.len(),
        fnv1a64(data.as_bytes())
    );
    assert_eq!(output, expected);
}

#[test]
fn defaults_the_kind_to_document() {
    let output = ok(json!({ "a": 1 }));
    assert_eq!(output["kind"], "document");
    assert_eq!(output["data"], json!({ "a": 1 }));
}

#[test]
fn wraps_an_empty_object() {
    let output = ok(json!({}));
    assert_eq!(output["data"], json!({}));
    assert_eq!(output["meta"]["fields"], 0);
    assert_eq!(
        output["meta"]["digest"],
        format!("fnv1a64:{:016x}", fnv1a64(b"{}"))
    );
}

#[test]
fn preserves_nested_data_verbatim() {
    let data = json!({ "deep": { "list": [null, true, -1.5e3, "é"], "empty": {} } });
    assert_eq!(ok(data.clone())["data"], data);
}

#[test]
fn digest_is_deterministic_and_tracks_content() {
    let first = ok(json!({ "b": 2, "a": 1 }));
    let reordered = ok(json!({ "a": 1, "b": 2 }));
    let changed = ok(json!({ "a": 1, "b": 3 }));
    assert_eq!(first["meta"]["digest"], reordered["meta"]["digest"]);
    assert_ne!(first["meta"]["digest"], changed["meta"]["digest"]);
}

#[test]
fn validates_the_kind() {
    for bad in ["", "Welcome", "has space", "ünïcode", &"k".repeat(65)] {
        let error = err(&json!({ "kind": bad }).to_string());
        assert_eq!(
            (error["code"].as_str(), error["field"].as_str()),
            (Some("invalid_value"), Some("kind")),
            "kind {bad:?}"
        );
        assert_eq!(error["service"], "json-envelope");
    }
    for good in ["a", "welcome.v2", "user_signup-1", &"k".repeat(64)] {
        assert_eq!(ok(json!({ "kind": good }))["kind"], good);
    }
    let mistyped = err(r#"{"kind":1}"#);
    assert_eq!(
        (mistyped["code"].as_str(), mistyped["field"].as_str()),
        (Some("invalid_type"), Some("kind"))
    );
}

#[test]
fn bounds_input_size_depth_and_field_count() {
    let limits = json_envelope::SERVICE.limits;

    let oversized = format!(r#"{{"pad":"{}"}}"#, "x".repeat(limits.max_input_bytes));
    assert_eq!(err(&oversized)["code"], "limit_exceeded");

    let deep = format!(
        r#"{{"a":{}{}}}"#,
        "[".repeat(limits.max_depth),
        "]".repeat(limits.max_depth)
    );
    assert_eq!(err(&deep)["code"], "limit_exceeded");

    let fields: serde_json::Map<String, Value> = (0..=limits.max_fields)
        .map(|index| (format!("f{index}"), Value::from(index)))
        .collect();
    assert_eq!(
        err(&Value::Object(fields).to_string())["code"],
        "limit_exceeded"
    );
}

#[test]
fn rejects_malformed_input() {
    assert_eq!(err("{\"a\":}")["code"], "invalid_json");
    assert_eq!(err("42")["code"], "invalid_type");
}
