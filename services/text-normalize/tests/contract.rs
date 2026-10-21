use serde_json::{Value, json};
use text_normalize::handle;

fn ok(input: Value) -> Value {
    let output = handle(&input.to_string()).expect("normalization must succeed");
    serde_json::from_str(&output).expect("output must be JSON")
}

fn err(input: &str) -> Value {
    let output = handle(input).expect_err("input must be rejected");
    serde_json::from_str::<Value>(&output).expect("error must be JSON")["error"].clone()
}

#[test]
fn trims_and_collapses_unicode_whitespace() {
    let output = ok(json!({ "text": " \t Ada\u{00A0}\u{3000} Lovelace \n" }));
    assert_eq!(output, json!({ "text": "Ada Lovelace" }));
}

#[test]
fn removes_control_characters() {
    let output = ok(json!({ "text": "Ada\u{0007}\u{001B} Love\u{0000}lace" }));
    assert_eq!(output["text"], "Ada Lovelace");
}

#[test]
fn applies_each_supported_case() {
    let cases = [
        ("keep", "ada LoveLace"),
        ("lower", "ada lovelace"),
        ("upper", "ADA LOVELACE"),
        ("title", "Ada Lovelace"),
    ];
    for (case, expected) in cases {
        let output = ok(json!({ "text": " ada   LoveLace ", "case": case }));
        assert_eq!(output["text"], expected, "case {case}");
        assert_eq!(output["case"], case, "the case option passes through");
    }
}

#[test]
fn keeps_letter_case_by_default() {
    assert_eq!(
        ok(json!({ "text": "ada LOVELACE" }))["text"],
        "ada LOVELACE"
    );
}

#[test]
fn case_mapping_handles_non_ascii_letters() {
    assert_eq!(
        ok(json!({ "text": "élodie ÇA", "case": "title" }))["text"],
        "Élodie Ça"
    );
    assert_eq!(
        ok(json!({ "text": "straße", "case": "upper" }))["text"],
        "STRASSE"
    );
    assert_eq!(
        ok(json!({ "text": "ОЛЕНА", "case": "lower" }))["text"],
        "олена"
    );
}

#[test]
fn copies_every_other_field_unchanged() {
    let input = json!({
        "text": "  x ",
        "request_id": "req-1",
        "nested": { "list": [1, 2.5, null, true], "text": "  untouched  " },
    });
    let output = ok(input.clone());
    assert_eq!(output["request_id"], input["request_id"]);
    assert_eq!(output["nested"], input["nested"]);
    assert_eq!(output.as_object().unwrap().len(), 3);
}

#[test]
fn reports_missing_or_mistyped_text() {
    let missing = err(r#"{"name":"Ada"}"#);
    assert_eq!(
        (missing["code"].as_str(), missing["field"].as_str()),
        (Some("missing_field"), Some("text"))
    );
    assert_eq!(missing["service"], "text-normalize");

    let mistyped = err(r#"{"text":42}"#);
    assert_eq!(
        (mistyped["code"].as_str(), mistyped["field"].as_str()),
        (Some("invalid_type"), Some("text"))
    );
}

#[test]
fn rejects_text_that_normalizes_to_nothing() {
    let error = err(&json!({ "text": " \u{0007}\t " }).to_string());
    assert_eq!(
        (error["code"].as_str(), error["field"].as_str()),
        (Some("invalid_value"), Some("text"))
    );
}

#[test]
fn rejects_unknown_case_listing_the_supported_values() {
    let error = err(r#"{"text":"x","case":"shout"}"#);
    assert_eq!(
        (error["code"].as_str(), error["field"].as_str()),
        (Some("invalid_value"), Some("case"))
    );
    assert_eq!(
        error["message"],
        "field `case` is \"shout\"; expected one of \"keep\", \"lower\", \"upper\", \"title\""
    );
}

#[test]
fn bounds_input_size_before_normalizing() {
    let limit = text_normalize::SERVICE.limits.max_input_bytes;
    let at_limit = format!(r#"{{"text":"{}"}}"#, "a".repeat(limit - 11));
    assert_eq!(at_limit.len(), limit);
    assert!(handle(&at_limit).is_ok());

    let over = format!(r#"{{"text":"{}"}}"#, "a".repeat(limit - 10));
    assert_eq!(err(&over)["code"], "limit_exceeded");
}

#[test]
fn rejects_malformed_input() {
    assert_eq!(err("text=Ada")["code"], "invalid_json");
    assert_eq!(err(r#"["text"]"#)["code"], "invalid_type");
}
