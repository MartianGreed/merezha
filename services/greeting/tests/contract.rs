use greeting::handle;
use serde_json::{Value, json};

fn ok(input: Value) -> Value {
    let output = handle(&input.to_string()).expect("greeting must succeed");
    serde_json::from_str(&output).expect("output must be JSON")
}

fn err(input: Value) -> Value {
    let output = handle(&input.to_string()).expect_err("input must be rejected");
    serde_json::from_str::<Value>(&output).expect("error must be JSON")["error"].clone()
}

#[cfg(not(feature = "formal"))]
#[test]
fn greets_in_every_supported_locale() {
    let expected = [
        ("en", "Hello, Ada!"),
        ("fr", "Bonjour, Ada !"),
        ("es", "¡Hola, Ada!"),
        ("de", "Hallo, Ada!"),
        ("uk", "Привіт, Ada!"),
    ];
    for (locale, greeting) in expected {
        let output = ok(json!({ "name": "Ada", "locale": locale }));
        assert_eq!(output["greeting"], greeting, "locale {locale}");
        assert_eq!(output["greeting_style"], "casual");
    }
}

#[cfg(feature = "formal")]
#[test]
fn formal_revision_greets_in_every_supported_locale() {
    let expected = [
        ("en", "Good day, Ada."),
        ("fr", "Bien le bonjour, Ada."),
        ("es", "Buenos días, Ada."),
        ("de", "Guten Tag, Ada."),
        ("uk", "Добрий день, Ada."),
    ];
    for (locale, greeting) in expected {
        let output = ok(json!({ "name": "Ada", "locale": locale }));
        assert_eq!(output["greeting"], greeting, "locale {locale}");
        assert_eq!(output["greeting_style"], "formal");
    }
}

#[test]
fn defaults_to_english() {
    let output = ok(json!({ "name": "Ada" }));
    let expected = if cfg!(feature = "formal") {
        "Good day, Ada."
    } else {
        "Hello, Ada!"
    };
    assert_eq!(output["greeting"], expected);
}

#[test]
fn uses_text_when_name_is_absent_and_prefers_name() {
    assert!(
        ok(json!({ "text": "Ada Lovelace" }))["greeting"]
            .as_str()
            .unwrap()
            .contains("Ada Lovelace")
    );
    let both = ok(json!({ "name": "Grace", "text": "Ada" }));
    assert!(both["greeting"].as_str().unwrap().contains("Grace"));
    assert!(!both["greeting"].as_str().unwrap().contains("Ada"));
}

#[test]
fn trims_the_name() {
    assert!(
        ok(json!({ "name": "  Ada  " }))["greeting"]
            .as_str()
            .unwrap()
            .contains(", Ada")
    );
}

#[test]
fn copies_every_other_field_and_replaces_a_previous_greeting() {
    let output =
        ok(json!({ "name": "Ada", "request_id": "r-9", "greeting": "stale", "tags": ["a"] }));
    assert_eq!(output["request_id"], "r-9");
    assert_eq!(output["tags"], json!(["a"]));
    assert_ne!(output["greeting"], "stale");
    assert_eq!(output["name"], "Ada");
}

#[test]
fn reports_a_missing_name() {
    let error = err(json!({ "locale": "fr" }));
    assert_eq!(
        (error["code"].as_str(), error["field"].as_str()),
        (Some("missing_field"), Some("name"))
    );
    assert_eq!(error["service"], "greeting");
    assert_eq!(
        error["message"],
        "field `name` (or `text`) is required and must be a string"
    );
}

#[test]
fn reports_mistyped_name_or_locale() {
    let name = err(json!({ "name": ["Ada"] }));
    assert_eq!(
        (name["code"].as_str(), name["field"].as_str()),
        (Some("invalid_type"), Some("name"))
    );
    let locale = err(json!({ "name": "Ada", "locale": 7 }));
    assert_eq!(
        (locale["code"].as_str(), locale["field"].as_str()),
        (Some("invalid_type"), Some("locale"))
    );
}

#[test]
fn rejects_unknown_locale_listing_the_supported_values() {
    let error = err(json!({ "name": "Ada", "locale": "pt" }));
    assert_eq!(
        (error["code"].as_str(), error["field"].as_str()),
        (Some("invalid_value"), Some("locale"))
    );
    assert_eq!(
        error["message"],
        "field `locale` is \"pt\"; expected one of \"en\", \"fr\", \"es\", \"de\", \"uk\""
    );
}

#[test]
fn rejects_blank_names_and_names_with_control_characters() {
    let blank = err(json!({ "name": "   " }));
    assert_eq!(
        (blank["code"].as_str(), blank["field"].as_str()),
        (Some("invalid_value"), Some("name"))
    );

    let control = err(json!({ "name": "Ada\nLovelace" }));
    assert_eq!(
        (control["code"].as_str(), control["field"].as_str()),
        (Some("invalid_value"), Some("name"))
    );
    assert_eq!(
        control["message"],
        "field `name` contains control characters; normalize it first, for example with text-normalize"
    );
}

#[test]
fn bounds_the_name_length_in_characters() {
    let limit = greeting::MAX_NAME_CHARS;
    assert!(handle(&json!({ "name": "é".repeat(limit) }).to_string()).is_ok());

    let error = err(json!({ "name": "é".repeat(limit + 1) }));
    assert_eq!(
        (error["code"].as_str(), error["field"].as_str()),
        (Some("limit_exceeded"), Some("name"))
    );
    assert_eq!(
        error["message"],
        format!(
            "field `name` has {} characters; greeting accepts at most {limit}",
            limit + 1
        )
    );
}

#[test]
fn bounds_input_size() {
    let limit = greeting::SERVICE.limits.max_input_bytes;
    let input = format!(r#"{{"name":"Ada","pad":"{}"}}"#, "x".repeat(limit));
    assert_eq!(
        err(serde_json::from_str(&input).unwrap())["code"],
        "limit_exceeded"
    );
}
