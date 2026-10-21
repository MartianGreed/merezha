use merezha_service_kit::{ErrorCode, Limits, Service, Value};

const SERVICE: Service = Service {
    name: "probe",
    limits: Limits {
        max_input_bytes: 64,
        max_depth: 3,
        max_fields: 2,
    },
};

fn error_code(input: &str) -> ErrorCode {
    SERVICE
        .parse_object(input)
        .expect_err("input must be rejected")
        .code()
}

#[test]
fn accepts_input_exactly_at_the_byte_limit() {
    let padding = "x".repeat(64 - r#"{"a":""}"#.len());
    let input = format!(r#"{{"a":"{padding}"}}"#);
    assert_eq!(input.len(), 64);
    assert!(SERVICE.parse_object(&input).is_ok());
}

#[test]
fn rejects_input_one_byte_over_the_limit_before_parsing() {
    // Invalid JSON one byte over the limit still reports the limit, proving
    // the size check runs before any parsing work.
    let input = "{".repeat(65);
    let error = SERVICE.parse_object(&input).unwrap_err();
    assert_eq!(error.code(), ErrorCode::LimitExceeded);
    assert_eq!(
        error.message(),
        "input is 65 bytes; probe accepts at most 64 bytes"
    );
}

#[test]
fn accepts_nesting_at_the_depth_limit() {
    assert!(SERVICE.parse_object(r#"{"a":{"b":[1]}}"#).is_ok());
}

#[test]
fn rejects_nesting_one_level_over_the_depth_limit() {
    assert_eq!(error_code(r#"{"a":{"b":[[]]}}"#), ErrorCode::LimitExceeded);
    assert_eq!(error_code(r#"{"a":[{"b":{}}]}"#), ErrorCode::LimitExceeded);
}

#[test]
fn reports_parser_recursion_overflow_as_a_depth_limit() {
    let limits = Limits {
        max_input_bytes: 4096,
        max_depth: 3,
        max_fields: 2,
    };
    let service = Service {
        name: "probe",
        limits,
    };
    let input = format!(r#"{{"a":{}{}}}"#, "[".repeat(200), "]".repeat(200));
    let error = service.parse_object(&input).unwrap_err();
    assert_eq!(error.code(), ErrorCode::LimitExceeded);
}

#[test]
fn rejects_more_top_level_fields_than_allowed() {
    assert!(SERVICE.parse_object(r#"{"a":1,"b":2}"#).is_ok());
    assert_eq!(
        error_code(r#"{"a":1,"b":2,"c":3}"#),
        ErrorCode::LimitExceeded
    );
}

#[test]
fn rejects_non_objects_and_invalid_json() {
    assert_eq!(error_code("[1,2]"), ErrorCode::InvalidType);
    assert_eq!(error_code(r#""text""#), ErrorCode::InvalidType);
    assert_eq!(error_code("{"), ErrorCode::InvalidJson);
    assert_eq!(error_code(""), ErrorCode::InvalidJson);
}

#[test]
fn string_fields_distinguish_missing_from_wrong_type() {
    let object = SERVICE.parse_object(r#"{"n":5,"s":"ok"}"#).unwrap();
    assert_eq!(SERVICE.required_str(&object, "s").unwrap(), "ok");
    assert_eq!(SERVICE.optional_str(&object, "absent").unwrap(), None);

    let missing = SERVICE.required_str(&object, "absent").unwrap_err();
    assert_eq!(
        (missing.code(), missing.field()),
        (ErrorCode::MissingField, Some("absent"))
    );

    let wrong = SERVICE.optional_str(&object, "n").unwrap_err();
    assert_eq!(
        (wrong.code(), wrong.field()),
        (ErrorCode::InvalidType, Some("n"))
    );
    assert_eq!(wrong.message(), "field `n` must be a string, got a number");
}

#[test]
fn error_document_is_stable_json() {
    let document = SERVICE
        .error(ErrorCode::InvalidValue, "explain the fix")
        .with_field("case")
        .to_json();
    assert_eq!(
        document,
        r#"{"error":{"code":"invalid_value","field":"case","message":"explain the fix","service":"probe"}}"#
    );
    let whole_input: Value =
        serde_json::from_str(&SERVICE.error(ErrorCode::InvalidJson, "m").to_json()).unwrap();
    assert!(whole_input["error"].get("field").is_none());
}

#[test]
fn run_serializes_success_and_renders_every_failure_as_an_error_document() {
    assert_eq!(
        SERVICE.run(r#"{"a":1}"#, |object| Ok(Value::Object(object))),
        Ok(r#"{"a":1}"#.to_owned())
    );

    let parse_failure = SERVICE
        .run("nope", |_| unreachable!("transform must not run"))
        .unwrap_err();
    assert!(
        parse_failure.starts_with(r#"{"error":{"code":"invalid_json""#),
        "{parse_failure}"
    );

    let transform_failure = SERVICE
        .run("{}", |_| Err(SERVICE.error(ErrorCode::InvalidValue, "no")))
        .unwrap_err();
    assert_eq!(
        transform_failure,
        r#"{"error":{"code":"invalid_value","message":"no","service":"probe"}}"#
    );
}
