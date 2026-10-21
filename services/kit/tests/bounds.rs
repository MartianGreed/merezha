use merezha_service_kit::{Document, ErrorCode, Limits, Service};
use serde_json::Value;

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
    assert_eq!(SERVICE.run(r#"{"a":1}"#, Ok), Ok(r#"{"a":1}"#.to_owned()));

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

#[test]
fn run_copies_numbers_exactly_as_written() {
    // Regression for verifier finding V1-F1: integers beyond u64, long
    // decimals, negative zero and huge exponents keep their exact text.
    let input = r#"{"a":18446744073709551617,"b":18446744073709551618,"c":0.1000000000000000055511151231257827,"d":-0,"e":1e400}"#;
    let service = Service {
        name: "probe",
        limits: Limits {
            max_input_bytes: 1024,
            max_depth: 3,
            max_fields: 8,
        },
    };
    let output = service.run(input, Ok).unwrap();
    assert_eq!(output, input);
}

const WIDE: Service = Service {
    name: "probe",
    limits: Limits {
        max_input_bytes: 4096,
        max_depth: 8,
        max_fields: 16,
    },
};

fn identity(input: &str) -> String {
    WIDE.run(input, Ok).expect("identity transform succeeds")
}

#[test]
fn run_keeps_serde_json_reserved_keys_as_ordinary_data() {
    // Regression for the coordinator's boundary finding on the V1-F1 fix:
    // serde_json's internal Number and RawValue map keys must stay data.
    for input in [
        r#"{"opaque":{"$serde_json::private::Number":"123"}}"#,
        r#"{"opaque":{"$serde_json::private::Number":"123","keep":"yes"}}"#,
        r#"{"opaque":{"$serde_json::private::RawValue":"[1]"}}"#,
        r#"{"opaque":{"\u0024serde_json::private::Number":"7"}}"#,
        r#"{"$serde_json::private::Number":"1"}"#,
    ] {
        assert_eq!(identity(input), input);
    }
}

#[test]
fn run_keeps_nested_values_as_received_except_whitespace() {
    let input = "{ \"b\" : { \"z\" : [1 , \"a b\\\" c\"], \"a\":{} , \"z\":2 }, \"a\" : \"x\" }";
    assert_eq!(
        identity(input),
        r#"{"a":"x","b":{"z":[1,"a b\" c"],"a":{},"z":2}}"#
    );
}

#[test]
fn opaque_fields_are_not_reinterpreted_but_owned_fields_are_validated() {
    let input = r#"{"opaque":"\ud800","s":"\ud800"}"#;
    let document = WIDE.parse_object(input).unwrap();
    assert_eq!(document.to_json(), input);
    let error = WIDE.required_str(&document, "s").unwrap_err();
    assert_eq!(
        (error.code(), error.field()),
        (ErrorCode::InvalidValue, Some("s"))
    );
}

#[test]
fn documents_build_and_serialize_with_sorted_keys() {
    let mut inner = Document::new();
    inner.insert_u64("n", 7);
    let mut document = Document::new();
    document.insert_str("s", "quote \" and é");
    document.insert_document("inner", &inner);
    assert_eq!(document.len(), 2);
    assert_eq!(
        document.to_json(),
        r#"{"inner":{"n":7},"s":"quote \" and é"}"#
    );
    assert_eq!(document.remove("inner").unwrap().get(), r#"{"n":7}"#);
}

#[test]
fn brackets_inside_strings_do_not_count_as_nesting() {
    let input = r#"{"a":"[[[[{{{{","b":"\"[[[[","c":{"d":["]]]]"]}}"#;
    assert_eq!(identity(input), input);
}
