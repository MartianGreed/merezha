//! Native composition of the example services, mirroring how the host passes
//! each successful output to the next pipeline step. The fixtures and
//! expected outputs are the same files the real-host demo checks.

use serde_json::Value;

type Step = fn(&str) -> Result<String, String>;

const NORMALIZE: Step = text_normalize::handle;
const GREET: Step = greeting::handle;
const ENVELOPE: Step = json_envelope::handle;

const WELCOME: &str = include_str!("../../../examples/welcome/fixtures/welcome.json");
const GRACE: &str = include_str!("../../../examples/welcome/fixtures/grace.json");

fn run(steps: &[Step], input: &str) -> Result<String, String> {
    steps
        .iter()
        .try_fold(input.trim_end().to_owned(), |payload, step| step(&payload))
}

fn expected(stage: &str) -> &'static str {
    match stage {
        "stage-1-welcome" => {
            include_str!("../../../examples/welcome/expected/stage-1-welcome.json")
        }
        "stage-2-welcome" => {
            include_str!("../../../examples/welcome/expected/stage-2-welcome.json")
        }
        other => panic!("no expected output named {other}"),
    }
    .trim_end()
}

#[test]
fn normalize_then_greet_matches_stage_one() {
    assert_eq!(
        run(&[NORMALIZE, GREET], WELCOME).unwrap(),
        expected("stage-1-welcome")
    );
}

#[test]
fn normalize_greet_envelope_matches_stage_two() {
    assert_eq!(
        run(&[NORMALIZE, GREET, ENVELOPE], WELCOME).unwrap(),
        expected("stage-2-welcome")
    );
}

#[test]
fn caller_fields_survive_every_step() {
    let output: Value =
        serde_json::from_str(&run(&[NORMALIZE, GREET, ENVELOPE], WELCOME).unwrap()).unwrap();
    assert_eq!(output["kind"], "welcome");
    assert_eq!(output["data"]["request_id"], "demo-0001");
    assert_eq!(output["data"]["locale"], "fr");
}

#[test]
fn greeting_rejects_unnormalized_text_so_step_order_matters() {
    let error = run(&[GREET, ENVELOPE], WELCOME).unwrap_err();
    let error: Value = serde_json::from_str(&error).unwrap();
    assert_eq!(error["error"]["service"], "greeting");
    assert_eq!(error["error"]["field"], "text");
}

#[test]
fn clean_input_needs_no_normalization() {
    let output: Value = serde_json::from_str(&run(&[GREET, ENVELOPE], GRACE).unwrap()).unwrap();
    assert_eq!(output["data"]["name"], "Grace Hopper");
}

#[test]
fn first_error_stops_the_pipeline() {
    let error = run(&[NORMALIZE, GREET, ENVELOPE], r#"{"name":"Ada"}"#).unwrap_err();
    let error: Value = serde_json::from_str(&error).unwrap();
    assert_eq!(error["error"]["service"], "text-normalize");
    assert_eq!(error["error"]["code"], "missing_field");
}

#[test]
fn large_and_precise_numbers_survive_every_step_and_change_the_digest() {
    // Regression for verifier finding V1-F1.
    let first =
        r#"{"amount":0.1000000000000000055511151231257827,"id":18446744073709551617,"text":"ada"}"#;
    let second =
        r#"{"amount":0.1000000000000000055511151231257827,"id":18446744073709551618,"text":"ada"}"#;
    let first = run(&[NORMALIZE, GREET, ENVELOPE], first).unwrap();
    let second = run(&[NORMALIZE, GREET, ENVELOPE], second).unwrap();
    assert!(first.contains(r#""id":18446744073709551617"#), "{first}");
    assert!(
        first.contains(r#""amount":0.1000000000000000055511151231257827"#),
        "{first}"
    );
    assert!(second.contains(r#""id":18446744073709551618"#), "{second}");
    let digest =
        |output: &str| serde_json::from_str::<Value>(output).unwrap()["meta"]["digest"].clone();
    assert_ne!(digest(&first), digest(&second));
}

#[test]
fn serde_json_reserved_keys_survive_every_step_as_data() {
    // Regression for the coordinator's boundary finding on the V1-F1 fix.
    let input = r#"{"opaque":{"$serde_json::private::Number":"123","keep":"yes"},"raw":{"$serde_json::private::RawValue":"[1]"},"text":"ada"}"#;
    let output = run(&[NORMALIZE, GREET, ENVELOPE], input).unwrap();
    assert!(
        output.contains(r#""opaque":{"$serde_json::private::Number":"123","keep":"yes"}"#),
        "{output}"
    );
    assert!(
        output.contains(r#""raw":{"$serde_json::private::RawValue":"[1]"}"#),
        "{output}"
    );
}
