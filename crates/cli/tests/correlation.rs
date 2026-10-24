//! Host-minted correlation and message ids (TE §2.2, AP4, §8 "Propagation"):
//! the HTTP adapter fills a missing `correlation_id` with the current trace
//! id and a missing `message_id` with 128 random bits, and keeps
//! caller-supplied ones.
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use merezha_application::{AllowAll, Application, Guest, GuestSession, config::Config};
use merezha_domain::{Event, Failure, Identity, Operation, Reply};
use merezha_telemetry::{
    names::key,
    testing::{Harness, span_attribute},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;
use tracing::Instrument;

#[derive(Clone)]
struct Fake;
impl Guest for Fake {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["domain".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(Fake))
    }
}
impl GuestSession for Fake {
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        Ok(match operation {
            Operation::Validate { payload, .. } => Reply::Validated { payload },
            Operation::Decide { .. } => Reply::Decided {
                events: vec![Event {
                    tag: Identity::new("Added").unwrap(),
                    schema_version: 1,
                    payload: json!({}),
                }],
            },
            Operation::Project { state, .. } => Reply::Projected { state },
            Operation::Query { state, .. } => Reply::Queried { value: state },
        })
    }
}
fn router() -> Router {
    let config = Config::parse(
        r#"version=1
[[commands]]
tag="Add"
service="domain"
aggregate="Counter"
[[queries]]
tag="Total"
service="domain"
projection="total"
[[projections]]
name="total"
service="domain"
initial=0
"#,
    )
    .unwrap();
    let app = Application::in_memory(config, Arc::new(Fake), Arc::new(AllowAll)).unwrap();
    merezha_cli::application_http::router(app, None)
}
/// POST `body` inside a server-like span, as the edge runs handlers (its
/// target is Merezha's, so the span bridge records it).
async fn post(router: &Router, path: &str, body: Value) -> u16 {
    let request = Request::post(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = router
        .clone()
        .oneshot(request)
        .instrument(tracing::info_span!(target: "merezha", "request"))
        .await
        .unwrap();
    let status = response.status().as_u16();
    let _ = to_bytes(response.into_body(), 65_536).await.unwrap();
    status
}
fn hex32(text: &str) -> bool {
    text.len() == 32
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[tokio::test]
async fn a_host_minted_correlation_id_is_the_trace_id_and_a_supplied_one_is_kept() {
    let harness = Harness::new();
    let router = router();
    let path = "/v1/application/commands";
    for _ in 0..2 {
        assert_eq!(
            post(
                &router,
                path,
                json!({"tag":"Add","stream":"one","payload":{}})
            )
            .await,
            200
        );
    }
    assert_eq!(
        post(
            &router,
            path,
            json!({"tag":"Add","stream":"one","payload":{},"metadata":{"correlation_id":"order-42","message_id":"m-1"}}),
        )
        .await,
        200
    );
    // A metadata object without ids gets both.
    assert_eq!(
        post(
            &router,
            "/v1/application/queries",
            json!({"tag":"Total","payload":{},"metadata":{"actor":"alice"}}),
        )
        .await,
        200
    );
    let commands = harness.spans_named("command Add");
    assert_eq!(commands.len(), 3);
    // Each command is a child of its request.
    for (command, request) in commands.iter().zip(harness.spans_named("request")) {
        assert_eq!(command.parent_span_id, request.span_context.span_id());
    }
    let mut messages = Vec::new();
    for span in &commands[..2] {
        let correlation = span_attribute(span, key::CORRELATION_ID).unwrap();
        assert_eq!(correlation, span.span_context.trace_id().to_string());
        let message = span_attribute(span, key::MESSAGE_ID).unwrap();
        assert!(hex32(&message), "{message}");
        messages.push(message);
    }
    assert_ne!(messages[0], messages[1]);
    assert_ne!(
        commands[0].span_context.trace_id(),
        commands[1].span_context.trace_id()
    );
    let supplied = &commands[2];
    assert_eq!(
        span_attribute(supplied, key::CORRELATION_ID).as_deref(),
        Some("order-42")
    );
    assert_eq!(
        span_attribute(supplied, key::MESSAGE_ID).as_deref(),
        Some("m-1")
    );
    let query = harness.span("query Total");
    assert_eq!(
        span_attribute(&query, key::CORRELATION_ID).unwrap(),
        query.span_context.trace_id().to_string()
    );
}

#[test]
fn without_a_trace_the_correlation_id_is_random_hex_and_bad_bodies_stay_invalid() {
    let filled: Value = serde_json::from_str(
        &merezha_cli::application_http::with_host_ids(r#"{"tag":"Add","payload":1}"#).unwrap(),
    )
    .unwrap();
    let metadata = &filled["metadata"];
    let (correlation, message) = (
        metadata["correlation_id"].as_str().unwrap(),
        metadata["message_id"].as_str().unwrap(),
    );
    assert!(hex32(correlation) && hex32(message));
    assert_ne!(correlation, message);
    for bad in ["[]", "not json", r#"{"metadata":3}"#] {
        let result = merezha_cli::application_http::with_host_ids(bad)
            .and_then(|text| merezha_domain::decode::<merezha_domain::Command>(&text));
        assert_eq!(result.unwrap_err().code, "validation_failed", "{bad}");
    }
}
