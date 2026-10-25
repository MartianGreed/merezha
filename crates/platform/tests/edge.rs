//! The edge's telemetry (TE §1.2, §1.3, §2.1, §4; §8 "Signals and
//! attributes", "Propagation" at the edge): HTTP server spans and metrics,
//! the traceparent trust policy and its links, `x-request-id`, the
//! `starting` rejection and the `http.request.completed` levels. No
//! database: the edge wraps a small router, under the in-memory harness.
use axum::{
    Router,
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
    routing::{get, post},
};
use merezha_platform::{
    ErrorRegistry, ProjectConfig,
    edge::{self, EdgeSettings, HttpMetrics},
    health,
};
use merezha_telemetry::{
    Service, TelemetryConfig,
    config::{LogLevel, Signal, TrustInbound},
    testing::Harness,
};
use opentelemetry::trace::{SpanKind, Status, TraceId};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};
use tower::ServiceExt;

const PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const PARENT_TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const PARENT_SPAN: &str = "00f067aa0ba902b7";
const PUBLIC_PEER: &str = "203.0.113.7:4000";
const LOOPBACK_PEER: &str = "127.0.0.1:4000";
const PROXY_PEER: &str = "10.0.0.2:4000";

fn project(extra: &str) -> ProjectConfig {
    let text = format!(
        "[app]\nid = \"edge\"\n[database]\nurl = {{ env = \"EDGE_DB\" }}\n[runtime]\nmanifest = \"runtime.toml\"\n[http]\ntrusted_proxies = [\"10.0.0.0/8\"]\n{extra}"
    );
    ProjectConfig::parse(
        &text,
        std::env::temp_dir(),
        &[("EDGE_DB".into(), "postgres://u@127.0.0.1:1/db".into())],
        &[],
    )
    .unwrap()
}
fn harness(ratio: f64) -> Harness {
    let mut config = TelemetryConfig {
        log_level: LogLevel::Debug,
        signals: Signal::ALL.to_vec(),
        ..TelemetryConfig::default()
    };
    config.logs.level = LogLevel::Debug;
    config.traces.ratio = ratio;
    config.metrics.interval = Duration::from_secs(3_600);
    Harness::with(config, Service::raw())
}
fn routes() -> Router {
    Router::new()
        .route("/v1/notes/{id}", get(|| async { "note" }))
        .route("/v1/notes", post(|body: String| async move { body }))
        .route(
            "/v1/broken",
            get(|| async {
                merezha_platform::error_response(&merezha_domain::Failure::transient("internal"))
            }),
        )
        .route(
            "/v1/busy",
            get(|| async {
                merezha_platform::error_response(&merezha_domain::Failure::transient("overloaded"))
            }),
        )
        .route(
            "/v1/late",
            get(|| async {
                merezha_platform::error_response(&merezha_domain::Failure::transient("timeout"))
            }),
        )
        .route(
            "/v1/crash",
            get(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "boom") }),
        )
        .route(
            "/v1/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(1_050)).await;
                "slow"
            }),
        )
        .merge(health::router(health::Lifecycle::new()))
}
fn edge_for(harness: &Harness, trust: TrustInbound, router: Router) -> Router {
    let config = project("");
    edge::apply(
        router,
        EdgeSettings::new(&config.http, Arc::new(ErrorRegistry::kernel()))
            .trust(trust)
            .metrics(HttpMetrics::new(&harness.telemetry().meter("merezha.edge"))),
    )
}
fn request(method: &str, path: &str, peer: &str, traceparent: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(parent) = traceparent {
        builder = builder.header("traceparent", parent);
    }
    let mut request = builder.body(Body::empty()).unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo::<SocketAddr>(peer.parse().unwrap()));
    request
}
async fn send(router: &Router, request: Request<Body>) -> axum::response::Response {
    router.clone().oneshot(request).await.unwrap()
}
fn attributes(kvs: &[opentelemetry::KeyValue]) -> BTreeMap<String, String> {
    kvs.iter()
        .map(|kv| (kv.key.to_string(), kv.value.to_string()))
        .collect()
}

#[tokio::test]
async fn server_spans_carry_the_route_template_and_semconv_attributes() {
    let harness = harness(1.0);
    let router = edge_for(&harness, TrustInbound::TrustedProxies, routes());
    let response = send(&router, request("GET", "/v1/notes/42", PUBLIC_PEER, None)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let request_id = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let span = harness.span("GET /v1/notes/{id}");
    assert_eq!(span.span_kind, SpanKind::Server);
    assert_eq!(span.status, Status::Unset);
    let attrs = attributes(&span.attributes);
    assert_eq!(attrs["http.request.method"], "GET");
    assert_eq!(attrs["http.route"], "/v1/notes/{id}");
    assert_eq!(attrs["http.response.status_code"], "200");
    assert_eq!(attrs["url.scheme"], "http");
    assert_eq!(attrs["merezha.request_id"], request_id);
    assert!(!attrs.contains_key("error.type"));
    // The raw path never reaches a span.
    assert!(!format!("{:?}", harness.spans()).contains("/v1/notes/42"));

    // Unmatched routes omit `http.route`; a 4xx leaves the status unset.
    let response = send(&router, request("GET", "/nope", PUBLIC_PEER, None)).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let span = harness.span("GET");
    assert!(!attributes(&span.attributes).contains_key("http.route"));
    assert_eq!(span.status, Status::Unset);

    // Methods outside the registry record `_OTHER`.
    let response = send(&router, request("PROPFIND", "/nope", PUBLIC_PEER, None)).await;
    assert!(response.status().is_client_error());
    let span = harness.span("_OTHER");
    assert_eq!(
        attributes(&span.attributes)["http.request.method"],
        "_OTHER"
    );

    // A host failure sets the span status and `error.type`.
    let response = send(&router, request("GET", "/v1/broken", PUBLIC_PEER, None)).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let span = harness.span("GET /v1/broken");
    assert!(matches!(span.status, Status::Error { .. }));
    assert_eq!(attributes(&span.attributes)["error.type"], "503");

    // `/health/*` gives metrics but no spans.
    let before = harness.spans().len();
    let response = send(&router, request("GET", "/health/live", PUBLIC_PEER, None)).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("x-request-id"));
    assert_eq!(harness.spans().len(), before, "no span for /health/*");

    // A declared body is measured.
    let mut post = request("POST", "/v1/notes", PUBLIC_PEER, None);
    post.headers_mut()
        .insert("content-length", "5".parse().unwrap());
    *post.body_mut() = Body::from("hello");
    assert_eq!(send(&router, post).await.status(), StatusCode::OK);

    let duration = harness.metric("http.server.request.duration");
    let point = |route: Option<&str>, status: &str| {
        duration.points().iter().any(|p| {
            p.attributes.get("http.route").map(String::as_str) == route
                && p.attributes["http.response.status_code"] == status
        })
    };
    assert!(point(Some("/v1/notes/{id}"), "200"));
    assert!(point(None, "404"), "unmatched routes omit http.route");
    assert!(point(Some("/health/live"), "200"), "health requests count");
    let broken = duration
        .points()
        .iter()
        .find(|p| p.attributes.get("http.route").map(String::as_str) == Some("/v1/broken"))
        .unwrap();
    assert_eq!(broken.attributes["error.type"], "503");
    assert_eq!(broken.attributes["url.scheme"], "http");
    assert!(
        duration
            .points()
            .iter()
            .any(|p| p.attributes["http.request.method"] == "_OTHER")
    );
    for p in duration.points() {
        for key in p.attributes.keys() {
            assert!(
                [
                    "http.request.method",
                    "http.route",
                    "http.response.status_code",
                    "error.type",
                    "url.scheme"
                ]
                .contains(&key.as_str()),
                "{key} is not an http.server.request.duration attribute"
            );
        }
    }
    let body = harness.metric("http.server.request.body.size");
    assert_eq!(
        body.value(&[("http.request.method", "POST"), ("http.route", "/v1/notes")]),
        Some(1.0)
    );
    let active = harness.metric("http.server.active_requests");
    assert!(active.points().iter().all(|p| p.value == 0.0), "{active:?}");
    harness.assert_catalogued();
}

#[tokio::test]
async fn the_trust_policy_parents_trusted_callers_and_links_untrusted_ones() {
    let harness = harness(1.0);
    let router = edge_for(&harness, TrustInbound::TrustedProxies, routes());
    let parent_trace = TraceId::from_hex(PARENT_TRACE).unwrap();

    // Untrusted: a new root that links to the caller's context, and no
    // traceparent in the response.
    let response = send(
        &router,
        request("GET", "/v1/notes/1", PUBLIC_PEER, Some(PARENT)),
    )
    .await;
    assert!(response.headers().contains_key("x-request-id"));
    assert!(!response.headers().contains_key("traceparent"));
    let untrusted = harness.spans().pop().unwrap();
    assert_ne!(untrusted.span_context.trace_id(), parent_trace);
    assert_eq!(
        untrusted.parent_span_id,
        opentelemetry::trace::SpanId::INVALID
    );
    let links = &untrusted.links.links;
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].span_context.trace_id(), parent_trace);
    assert_eq!(
        links[0]
            .attributes
            .iter()
            .map(|kv| (kv.key.to_string(), kv.value.to_string()))
            .collect::<Vec<_>>(),
        [(
            "merezha.link.reason".to_owned(),
            "untrusted_parent".to_owned()
        )]
    );

    // Trusted, by loopback or through a trusted proxy: the server span is
    // the caller's child and the response carries its context.
    for peer in [LOOPBACK_PEER, PROXY_PEER] {
        let response = send(&router, request("GET", "/v1/notes/1", peer, Some(PARENT))).await;
        let header = response.headers()["traceparent"]
            .to_str()
            .unwrap()
            .to_owned();
        let trusted = harness.spans().pop().unwrap();
        assert_eq!(trusted.span_context.trace_id(), parent_trace, "{peer}");
        assert_eq!(trusted.parent_span_id.to_string(), PARENT_SPAN, "{peer}");
        assert!(trusted.links.links.is_empty());
        assert_eq!(
            header,
            format!("00-{PARENT_TRACE}-{}-01", trusted.span_context.span_id())
        );
    }

    // A malformed header is ignored: a new root without a link.
    let response = send(
        &router,
        request("GET", "/v1/notes/1", LOOPBACK_PEER, Some("00-garbage-01")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let malformed = harness.spans().pop().unwrap();
    assert_ne!(malformed.span_context.trace_id(), parent_trace);
    assert_eq!(
        malformed.parent_span_id,
        opentelemetry::trace::SpanId::INVALID
    );
    assert!(malformed.links.links.is_empty());

    // `never` refuses even loopback; `always` honors a public peer.
    let never = edge_for(&harness, TrustInbound::Never, routes());
    let response = send(
        &never,
        request("GET", "/v1/notes/1", LOOPBACK_PEER, Some(PARENT)),
    )
    .await;
    assert!(!response.headers().contains_key("traceparent"));
    assert_ne!(
        harness.spans().pop().unwrap().span_context.trace_id(),
        parent_trace
    );
    let always = edge_for(&harness, TrustInbound::Always, routes());
    let response = send(
        &always,
        request("GET", "/v1/notes/1", PUBLIC_PEER, Some(PARENT)),
    )
    .await;
    assert!(response.headers().contains_key("traceparent"));
    assert_eq!(
        harness.spans().pop().unwrap().span_context.trace_id(),
        parent_trace
    );
}

#[tokio::test]
async fn an_untrusted_caller_cannot_force_sampling() {
    // Ratio 0: roots are never sampled; a trusted sampled parent still is.
    let harness = harness(0.0);
    let router = edge_for(&harness, TrustInbound::TrustedProxies, routes());
    send(
        &router,
        request("GET", "/v1/notes/1", PUBLIC_PEER, Some(PARENT)),
    )
    .await;
    assert!(harness.spans().is_empty(), "the sampled flag was honored");
    let response = send(
        &router,
        request("GET", "/v1/notes/1", LOOPBACK_PEER, Some(PARENT)),
    )
    .await;
    assert!(
        response.headers()["traceparent"]
            .to_str()
            .unwrap()
            .ends_with("-01")
    );
    assert_eq!(
        harness.spans().len(),
        1,
        "a trusted sampled parent is honored"
    );
}

#[tokio::test]
async fn request_completion_levels_follow_status_and_duration() {
    let harness = harness(1.0);
    let router = edge_for(&harness, TrustInbound::TrustedProxies, routes());
    for path in [
        "/v1/notes/1",
        "/nope",
        "/v1/broken",
        "/v1/crash",
        "/v1/slow",
    ] {
        send(&router, request("GET", path, PUBLIC_PEER, None)).await;
    }
    let levels: Vec<tracing::Level> = harness
        .recorded()
        .into_iter()
        .filter(|e| e.name.as_deref() == Some("http.request.completed"))
        .map(|e| e.level)
        .collect();
    use tracing::Level;
    assert_eq!(
        levels,
        [
            Level::DEBUG, // 200
            Level::DEBUG, // 404: client errors are never warn or error
            Level::INFO,  // 503
            Level::ERROR, // 500
            Level::INFO,  // slow 200
        ]
    );
    harness.assert_catalogued();
}

#[tokio::test]
async fn the_starting_listener_refuses_routes_with_a_registered_code() {
    let harness = harness(1.0);
    let config = project("");
    let lifecycle = health::Lifecycle::new();
    let router = edge::apply(
        health::router(lifecycle),
        EdgeSettings::new(&config.http, Arc::new(ErrorRegistry::kernel()))
            .metrics(HttpMetrics::new(&harness.telemetry().meter("merezha.edge")))
            .starting(),
    );
    let response = send(&router, request("POST", "/v1/notes", PUBLIC_PEER, None)).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["retry-after"], "1");
    let body = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    assert_eq!(
        &body[..],
        br#"{"error":{"classification":"transient","code":"starting"}}"#
    );
    for (path, status, body) in [
        ("/health/live", 200, r#"{"status":"live"}"#),
        ("/health/startup", 503, r#"{"status":"starting"}"#),
        ("/health/ready", 503, r#"{"status":"not_ready"}"#),
    ] {
        let response = send(&router, request("GET", path, PUBLIC_PEER, None)).await;
        assert_eq!(response.status().as_u16(), status, "{path}");
        let bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&bytes).unwrap(), body, "{path}");
    }
    let rejected = harness.metric("merezha.http.server.rejected");
    assert_eq!(rejected.value(&[("merezha.reason", "starting")]), Some(1.0));
    assert_eq!(rejected.points().len(), 1, "health is never rejected");
    assert_eq!(
        ErrorRegistry::kernel().status_of("starting"),
        Some(StatusCode::SERVICE_UNAVAILABLE)
    );
}

/// Review finding 4: `merezha.http.server.rejected` counts what the edge
/// itself refused (an oversized body here), never a module's `overloaded`
/// or `timeout` reply.
#[tokio::test]
async fn only_the_edges_own_refusals_count_as_rejected() {
    let harness = harness(1.0);
    let router = edge_for(&harness, TrustInbound::TrustedProxies, routes());
    for path in ["/v1/busy", "/v1/late"] {
        let response = send(&router, request("GET", path, PUBLIC_PEER, None)).await;
        assert!(response.status().is_server_error(), "{path}");
    }
    assert!(
        harness
            .metric("merezha.http.server.rejected")
            .points()
            .is_empty(),
        "module replies are not edge rejections"
    );
    // A declared body over `http.body_limit` (1 MiB) and an undeclared one
    // refused while it is read.
    let mut declared = request("POST", "/v1/notes", PUBLIC_PEER, None);
    declared
        .headers_mut()
        .insert("content-length", "2097152".parse().unwrap());
    assert_eq!(
        send(&router, declared).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let mut streamed = request("POST", "/v1/notes", PUBLIC_PEER, None);
    *streamed.body_mut() = Body::from(vec![b'x'; 2 << 20]);
    assert_eq!(
        send(&router, streamed).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let rejected = harness.metric("merezha.http.server.rejected");
    assert_eq!(
        rejected.value(&[("merezha.reason", "body_limit")]),
        Some(2.0)
    );
    assert_eq!(rejected.points().len(), 1);
}

/// Count a body's bytes as a route bounding its own body does: `limit`
/// when the edge stopped it.
async fn count_body(body: Body) -> String {
    use http_body_util::BodyExt;
    let mut body = body;
    let mut total = 0usize;
    while let Some(frame) = body.frame().await {
        match frame {
            Ok(frame) => total += frame.data_ref().map_or(0, |d| d.len()),
            Err(error) if edge::is_body_limit(&error) => return "limit".into(),
            Err(_) => return "error".into(),
        }
    }
    total.to_string()
}

#[tokio::test]
async fn a_route_with_its_own_body_limit_reads_past_the_edge_limit_and_stops_at_its_own() {
    let config = project("");
    let router = Router::new()
        .route("/v1/media/upload", post(count_body))
        .route("/v1/media/o/{org}/upload", post(count_body))
        .route("/v1/media/o/{org}/upload/more", post(count_body))
        .route("/v1/notes", post(|body: String| async move { body }));
    let router = edge::apply(
        router,
        EdgeSettings::new(&config.http, Arc::new(ErrorRegistry::kernel())).route_body_limits(vec![
            (axum::http::Method::POST, "/v1/media/upload".into(), 3 << 20),
            (
                axum::http::Method::POST,
                "/v1/media/o/{org}/upload".into(),
                3 << 20,
            ),
        ]),
    );
    let text = |response: axum::response::Response| async move {
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    };
    // Over `http.body_limit` (1 MiB): declared and read, the route gets it.
    let mut upload = request("POST", "/v1/media/upload", PUBLIC_PEER, None);
    upload
        .headers_mut()
        .insert("content-length", (2u32 << 20).to_string().parse().unwrap());
    *upload.body_mut() = Body::from(vec![b'x'; 2 << 20]);
    let response = send(&router, upload).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(text(response).await, (2u32 << 20).to_string());
    // A declared length past the route's own limit is the route's to
    // answer; the bytes past it never reach the route.
    let mut over = request("POST", "/v1/media/upload", PUBLIC_PEER, None);
    over.headers_mut()
        .insert("content-length", (4u32 << 20).to_string().parse().unwrap());
    *over.body_mut() = Body::from(vec![b'x'; 4 << 20]);
    let response = send(&router, over).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(text(response).await, "limit");
    // A `{name}` segment matches one segment of any value, and only one.
    let mut nested = request("POST", "/v1/media/o/org_a/upload", PUBLIC_PEER, None);
    *nested.body_mut() = Body::from(vec![b'x'; 2 << 20]);
    let response = send(&router, nested).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(text(response).await, (2u32 << 20).to_string());
    let mut deeper = request("POST", "/v1/media/o/org_a/upload/more", PUBLIC_PEER, None);
    *deeper.body_mut() = Body::from(vec![b'x'; 2 << 20]);
    assert_eq!(
        text(send(&router, deeper).await).await,
        "limit",
        "past `http.body_limit` on a route without its own"
    );
    // Other routes, and other methods on the same path, keep the edge's.
    let mut other = request("POST", "/v1/notes", PUBLIC_PEER, None);
    *other.body_mut() = Body::from(vec![b'x'; 2 << 20]);
    assert_eq!(
        send(&router, other).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let mut get = request("GET", "/v1/media/upload", PUBLIC_PEER, None);
    get.headers_mut()
        .insert("content-length", (2u32 << 20).to_string().parse().unwrap());
    assert_eq!(
        send(&router, get).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}
