//! The HTTP edge: client address through trusted proxies, request id, W3C
//! trace context, request logging, CORS, body limits, authentication and the
//! single error shape `{ "error": { "code", "classification" } }`.
use crate::{
    config::{CorsConfig, HttpConfig},
    context::{OsRandom, Random, hex},
    net::Cidr,
    ports::{Authenticator, Principal},
};
use axum::{
    Json, Router,
    body::Body,
    extract::{ConnectInfo, MatchedPath, Request, State},
    http::{HeaderName, HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use merezha_domain::{Classification, Failure};
use serde_json::json;
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Instant,
};
use tracing::Instrument;

pub const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
pub const TRACEPARENT: HeaderName = HeaderName::from_static("traceparent");
const FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");
const FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");
const FORWARDED: HeaderName = HeaderName::from_static("forwarded");
/// Hops read from `X-Forwarded-For`; longer chains use the last ones.
const MAX_FORWARDED_HOPS: usize = 32;

/// HTTP status for a classified failure. Stable codes with a fixed meaning
/// map first; anything else maps by classification.
pub fn status_for(failure: &Failure) -> StatusCode {
    match failure.code.as_str() {
        "not_found" | "handler_not_found" | "projection_not_found" => StatusCode::NOT_FOUND,
        "method_not_allowed" => StatusCode::METHOD_NOT_ALLOWED,
        "unauthenticated" => StatusCode::UNAUTHORIZED,
        "unauthorized" => StatusCode::FORBIDDEN,
        "body_limit" | "input_limit" | "payload_limit" => StatusCode::PAYLOAD_TOO_LARGE,
        "timeout" | "upstream_timeout" => StatusCode::GATEWAY_TIMEOUT,
        "overloaded" | "database_busy" => StatusCode::TOO_MANY_REQUESTS,
        "invalid_input" => StatusCode::BAD_REQUEST,
        _ => match failure.classification {
            Classification::Permanent => StatusCode::UNPROCESSABLE_ENTITY,
            Classification::Conflict => StatusCode::CONFLICT,
            Classification::Transient => StatusCode::SERVICE_UNAVAILABLE,
        },
    }
}
/// The one error response. Only the stable code and classification are
/// sent: never messages, secrets or guest text.
pub fn error_response(failure: &Failure) -> Response {
    (status_for(failure), Json(json!({ "error": failure }))).into_response()
}

/// The request id, from a trusted proxy or generated here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestId(pub String);

/// Client facts after trusted-proxy processing. `X-Forwarded-*` headers are
/// removed from the request; read them only from here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientInfo {
    pub address: Option<IpAddr>,
    /// `https` or `http` from a trusted proxy, otherwise `None`.
    pub forwarded_proto: Option<String>,
    pub forwarded_host: Option<String>,
    pub via_trusted_proxy: bool,
}

/// The authenticated principal, inserted by the edge when an
/// [`Authenticator`] is configured. `None` is an anonymous request.
#[derive(Clone, Debug)]
pub struct Authenticated(pub Option<Principal>);

/// W3C trace context for one server span.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: [u8; 16],
    /// The caller's span, when the request carried a valid `traceparent`.
    pub parent_id: Option<[u8; 8]>,
    /// This request's span.
    pub span_id: [u8; 8],
    pub sampled: bool,
}
impl TraceContext {
    /// Start a new trace.
    pub fn root(random: &dyn Random) -> Self {
        let mut trace_id = [0u8; 16];
        let mut span_id = [0u8; 8];
        // Trace identifiers are not secrets; fall back to a time-derived
        // value rather than failing the request if the CSPRNG is unavailable.
        if random.fill(&mut trace_id).is_err() || trace_id == [0; 16] {
            trace_id = fallback_id();
        }
        if random.fill(&mut span_id).is_err() || span_id == [0; 8] {
            span_id.copy_from_slice(&fallback_id()[..8]);
        }
        Self {
            trace_id,
            parent_id: None,
            span_id,
            sampled: false,
        }
    }
    /// Continue the trace in a `traceparent` header value, with a new span.
    /// Returns `None` for anything but a valid version-00 header.
    pub fn parse(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.trim().split('-').collect();
        let [version, trace, parent, flags] = parts.as_slice() else {
            return None;
        };
        if *version != "00" {
            return None;
        }
        let trace_id: [u8; 16] = decode_hex(trace)?;
        let parent_id: [u8; 8] = decode_hex(parent)?;
        let flags: [u8; 1] = decode_hex(flags)?;
        if trace_id == [0; 16] || parent_id == [0; 8] {
            return None;
        }
        let mut context = Self::root(&OsRandom);
        context.trace_id = trace_id;
        context.parent_id = Some(parent_id);
        context.sampled = flags[0] & 1 == 1;
        Some(context)
    }
    pub fn trace_id_hex(&self) -> String {
        hex(&self.trace_id)
    }
    /// `traceparent` naming this span as the parent of a downstream call.
    pub fn child_header(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            hex(&self.trace_id),
            hex(&self.span_id),
            u8::from(self.sampled)
        )
    }
}
fn decode_hex<const N: usize>(text: &str) -> Option<[u8; N]> {
    if text.len() != N * 2 || !text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let mut out = [0u8; N];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(out)
}
fn fallback_id() -> [u8; 16] {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        | 1;
    nanos.to_be_bytes()
}
fn valid_request_id(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-:/".contains(&b))
}

#[derive(Clone)]
struct EdgeState {
    trusted: Arc<[Cidr]>,
    body_limit: usize,
    authenticator: Option<Arc<dyn Authenticator>>,
}

/// Wrap a composed router with the edge layers. Handlers can extract
/// [`RequestId`], [`ClientInfo`], [`TraceContext`] and, when an
/// authenticator is set, [`Authenticated`] from request extensions.
pub fn apply(
    router: Router,
    http: &HttpConfig,
    authenticator: Option<Arc<dyn Authenticator>>,
) -> Router {
    let state = EdgeState {
        trusted: http.trusted_proxies.clone().into(),
        body_limit: http.body_limit,
        authenticator,
    };
    let router = router
        .fallback(|| async { error_response(&Failure::permanent("not_found")) })
        .method_not_allowed_fallback(|| async {
            error_response(&Failure::permanent("method_not_allowed"))
        })
        .layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .layer(middleware::from_fn_with_state(state.clone(), limit_body))
        // Extractors such as `Bytes` and `Json` carry their own 2 MiB default;
        // `http.body_limit` replaces it in both directions.
        .layer(axum::extract::DefaultBodyLimit::max(http.body_limit))
        .layer(middleware::from_fn(normalize_errors));
    let router = match cors(&http.cors) {
        Some(layer) => router.layer(layer),
        None => router,
    };
    router.layer(middleware::from_fn_with_state(state, context))
}

fn cors(config: &CorsConfig) -> Option<tower_http::cors::CorsLayer> {
    use tower_http::cors::{AllowOrigin, CorsLayer};
    if config.allowed_origins.is_empty() {
        return None;
    }
    let origin = if config.allowed_origins.iter().any(|o| o == "*") {
        AllowOrigin::any()
    } else {
        AllowOrigin::list(
            config
                .allowed_origins
                .iter()
                .filter_map(|o| HeaderValue::from_str(o).ok()),
        )
    };
    Some(
        CorsLayer::new()
            .allow_origin(origin)
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::PATCH,
                Method::DELETE,
            ])
            .allow_headers([
                header::CONTENT_TYPE,
                header::AUTHORIZATION,
                REQUEST_ID,
                TRACEPARENT,
            ])
            .expose_headers([REQUEST_ID])
            .allow_credentials(config.allow_credentials)
            .max_age(config.max_age),
    )
}

/// Bound on the joined `X-Forwarded-For` value; a longer chain is ignored
/// and the trusted peer itself is the client.
const MAX_FORWARDED_BYTES: usize = 4096;

/// The client behind a trusted `peer`: walk every `X-Forwarded-For` line,
/// joined in order, from the right. Trusted hops are skipped and the first
/// untrusted one is the client. An unparseable hop ends the trusted chain,
/// so the last trusted hop seen is the client. When every hop is trusted,
/// the leftmost one read is.
fn forwarded_client(
    headers: &axum::http::HeaderMap,
    peer: Option<IpAddr>,
    trusted: &[Cidr],
) -> Option<IpAddr> {
    let mut lines = Vec::new();
    for value in headers.get_all(&FORWARDED_FOR) {
        let Ok(line) = value.to_str() else {
            return peer;
        };
        lines.push(line);
    }
    let chain = lines.join(",");
    if chain.len() > MAX_FORWARDED_BYTES {
        return peer;
    }
    let mut client = peer;
    for hop in chain.rsplit(',').take(MAX_FORWARDED_HOPS) {
        let Ok(ip) = hop.trim().parse::<IpAddr>() else {
            break;
        };
        let ip = ip.to_canonical();
        client = Some(ip);
        if !trusted.iter().any(|c| c.contains(ip)) {
            break;
        }
    }
    client
}
/// The last comma-separated element of the last line of `name`.
fn last_value(headers: &axum::http::HeaderMap, name: &HeaderName) -> Option<String> {
    let line = headers.get_all(name).iter().next_back()?.to_str().ok()?;
    line.rsplit(',').next().map(|v| v.trim().to_owned())
}

async fn context(State(state): State<EdgeState>, mut request: Request, next: Next) -> Response {
    let started = Instant::now();
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip().to_canonical());
    let via_trusted_proxy = peer.is_some_and(|ip| state.trusted.iter().any(|c| c.contains(ip)));
    let headers = request.headers();
    let text = |name: &HeaderName| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let mut info = ClientInfo {
        address: peer,
        forwarded_proto: None,
        forwarded_host: None,
        via_trusted_proxy,
    };
    let mut request_id = None;
    if via_trusted_proxy {
        info.address = forwarded_client(headers, peer, &state.trusted);
        // Proxies append: the last value is the one the nearest trusted
        // proxy wrote.
        info.forwarded_proto = last_value(headers, &FORWARDED_PROTO)
            .map(|p| p.to_ascii_lowercase())
            .filter(|p| p == "https" || p == "http");
        info.forwarded_host = last_value(headers, &FORWARDED_HOST)
            .map(|h| h.to_ascii_lowercase())
            .filter(|h| {
                (1..=253).contains(&h.len())
                    && h.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-.:[]".contains(&b))
            });
        request_id = text(&REQUEST_ID).filter(|id| valid_request_id(id));
    }
    let request_id = request_id.unwrap_or_else(|| {
        let mut bytes = [0u8; 16];
        if OsRandom.fill(&mut bytes).is_err() {
            bytes = fallback_id();
        }
        hex(&bytes)
    });
    let trace = text(&TRACEPARENT)
        .and_then(|h| TraceContext::parse(&h))
        .unwrap_or_else(|| TraceContext::root(&OsRandom));
    let headers = request.headers_mut();
    for name in [FORWARDED_FOR, FORWARDED_PROTO, FORWARDED_HOST, FORWARDED] {
        headers.remove(name);
    }
    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned());
    let extensions = request.extensions_mut();
    extensions.insert(RequestId(request_id.clone()));
    extensions.insert(info);
    extensions.insert(trace);
    let span = tracing::info_span!(
        "request",
        request_id = request_id.as_str(),
        trace_id = trace.trace_id_hex().as_str()
    );
    let mut response = next.run(request).instrument(span).await;
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID, value);
    }
    tracing::info!(
        event = "http.request",
        method = method.as_str(),
        route = route.as_deref().unwrap_or("unmatched"),
        status = response.status().as_u16(),
        duration_ms = started.elapsed().as_millis() as u64,
        request_id = request_id.as_str(),
        trace_id = trace.trace_id_hex().as_str()
    );
    response
}

/// Largest error body inspected by [`normalize_errors`].
const MAX_ERROR_BODY: usize = 65_536;

/// The classification implied by an HTTP status when a handler did not say.
fn classification_for(status: StatusCode) -> Classification {
    match status.as_u16() {
        409 => Classification::Conflict,
        408 | 429 => Classification::Transient,
        code if code >= 500 => Classification::Transient,
        _ => Classification::Permanent,
    }
}
/// A stable code for an error response that carried none.
fn code_for(status: StatusCode) -> &'static str {
    match status.as_u16() {
        401 => "unauthenticated",
        403 => "unauthorized",
        404 => "not_found",
        405 => "method_not_allowed",
        408 | 504 => "timeout",
        409 => "conflict",
        413 => "body_limit",
        415 => "unsupported_media_type",
        429 => "overloaded",
        503 => "unavailable",
        code if code >= 500 => "internal",
        _ => "invalid_input",
    }
}

/// Give every error response the one shape. Handler failures already in
/// shape and JSON objects without an `error` member pass through; `{ "error": { "code" } }` documents from the raw
/// runtime gain the classification implied by their status; anything else,
/// such as an extractor's plain-text rejection, is replaced by a code derived
/// from the status. The status itself is never changed.
async fn normalize_errors(request: Request, next: Next) -> Response {
    let response = next.run(request).await;
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error()) {
        return response;
    }
    let json = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    let (mut parts, body) = response.into_parts();
    let bytes = if json {
        axum::body::to_bytes(body, MAX_ERROR_BODY).await.ok()
    } else {
        None
    };
    let document = bytes
        .as_ref()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(b).ok());
    let error = document.as_ref().and_then(|d| d.get("error"));
    // Shaped failures and JSON documents that are not errors, such as the
    // readiness status, pass through unchanged.
    if let (Some(bytes), Some(document)) = (bytes.as_ref(), document.as_ref())
        && error.is_none_or(|e| serde_json::from_value::<Failure>(e.clone()).is_ok())
        && document.is_object()
    {
        return Response::from_parts(parts, Body::from(bytes.clone()));
    }
    let code = error
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_str())
        .filter(|c| merezha_domain::valid_name(c))
        .unwrap_or_else(|| code_for(status));
    let failure = Failure::new(code, classification_for(status));
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    let body = serde_json::to_vec(&json!({ "error": failure })).unwrap_or_default();
    Response::from_parts(parts, Body::from(body))
}

async fn limit_body(State(state): State<EdgeState>, request: Request, next: Next) -> Response {
    let declared = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|length| length > state.body_limit as u64) {
        return error_response(&Failure::permanent("body_limit"));
    }
    // Chunked or undeclared bodies fail while being read past the limit.
    let request =
        request.map(|body| Body::new(http_body_util::Limited::new(body, state.body_limit)));
    next.run(request).await
}

async fn authenticate(State(state): State<EdgeState>, request: Request, next: Next) -> Response {
    let Some(authenticator) = state.authenticator.clone() else {
        return next.run(request).await;
    };
    let (mut parts, body) = request.into_parts();
    match authenticator.authenticate(&parts).await {
        Ok(principal) => {
            parts.extensions.insert(Authenticated(principal));
            next.run(Request::from_parts(parts, body)).await
        }
        Err(failure) => error_response(&failure),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traceparent_is_validated_and_continued_with_a_new_span() {
        let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let context = TraceContext::parse(header).unwrap();
        assert_eq!(context.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(
            context.parent_id.map(|p| hex(&p)).unwrap(),
            "00f067aa0ba902b7"
        );
        assert!(context.sampled);
        assert_ne!(hex(&context.span_id), "00f067aa0ba902b7");
        assert!(
            context
                .child_header()
                .starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-")
        );
        for bad in [
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",
            "garbage",
        ] {
            assert!(TraceContext::parse(bad).is_none(), "{bad}");
        }
    }
    #[test]
    fn forwarded_for_uses_every_header_line_and_stops_at_garbage() {
        let trusted: Vec<Cidr> = vec!["10.0.0.0/8".parse().unwrap()];
        let peer = Some("10.0.0.2".parse().unwrap());
        let headers = |lines: &[&str]| {
            let mut map = axum::http::HeaderMap::new();
            for line in lines {
                map.append(FORWARDED_FOR, HeaderValue::from_str(line).unwrap());
            }
            map
        };
        let client = |lines: &[&str]| {
            forwarded_client(&headers(lines), peer, &trusted).map(|ip| ip.to_string())
        };
        // The client's own line comes first; the trusted proxy appends.
        assert_eq!(
            client(&["6.6.6.6", "203.0.113.7"]).as_deref(),
            Some("203.0.113.7")
        );
        assert_eq!(
            client(&["6.6.6.6, 203.0.113.7, 10.1.1.1"]).as_deref(),
            Some("203.0.113.7")
        );
        // Garbage ends the trusted chain instead of being skipped.
        assert_eq!(
            client(&["6.6.6.6, garbage, 10.1.1.1"]).as_deref(),
            Some("10.1.1.1")
        );
        assert_eq!(client(&["garbage"]).as_deref(), Some("10.0.0.2"));
        assert_eq!(client(&[]).as_deref(), Some("10.0.0.2"));
        assert_eq!(client(&["10.3.3.3, 10.1.1.1"]).as_deref(), Some("10.3.3.3"));
        let long = vec!["203.0.113.7"; 400].join(",");
        assert_eq!(client(&[&long]).as_deref(), Some("10.0.0.2"));
        let mut map = headers(&[]);
        map.append(FORWARDED_PROTO, HeaderValue::from_static("http"));
        map.append(FORWARDED_PROTO, HeaderValue::from_static("http, https"));
        assert_eq!(last_value(&map, &FORWARDED_PROTO).as_deref(), Some("https"));
    }
    #[test]
    fn statuses_follow_codes_then_classification() {
        assert_eq!(status_for(&Failure::permanent("not_found")), 404);
        assert_eq!(status_for(&Failure::permanent("body_limit")), 413);
        assert_eq!(status_for(&Failure::permanent("other")), 422);
        assert_eq!(status_for(&Failure::conflict("other")), 409);
        assert_eq!(status_for(&Failure::transient("other")), 503);
    }
}
