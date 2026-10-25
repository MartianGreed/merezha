//! The HTTP edge: client address through trusted proxies, request id, the
//! server span with the W3C trace context trust policy (TE §2.1), HTTP
//! server metrics, request logging, CORS, body limits, authentication and
//! the single error shape `{ "error": { "code", "classification",
//! "details"? } }` with statuses from the [`ErrorRegistry`].
use crate::{
    config::{CorsConfig, HttpConfig},
    context::{OsRandom, Random, hex},
    errors::{
        ErrorBody, ErrorRegistry, FieldError, MAX_DETAILS, kernel_registry, needs_retry_after,
    },
    net::Cidr,
    ports::{Authenticator, Principal, PrincipalKind},
};
use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, MatchedPath, Request, State},
    http::{HeaderName, HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use merezha_domain::{Classification, Failure};
use merezha_telemetry::{
    config::TrustInbound,
    names::{self, Reason, UrlScheme, attr},
    propagation::{self, Inbound},
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter, UpDownCounter},
};
use serde_json::json;
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::{Instrument, field::Empty};

pub const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
pub const TRACEPARENT: HeaderName = HeaderName::from_static("traceparent");
pub const TRACESTATE: HeaderName = HeaderName::from_static("tracestate");
/// The CSRF token cookie-authenticated unsafe requests carry (identity
/// section 4); CORS allows it so a cross-origin front end can send it.
pub const CSRF_TOKEN: HeaderName = HeaderName::from_static("x-csrf-token");
/// The header that selects the active organization for one request
/// (identity section 4).
pub const ORGANIZATION: HeaderName = HeaderName::from_static("merezha-organization");
const FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");
const FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");
const FORWARDED: HeaderName = HeaderName::from_static("forwarded");
/// Hops read from `X-Forwarded-For`; longer chains use the last ones.
const MAX_FORWARDED_HOPS: usize = 32;
/// Requests at least this slow log `http.request.completed` at info.
const SLOW_REQUEST: Duration = Duration::from_secs(1);

/// HTTP status for a classified failure from the kernel's codes alone. Behind
/// the edge, [`ErrorReply`] responses get the host registry's status instead,
/// which includes every module's codes.
pub fn status_for(failure: &Failure) -> StatusCode {
    kernel_registry().status(failure)
}
/// The one error response for `failure`. Only the stable code and
/// classification are sent: never messages, secrets or guest text.
pub fn error_response(failure: &Failure) -> Response {
    ErrorReply::new(failure.clone()).into_response()
}

/// An error response under construction: a failure, optional field-level
/// `details` and an optional `Retry-After`.
///
/// Outside a host its status comes from the kernel codes; behind the edge
/// the host's [`ErrorRegistry`] sets it, so a module code keeps one status
/// everywhere. Every 429 and 503 carries `Retry-After`, one second unless
/// set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorReply {
    failure: Failure,
    details: Vec<FieldError>,
    retry_after: Option<Duration>,
}
impl ErrorReply {
    pub fn new(failure: Failure) -> Self {
        Self {
            failure,
            details: Vec::new(),
            retry_after: None,
        }
    }
    /// Field-level validation errors, at most [`MAX_DETAILS`]; later ones
    /// are dropped.
    pub fn details(mut self, details: Vec<FieldError>) -> Self {
        self.details = details;
        self.details.truncate(MAX_DETAILS);
        self
    }
    pub fn retry_after(mut self, after: Duration) -> Self {
        self.retry_after = Some(after);
        self
    }
    pub fn failure(&self) -> &Failure {
        &self.failure
    }
    /// The JSON body bytes. Without details they are byte for byte the
    /// kernel's earlier `{"error":{"classification":..,"code":..}}`.
    pub fn body(&self) -> Vec<u8> {
        let body = json!({ "error": ErrorBody {
            code: &self.failure.code,
            classification: self.failure.classification,
            details: &self.details,
        }});
        serde_json::to_vec(&body).unwrap_or_default()
    }
    fn response(&self, status: StatusCode) -> Response {
        let mut response = (
            status,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            self.body(),
        )
            .into_response();
        set_retry_after(&mut response, self.retry_after);
        response.extensions_mut().insert(Registered {
            failure: self.failure.clone(),
            retry_after: self.retry_after,
        });
        response
    }
}
impl IntoResponse for ErrorReply {
    fn into_response(self) -> Response {
        self.response(status_for(&self.failure))
    }
}
/// Marks an [`ErrorReply`] response so the edge applies the host registry.
#[derive(Clone, Debug)]
struct Registered {
    failure: Failure,
    retry_after: Option<Duration>,
}
/// Add `Retry-After` in whole seconds (at least 1) to a 429 or 503 lacking it.
fn set_retry_after(response: &mut Response, after: Option<Duration>) {
    if !needs_retry_after(response.status()) {
        return;
    }
    let seconds = after
        .map(|d| d.as_secs() + u64::from(d.subsec_nanos() > 0))
        .unwrap_or(1)
        .max(1);
    if after.is_some() || !response.headers().contains_key(header::RETRY_AFTER) {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
    }
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
pub struct Authenticated(pub Option<Arc<Principal>>);

/// W3C trace context of one server span. With the OTLP pipeline these are
/// the span's own ids; without it the edge mints them the same way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: [u8; 16],
    /// The caller's span, when a trusted caller sent a valid `traceparent`
    /// (TE §2.1). An untrusted caller's context is a link, not a parent.
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

/// HTTP server instruments (TE §1.3, "HTTP edge"), built once from the
/// host's meter; a no-op meter records nothing.
#[derive(Clone, Debug)]
pub struct HttpMetrics {
    duration: Histogram<f64>,
    active: UpDownCounter<i64>,
    body_size: Histogram<u64>,
    rejected: Counter<u64>,
}
/// Instrumentation scope of the edge's HTTP instruments.
pub const EDGE_SCOPE: &str = "merezha.edge";

impl HttpMetrics {
    pub fn new(meter: &Meter) -> Self {
        use names::metric::{
            HTTP_SERVER_ACTIVE_REQUESTS as ACTIVE, HTTP_SERVER_REJECTED as REJECTED,
            HTTP_SERVER_REQUEST_BODY_SIZE as BODY, HTTP_SERVER_REQUEST_DURATION as DURATION,
        };
        Self {
            duration: meter
                .f64_histogram(DURATION.name)
                .with_unit(DURATION.unit)
                .with_boundaries(DURATION.buckets.to_vec())
                .build(),
            active: meter
                .i64_up_down_counter(ACTIVE.name)
                .with_unit(ACTIVE.unit)
                .build(),
            body_size: meter
                .u64_histogram(BODY.name)
                .with_unit(BODY.unit)
                .with_boundaries(BODY.buckets.to_vec())
                .build(),
            rejected: meter
                .u64_counter(REJECTED.name)
                .with_unit(REJECTED.unit)
                .build(),
        }
    }
    /// Instruments on a no-op meter.
    pub fn noop() -> Self {
        use opentelemetry::metrics::MeterProvider;
        Self::new(&opentelemetry::metrics::noop::NoopMeterProvider::new().meter(EDGE_SCOPE))
    }
}
impl Default for HttpMetrics {
    fn default() -> Self {
        Self::noop()
    }
}

/// What [`apply`] wraps a router with.
#[derive(Clone)]
pub struct EdgeSettings {
    http: HttpConfig,
    authenticator: Option<Arc<dyn Authenticator>>,
    errors: Arc<ErrorRegistry>,
    trust: TrustInbound,
    metrics: HttpMetrics,
    starting: bool,
    body_limits: Vec<(Method, String, usize)>,
}
impl std::fmt::Debug for EdgeSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeSettings")
            .field("trust", &self.trust)
            .field("starting", &self.starting)
            .finish_non_exhaustive()
    }
}
impl EdgeSettings {
    /// `http` limits, proxies and CORS with `errors` statuses; no
    /// authenticator, the default `trusted_proxies` trust policy and no
    /// metrics.
    pub fn new(http: &HttpConfig, errors: Arc<ErrorRegistry>) -> Self {
        Self {
            http: http.clone(),
            authenticator: None,
            errors,
            trust: TrustInbound::TrustedProxies,
            metrics: HttpMetrics::noop(),
            starting: false,
            body_limits: Vec::new(),
        }
    }
    /// Routes that bound their own body (`Module::body_limits`), as
    /// `(method, full path, limit)`: for an exact match the edge stops the
    /// body past `limit` instead of `http.body_limit` and leaves a declared
    /// `Content-Length` to the route, which answers its own code.
    pub fn route_body_limits(mut self, limits: Vec<(Method, String, usize)>) -> Self {
        self.body_limits = limits;
        self
    }
    pub fn authenticator(mut self, authenticator: Option<Arc<dyn Authenticator>>) -> Self {
        self.authenticator = authenticator;
        self
    }
    /// `telemetry.traces.trust_inbound`.
    pub fn trust(mut self, trust: TrustInbound) -> Self {
        self.trust = trust;
        self
    }
    pub fn metrics(mut self, metrics: HttpMetrics) -> Self {
        self.metrics = metrics;
        self
    }
    /// Unmatched routes answer `503 starting` with `Retry-After: 1` instead
    /// of `404 not_found`: the early listener while startup runs.
    pub fn starting(mut self) -> Self {
        self.starting = true;
        self
    }
}

#[derive(Clone)]
struct EdgeState {
    trusted: Arc<[Cidr]>,
    body_limit: usize,
    route_limits: Arc<[(Method, String, usize)]>,
    authenticator: Option<Arc<dyn Authenticator>>,
    errors: Arc<ErrorRegistry>,
    trust: TrustInbound,
    metrics: HttpMetrics,
}

/// Wrap a composed router with the edge layers. Handlers can extract
/// [`RequestId`], [`ClientInfo`], [`TraceContext`] and, when an
/// authenticator is set, [`Authenticated`] from request extensions. Error
/// statuses come from the settings' registry.
pub fn apply(router: Router, settings: EdgeSettings) -> Router {
    let http = &settings.http;
    let state = EdgeState {
        trusted: http.trusted_proxies.clone().into(),
        body_limit: http.body_limit,
        route_limits: settings.body_limits.clone().into(),
        authenticator: settings.authenticator.clone(),
        errors: settings.errors.clone(),
        trust: settings.trust,
        metrics: settings.metrics.clone(),
    };
    let router = if settings.starting {
        router.fallback(|| async {
            let mut response = ErrorReply::new(Failure::transient("starting"))
                .retry_after(Duration::from_secs(1))
                .into_response();
            response.extensions_mut().insert(Rejected(Reason::Starting));
            response
        })
    } else {
        router.fallback(|| async { error_response(&Failure::permanent("not_found")) })
    };
    let router = router
        .method_not_allowed_fallback(|| async {
            error_response(&Failure::permanent("method_not_allowed"))
        })
        .layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .layer(middleware::from_fn_with_state(state.clone(), limit_body))
        // Extractors such as `Bytes` and `Json` carry their own 2 MiB default;
        // `http.body_limit` replaces it in both directions.
        .layer(axum::extract::DefaultBodyLimit::max(http.body_limit))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            normalize_errors,
        ));
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
                TRACESTATE,
                CSRF_TOKEN,
                ORGANIZATION,
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

/// The one value of a header that may appear once, if valid text.
fn single(headers: &axum::http::HeaderMap, name: &HeaderName) -> Option<String> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    value.to_str().ok().map(str::to_owned)
}
/// Every `tracestate` line joined in order, as W3C allows splitting it;
/// `None` past the stored bound, so the parent is kept without state.
fn trace_state(headers: &axum::http::HeaderMap) -> Option<String> {
    let mut joined = String::new();
    for value in headers.get_all(&TRACESTATE) {
        let line = value.to_str().ok()?;
        if !joined.is_empty() {
            joined.push(',');
        }
        joined.push_str(line);
        if joined.len() > propagation::MAX_TRACE_STATE {
            return None;
        }
    }
    (!joined.is_empty()).then_some(joined)
}
/// The handler-visible context: the server span's own ids when a span
/// bridge records it, otherwise ids minted here, continuing a trusted
/// parent's trace.
fn trace_context(span: &tracing::Span, inbound: &Inbound) -> TraceContext {
    let parent = match inbound {
        Inbound::Parent(parent) => Some(parent),
        _ => None,
    };
    if let Some(own) = propagation::span_context(span) {
        return TraceContext {
            trace_id: own.trace_id().to_bytes(),
            parent_id: parent.map(|p| p.span_id().to_bytes()),
            span_id: own.span_id().to_bytes(),
            sampled: own.is_sampled(),
        };
    }
    let mut context = TraceContext::root(&OsRandom);
    if let Some(parent) = parent {
        context.trace_id = parent.trace_id().to_bytes();
        context.parent_id = Some(parent.span_id().to_bytes());
        context.sampled = parent.is_sampled();
    }
    context
}
/// Counts a request in `http.server.active_requests` until dropped, so a
/// cancelled request is not left counted.
struct Active<'a> {
    metrics: &'a HttpMetrics,
    attributes: [KeyValue; 2],
}
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.metrics.active.add(-1, &self.attributes);
    }
}
/// `merezha.principal.kind` values (TE §1.5), in [`principal_kind`] order.
const PRINCIPAL_KINDS: [&str; 4] = ["anonymous", "user", "api_key", "system"];
/// `merezha.principal.kind` of an authenticated principal.
fn principal_kind(principal: Option<&Principal>) -> &'static str {
    let index = match principal.map(|p| p.kind) {
        None => 0,
        Some(PrincipalKind::User) => 1,
        Some(PrincipalKind::ApiKey) => 2,
        Some(PrincipalKind::System) => 3,
    };
    PRINCIPAL_KINDS[index]
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
    let traceparent = single(headers, &TRACEPARENT);
    let tracestate = trace_state(headers);
    let declared_length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let headers = request.headers_mut();
    for name in [FORWARDED_FOR, FORWARDED_PROTO, FORWARDED_HOST, FORWARDED] {
        headers.remove(name);
    }
    let method = request.method().clone();
    let matched = request.extensions().get::<MatchedPath>().cloned();
    let route = matched.as_ref().map(|p| p.as_str().to_owned());
    let scheme = match info.forwarded_proto.as_deref() {
        Some("https") => UrlScheme::Https,
        _ => UrlScheme::Http,
    };
    // `/health/*` requests get metrics, never spans (TE §1.2).
    let health = route
        .as_deref()
        .is_some_and(|r| r == "/health" || r.starts_with("/health/"));
    let span = if health {
        tracing::Span::none()
    } else {
        let span = tracing::info_span!(
            parent: None,
            "http.server",
            otel.name = names::http_server_span(method.as_str(), route.as_deref()),
            otel.kind = "server",
            otel.status_code = Empty,
            http.request.method = names::http_method(method.as_str()),
            url.scheme = scheme.as_str(),
            http.route = Empty,
            http.response.status_code = Empty,
            error.type = Empty,
            merezha.request_id = request_id.as_str(),
            merezha.principal.kind = Empty,
        );
        if let Some(route) = &route {
            span.record("http.route", route.as_str());
        }
        span
    };
    let trusted = propagation::trusts(state.trust, peer, via_trusted_proxy);
    let inbound = propagation::continue_inbound(
        &span,
        traceparent.as_deref(),
        tracestate.as_deref(),
        trusted,
    );
    let trace = trace_context(&span, &inbound);
    let extensions = request.extensions_mut();
    extensions.insert(RequestId(request_id.clone()));
    extensions.insert(info);
    extensions.insert(trace);
    let method_attribute = attr::method(method.as_str());
    let route_attribute = matched.as_ref().map(attr::route);
    let metrics = &state.metrics;
    let active = Active {
        metrics,
        attributes: [method_attribute.clone(), attr::url_scheme(scheme)],
    };
    metrics.active.add(1, &active.attributes);
    if let Some(length) = declared_length {
        let mut attributes = vec![method_attribute.clone()];
        attributes.extend(route_attribute.clone());
        metrics.body_size.record(length, &attributes);
    }
    let mut response = next.run(request).instrument(span.clone()).await;
    drop(active);
    let status = response.status().as_u16();
    let elapsed = started.elapsed();
    span.record("http.response.status_code", i64::from(status));
    if response.status().is_server_error() {
        span.record("error.type", status.to_string().as_str());
        span.record("otel.status_code", "error");
    }
    let mut attributes = vec![method_attribute, attr::status_code(status)];
    attributes.extend(route_attribute);
    attributes.extend(attr::server_error(status));
    attributes.push(attr::url_scheme(scheme));
    metrics.duration.record(elapsed.as_secs_f64(), &attributes);
    if let Some(Rejected(reason)) = response.extensions_mut().remove::<Rejected>() {
        metrics.rejected.add(1, &[attr::reason(reason)]);
    }
    drop(span);
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID, value);
    }
    // Only trusted callers learn the server span's context (TE §2.1).
    if trusted && let Ok(value) = HeaderValue::from_str(&trace.child_header()) {
        response.headers_mut().insert(TRACEPARENT, value);
    }
    let duration_ms = elapsed.as_millis() as u64;
    let trace_id = trace.trace_id_hex();
    macro_rules! completed {
        ($level:ident) => {
            merezha_telemetry::event!(
                $level,
                "http.request.completed",
                method = names::http_method(method.as_str()),
                route = route.as_deref().unwrap_or("unmatched"),
                status,
                duration_ms,
                request_id = request_id.as_str(),
                trace_id = trace_id.as_str()
            )
        };
    }
    // TE §4: error for 500, info for other 5xx and slow requests, debug
    // otherwise.
    match status {
        500 => completed!(ERROR),
        501..=599 => completed!(INFO),
        _ if elapsed >= SLOW_REQUEST => completed!(INFO),
        _ => completed!(DEBUG),
    }
    response
}

/// A request the edge itself refused, counted in
/// `merezha.http.server.rejected`. Only the edge's own responses carry it:
/// the starting fallback and the body limit. Codes a module or the runtime
/// returns (`overloaded`, `timeout`) are not edge rejections.
#[derive(Clone, Copy, Debug)]
struct Rejected(Reason);

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

/// An error member already in the one shape.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Shaped {
    #[allow(dead_code)]
    code: String,
    #[allow(dead_code)]
    classification: Classification,
    #[serde(default)]
    details: Option<Vec<FieldError>>,
}
fn shaped(error: &serde_json::Value) -> bool {
    serde_json::from_value::<Failure>(error.clone()).is_ok()
        || serde_json::from_value::<Shaped>(error.clone()).is_ok_and(|s| {
            let details = s.details.unwrap_or_default();
            details.len() <= MAX_DETAILS && details.iter().all(FieldError::is_valid)
        })
}

/// Give every error response the one shape. [`ErrorReply`] responses get
/// the registry's status for their code and `Retry-After` on 429 and 503.
/// Other failures already in shape and JSON objects without an `error`
/// member pass through; `{ "error": { "code" } }` documents from the raw
/// runtime gain the classification implied by their status; anything else,
/// such as an extractor's plain-text rejection, is replaced by a code derived
/// from the status. Only [`ErrorReply`] statuses are ever changed.
async fn normalize_errors(
    State(state): State<EdgeState>,
    request: Request,
    next: Next,
) -> Response {
    let mut response = next.run(request).await;
    if let Some(registered) = response.extensions_mut().remove::<Registered>() {
        *response.status_mut() = state.errors.status(&registered.failure);
        set_retry_after(&mut response, registered.retry_after);
        return response;
    }
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
        && error.is_none_or(shaped)
        && document.is_object()
    {
        let mut response = Response::from_parts(parts, Body::from(bytes.clone()));
        if error.is_some() {
            set_retry_after(&mut response, None);
        }
        return response;
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
    let mut response = Response::from_parts(parts, Body::from(body));
    set_retry_after(&mut response, None);
    // An extractor's plain 413: `http.body_limit` refused the body while
    // it was read.
    if status == StatusCode::PAYLOAD_TOO_LARGE && error.is_none() {
        response
            .extensions_mut()
            .insert(Rejected(Reason::BodyLimit));
    }
    response
}

/// Whether `error`, from reading a request body, is the edge's limit
/// stopping it: a route listed in `Module::body_limits` answers its own
/// code for it.
pub fn is_body_limit(error: &axum::Error) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(error) = current {
        if error.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        current = error.source();
    }
    false
}

async fn limit_body(State(state): State<EdgeState>, request: Request, next: Next) -> Response {
    // A route that bounds its own body: stopped past its limit, its
    // declared length left to the route.
    if let Some((_, _, limit)) = state
        .route_limits
        .iter()
        .find(|(method, path, _)| request.method() == method && request.uri().path() == path)
    {
        let limit = *limit;
        let request = request.map(|body| Body::new(http_body_util::Limited::new(body, limit)));
        return next.run(request).await;
    }
    let declared = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|length| length > state.body_limit as u64) {
        let mut response = error_response(&Failure::permanent("body_limit"));
        response
            .extensions_mut()
            .insert(Rejected(Reason::BodyLimit));
        return response;
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
            tracing::Span::current().record(
                "merezha.principal.kind",
                principal_kind(principal.as_deref()),
            );
            parts.extensions.insert(Authenticated(principal));
            next.run(Request::from_parts(parts, body)).await
        }
        Err(failure) => {
            let mut response = error_response(&failure);
            for (name, value) in &authenticator.rejection_headers(&parts, &failure) {
                response.headers_mut().append(name, value.clone());
            }
            response
        }
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
        assert_eq!(status_for(&Failure::transient("overloaded")), 503);
    }
    #[tokio::test]
    async fn error_replies_keep_the_shape_and_add_details_and_retry_after() {
        let plain = error_response(&Failure::permanent("not_found"));
        assert_eq!(plain.status(), 404);
        let bytes = axum::body::to_bytes(plain.into_body(), 1024).await.unwrap();
        // Byte for byte what `json!({"error": failure})` always produced.
        assert_eq!(
            &bytes[..],
            serde_json::to_vec(&json!({"error": Failure::permanent("not_found")})).unwrap()
        );
        assert_eq!(
            &bytes[..],
            br#"{"error":{"classification":"permanent","code":"not_found"}}"#
        );
        let invalid = ErrorReply::new(Failure::permanent("validation_failed"))
            .details(vec![
                FieldError::new("/email", "invalid").unwrap(),
                FieldError::new("/fields/title", "required").unwrap(),
            ])
            .into_response();
        assert_eq!(invalid.status(), 422);
        let bytes = axum::body::to_bytes(invalid.into_body(), 1024)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body,
            json!({"error": {"code": "validation_failed", "classification": "permanent",
                "details": [{"path": "/email", "code": "invalid"},
                            {"path": "/fields/title", "code": "required"}]}})
        );
        assert!(shaped(&body["error"]));
        assert!(!shaped(
            &json!({"code": "x", "classification": "permanent", "details": [{"path": "no-slash", "code": "x"}]})
        ));
        assert!(!shaped(
            &json!({"code": "x", "classification": "permanent", "message": "leak"})
        ));
        let limited = ErrorReply::new(Failure::transient("rate_limited"))
            .retry_after(Duration::from_millis(2500))
            .into_response();
        assert_eq!(limited.status(), 429);
        assert_eq!(limited.headers()[header::RETRY_AFTER], "3");
        let overloaded = error_response(&Failure::transient("overloaded"));
        assert_eq!(overloaded.status(), 503);
        assert_eq!(overloaded.headers()[header::RETRY_AFTER], "1");
        assert!(
            !error_response(&Failure::permanent("other"))
                .headers()
                .contains_key(header::RETRY_AFTER)
        );
    }
}
