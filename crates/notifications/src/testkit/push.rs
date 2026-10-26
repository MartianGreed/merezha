//! `FakePushService` and `TestResolver` (NT §8).
use crate::{egress::Lookup, push::b64url_decode};
use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use base64::Engine;
use merezha_platform::BoxFuture;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use web_push_native::{
    Auth,
    jwt_simple::prelude::{
        Audiences, ECDSAP256PublicKeyLike, ES256PublicKey, NoCustomClaims, VerificationOptions,
    },
    p256::{SecretKey, elliptic_curve::sec1::ToEncodedPoint},
};

/// RFC 8292 caps the VAPID JWT lifetime at 24 h.
pub const MAX_JWT_SECONDS: u64 = 86_400;

/// One request the fake received, decrypted and checked.
#[derive(Clone, Debug)]
pub struct PushReceived {
    /// The subscription name of the endpoint path.
    pub name: String,
    /// The decrypted payload, parsed as JSON (`Null` when it did not
    /// decrypt or parse).
    pub payload: Value,
    /// The decrypted bytes' length.
    pub plaintext_len: usize,
    pub ttl: Option<String>,
    pub urgency: Option<String>,
    pub topic: Option<String>,
    pub content_encoding: Option<String>,
    /// The JWT audience and how far in the future `exp` was, in seconds.
    pub audience: Option<String>,
    pub expires_in: Option<u64>,
    pub subject: Option<String>,
    /// The problem with the request, if any: `vapid_missing`,
    /// `vapid_signature`, `vapid_key`, `aud_mismatch`, `exp_too_far`,
    /// `decrypt_failed`. A request with a problem is answered 401 or 400.
    pub problem: Option<&'static str>,
    pub at: Instant,
}

/// A scripted answer: a status and extra headers.
type Answer = (u16, Vec<(String, String)>);

struct Inner {
    secret: SecretKey,
    auth: [u8; 16],
    origin: String,
    /// The VAPID public key requests must carry (base64url), when known.
    vapid: Mutex<Option<String>>,
    received: Mutex<Vec<PushReceived>>,
    /// Scripted answers, used before the default one.
    script: Mutex<VecDeque<Answer>>,
    default_status: Mutex<u16>,
    counter: AtomicUsize,
}

/// A push service on `127.0.0.1:0`. Endpoints are
/// `http://127.0.0.1:<port>/push/<name>`, so tests configure
/// `push.allowed_hosts = ["127.0.0.1"]` with
/// `egress.allow_private_networks = true`. It decrypts every request with
/// `web_push_native::decrypt` under its user-agent key, verifies the VAPID
/// JWT (signature, `aud` = the origin the client addressed, from `Host`,
/// `exp` at most 24 h out, a `sub`) and answers 201 with a `Location`, or a scripted status.
#[derive(Clone)]
pub struct FakePushService {
    inner: Arc<Inner>,
    address: SocketAddr,
}
impl std::fmt::Debug for FakePushService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakePushService")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}
impl FakePushService {
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the fake push service");
        let address = listener.local_addr().expect("a local address");
        let mut auth = [0u8; 16];
        getrandom_fill(&mut auth);
        let inner = Arc::new(Inner {
            secret: SecretKey::random(&mut web_push_native::p256::elliptic_curve::rand_core::OsRng),
            auth,
            origin: format!("http://{address}"),
            vapid: Mutex::new(None),
            received: Mutex::new(Vec::new()),
            script: Mutex::new(VecDeque::new()),
            default_status: Mutex::new(201),
            counter: AtomicUsize::new(0),
        });
        let router = Router::new()
            .route("/push/{name}", post(receive))
            .with_state(inner.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Self { inner, address }
    }
    /// `http://127.0.0.1:<port>`.
    pub fn origin(&self) -> &str {
        &self.inner.origin
    }
    pub fn endpoint(&self, name: &str) -> String {
        format!("{}/push/{name}", self.inner.origin)
    }
    /// The body of `POST /v1/notifications/push/subscriptions` for a
    /// browser subscribed at `name`, as `PushSubscription.toJSON()` gives.
    pub fn subscription(&self, name: &str) -> Value {
        let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let public = self.inner.secret.public_key().to_encoded_point(false);
        json!({
            "endpoint": self.endpoint(name),
            "expirationTime": null,
            "keys": {
                "p256dh": engine.encode(public.as_bytes()),
                "auth": engine.encode(self.inner.auth),
            },
        })
    }
    /// Require requests to carry this VAPID public key (base64url).
    pub fn expect_vapid_key(&self, public_key: &str) {
        if let Ok(mut vapid) = self.inner.vapid.lock() {
            *vapid = Some(public_key.to_owned());
        }
    }
    /// Answer the next `n` requests with `status`.
    pub fn fail_next(&self, n: usize, status: u16) {
        if let Ok(mut script) = self.inner.script.lock() {
            for _ in 0..n {
                script.push_back((status, Vec::new()));
            }
        }
    }
    /// Answer the next request 429 with `Retry-After: <seconds>`.
    pub fn rate_limit_next(&self, seconds: u64) {
        if let Ok(mut script) = self.inner.script.lock() {
            script.push_back((429, vec![("retry-after".into(), seconds.to_string())]));
        }
    }
    /// Answer every unscripted request with `status` (201 by default).
    pub fn respond(&self, status: u16) {
        if let Ok(mut default) = self.inner.default_status.lock() {
            *default = status;
        }
    }
    pub fn received(&self) -> Vec<PushReceived> {
        self.inner
            .received
            .lock()
            .map(|r| r.clone())
            .unwrap_or_default()
    }
}

fn getrandom_fill(bytes: &mut [u8]) {
    use web_push_native::p256::elliptic_curve::rand_core::RngCore;
    web_push_native::p256::elliptic_curve::rand_core::OsRng.fill_bytes(bytes);
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

/// `vapid t=<jwt>, k=<key>`.
fn vapid_parts(value: &str) -> Option<(String, String)> {
    let rest = value.strip_prefix("vapid ")?;
    let mut token = None;
    let mut key = None;
    for part in rest.split(',') {
        let part = part.trim();
        if let Some(t) = part.strip_prefix("t=") {
            token = Some(t.to_owned());
        } else if let Some(k) = part.strip_prefix("k=") {
            key = Some(k.to_owned());
        }
    }
    Some((token?, key?))
}

async fn receive(
    State(inner): State<Arc<Inner>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut record = PushReceived {
        name,
        payload: Value::Null,
        plaintext_len: 0,
        ttl: header(&headers, "ttl"),
        urgency: header(&headers, "urgency"),
        topic: header(&headers, "topic"),
        content_encoding: header(&headers, "content-encoding"),
        audience: None,
        expires_in: None,
        subject: None,
        problem: None,
        at: Instant::now(),
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    match header(&headers, "authorization")
        .as_deref()
        .and_then(vapid_parts)
    {
        None => record.problem = Some("vapid_missing"),
        Some((token, key)) => {
            let expected = inner.vapid.lock().ok().and_then(|v| v.clone());
            let verified = b64url_decode(&key)
                .and_then(|bytes| ES256PublicKey::from_bytes(&bytes).ok())
                .map(|public| {
                    public.verify_token::<NoCustomClaims>(
                        &token,
                        Some(VerificationOptions::default()),
                    )
                });
            if expected.is_some_and(|expected| expected != key) {
                record.problem = Some("vapid_key");
            }
            match verified {
                Some(Ok(claims)) => {
                    record.audience = match claims.audiences {
                        Some(Audiences::AsString(aud)) => Some(aud),
                        Some(Audiences::AsSet(set)) if set.len() == 1 => set.into_iter().next(),
                        _ => None,
                    };
                    record.expires_in = claims
                        .expires_at
                        .map(|exp| exp.as_secs().saturating_sub(now));
                    record.subject = claims.subject;
                    // The origin the client addressed: the name it
                    // resolved, with the port.
                    let origin = header(&headers, "host")
                        .map(|host| format!("http://{host}"))
                        .unwrap_or_else(|| inner.origin.clone());
                    if record.audience.as_deref() != Some(origin.as_str()) {
                        record.problem = record.problem.or(Some("aud_mismatch"));
                    }
                    if record.expires_in.is_none_or(|s| s > MAX_JWT_SECONDS) {
                        record.problem = record.problem.or(Some("exp_too_far"));
                    }
                    if record.subject.is_none() {
                        record.problem = record.problem.or(Some("vapid_signature"));
                    }
                }
                _ => record.problem = record.problem.or(Some("vapid_signature")),
            }
        }
    }
    let auth = Auth::clone_from_slice(&inner.auth);
    match web_push_native::decrypt(body.to_vec(), &inner.secret, &auth) {
        Ok(plaintext) => {
            record.plaintext_len = plaintext.len();
            record.payload = serde_json::from_slice(&plaintext).unwrap_or(Value::Null);
        }
        Err(_) => record.problem = record.problem.or(Some("decrypt_failed")),
    }
    if record.content_encoding.as_deref() != Some("aes128gcm") {
        record.problem = record.problem.or(Some("decrypt_failed"));
    }
    let problem = record.problem;
    if let Ok(mut received) = inner.received.lock() {
        received.push(record);
    }
    match problem {
        Some("decrypt_failed") => return StatusCode::BAD_REQUEST.into_response(),
        Some(_) => return StatusCode::UNAUTHORIZED.into_response(),
        None => {}
    }
    let scripted = inner.script.lock().ok().and_then(|mut s| s.pop_front());
    let (status, extra) = scripted.unwrap_or_else(|| {
        (
            inner.default_status.lock().map(|s| *s).unwrap_or(201),
            Vec::new(),
        )
    });
    let mut response = StatusCode::from_u16(status)
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
        .into_response();
    if status == 201 {
        let n = inner.counter.fetch_add(1, Ordering::SeqCst);
        if let Ok(location) = HeaderValue::from_str(&format!("{}/messages/{n}", inner.origin)) {
            response.headers_mut().insert("location", location);
        }
    }
    for (name, value) in extra {
        if let (Ok(name), Ok(value)) = (
            axum::http::HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            response.headers_mut().insert(name, value);
        }
    }
    response
}

/// Fixed addresses for host names (NT §8), under the egress guard; an
/// unknown name fails to resolve. It counts lookups.
#[derive(Clone, Debug, Default)]
pub struct TestResolver {
    names: Arc<Mutex<BTreeMap<String, Vec<IpAddr>>>>,
    lookups: Arc<AtomicUsize>,
}
impl TestResolver {
    pub fn new() -> Self {
        Self::default()
    }
    /// `host` resolves to `addresses`.
    pub fn map(&self, host: &str, addresses: &[IpAddr]) {
        if let Ok(mut names) = self.names.lock() {
            names.insert(host.to_ascii_lowercase(), addresses.to_vec());
        }
    }
    pub fn lookups(&self) -> usize {
        self.lookups.load(Ordering::SeqCst)
    }
}
impl Lookup for TestResolver {
    fn lookup(&self, host: &str) -> BoxFuture<'static, std::io::Result<Vec<IpAddr>>> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        let found = self
            .names
            .lock()
            .ok()
            .and_then(|names| names.get(&host.to_ascii_lowercase()).cloned());
        Box::pin(async move {
            found.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "unknown host"))
        })
    }
}
