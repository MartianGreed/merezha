//! `WebhookReceiver` (NT §8): a webhook endpoint on `127.0.0.1:0` that
//! verifies Standard Webhooks signatures, records `webhook-id`, and
//! answers with scripted statuses, headers, bodies or delays.
use crate::webhooks::signing::{self, SigningSecret};
use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// One request the receiver got.
#[derive(Clone, Debug)]
pub struct WebhookReceived {
    /// The name in `/hooks/<name>`.
    pub name: String,
    pub webhook_id: Option<String>,
    pub timestamp: Option<i64>,
    /// The raw `webhook-signature` header.
    pub signature: Option<String>,
    /// The indexes (in [`WebhookReceiver::trust`] order) of the trusted
    /// secrets one of the signatures verified with.
    pub verified_by: Vec<usize>,
    pub body: Vec<u8>,
    pub user_agent: Option<String>,
    pub content_type: Option<String>,
    pub at: Instant,
}
impl WebhookReceived {
    /// The `v1,` signatures of the header.
    pub fn signatures(&self) -> Vec<String> {
        self.signature
            .as_deref()
            .unwrap_or_default()
            .split(' ')
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect()
    }
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }
}

/// A scripted answer.
#[derive(Clone, Debug, Default)]
struct Answer {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
    delay: Duration,
}

struct Inner {
    secrets: Mutex<Vec<SigningSecret>>,
    received: Mutex<Vec<WebhookReceived>>,
    script: Mutex<VecDeque<Answer>>,
    default: Mutex<Answer>,
}

/// A webhook endpoint on `127.0.0.1:0`; URLs are
/// `http://127.0.0.1:<port>/hooks/<name>`, so tests configure
/// `webhooks.allow_http = true` and `egress.allow_private_networks =
/// true`. Every request is recorded before it is answered, 204 by default.
#[derive(Clone)]
pub struct WebhookReceiver {
    inner: Arc<Inner>,
    address: SocketAddr,
}
impl std::fmt::Debug for WebhookReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookReceiver")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}
impl WebhookReceiver {
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the webhook receiver");
        let address = listener.local_addr().expect("a local address");
        let inner = Arc::new(Inner {
            secrets: Mutex::new(Vec::new()),
            received: Mutex::new(Vec::new()),
            script: Mutex::new(VecDeque::new()),
            default: Mutex::new(Answer {
                status: 204,
                ..Answer::default()
            }),
        });
        let router = Router::new()
            .route("/hooks/{name}", post(receive))
            .with_state(inner.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Self { inner, address }
    }
    pub fn port(&self) -> u16 {
        self.address.port()
    }
    /// `http://127.0.0.1:<port>/hooks/<name>`.
    pub fn url(&self, name: &str) -> String {
        format!("http://{}/hooks/{name}", self.address)
    }
    /// The same receiver under another host name (for a `TestResolver`).
    pub fn url_at(&self, host: &str, name: &str) -> String {
        format!("http://{host}:{}/hooks/{name}", self.port())
    }
    /// Verify signatures against `secret` (`whsec_…`) too.
    pub fn trust(&self, secret: &str) {
        let secret = SigningSecret::parse(secret).expect("a whsec_ secret");
        if let Ok(mut secrets) = self.inner.secrets.lock() {
            secrets.push(secret);
        }
    }
    fn push(&self, answer: Answer) {
        if let Ok(mut script) = self.inner.script.lock() {
            script.push_back(answer);
        }
    }
    /// Answer the next `n` requests with `status` and body `body`.
    pub fn fail_next(&self, n: usize, status: u16, body: &str) {
        for _ in 0..n {
            self.push(Answer {
                status,
                body: body.to_owned(),
                ..Answer::default()
            });
        }
    }
    /// Answer the next request `status` with `Location: location`.
    pub fn redirect_next(&self, status: u16, location: &str) {
        self.push(Answer {
            status,
            headers: vec![("location".into(), location.to_owned())],
            ..Answer::default()
        });
    }
    /// Answer the next request after `delay`.
    pub fn delay_next(&self, delay: Duration) {
        self.push(Answer {
            status: 204,
            delay,
            ..Answer::default()
        });
    }
    /// Answer every unscripted request after `delay`.
    pub fn delay_all(&self, delay: Duration) {
        if let Ok(mut default) = self.inner.default.lock() {
            default.delay = delay;
        }
    }
    /// Answer every unscripted request with `status` (204 by default).
    pub fn respond(&self, status: u16) {
        if let Ok(mut default) = self.inner.default.lock() {
            default.status = status;
        }
    }
    pub fn received(&self) -> Vec<WebhookReceived> {
        self.inner
            .received
            .lock()
            .map(|r| r.clone())
            .unwrap_or_default()
    }
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

async fn receive(
    State(inner): State<Arc<Inner>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let webhook_id = header(&headers, "webhook-id");
    let timestamp = header(&headers, "webhook-timestamp").and_then(|t| t.parse().ok());
    let signature = header(&headers, "webhook-signature");
    let verified_by = match (&webhook_id, timestamp, &signature) {
        (Some(id), Some(ts), Some(sig)) => inner
            .secrets
            .lock()
            .map(|secrets| {
                secrets
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| signing::verify(s, id, ts, &body, sig))
                    .map(|(i, _)| i)
                    .collect()
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    if let Ok(mut received) = inner.received.lock() {
        received.push(WebhookReceived {
            name,
            webhook_id,
            timestamp,
            signature,
            verified_by,
            body: body.to_vec(),
            user_agent: header(&headers, "user-agent"),
            content_type: header(&headers, "content-type"),
            at: Instant::now(),
        });
    }
    let answer = inner
        .script
        .lock()
        .ok()
        .and_then(|mut s| s.pop_front())
        .or_else(|| inner.default.lock().ok().map(|d| d.clone()))
        .unwrap_or_default();
    if !answer.delay.is_zero() {
        tokio::time::sleep(answer.delay).await;
    }
    let mut response = (
        StatusCode::from_u16(answer.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        answer.body,
    )
        .into_response();
    for (name, value) in answer.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::try_from(name.as_str()),
            HeaderValue::from_str(&value),
        ) {
            response.headers_mut().insert(name, value);
        }
    }
    response
}
