//! `FakeBrevo`: an in-process Brevo API for tests (NT §8).
//!
//! It listens on `127.0.0.1:0`, answers one HTTP/1.1 request per
//! connection (`Connection: close`) and serves:
//!
//! - `POST /v3/smtp/email` and `POST /v3/transactionalSMS/send`;
//! - `POST /v3/webhooks` (`merezha notifications brevo register-webhook`).
//!
//! It is written over a plain tokio listener rather than axum because
//! [`FakeBrevo::close_after_accept`] must process a request and then drop
//! the connection without replying, which a server framework does not let
//! a handler do.
//!
//! # What it assumes about Brevo
//!
//! Brevo documents `headers.idempotencyKey` (a UUID) on `POST
//! /v3/smtp/email`: a key used in the last 30 minutes gets the error code
//! `duplicate_parameter` and the request is not processed. Brevo does not
//! document the HTTP status of that answer, nor whether a request that
//! failed consumes the key. The fake assumes, and the block relies on:
//!
//! 1. the duplicate answer is HTTP 400 `{"code":"duplicate_parameter"}`;
//! 2. only a processed request (one that got its 201) registers its key; a
//!    refused one (401, 4xx, 429, 5xx) leaves the key free;
//! 3. the window is 30 minutes from the processed request
//!    ([`IDEMPOTENCY_WINDOW`], on the fake's own clock, see
//!    [`FakeBrevo::advance`]);
//! 4. idempotency applies to single-recipient sends, not only batches.
//!
//! These need a check against a real Brevo account before production use
//! (NT §12 "What could not be verified").
//!
//! For `POST /v3/transactionalSMS/send` (NT-N4) it has no idempotency, as
//! Brevo documents none: every accepted request is a processed message,
//! answered `{"messageId": <number>}`. It refuses with 400
//! `invalid_parameter` a `recipient` that is not 8 to 15 digits (a leading
//! `+` included: whether Brevo accepts one is not documented, so the block
//! never sends it), a `sender` that is empty or longer than 11 characters
//! (15 digits), empty `content`, a `type` other than `transactional` or
//! `marketing` and a `webUrl` that is not a URL. [`FakeBrevo::sms_event`]
//! builds a status report and [`FakeBrevo::sms_event_request`] the call to
//! the message's `webUrl`.
use axum::{
    body::Body,
    http::{Request, header},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

/// How long a processed `idempotencyKey` is refused (assumption 3).
pub const IDEMPOTENCY_WINDOW: Duration = Duration::from_secs(30 * 60);
/// SMS message ids are numbers of Brevo's size, this plus a counter.
const SMS_MESSAGE_BASE: u64 = 1_511_882_900_000_000;
/// Most bytes of a request the fake reads.
const MAX_REQUEST: usize = 4 * 1024 * 1024;

/// One request the fake received, before any decision.
#[derive(Clone, Debug)]
pub struct Recorded {
    pub method: String,
    pub path: String,
    /// Header names lowercased.
    pub headers: BTreeMap<String, String>,
    /// The JSON body, `Null` when it was not JSON.
    pub body: Value,
    /// When the request was read.
    pub at: Instant,
}
/// One message the fake processed (it answered, or would have answered,
/// 201).
#[derive(Clone, Debug)]
pub struct Processed {
    pub path: String,
    pub body: Value,
    pub message_id: String,
}

#[derive(Default)]
struct State {
    requests: Vec<Recorded>,
    processed: Vec<Processed>,
    /// Processed idempotency keys and when, on the fake clock.
    keys: BTreeMap<String, Duration>,
    /// How far the fake clock was advanced.
    offset: Duration,
    fail: VecDeque<(u16, Option<&'static str>)>,
    rate_limit: VecDeque<Vec<(String, String)>>,
    delay: Duration,
    close_after_accept: u32,
    next_id: u64,
    events: u64,
}

struct Inner {
    api_key: String,
    started: Instant,
    state: Mutex<State>,
}
impl Inner {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// The fake. Dropping it stops the listener.
pub struct FakeBrevo {
    inner: Arc<Inner>,
    address: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for FakeBrevo {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeBrevo {
    /// A fake accepting `api_key`.
    pub async fn start(api_key: &str) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let inner = Arc::new(Inner {
            api_key: api_key.to_owned(),
            started: Instant::now(),
            state: Mutex::new(State::default()),
        });
        let serving = inner.clone();
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let inner = serving.clone();
                tokio::spawn(async move {
                    let _ = serve(&inner, socket).await;
                });
            }
        });
        Ok(Self {
            inner,
            address,
            task,
        })
    }
    /// `http://127.0.0.1:<port>`, for `brevo.base_url`.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }
    /// Every request received, in order.
    pub fn requests(&self) -> Vec<Recorded> {
        self.inner.state().requests.clone()
    }
    /// The requests to `path`.
    pub fn requests_to(&self, path: &str) -> Vec<Recorded> {
        self.requests()
            .into_iter()
            .filter(|r| r.path == path)
            .collect()
    }
    /// Every processed message, in order.
    pub fn processed(&self) -> Vec<Processed> {
        self.inner.state().processed.clone()
    }
    /// The next `n` send requests get `status` with a Brevo error body
    /// (`unauthorized` for 401, `not_enough_credits` for 402,
    /// `permission_denied` for 403, `invalid_parameter` for another 4xx,
    /// `internal_error` otherwise) and are not processed.
    pub fn fail_next(&self, n: usize, status: u16) {
        let mut state = self.inner.state();
        for _ in 0..n {
            state.fail.push_back((status, None));
        }
    }
    /// The next send request gets `status` with the Brevo error `code`
    /// (such as `duplicate_parameter`) and is not processed.
    pub fn fail_next_with(&self, status: u16, code: &'static str) {
        self.inner.state().fail.push_back((status, Some(code)));
    }
    /// The next send request gets 429 with `headers` (such as
    /// `Retry-After` or `x-sib-ratelimit-reset`).
    pub fn rate_limit(&self, headers: &[(&str, &str)]) {
        self.inner.state().rate_limit.push_back(
            headers
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        );
    }
    /// Answer every request after `delay`.
    pub fn delay(&self, delay: Duration) {
        self.inner.state().delay = delay;
    }
    /// The next send request is processed, then its connection is dropped
    /// without a reply.
    pub fn close_after_accept(&self) {
        self.inner.state().close_after_accept += 1;
    }
    /// Move the fake clock forward, to expire idempotency keys.
    pub fn advance(&self, by: Duration) {
        self.inner.state().offset += by;
    }

    /// A Brevo transactional webhook payload for `event` (`delivered`,
    /// `hard_bounce`, `spam`...) about `message`, with a fresh `ts_event`.
    pub fn event(&self, event: &str, message: &Processed) -> Value {
        let ts = {
            let mut state = self.inner.state();
            state.events += 1;
            1_700_000_000 + state.events
        };
        let body = &message.body;
        json!({
            "event": event,
            "email": body["to"][0]["email"],
            "id": 1,
            "date": "2026-10-25 12:00:00",
            "ts": ts,
            "ts_event": ts,
            "ts_epoch": ts * 1000,
            "message-id": message.message_id,
            "subject": body["subject"],
            "X-Mailin-custom": body["headers"]["X-Mailin-custom"],
            "tags": body["tags"],
            "reason": if event.contains("bounce") { "550 5.1.1 unknown mailbox" } else { "" },
        })
    }
    /// A Brevo transactional SMS status report (`msg_status` `delivered`,
    /// `hard_bounce`, `unsubscribed`...) about `message`, with a fresh
    /// `ts_event`.
    pub fn sms_event(&self, status: &str, message: &Processed) -> Value {
        let ts = {
            let mut state = self.inner.state();
            state.events += 1;
            1_700_000_000 + state.events
        };
        let body = &message.body;
        json!({
            "id": 1,
            "to": body["recipient"],
            "sms_count": 1,
            "credits_used": 0.045,
            "remaining_credit": 99.5,
            "msg_status": status,
            "date": "2026-10-26 12:00:00",
            "messageId": message.message_id.parse::<u64>().unwrap_or_default(),
            "type": body["type"],
            "tag": body["tag"],
            "status": "OK",
            "description": status,
            "ts_event": ts,
        })
    }
    /// The status report call Brevo would make for `message`: `POST` to the
    /// path of its `webUrl` (no credential) with `payload`.
    pub fn sms_event_request(message: &Processed, payload: &Value) -> Request<Body> {
        let path = message.body["webUrl"]
            .as_str()
            .and_then(|u| url::Url::parse(u).ok())
            .map(|u| u.path().to_owned())
            .unwrap_or_default();
        Request::builder()
            .method("POST")
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap_or_else(|_| Request::new(Body::empty()))
    }
    /// The callback request Brevo would make: `POST` `path` with
    /// `Authorization: Bearer <token>` and `payload` (an event or an array).
    pub fn event_request(path: &str, token: &str, payload: &Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap_or_else(|_| Request::new(Body::empty()))
    }
}

/// What a request gets.
enum Reply {
    Json(u16, Vec<(String, String)>, Value),
    /// Drop the connection without a reply.
    Close,
}

async fn serve(inner: &Inner, mut socket: TcpStream) -> std::io::Result<()> {
    let Some(request) = read_request(&mut socket).await? else {
        return Ok(());
    };
    let delay = inner.state().delay;
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    match decide(inner, request) {
        Reply::Close => Ok(()),
        Reply::Json(status, headers, body) => {
            let body = body.to_string();
            let mut response = format!(
                "HTTP/1.1 {status} {}\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n",
                reason(status),
                body.len()
            );
            for (name, value) in headers {
                response.push_str(&format!("{name}: {value}\r\n"));
            }
            response.push_str("\r\n");
            response.push_str(&body);
            socket.write_all(response.as_bytes()).await?;
            socket.shutdown().await
        }
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        429 => "Too Many Requests",
        _ => "Status",
    }
}

async fn read_request(socket: &mut TcpStream) -> std::io::Result<Option<Recorded>> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        if let Some(at) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break at;
        }
        let n = socket.read(&mut chunk).await?;
        if n == 0 || buffer.len() > MAX_REQUEST {
            return Ok(None);
        }
        buffer.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap_or_default().split(' ');
    let method = first.next().unwrap_or_default().to_owned();
    let path = first.next().unwrap_or_default().to_owned();
    let headers: BTreeMap<String, String> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .and_then(|l| l.parse().ok())
        .unwrap_or(0)
        .min(MAX_REQUEST);
    let mut body = buffer[head_end + 4..].to_vec();
    while body.len() < length {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    Ok(Some(Recorded {
        method,
        path,
        headers,
        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
        at: Instant::now(),
    }))
}

fn error(status: u16, code: &str) -> Reply {
    Reply::Json(
        status,
        Vec::new(),
        json!({ "code": code, "message": "fake brevo refused the request" }),
    )
}

/// Why the fake refuses an SMS body, if it does.
fn sms_refusal(body: &Value) -> Option<&'static str> {
    let text = |key: &str| body[key].as_str().unwrap_or_default();
    let recipient = text("recipient");
    let sender = text("sender");
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let sender_ok = !sender.is_empty()
        && sender.bytes().all(|b| b.is_ascii_alphanumeric())
        && sender.len() <= if digits(sender) { 15 } else { 11 };
    let web_url_ok = body
        .get("webUrl")
        .is_none_or(|u| u.as_str().is_some_and(|u| url::Url::parse(u).is_ok()));
    let valid = (8..=15).contains(&recipient.len())
        && digits(recipient)
        && sender_ok
        && !text("content").is_empty()
        && matches!(text("type"), "transactional" | "marketing")
        && web_url_ok;
    (!valid).then_some("invalid_parameter")
}

fn decide(inner: &Inner, request: Recorded) -> Reply {
    let mut state = inner.state();
    state.requests.push(request.clone());
    if request.headers.get("api-key").map(String::as_str) != Some(inner.api_key.as_str()) {
        return error(401, "unauthorized");
    }
    let send = matches!(
        request.path.as_str(),
        "/v3/smtp/email" | "/v3/transactionalSMS/send"
    );
    if request.method != "POST" {
        return error(405, "method_not_allowed");
    }
    match request.path.as_str() {
        "/v3/webhooks" => {
            state.next_id += 1;
            return Reply::Json(201, Vec::new(), json!({ "id": state.next_id }));
        }
        _ if send => {}
        _ => return error(404, "document_not_found"),
    }
    if let Some((status, code)) = state.fail.pop_front() {
        let code = code.unwrap_or(match status {
            401 => "unauthorized",
            402 => "not_enough_credits",
            403 => "permission_denied",
            400..=499 => "invalid_parameter",
            _ => "internal_error",
        });
        return error(status, code);
    }
    if let Some(headers) = state.rate_limit.pop_front() {
        return Reply::Json(
            429,
            headers,
            json!({ "code": "too_many_requests", "message": "slow down" }),
        );
    }
    let now = inner.started.elapsed() + state.offset;
    let email = request.path == "/v3/smtp/email";
    let key = request.body["headers"]["idempotencyKey"]
        .as_str()
        .map(str::to_owned);
    if !email && let Some(code) = sms_refusal(&request.body) {
        return error(400, code);
    }
    if email {
        let recipients = request.body["to"].as_array().map_or(0, Vec::len);
        if recipients != 1 || !request.body["sender"].is_object() {
            return error(400, "missing_parameter");
        }
        if let Some(key) = &key {
            if uuid::Uuid::parse_str(key).is_err() {
                return error(400, "invalid_parameter");
            }
            if state
                .keys
                .get(key)
                .is_some_and(|at| now.saturating_sub(*at) < IDEMPOTENCY_WINDOW)
            {
                return error(400, "duplicate_parameter");
            }
        }
    }
    state.next_id += 1;
    let message_id = if email {
        format!("<{}.fake@smtp-relay.brevo.test>", state.next_id)
    } else {
        (SMS_MESSAGE_BASE + state.next_id).to_string()
    };
    if let Some(key) = key.filter(|_| email) {
        state.keys.insert(key, now);
    }
    state.processed.push(Processed {
        path: request.path.clone(),
        body: request.body.clone(),
        message_id: message_id.clone(),
    });
    if state.close_after_accept > 0 {
        state.close_after_accept -= 1;
        return Reply::Close;
    }
    let id = if email {
        Value::String(message_id)
    } else {
        Value::from(SMS_MESSAGE_BASE + state.next_id)
    };
    Reply::Json(201, Vec::new(), json!({ "messageId": id }))
}
