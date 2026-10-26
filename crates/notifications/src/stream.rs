//! The inbox stream (NT §5.4, slice NT-N5): `GET inbox/stream` as
//! Server-Sent Events, the per-instance stream registry and its limits, and
//! the `LISTEN merezha_notify_inbox` task that wakes local streams.
//!
//! Every inbox change takes its `change_seq` from the user's
//! `inbox_counters` row, locked until commit, and calls `pg_notify` with the
//! user id in the same transaction (NT-N2). So a stream that has seen
//! `change_seq = n` has seen every committed change below it, and resuming
//! with `Last-Event-ID = n` asks for exactly the rows whose current
//! `change_seq` is above `n` (invariant 11).
//!
//! A stream subscribes to its user's wake signal before its first query
//! (ADR-0022). A wake, or the reconcile interval, runs one bounded query
//! from the stream's cursor. Events pass through a bounded channel; a
//! client that does not take an event within one heartbeat interval is
//! disconnected and resumes with `Last-Event-ID`, so nothing is lost and
//! nothing is buffered without bound. The payload of a notification is the
//! user id only; it never carries content.
use crate::{
    METER_SCOPE, inbox,
    notifier::{Core, Slot},
    sql,
};
use axum::{
    Extension, Router,
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue, Method, Uri, header, request::Parts},
    response::{
        IntoResponse, Response,
        sse::{Event, Sse},
    },
    routing::get,
};
use merezha_domain::Failure;
use merezha_platform::{
    ErrorReply,
    edge::{Authenticated, ClientInfo, ORGANIZATION},
    ports::{Authenticator, PrincipalKind},
};
use merezha_postgres::{
    Database, Listener, MAX_PENDING_NOTIFICATIONS, Statement, classify, tokio_postgres::Row,
};
use merezha_telemetry::{instruments, names::metric};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Meter, UpDownCounter},
};
use serde_json::json;
use std::{
    collections::HashMap,
    convert::Infallible,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};
use tracing::Instrument;

/// Events waiting for one client (NT §5.4).
pub const CHANNEL_CAPACITY: usize = 64;
/// Rows one stream query reads.
pub const BATCH: i64 = 100;
/// The `retry:` the stream opens with.
pub const RETRY: Duration = Duration::from_millis(5000);
/// `Retry-After` of a refused stream (503 `overloaded`).
const RETRY_AFTER: Duration = Duration::from_secs(5);
/// Bound on one stream query, its pool wait included.
const QUERY_DEADLINE: Duration = Duration::from_secs(10);
/// Bound on one re-authentication.
const REAUTH_DEADLINE: Duration = Duration::from_secs(10);
/// Opening the `LISTEN` session, and each liveness probe.
const LISTEN_DEADLINE: Duration = Duration::from_secs(5);
/// How often an idle `LISTEN` session is probed: a half-open connection
/// never ends by itself.
const LISTEN_CHECK_EVERY: Duration = Duration::from_secs(30);
/// Reconnect backoff of the `LISTEN` session (NT §5.4).
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A session must stay up this long before the backoff starts again from
/// [`BACKOFF_MIN`]: one that connects and drops keeps backing off.
const STABLE_AFTER: Duration = LISTEN_CHECK_EVERY;
/// [`Streams::wake_all`] spreads its wakes over this long, and runs at most
/// once per this long, so the streams of an instance do not all query at
/// once.
pub const WAKE_ALL_SPREAD: Duration = Duration::from_secs(1);

// ----- statements -----

/// The user's counter: `last_seq` and `purged_through`.
pub const STREAM_COUNTER: Statement = Statement::new(
    "notify.stream.counter",
    "SELECT merezha_notify.inbox_counters",
    "SELECT last_seq, purged_through FROM merezha_notify.inbox_counters WHERE user_id = $1",
);
/// How many rows in scope changed after `$2`, counted up to `$4`.
pub const STREAM_CHANGED_SINCE: Statement = Statement::new(
    "notify.stream.changed_since",
    "SELECT merezha_notify.inbox_items",
    "SELECT count(*) FROM (SELECT 1 FROM merezha_notify.inbox_items \
     WHERE user_id = $1 AND change_seq > $2 AND (tenant_id IS NULL OR tenant_id = $3::text) \
     LIMIT $4) t",
);
/// The rows in scope whose current `change_seq` is above `$2`, in order.
pub const STREAM_BATCH: Statement = Statement::new(
    "notify.stream.batch",
    "SELECT merezha_notify.inbox_items",
    concat!(
        "SELECT ",
        sql::item_columns!(),
        " FROM merezha_notify.inbox_items \
         WHERE user_id = $1 AND change_seq > $2 AND (tenant_id IS NULL OR tenant_id = $3::text) \
         ORDER BY change_seq LIMIT $4"
    ),
);
/// Every statement of the stream.
pub const STATEMENTS: &[Statement] = &[STREAM_COUNTER, STREAM_CHANGED_SINCE, STREAM_BATCH];

// ----- metrics -----

/// `merezha.inbox.sse.*` and `merezha.inbox.listen.reconnects` (NT §9).
#[derive(Clone)]
struct StreamMetrics {
    connections: UpDownCounter<i64>,
    events: Counter<u64>,
    rejected: Counter<u64>,
    reconnects: Counter<u64>,
}
impl StreamMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            connections: instruments::up_down_counter(meter, &metric::INBOX_SSE_CONNECTIONS),
            events: instruments::counter(meter, &metric::INBOX_SSE_EVENTS),
            rejected: instruments::counter(meter, &metric::INBOX_SSE_REJECTED),
            reconnects: instruments::counter(meter, &metric::INBOX_LISTEN_RECONNECTS),
        }
    }
    /// `item`, `reset`, `hello` or `unread`.
    fn event(&self, kind: &'static str) {
        self.events.add(1, &[KeyValue::new("notify.event", kind)]);
    }
    /// `per_user`, `capacity` or `auth`.
    fn rejected(&self, reason: &'static str) {
        self.rejected
            .add(1, &[KeyValue::new("notify.reason", reason)]);
    }
}

// ----- registry -----

/// One user's local streams and their wake signal.
struct Entry {
    wake: watch::Sender<u64>,
    streams: u32,
}
struct Registry {
    users: HashMap<String, Entry>,
    total: u32,
}

struct Inner {
    settings: crate::config::SseSettings,
    registry: Mutex<Registry>,
    /// `true` once the instance drains: streams end, new ones are refused.
    closing: watch::Sender<bool>,
    metrics: StreamMetrics,
    /// Stops the `LISTEN` task.
    stop: watch::Sender<bool>,
    /// Asks the `LISTEN` task for one spread wake of every stream; requests
    /// made while one runs coalesce into a single next one.
    sweep: tokio::sync::Notify,
    listen: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Inner {
    fn registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Why a stream was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// `sse.max_streams_per_user` streams of this user are open here.
    PerUser,
    /// `sse.max_streams` streams are open here, or the instance drains.
    Capacity,
}

/// The local stream registry, shared by the route, the `LISTEN` task and
/// the module's drain. Cheap to clone.
#[derive(Clone)]
pub struct Streams(Arc<Inner>);
impl std::fmt::Debug for Streams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streams")
            .field("open", &self.open())
            .finish_non_exhaustive()
    }
}

/// One admitted stream: it counts against the limits until dropped, and
/// sees its user's wake signal from the moment it was admitted.
pub struct Lease {
    inner: Arc<Inner>,
    user: String,
    wake: watch::Receiver<u64>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut registry = self.inner.registry();
        if let Some(entry) = registry.users.get_mut(&self.user) {
            entry.streams = entry.streams.saturating_sub(1);
            if entry.streams == 0 {
                registry.users.remove(&self.user);
            }
        }
        registry.total = registry.total.saturating_sub(1);
        drop(registry);
        self.inner.metrics.connections.add(-1, &[]);
    }
}

impl Streams {
    /// A registry with `settings`' limits, its instruments on `meter`.
    pub fn new(settings: crate::config::SseSettings, meter: &Meter) -> Self {
        Self(Arc::new(Inner {
            settings,
            registry: Mutex::new(Registry {
                users: HashMap::new(),
                total: 0,
            }),
            closing: watch::Sender::new(false),
            metrics: StreamMetrics::new(meter),
            stop: watch::Sender::new(false),
            sweep: tokio::sync::Notify::new(),
            listen: Mutex::new(None),
        }))
    }
    /// The registry without instruments.
    pub fn without_metrics(settings: crate::config::SseSettings) -> Self {
        Self::new(settings, &instruments::noop_meter(METER_SCOPE))
    }
    /// Admit one more stream of `user`: per-user limit first (no existing
    /// stream is evicted), then the instance limit.
    pub fn admit(&self, user: &str) -> Result<Lease, Refusal> {
        let inner = &self.0;
        if *inner.closing.borrow() {
            return Err(Refusal::Capacity);
        }
        let mut registry = inner.registry();
        let held = registry.users.get(user).map_or(0, |e| e.streams);
        if held >= inner.settings.max_streams_per_user {
            return Err(Refusal::PerUser);
        }
        if registry.total >= inner.settings.max_streams {
            return Err(Refusal::Capacity);
        }
        let entry = registry
            .users
            .entry(user.to_owned())
            .or_insert_with(|| Entry {
                wake: watch::Sender::new(0),
                streams: 0,
            });
        entry.streams += 1;
        // A fresh receiver has seen the current value: what wakes it from
        // now on is a change after this point.
        let wake = entry.wake.subscribe();
        registry.total += 1;
        drop(registry);
        inner.metrics.connections.add(1, &[]);
        Ok(Lease {
            inner: inner.clone(),
            user: user.to_owned(),
            wake,
        })
    }
    /// Streams open on this instance.
    pub fn open(&self) -> u32 {
        self.0.registry().total
    }
    /// Streams of `user` open on this instance.
    pub fn open_for(&self, user: &str) -> u32 {
        self.0.registry().users.get(user).map_or(0, |e| e.streams)
    }
    /// Wake the local streams of `user`; nothing when it has none.
    pub fn wake(&self, user: &str) {
        if let Some(entry) = self.0.registry().users.get(user) {
            entry.wake.send_modify(|n| *n = n.wrapping_add(1));
        }
    }
    /// Ask for a wake of every local stream, after the `LISTEN` session was
    /// lost or its queue overflowed and notifications may be missing. The
    /// `LISTEN` task runs it spread over [`WAKE_ALL_SPREAD`], one pass at a
    /// time: requests made during a pass coalesce into one more pass.
    pub fn wake_all(&self) {
        self.0.sweep.notify_one();
    }
    /// The users with local streams, now.
    fn users(&self) -> Vec<String> {
        self.0.registry().users.keys().cloned().collect()
    }
    /// End every stream and refuse new ones: the instance drains. Clients
    /// reconnect to another instance with `Last-Event-ID`.
    pub fn close(&self) {
        self.0.closing.send_replace(true);
    }
    /// Whether [`Streams::close`] ran.
    pub fn closing(&self) -> bool {
        *self.0.closing.borrow()
    }
    /// Start the `LISTEN` task on `database`, its reconnect jitter drawn
    /// from `random`; a second call does nothing.
    pub fn listen(&self, database: Database, random: Arc<dyn merezha_platform::Random>) {
        let mut slot = self.0.listen.lock().unwrap_or_else(PoisonError::into_inner);
        if slot.is_none() {
            let listening = listen_loop(self.clone(), database, random, self.0.stop.subscribe());
            let sweeping = sweeper(self.clone(), self.0.stop.subscribe());
            *slot = Some(tokio::spawn(async move {
                tokio::join!(listening, sweeping);
            }));
        }
    }
    /// Close every stream, stop the `LISTEN` task and wait for both
    /// before `deadline`.
    pub async fn shutdown(&self, deadline: Instant) {
        self.close();
        self.0.stop.send_replace(true);
        let at = tokio::time::Instant::from_std(deadline);
        let task = self
            .0
            .listen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(mut task) = task
            && tokio::time::timeout_at(at, &mut task).await.is_err()
        {
            task.abort();
        }
        while self.open() > 0 && tokio::time::Instant::now() < at {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// ----- LISTEN -----

/// The `LISTEN` session's reconnect delays (NT §5.4): from 1 s, doubling to
/// 30 s, drawn from the upper half of the step and never below 1 s. Only a
/// session that stayed up one probe interval (30 s) starts again from 1 s.
#[derive(Debug)]
pub struct Backoff {
    step: Duration,
}
impl Default for Backoff {
    fn default() -> Self {
        Self { step: BACKOFF_MIN }
    }
}
impl Backoff {
    /// The delay before the next attempt, `unit` in `[0, 1]` its jitter;
    /// the step doubles for the attempt after.
    pub fn next(&mut self, unit: f64) -> Duration {
        let delay = self
            .step
            .mul_f64(0.5 + 0.5 * unit.clamp(0.0, 1.0))
            .max(BACKOFF_MIN);
        self.step = (self.step * 2).min(BACKOFF_MAX);
        delay
    }
    /// A session ended after being up `up`.
    pub fn lost_after(&mut self, up: Duration) {
        if up >= STABLE_AFTER {
            self.step = BACKOFF_MIN;
        }
    }
}

/// A jitter unit in `[0, 1]` from `random`; 0.5 when it fails.
fn unit(random: &dyn merezha_platform::Random) -> f64 {
    let mut bytes = [0u8; 8];
    match random.fill(&mut bytes) {
        Ok(()) => u64::from_le_bytes(bytes) as f64 / u64::MAX as f64,
        Err(_) => 0.5,
    }
}

/// One dedicated session outside the pool, `LISTEN merezha_notify_inbox`.
/// A notification wakes only the local streams of the user it names. A
/// lost session is logged once (`notification.sse_listen_lost`) and
/// reopened after a [`Backoff`] delay, every attempt after the first; once
/// restored it logs `notification.sse_listen_restored`, counts
/// `merezha.inbox.listen.reconnects` and asks for [`Streams::wake_all`].
/// Readiness never depends on it: streams reconcile on their own.
async fn listen_loop(
    streams: Streams,
    database: Database,
    random: Arc<dyn merezha_platform::Random>,
    mut stop: watch::Receiver<bool>,
) {
    let mut backoff = Backoff::default();
    let mut lost = false;
    let mut connected_once = false;
    let mut first = true;
    loop {
        if *stop.borrow() {
            return;
        }
        if !first {
            let delay = backoff.next(unit(random.as_ref()));
            tokio::select! {
                _ = stop.changed() => return,
                () = tokio::time::sleep(delay) => {}
            }
        }
        first = false;
        let session = database
            .listen(&[sql::INBOX_CHANNEL], Instant::now() + LISTEN_DEADLINE)
            .await;
        let mut session = match session {
            Ok(session) => session,
            Err(failure) => {
                if !lost {
                    merezha_telemetry::event!(
                        WARN,
                        "notification.sse_listen_lost",
                        code = failure.code.as_str()
                    );
                    lost = true;
                }
                continue;
            }
        };
        if lost {
            merezha_telemetry::event!(INFO, "notification.sse_listen_restored");
        }
        if connected_once {
            streams.0.metrics.reconnects.add(1, &[]);
            // Notifications sent while the session was down are gone.
            streams.wake_all();
        }
        connected_once = true;
        let up_since = Instant::now();
        let code = serve(&streams, &mut session, &mut stop).await;
        drop(session);
        match code {
            None => return,
            Some(code) => {
                merezha_telemetry::event!(WARN, "notification.sse_listen_lost", code = code);
                lost = true;
                backoff.lost_after(up_since.elapsed());
            }
        }
    }
}

/// Run the [`Streams::wake_all`] passes until the task stops: each wakes
/// the users that have local streams at its start, spread evenly over
/// [`WAKE_ALL_SPREAD`], and the next pass starts no sooner than one spread
/// after it.
async fn sweeper(streams: Streams, mut stop: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            _ = stop.changed() => return,
            () = streams.0.sweep.notified() => {}
        }
        let started = tokio::time::Instant::now();
        let users = streams.users();
        let count = users.len().max(1) as f64;
        for (n, user) in users.iter().enumerate() {
            let at = started + WAKE_ALL_SPREAD.mul_f64(n as f64 / count);
            tokio::select! {
                _ = stop.changed() => return,
                () = tokio::time::sleep_until(at) => {}
            }
            streams.wake(user);
        }
        tokio::select! {
            _ = stop.changed() => return,
            () = tokio::time::sleep_until(started + WAKE_ALL_SPREAD) => {}
        }
    }
}

/// Forward notifications until the session is lost (`Some(code)`) or the
/// task stops (`None`).
async fn serve(
    streams: &Streams,
    session: &mut Listener,
    stop: &mut watch::Receiver<bool>,
) -> Option<&'static str> {
    let mut next_check = tokio::time::Instant::now() + LISTEN_CHECK_EVERY;
    loop {
        tokio::select! {
            _ = stop.changed() => return None,
            () = tokio::time::sleep_until(next_check) => {
                next_check = tokio::time::Instant::now() + LISTEN_CHECK_EVERY;
                if let Err(failure) = session.check(Instant::now() + LISTEN_DEADLINE).await {
                    return Some(code_of(&failure));
                }
            }
            received = session.recv() => {
                let Some(first) = received else {
                    return Some("database_unavailable");
                };
                streams.wake(&first.payload);
                let mut burst = 1;
                while let Some(next) = session.try_recv() {
                    streams.wake(&next.payload);
                    burst += 1;
                }
                // The session's queue may have overflowed and dropped
                // some: wake everyone rather than wait for reconcile.
                if burst >= MAX_PENDING_NOTIFICATIONS {
                    streams.wake_all();
                }
            }
        }
    }
}

/// A failure code from the kernel's closed set, as a static string.
fn code_of(failure: &Failure) -> &'static str {
    match failure.code.as_str() {
        "database_timeout" => "database_timeout",
        "database_closed" => "database_closed",
        _ => "database_unavailable",
    }
}

// ----- route -----

#[derive(Clone)]
pub(crate) struct StreamState {
    pub streams: Arc<std::sync::OnceLock<Streams>>,
    pub slot: Slot,
    pub authenticator: Option<Arc<dyn Authenticator>>,
}

pub(crate) fn router(state: StreamState) -> Router {
    Router::new()
        .route("/inbox/stream", get(stream))
        .with_state(state)
}

fn reply(failure: Failure) -> Response {
    ErrorReply::new(failure).into_response()
}

/// The request credentials kept for re-authentication: the method, path,
/// the credential and organization headers and the client facts. Never
/// shown by `Debug`, never logged.
struct Credentials(Parts);
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credentials(<redacted>)")
    }
}
impl Credentials {
    fn keep(uri: &Uri, headers: &HeaderMap, client: Option<ClientInfo>) -> Self {
        let (mut parts, ()) = axum::http::Request::builder()
            .method(Method::GET)
            .uri(uri.clone())
            .body(())
            .unwrap_or_default()
            .into_parts();
        let kept: [HeaderName; 4] = [
            header::COOKIE,
            header::AUTHORIZATION,
            ORGANIZATION,
            header::USER_AGENT,
        ];
        for name in kept {
            for value in headers.get_all(&name) {
                parts.headers.append(name.clone(), value.clone());
            }
        }
        if let Some(client) = client {
            parts.extensions.insert(client);
        }
        Self(parts)
    }
}

/// `Last-Event-ID` as a decimal `change_seq`; anything else is absent.
pub fn last_event_id(headers: &HeaderMap) -> Option<i64> {
    let text = headers.get("last-event-id")?.to_str().ok()?.trim();
    if text.is_empty() || text.len() > 19 || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse::<i64>().ok()
}

/// What the stream starts with (NT §5.4 "Resume").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Opening {
    /// No (usable) `Last-Event-ID`: `hello` at `last_seq`.
    Hello { last: i64, unread: i64 },
    /// The id cannot be resumed from: `reset` at `last_seq`.
    Reset { last: i64 },
    /// Replay every row in scope whose `change_seq` is above `from`.
    Replay { from: i64 },
}

/// Decide the opening of a resume at `id` from the user's counter
/// (`last`, `purged`) and the rows in scope changed since, counted up to
/// `replay_max + 1`. An id at or below `purged_through` (once anything
/// was purged), above `last_seq`, or with more than `replay_max` rows
/// changed since resets.
pub fn decide(id: i64, last: i64, purged: i64, changed: i64, replay_max: u32) -> Opening {
    if (purged > 0 && id <= purged) || id > last || changed > i64::from(replay_max) {
        Opening::Reset { last }
    } else {
        Opening::Replay { from: id }
    }
}

/// Run `statement` before `deadline` on a pooled connection.
async fn rows(
    database: &Database,
    statement: &Statement,
    params: &[&(dyn merezha_postgres::tokio_postgres::types::ToSql + Sync)],
) -> Result<Vec<Row>, Failure> {
    let deadline = Instant::now() + QUERY_DEADLINE;
    let work = async {
        let client = database.acquire(deadline).await?;
        database
            .query(&client, statement, params)
            .await
            .map_err(|e| classify(&e))
    };
    match tokio::time::timeout_at(deadline.into(), work).await {
        Ok(result) => result,
        Err(_) => Err(Failure::transient("database_timeout")),
    }
}
async fn scalar(
    database: &Database,
    statement: &Statement,
    params: &[&(dyn merezha_postgres::tokio_postgres::types::ToSql + Sync)],
) -> Result<i64, Failure> {
    let found = rows(database, statement, params).await?;
    Ok(found.first().map_or(0, |r| r.get(0)))
}

async fn open(
    core: &Core,
    user: &str,
    tenant: Option<&str>,
    resume: Option<i64>,
    replay_max: u32,
) -> Result<Opening, Failure> {
    let counter = rows(&core.database, &STREAM_COUNTER, &[&user]).await?;
    let (last, purged) = counter
        .first()
        .map_or((0, 0), |r| (r.get::<_, i64>(0), r.get::<_, i64>(1)));
    match resume {
        None => {
            let unread = scalar(&core.database, &sql::INBOX_UNREAD, &[&user, &tenant]).await?;
            Ok(Opening::Hello { last, unread })
        }
        Some(id) => {
            let cap = i64::from(replay_max) + 1;
            let changed = scalar(
                &core.database,
                &STREAM_CHANGED_SINCE,
                &[&user, &id, &tenant, &cap],
            )
            .await?;
            Ok(decide(id, last, purged, changed, replay_max))
        }
    }
}

/// The calling user and its active tenant: 401 without a principal, 403
/// for a principal that is not a user (NT §5.4 "Authentication").
fn caller(principal: Option<&Authenticated>) -> Result<(String, Option<String>), Failure> {
    let principal = principal
        .and_then(|a| a.0.as_deref())
        .ok_or_else(|| Failure::permanent("unauthenticated"))?;
    match (&principal.kind, &principal.user) {
        (PrincipalKind::User, Some(user)) => Ok((
            user.as_str().to_owned(),
            principal.tenant.as_ref().map(|t| t.as_str().to_owned()),
        )),
        _ => Err(Failure::permanent("unauthorized")),
    }
}

/// `GET /v1/notifications/inbox/stream` (NT §5.4, §7.1).
async fn stream(
    State(state): State<StreamState>,
    principal: Option<Extension<Authenticated>>,
    client: Option<Extension<ClientInfo>>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let Some(streams) = state.streams.get().cloned() else {
        return reply(Failure::transient("unavailable"));
    };
    let metrics = streams.0.metrics.clone();
    let (user, tenant) = match caller(principal.as_deref()) {
        Ok(found) => found,
        Err(failure) => {
            metrics.rejected("auth");
            return reply(failure);
        }
    };
    // Credentials travel in the cookie or the Authorization header, never
    // in the query string: the route takes no parameters.
    if uri.query().is_some_and(|q| !q.is_empty()) {
        return reply(Failure::permanent("invalid_input"));
    }
    let Some(core) = state.slot.get().cloned() else {
        return reply(Failure::transient("unavailable"));
    };
    let lease = match streams.admit(&user) {
        Ok(lease) => lease,
        Err(Refusal::PerUser) => {
            metrics.rejected("per_user");
            return reply(Failure::permanent("stream_limit"));
        }
        Err(Refusal::Capacity) => {
            metrics.rejected("capacity");
            return ErrorReply::new(Failure::transient("overloaded"))
                .retry_after(RETRY_AFTER)
                .into_response();
        }
    };
    let settings = streams.0.settings.clone();
    let resume = last_event_id(&headers);
    let opening = match open(&core, &user, tenant.as_deref(), resume, settings.replay_max).await {
        Ok(opening) => opening,
        Err(failure) => return reply(failure),
    };
    let lifetime = jittered(settings.max_lifetime, core.random.as_ref());
    let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let task = Task {
        core,
        closing: streams.0.closing.subscribe(),
        metrics,
        settings,
        lease,
        user,
        tenant,
        cursor: 0,
        resumed: resume.is_some(),
        sender,
        credentials: Credentials::keep(&uri, &headers, client.map(|c| c.0)),
        authenticator: state.authenticator.clone(),
        ends_at: tokio::time::Instant::now() + lifetime,
        next_heartbeat: tokio::time::Instant::now(),
    };
    tokio::spawn(task.run(opening));
    let mut response = Sse::new(Events(receiver)).into_response();
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
}

/// `max_lifetime` ±10 %, so streams opened together do not reconnect
/// together.
pub fn jittered(lifetime: Duration, random: &dyn merezha_platform::Random) -> Duration {
    lifetime.mul_f64(0.9 + 0.2 * unit(random))
}

/// The response body: what the task sends, in order.
struct Events(mpsc::Receiver<Event>);
impl futures_core::Stream for Events {
    type Item = Result<Event, Infallible>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx).map(|event| event.map(Ok))
    }
}

/// Why a stream ended.
enum End {
    /// The client went away, fell behind, or the stream reached its end.
    Done,
}

/// One stream's producer.
struct Task {
    core: Arc<Core>,
    closing: watch::Receiver<bool>,
    metrics: StreamMetrics,
    settings: crate::config::SseSettings,
    lease: Lease,
    user: String,
    tenant: Option<String>,
    /// The highest `change_seq` this stream sent or started from.
    cursor: i64,
    /// The stream opened with a usable `Last-Event-ID`.
    resumed: bool,
    sender: mpsc::Sender<Event>,
    credentials: Credentials,
    authenticator: Option<Arc<dyn Authenticator>>,
    ends_at: tokio::time::Instant,
    next_heartbeat: tokio::time::Instant,
}

impl Task {
    async fn run(mut self, opening: Opening) {
        if self.start(opening).await.is_err() {
            return;
        }
        let reconcile = self.settings.reconcile_interval;
        let reauth = self.settings.reauth_interval;
        let mut next_reconcile = tokio::time::Instant::now() + reconcile;
        let mut next_reauth = tokio::time::Instant::now() + reauth;
        loop {
            if *self.closing.borrow() {
                return;
            }
            let heartbeat = self.next_heartbeat;
            let ends_at = self.ends_at;
            tokio::select! {
                _ = self.closing.changed() => return,
                () = self.sender.closed() => return,
                woken = self.lease.wake.changed() => {
                    if woken.is_err() {
                        return;
                    }
                    let span = tracing::info_span!(
                        target: merezha_telemetry::TARGET,
                        parent: None,
                        "notify sse",
                        otel.name = "notify sse",
                        notify.resumed = self.resumed,
                    );
                    if self.batch().instrument(span).await.is_err() {
                        return;
                    }
                }
                () = tokio::time::sleep_until(next_reconcile) => {
                    next_reconcile = tokio::time::Instant::now() + reconcile;
                    if self.batch().await.is_err() {
                        return;
                    }
                }
                () = tokio::time::sleep_until(heartbeat) => {
                    if self.send(Event::default().comment("hb"), None).await.is_err() {
                        return;
                    }
                }
                () = tokio::time::sleep_until(next_reauth) => {
                    next_reauth = tokio::time::Instant::now() + reauth;
                    if !self.reauthenticate().await {
                        self.metrics.rejected("auth");
                        return;
                    }
                }
                () = tokio::time::sleep_until(ends_at) => return,
            }
        }
    }

    /// `retry:`, then `hello`, `reset` or the replay.
    async fn start(&mut self, opening: Opening) -> Result<(), End> {
        self.send(Event::default().retry(RETRY), None).await?;
        let span = tracing::info_span!(
            target: merezha_telemetry::TARGET,
            parent: None,
            "notify sse",
            otel.name = "notify sse",
            notify.resumed = self.resumed,
        );
        async {
            match opening {
                Opening::Hello { last, unread } => {
                    self.cursor = last;
                    let event = Event::default()
                        .event("hello")
                        .id(last.to_string())
                        .data(json!({"unread": unread}).to_string());
                    self.send(event, Some("hello")).await
                }
                Opening::Reset { last } => {
                    self.cursor = last;
                    let event = Event::default()
                        .event("reset")
                        .id(last.to_string())
                        .data("{}");
                    self.send(event, Some("reset")).await
                }
                Opening::Replay { from } => {
                    self.cursor = from;
                    self.batch().await
                }
            }
        }
        .instrument(span)
        .await
    }

    /// Send every row in scope above the cursor, in `change_seq` order,
    /// 100 per query, then `unread` when anything was sent. A failed query
    /// keeps the stream: the next wake or reconcile retries it.
    async fn batch(&mut self) -> Result<(), End> {
        let mut sent = false;
        loop {
            let found = rows(
                &self.core.database,
                &STREAM_BATCH,
                &[&self.user, &self.cursor, &self.tenant, &BATCH],
            )
            .await;
            let Ok(found) = found else {
                return Ok(());
            };
            for row in &found {
                let change_seq: i64 = row.get(8);
                let event = Event::default()
                    .event("item")
                    .id(change_seq.to_string())
                    .data(inbox::item(row).to_string());
                self.send(event, Some("item")).await?;
                self.cursor = change_seq;
                sent = true;
            }
            if (found.len() as i64) < BATCH {
                break;
            }
        }
        if sent
            && let Ok(unread) = scalar(
                &self.core.database,
                &sql::INBOX_UNREAD,
                &[&self.user, &self.tenant],
            )
            .await
        {
            let event = Event::default()
                .event("unread")
                .data(json!({"unread": unread}).to_string());
            self.send(event, Some("unread")).await?;
        }
        Ok(())
    }

    /// Queue `event`; a client that does not take it within one heartbeat
    /// interval fell behind and is disconnected.
    async fn send(&mut self, event: Event, kind: Option<&'static str>) -> Result<(), End> {
        let within = self.settings.heartbeat;
        match tokio::time::timeout(within, self.sender.send(event)).await {
            Ok(Ok(())) => {
                if let Some(kind) = kind {
                    self.metrics.event(kind);
                }
                self.next_heartbeat = tokio::time::Instant::now() + within;
                Ok(())
            }
            _ => Err(End::Done),
        }
    }

    /// Check the credentials again (NT §5.4): the same user with the same
    /// active tenant keeps the stream; anything else, a revoked or expired
    /// session or key, or a failure to tell, ends it.
    async fn reauthenticate(&self) -> bool {
        let Some(authenticator) = &self.authenticator else {
            return true;
        };
        let check = authenticator.authenticate(&self.credentials.0);
        match tokio::time::timeout(REAUTH_DEADLINE, check).await {
            Ok(Ok(Some(principal))) => {
                principal.kind == PrincipalKind::User
                    && principal.user.as_ref().map(|u| u.as_str()) == Some(self.user.as_str())
                    && principal.tenant.as_ref().map(|t| t.as_str()) == self.tenant.as_deref()
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(per_user: u32, total: u32) -> crate::config::SseSettings {
        crate::config::SseSettings {
            heartbeat: Duration::from_secs(15),
            reconcile_interval: Duration::from_secs(30),
            replay_max: 500,
            max_streams_per_user: per_user,
            max_streams: total,
            max_lifetime: Duration::from_secs(3600),
            reauth_interval: Duration::from_secs(300),
        }
    }

    #[test]
    fn the_registry_counts_per_user_then_per_instance_and_releases_on_drop() {
        let streams = Streams::without_metrics(settings(2, 3));
        let a1 = streams.admit("usr_a").unwrap();
        let _a2 = streams.admit("usr_a").unwrap();
        assert_eq!(streams.admit("usr_a").err(), Some(Refusal::PerUser));
        let _b1 = streams.admit("usr_b").unwrap();
        assert_eq!(streams.admit("usr_c").err(), Some(Refusal::Capacity));
        // Per-user comes first: a full user is told 429, not 503.
        assert_eq!(streams.admit("usr_a").err(), Some(Refusal::PerUser));
        drop(a1);
        assert_eq!(streams.open(), 2);
        assert_eq!(streams.open_for("usr_a"), 1);
        let _c1 = streams.admit("usr_c").unwrap();
        streams.close();
        assert!(streams.closing());
        assert_eq!(streams.admit("usr_d").err(), Some(Refusal::Capacity));
    }

    #[test]
    fn a_wake_reaches_only_the_users_streams_admitted_before_it() {
        let streams = Streams::without_metrics(settings(8, 10));
        let a = streams.admit("usr_a").unwrap();
        let b = streams.admit("usr_b").unwrap();
        assert!(!a.wake.has_changed().unwrap());
        streams.wake("usr_a");
        streams.wake("usr_nobody");
        assert!(a.wake.has_changed().unwrap());
        assert!(!b.wake.has_changed().unwrap());
        // The last stream gone, the user's entry goes too.
        drop(a);
        drop(b);
        assert!(streams.0.registry().users.is_empty());
    }

    /// Wakes seen by `lease` so far.
    fn wakes(lease: &Lease) -> u64 {
        *lease.wake.borrow()
    }

    /// `wake_all` wakes every user once, spread over `WAKE_ALL_SPREAD`
    /// rather than all at once; the next pass starts no sooner than one
    /// spread after the last, and requests made during a pass coalesce
    /// into one more pass.
    #[tokio::test]
    async fn wake_all_spreads_over_a_second_and_coalesces_requests() {
        let streams = Streams::without_metrics(settings(8, 100));
        // Four users: woken at 0, 250, 500 and 750 ms of each pass.
        let leases: Vec<Lease> = (0..4)
            .map(|n| streams.admit(&format!("usr_{n}")).unwrap())
            .collect();
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(sweeper(streams.clone(), stopped));
        let started = Instant::now();
        streams.wake_all();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let early = leases.iter().filter(|l| wakes(l) == 1).count();
        assert!(early < 4, "every user woken within 100 ms: not spread");
        // Three more requests during the pass give exactly one more pass.
        streams.wake_all();
        streams.wake_all();
        streams.wake_all();
        let until = |want: u64, all: bool| {
            let leases = &leases;
            async move {
                loop {
                    let reached = |l: &Lease| wakes(l) >= want;
                    let done = if all {
                        leases.iter().all(reached)
                    } else {
                        leases.iter().any(reached)
                    };
                    if done {
                        return started.elapsed();
                    }
                    assert!(started.elapsed() < Duration::from_secs(10));
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        };
        let first = until(1, true).await;
        assert!(
            first >= Duration::from_millis(700),
            "one pass took {first:?}"
        );
        let next = until(2, false).await;
        assert!(
            next >= Duration::from_millis(950),
            "the next pass started {next:?} after the first, within the spread"
        );
        until(2, true).await;
        tokio::time::sleep(WAKE_ALL_SPREAD + Duration::from_millis(500)).await;
        assert!(
            leases.iter().all(|l| wakes(l) == 2),
            "coalesced into one pass"
        );
        stop.send_replace(true);
        task.await.unwrap();
    }

    #[test]
    fn the_listen_backoff_doubles_with_jitter_and_resets_only_after_a_stable_session() {
        let mut backoff = Backoff::default();
        // 1 s, then the upper half of 2, 4, 8, 16 and 30 s steps.
        assert_eq!(backoff.next(0.0), Duration::from_secs(1));
        assert_eq!(backoff.next(0.0), Duration::from_secs(1));
        assert_eq!(backoff.next(1.0), Duration::from_secs(4));
        assert_eq!(backoff.next(0.5), Duration::from_secs(6));
        assert_eq!(backoff.next(1.0), Duration::from_secs(16));
        assert_eq!(backoff.next(1.0), Duration::from_secs(30));
        assert_eq!(backoff.next(1.0), Duration::from_secs(30));
        assert_eq!(backoff.next(0.0), Duration::from_secs(15));
        // A session that connects and drops again does not reset it.
        backoff.lost_after(STABLE_AFTER - Duration::from_millis(1));
        assert_eq!(backoff.next(1.0), Duration::from_secs(30));
        // One that stayed up a probe interval does.
        backoff.lost_after(STABLE_AFTER);
        assert_eq!(backoff.next(1.0), Duration::from_secs(1));
        assert_eq!(backoff.next(1.0), Duration::from_secs(2));
    }

    #[test]
    fn last_event_id_is_a_decimal_change_seq_or_absent() {
        let id = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert("last-event-id", HeaderValue::from_str(value).unwrap());
            last_event_id(&headers)
        };
        assert_eq!(id("42"), Some(42));
        assert_eq!(id("0"), Some(0));
        for bad in ["", "-1", "+3", "1e3", "abc", "9223372036854775808", "1 2"] {
            assert_eq!(id(bad), None, "{bad}");
        }
        assert_eq!(last_event_id(&HeaderMap::new()), None);
    }

    #[test]
    fn resume_resets_at_or_below_purged_through_above_last_seq_or_past_replay_max() {
        assert_eq!(decide(5, 10, 0, 5, 500), Opening::Replay { from: 5 });
        assert_eq!(decide(0, 10, 0, 10, 500), Opening::Replay { from: 0 });
        assert_eq!(decide(10, 10, 0, 0, 500), Opening::Replay { from: 10 });
        assert_eq!(decide(4, 10, 4, 6, 500), Opening::Reset { last: 10 });
        assert_eq!(decide(3, 10, 4, 7, 500), Opening::Reset { last: 10 });
        assert_eq!(decide(5, 10, 4, 5, 500), Opening::Replay { from: 5 });
        assert_eq!(decide(11, 10, 0, 0, 500), Opening::Reset { last: 10 });
        assert_eq!(decide(5, 10, 0, 3, 2), Opening::Reset { last: 10 });
        assert_eq!(decide(5, 10, 0, 2, 2), Opening::Replay { from: 5 });
    }

    #[test]
    fn lifetimes_spread_ten_percent_either_side() {
        struct Fixed(u8);
        impl merezha_platform::Random for Fixed {
            fn fill(&self, bytes: &mut [u8]) -> Result<(), Failure> {
                bytes.fill(self.0);
                Ok(())
            }
        }
        let hour = Duration::from_secs(3600);
        let low = jittered(hour, &Fixed(0));
        let high = jittered(hour, &Fixed(255));
        assert_eq!(low, Duration::from_secs(3240));
        assert_eq!(high, Duration::from_secs(3960));
    }

    #[test]
    fn credentials_are_never_shown() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer s3cr3t"),
        );
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("text/event-stream"),
        );
        let kept = Credentials::keep(
            &Uri::from_static("/v1/notifications/inbox/stream"),
            &headers,
            None,
        );
        assert_eq!(format!("{kept:?}"), "Credentials(<redacted>)");
        assert!(kept.0.headers.contains_key(header::AUTHORIZATION));
        assert!(!kept.0.headers.contains_key(header::ACCEPT));
    }

    #[test]
    fn the_stream_statements_are_registered_beside_the_kernel() {
        assert_eq!(
            merezha_postgres::check_registry(&[
                merezha_postgres::KERNEL_STATEMENTS,
                crate::drivers::STATEMENTS,
                crate::sql::STATEMENTS,
                super::STATEMENTS,
            ]),
            Ok(())
        );
    }
}
