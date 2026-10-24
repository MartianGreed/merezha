//! Metrics and spans of the PostgreSQL kernel (TE §1.3 "PostgreSQL", T4):
//! `db.client.operation.duration` and the statement spans, the pool gauges,
//! acquisition wait and timeouts, listener reconnects and the pending
//! migrations gauge. Instruments come from the `Meter` the pool was built
//! with; the default is a no-op meter.
//!
//! Spans follow one rule: a statement or transaction span opens only
//! inside a caller's span, so probes, polls and other background statements
//! with no caller count in the metrics without starting traces.
use crate::{failure_code, statement::Statement};
use merezha_domain::{Classification, Failure};
use merezha_telemetry::names::{self, db};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter, ObservableGauge},
};
use std::{
    borrow::Cow,
    collections::BTreeMap,
    future::Future,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{Instrument, Span, field::Empty};

/// Most modules and listener channel sets tracked; the kernel caps a run at
/// 64 modules and each block listens on fixed channels.
const MAX_TRACKED: usize = 256;
/// `db.pool_timeout` is logged at most this often; the counter counts all.
pub(crate) const POOL_TIMEOUT_LOG_EVERY: Duration = Duration::from_secs(10);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Whether the current span is one the span bridge records: it has a valid
/// OpenTelemetry span context. A caller span the bridge filtered out (a
/// non-`merezha` target, or below `INFO`) or a subscriber without the
/// bridge leaves none, so DB spans are never orphaned into roots.
pub(crate) fn traced_caller() -> bool {
    let current = Span::current();
    !current.is_none() && merezha_telemetry::propagation::span_context(&current).is_some()
}

/// Whether a failure breaks the host's own contract and sets span status
/// `Error` (TE §1.1): transient failures and statements the server refused
/// as invalid (`database_error`). Conflicts and constraint violations are
/// the caller's outcome and leave it unset.
pub(crate) fn host_failure(classification: Classification, code: &str) -> bool {
    classification == Classification::Transient || code == "database_error"
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ListenState {
    Connected,
    Lost,
}

pub(crate) struct Instruments {
    duration: Histogram<f64>,
    wait_time: Histogram<f64>,
    timeouts: Counter<u64>,
    reconnects: Counter<u64>,
    pending: Arc<Mutex<BTreeMap<String, u64>>>,
    listeners: Mutex<BTreeMap<String, ListenState>>,
    last_timeout_log: Mutex<Option<Instant>>,
    /// Callers waiting for a detached session slot.
    detached_waiting: Arc<AtomicUsize>,
    /// Listener sessions open now.
    listeners_open: Arc<AtomicUsize>,
    _gauges: Vec<ObservableGauge<u64>>,
}

impl Instruments {
    /// The pool's instruments, plus two more connection pools on the same
    /// gauges: `merezha.detached`, the sessions [`crate::Database::detach`]
    /// took out of the pool (at most `max_detached`), and `merezha.listen`,
    /// the open listener sessions (unbounded, so no `max`). Neither has
    /// idle connections.
    pub(crate) fn new(
        meter: &Meter,
        pool: &deadpool_postgres::Pool,
        detached: &Arc<tokio::sync::Semaphore>,
        max_detached: usize,
    ) -> Self {
        use names::metric as m;
        let histogram = |row: names::Metric| {
            meter
                .f64_histogram(row.name)
                .with_unit(row.unit)
                .with_boundaries(row.buckets.to_vec())
                .build()
        };
        let counter = |row: names::Metric| meter.u64_counter(row.name).with_unit(row.unit).build();
        let detached_waiting = Arc::new(AtomicUsize::new(0));
        let listeners_open = Arc::new(AtomicUsize::new(0));
        let slots = Arc::downgrade(detached);
        let listening = Arc::downgrade(&listeners_open);
        let weak = pool.weak();
        let count = meter
            .u64_observable_gauge(m::DB_CLIENT_CONNECTION_COUNT.name)
            .with_unit(m::DB_CLIENT_CONNECTION_COUNT.unit)
            .with_callback(move |observer| {
                if let Some(pool) = weak.upgrade() {
                    let status = pool.status();
                    let idle = status.available as u64;
                    let used = (status.size as u64).saturating_sub(idle);
                    for (value, state) in [
                        (idle, db::ConnectionState::Idle),
                        (used, db::ConnectionState::Used),
                    ] {
                        observer.observe(value, &[db::pool(), db::connection_state(state)]);
                    }
                }
                if let Some(slots) = slots.upgrade() {
                    let used = max_detached.saturating_sub(slots.available_permits());
                    observer.observe(
                        used as u64,
                        &[
                            db::pool_named(db::DETACHED_POOL),
                            db::connection_state(db::ConnectionState::Used),
                        ],
                    );
                }
                if let Some(open) = listening.upgrade() {
                    observer.observe(
                        open.load(Ordering::Relaxed) as u64,
                        &[
                            db::pool_named(db::LISTEN_POOL),
                            db::connection_state(db::ConnectionState::Used),
                        ],
                    );
                }
            })
            .build();
        let weak = pool.weak();
        let max = meter
            .u64_observable_gauge(m::DB_CLIENT_CONNECTION_MAX.name)
            .with_unit(m::DB_CLIENT_CONNECTION_MAX.unit)
            .with_callback(move |observer| {
                if let Some(pool) = weak.upgrade() {
                    observer.observe(pool.status().max_size as u64, &[db::pool()]);
                    observer.observe(max_detached as u64, &[db::pool_named(db::DETACHED_POOL)]);
                }
            })
            .build();
        let weak = pool.weak();
        let queued = Arc::downgrade(&detached_waiting);
        let waiting = meter
            .u64_observable_gauge(m::DB_CLIENT_CONNECTION_PENDING_REQUESTS.name)
            .with_unit(m::DB_CLIENT_CONNECTION_PENDING_REQUESTS.unit)
            .with_callback(move |observer| {
                if let Some(pool) = weak.upgrade() {
                    observer.observe(pool.status().waiting as u64, &[db::pool()]);
                }
                if let Some(queued) = queued.upgrade() {
                    observer.observe(
                        queued.load(Ordering::Relaxed) as u64,
                        &[db::pool_named(db::DETACHED_POOL)],
                    );
                }
            })
            .build();
        let pending = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
        let observed = Arc::downgrade(&pending);
        let migrations = meter
            .u64_observable_gauge(m::DB_MIGRATIONS_PENDING.name)
            .with_unit(m::DB_MIGRATIONS_PENDING.unit)
            .with_callback(move |observer| {
                if let Some(pending) = observed.upgrade() {
                    for (module, count) in lock(&pending).iter() {
                        observer.observe(*count, &[db::module(module.clone())]);
                    }
                }
            })
            .build();
        Self {
            duration: histogram(m::DB_CLIENT_OPERATION_DURATION),
            wait_time: histogram(m::DB_CLIENT_CONNECTION_WAIT_TIME),
            timeouts: counter(m::DB_CLIENT_CONNECTION_TIMEOUTS),
            reconnects: counter(m::DB_LISTENER_RECONNECTS),
            pending,
            listeners: Mutex::new(BTreeMap::new()),
            last_timeout_log: Mutex::new(None),
            detached_waiting,
            listeners_open,
            _gauges: vec![count, max, waiting, migrations],
        }
    }
}

/// The instruments plus the module a `Database` handle was scoped to.
#[derive(Clone)]
pub(crate) struct Telemetry {
    instruments: Arc<Instruments>,
    module: Cow<'static, str>,
}

impl Telemetry {
    pub(crate) fn new(instruments: Instruments) -> Self {
        Self {
            instruments: Arc::new(instruments),
            module: Cow::Borrowed(db::KERNEL_MODULE),
        }
    }
    /// The same instruments, labelled `merezha.module = module`.
    pub(crate) fn for_module(&self, module: impl Into<Cow<'static, str>>) -> Self {
        Self {
            instruments: self.instruments.clone(),
            module: module.into(),
        }
    }
    pub(crate) fn module(&self) -> &str {
        &self.module
    }

    /// Time `call`, one execution of `statement`, inside its span.
    pub(crate) async fn observe<T, F>(
        &self,
        statement: &Statement,
        call: F,
    ) -> Result<T, tokio_postgres::Error>
    where
        F: Future<Output = Result<T, tokio_postgres::Error>>,
    {
        let span = self.statement_span(statement);
        let mut pending = Pending {
            telemetry: self,
            statement: *statement,
            started: Instant::now(),
            span: span.clone(),
            done: false,
        };
        let result = call.instrument(span).await;
        pending.finish(result.as_ref().err().map(Outcome::Failed));
        result
    }

    fn statement_span(&self, statement: &Statement) -> Span {
        if !traced_caller() {
            return Span::none();
        }
        let span = tracing::info_span!(
            "db.statement",
            otel.name = statement.summary,
            otel.kind = "client",
            otel.status_code = Empty,
            db.system.name = db::SYSTEM,
            db.operation.name = statement.operation(),
            db.collection.name = Empty,
            db.query.summary = statement.summary,
            merezha.db.statement = statement.name,
            merezha.module = self.module(),
            db.response.status_code = Empty,
            error.type = Empty,
        );
        if let Some(collection) = statement.collection() {
            span.record("db.collection.name", collection);
        }
        span
    }

    /// The `transaction {module}` span, inside a caller's span only.
    pub(crate) fn transaction_span(&self) -> Span {
        if !traced_caller() {
            return Span::none();
        }
        let name = format!("transaction {}", self.module());
        tracing::info_span!(
            "db.transaction",
            otel.name = name.as_str(),
            otel.status_code = Empty,
            merezha.module = self.module(),
            merezha.db.pool.wait = Empty,
        )
    }

    /// The `migrate {module}` span of one module's pending migrations.
    pub(crate) fn migration_span(module: &str) -> Span {
        let name = format!("migrate {module}");
        tracing::info_span!(
            "db.migrate",
            otel.name = name.as_str(),
            otel.status_code = Empty,
            merezha.module = module,
        )
    }

    /// A successful acquisition after `waited`.
    pub(crate) fn acquired(&self, waited: Duration) {
        self.instruments
            .wait_time
            .record(waited.as_secs_f64(), &[db::pool()]);
    }
    /// An acquisition that gave up waiting for a free connection.
    pub(crate) fn pool_timeout(&self) {
        self.instruments.timeouts.add(1, &[db::pool()]);
        let now = Instant::now();
        let mut last = lock(&self.instruments.last_timeout_log);
        if last.is_none_or(|at| now.duration_since(at) >= POOL_TIMEOUT_LOG_EVERY) {
            *last = Some(now);
            drop(last);
            merezha_telemetry::event!(WARN, "db.pool_timeout", code = "database_busy");
        }
    }

    /// A caller waiting for a detached slot until the guard drops.
    pub(crate) fn detached_waiting(&self) -> Waiting {
        self.instruments
            .detached_waiting
            .fetch_add(1, Ordering::Relaxed);
        Waiting(self.instruments.detached_waiting.clone())
    }
    /// A detached slot granted after `waited`.
    pub(crate) fn detached_acquired(&self, waited: Duration) {
        self.instruments
            .wait_time
            .record(waited.as_secs_f64(), &[db::pool_named(db::DETACHED_POOL)]);
    }
    /// A wait for a detached slot that gave up.
    pub(crate) fn detached_timeout(&self) {
        self.instruments
            .timeouts
            .add(1, &[db::pool_named(db::DETACHED_POOL)]);
    }
    /// One open listener session until the guard drops.
    pub(crate) fn listener_open(&self) -> Waiting {
        self.instruments
            .listeners_open
            .fetch_add(1, Ordering::Relaxed);
        Waiting(self.instruments.listeners_open.clone())
    }
    /// A listener session on `channels` opened. After one on the same
    /// channels was lost, this is a reconnect.
    pub(crate) fn listener_connected(&self, channels: &str) {
        let mut listeners = lock(&self.instruments.listeners);
        if listeners.len() >= MAX_TRACKED && !listeners.contains_key(channels) {
            return;
        }
        if listeners.insert(channels.to_owned(), ListenState::Connected) == Some(ListenState::Lost)
        {
            self.instruments.reconnects.add(1, &[]);
        }
    }
    /// A listener session on `channels` ended or failed its probe, or an
    /// attempt to reopen one failed.
    pub(crate) fn listener_lost(&self, channels: &str) {
        if let Some(state) = lock(&self.instruments.listeners).get_mut(channels) {
            *state = ListenState::Lost;
        }
    }

    /// The pending count of each module a migration run checked.
    pub(crate) fn migrations_pending<'a>(&self, counts: impl IntoIterator<Item = (&'a str, u64)>) {
        let mut pending = lock(&self.instruments.pending);
        for (module, count) in counts {
            if pending.len() < MAX_TRACKED || pending.contains_key(module) {
                pending.insert(module.to_owned(), count);
            }
        }
    }
}

/// Record `failure` on a transaction or migration span.
pub(crate) fn record_failure(span: &Span, failure: &Failure) {
    if host_failure(failure.classification, &failure.code) {
        span.record("otel.status_code", "error");
    }
}

/// Decrements its counter when dropped: a waiter that gave up or was
/// cancelled, or a listener session that closed.
pub(crate) struct Waiting(Arc<AtomicUsize>);
impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

enum Outcome<'a> {
    Failed(&'a tokio_postgres::Error),
    Cancelled,
}

/// One statement in flight. Dropped before it finished, it records the
/// statement as `cancelled`.
struct Pending<'a> {
    telemetry: &'a Telemetry,
    statement: Statement,
    started: Instant,
    span: Span,
    done: bool,
}
impl Pending<'_> {
    fn finish(&mut self, outcome: Option<Outcome<'_>>) {
        self.done = true;
        let mut attributes: Vec<KeyValue> = Vec::with_capacity(6);
        attributes.push(db::system());
        attributes.push(db::operation(self.statement.operation()));
        if let Some(collection) = self.statement.collection() {
            attributes.push(db::collection(collection));
        }
        attributes.push(db::module(self.telemetry.module.clone()));
        match outcome {
            None => {}
            Some(Outcome::Cancelled) => {
                self.span.record("error.type", db::CANCELLED);
                attributes.push(db::error_type(db::CANCELLED));
            }
            Some(Outcome::Failed(error)) => {
                let (classification, code) = failure_code(error);
                self.span.record("error.type", code);
                attributes.push(db::error_type(code));
                if let Some(state) = error.code() {
                    let value = db::sqlstate_value(state.code());
                    self.span.record("db.response.status_code", value.as_str());
                    attributes.push(db::status_code(state.code()));
                }
                if host_failure(classification, code) {
                    self.span.record("otel.status_code", "error");
                }
            }
        }
        self.telemetry
            .instruments
            .duration
            .record(self.started.elapsed().as_secs_f64(), &attributes);
    }
}
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.finish(Some(Outcome::Cancelled));
        }
    }
}
