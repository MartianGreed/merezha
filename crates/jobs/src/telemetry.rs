//! Jobs and outbox telemetry (TE §1.2, §1.3, §2.3; TE-T5): the
//! `merezha.job.*` and `merezha.outbox.*` instruments, the cached gauges
//! over the queue status read, and the `send`, `process`, `relay outbox` and
//! `admin job.replay` spans with the stored trace context they write and
//! read. Names come from `merezha_telemetry::names` only.
use merezha_platform::ports::QueueSummary;
use merezha_telemetry::{
    names::{
        Metric,
        jobs::{self as names, Classification, Outcome, RecordKind, State, attr},
        metric,
    },
    propagation::StoredContext,
};
use opentelemetry::{
    metrics::{Counter, Histogram, Meter, MeterProvider, ObservableGauge},
    trace::{Status, TraceContextExt},
};
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tracing::{Span, field::Empty};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// `tokio::spawn` that keeps the caller's `tracing` dispatcher, so the
/// runtime's background tasks report to a scoped test dispatcher too.
pub(crate) fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    use tracing::instrument::WithSubscriber;
    tokio::spawn(future.with_current_subscriber())
}

/// The latest queue status sample the gauges report: database-level
/// numbers, the same on every instance (TE §1.3).
#[derive(Clone, Debug, Default)]
pub(crate) struct Sample {
    pub queues: Vec<QueueSummary>,
    /// Unrelayed records, quarantined ones included.
    pub outbox_pending: u64,
    /// Age in seconds of the oldest of them.
    pub outbox_lag: f64,
}

type Cache = Arc<Mutex<Option<Sample>>>;
fn read(cache: &Cache) -> Option<Sample> {
    cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Every jobs and outbox instrument, built once from the catalog rows.
pub(crate) struct Metrics {
    enqueued: Counter<u64>,
    duration: Histogram<f64>,
    queue_latency: Histogram<f64>,
    attempts: Histogram<f64>,
    dead_lettered: Counter<u64>,
    replayed: Counter<u64>,
    cancelled: Counter<u64>,
    lease_expired: Counter<u64>,
    schedule_fired: Counter<u64>,
    appended: Counter<u64>,
    relayed: Counter<u64>,
    unrouted: Counter<u64>,
    quarantined: Counter<u64>,
    cache: Cache,
    _count: ObservableGauge<u64>,
    _lag: ObservableGauge<f64>,
    _pending: ObservableGauge<u64>,
    _outbox_lag: ObservableGauge<f64>,
}
impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

fn counter(meter: &Meter, row: Metric) -> Counter<u64> {
    meter.u64_counter(row.name).with_unit(row.unit).build()
}
fn histogram(meter: &Meter, row: Metric) -> Histogram<f64> {
    meter
        .f64_histogram(row.name)
        .with_unit(row.unit)
        .with_boundaries(row.buckets.to_vec())
        .build()
}

/// The catalog rows this crate exports: every `merezha.job.*` and
/// `merezha.outbox.*` row (TE-T5).
pub const INSTRUMENTS: [Metric; 17] = [
    metric::JOB_ENQUEUED,
    metric::JOB_DURATION,
    metric::JOB_QUEUE_LATENCY,
    metric::JOB_ATTEMPTS,
    metric::JOB_DEAD_LETTERED,
    metric::JOB_REPLAYED,
    metric::JOB_CANCELLED,
    metric::JOB_LEASE_EXPIRED,
    metric::JOB_COUNT,
    metric::JOB_LAG,
    metric::JOB_SCHEDULE_FIRED,
    metric::OUTBOX_APPENDED,
    metric::OUTBOX_RELAYED,
    metric::OUTBOX_UNROUTED,
    metric::OUTBOX_QUARANTINED,
    metric::OUTBOX_PENDING,
    metric::OUTBOX_LAG,
];

impl Default for Metrics {
    /// Instruments on a no-op meter.
    fn default() -> Self {
        Self::new(&opentelemetry::metrics::noop::NoopMeterProvider::new().meter("jobs"))
    }
}

impl Metrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        let cache: Cache = Arc::default();
        let c = cache.clone();
        let count = meter
            .u64_observable_gauge(metric::JOB_COUNT.name)
            .with_unit(metric::JOB_COUNT.unit)
            .with_callback(move |observer| {
                for queue in read(&c).map(|s| s.queues).unwrap_or_default() {
                    for (state, value) in [
                        (State::Ready, queue.ready),
                        (State::Delayed, queue.delayed),
                        (State::Leased, queue.leased),
                        (State::Dead, queue.dead),
                    ] {
                        observer.observe(value, &[attr::queue(&queue.name), attr::state(state)]);
                    }
                }
            })
            .build();
        let c = cache.clone();
        let lag = meter
            .f64_observable_gauge(metric::JOB_LAG.name)
            .with_unit(metric::JOB_LAG.unit)
            .with_callback(move |observer| {
                for queue in read(&c).map(|s| s.queues).unwrap_or_default() {
                    observer.observe(queue.oldest_ready_age_s, &[attr::queue(&queue.name)]);
                }
            })
            .build();
        let c = cache.clone();
        let pending = meter
            .u64_observable_gauge(metric::OUTBOX_PENDING.name)
            .with_unit(metric::OUTBOX_PENDING.unit)
            .with_callback(move |observer| {
                if let Some(sample) = read(&c) {
                    observer.observe(sample.outbox_pending, &[]);
                }
            })
            .build();
        let c = cache.clone();
        let outbox_lag = meter
            .f64_observable_gauge(metric::OUTBOX_LAG.name)
            .with_unit(metric::OUTBOX_LAG.unit)
            .with_callback(move |observer| {
                if let Some(sample) = read(&c) {
                    observer.observe(sample.outbox_lag, &[]);
                }
            })
            .build();
        Self {
            enqueued: counter(meter, metric::JOB_ENQUEUED),
            duration: histogram(meter, metric::JOB_DURATION),
            queue_latency: histogram(meter, metric::JOB_QUEUE_LATENCY),
            attempts: histogram(meter, metric::JOB_ATTEMPTS),
            dead_lettered: counter(meter, metric::JOB_DEAD_LETTERED),
            replayed: counter(meter, metric::JOB_REPLAYED),
            cancelled: counter(meter, metric::JOB_CANCELLED),
            lease_expired: counter(meter, metric::JOB_LEASE_EXPIRED),
            schedule_fired: counter(meter, metric::JOB_SCHEDULE_FIRED),
            appended: counter(meter, metric::OUTBOX_APPENDED),
            relayed: counter(meter, metric::OUTBOX_RELAYED),
            unrouted: counter(meter, metric::OUTBOX_UNROUTED),
            quarantined: counter(meter, metric::OUTBOX_QUARANTINED),
            cache,
            _count: count,
            _lag: lag,
            _pending: pending,
            _outbox_lag: outbox_lag,
        }
    }

    /// Replace the sample the gauges report.
    pub(crate) fn sampled(&self, sample: Sample) {
        *self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sample);
    }

    /// New jobs on `queue` of `kind`: enqueued, relayed, fired or triggered.
    pub(crate) fn enqueued(&self, queue: &str, kind: &str, count: u64) {
        if count > 0 {
            self.enqueued
                .add(count, &[attr::queue(queue), attr::kind(kind)]);
        }
    }
    /// One attempt ended with `outcome` after `seconds`.
    pub(crate) fn attempt(
        &self,
        queue: &str,
        kind: &str,
        outcome: Outcome,
        classification: Option<Classification>,
        seconds: f64,
    ) {
        let mut attributes = vec![attr::queue(queue), attr::kind(kind), attr::outcome(outcome)];
        attributes.extend(classification.map(attr::classification));
        self.duration.record(seconds.max(0.0), &attributes);
    }
    /// A job reached its terminal outcome after `attempts` attempts.
    pub(crate) fn terminal(&self, queue: &str, kind: &str, outcome: Outcome, attempts: u32) {
        self.attempts.record(
            f64::from(attempts),
            &[attr::queue(queue), attr::kind(kind), attr::outcome(outcome)],
        );
        if outcome == Outcome::DeadLettered {
            self.dead_lettered
                .add(1, &[attr::queue(queue), attr::kind(kind)]);
        }
    }
    /// Lease start minus `run_at`.
    pub(crate) fn queue_latency(&self, queue: &str, seconds: f64) {
        self.queue_latency
            .record(seconds.max(0.0), &[attr::queue(queue)]);
    }
    pub(crate) fn replayed(&self, queue: &str, count: u64) {
        if count > 0 {
            self.replayed.add(count, &[attr::queue(queue)]);
        }
    }
    pub(crate) fn cancelled(&self, queue: &str) {
        self.cancelled.add(1, &[attr::queue(queue)]);
    }
    pub(crate) fn lease_expired(&self, queue: &str) {
        self.lease_expired.add(1, &[attr::queue(queue)]);
    }
    pub(crate) fn fired(&self, schedule: &str) {
        self.schedule_fired.add(1, &[attr::schedule(schedule)]);
    }
    pub(crate) fn appended(&self, module: &str, kind: RecordKind, count: u64) {
        if count > 0 {
            self.appended
                .add(count, &[attr::module(module), attr::record_kind(kind)]);
        }
    }
    pub(crate) fn relayed(&self, module: &str, count: u64) {
        self.relayed.add(count, &[attr::module(module)]);
    }
    pub(crate) fn unrouted(&self, module: &str, count: u64) {
        self.unrouted.add(count, &[attr::module(module)]);
    }
    pub(crate) fn quarantined(&self, module: &str, code: &str) {
        self.quarantined
            .add(1, &[attr::module(module), attr::error_type(code)]);
    }
}

/// Count `values` by value, for per-module and per-queue counters.
pub(crate) fn tally<'a>(
    values: impl IntoIterator<Item = &'a str>,
) -> std::collections::BTreeMap<&'a str, u64> {
    let mut counts = std::collections::BTreeMap::new();
    for value in values {
        *counts.entry(value).or_insert(0) += 1;
    }
    counts
}

/// The stored form of `span`'s context, sampled or not; `None` when no
/// OpenTelemetry layer records it.
pub(crate) fn stored(span: &Span) -> Option<StoredContext> {
    let context = span.context();
    let otel = context.span();
    let span_context = otel.span_context();
    span_context
        .is_valid()
        .then(|| StoredContext::from_span_context(span_context))
}

/// The `trace_parent` and `trace_state` column values of `context`.
pub(crate) fn columns(context: Option<&StoredContext>) -> (Option<String>, Option<String>) {
    match context {
        Some(context) => (
            Some(context.trace_parent().to_owned()),
            context.trace_state().map(str::to_owned),
        ),
        None => (None, None),
    }
}

/// Read stored columns; malformed values start a new trace.
pub(crate) fn parse(parent: Option<&str>, state: Option<&str>) -> Option<StoredContext> {
    parent.and_then(|parent| StoredContext::parse(parent, state))
}

/// `send {queue}` or `send {topic}`, PRODUCER, a child of the caller's
/// span. Its context goes into the row, sampled or not.
pub(crate) fn send_span(destination: &str, queue: Option<&str>, kind: Option<&str>) -> Span {
    tracing::info_span!(
        "send",
        otel.name = names::send_span(destination).as_str(),
        otel.kind = "producer",
        messaging.system = names::MESSAGING_SYSTEM,
        messaging.operation.type = names::OPERATION_SEND,
        messaging.destination.name = destination,
        job.queue = queue,
        job.kind = kind,
    )
}

/// What a `process` span describes.
pub(crate) struct Attempt<'a> {
    pub id: i64,
    pub queue: &'a str,
    pub kind: &'a str,
    pub attempt: u32,
    pub max_attempts: u32,
}

/// `process {queue}`, CONSUMER: a child of the stored producer context, so
/// every attempt and retry joins the producer's trace (R10), or a root
/// when none was stored (cron, or rows written before TE-T5). A replayed
/// job's attempts also link to the trace it died in.
pub(crate) fn process_span(
    attempt: &Attempt<'_>,
    parent: Option<&StoredContext>,
    replayed_from: Option<&StoredContext>,
) -> Span {
    let span = tracing::info_span!(
        parent: None,
        "process",
        otel.name = names::process_span(attempt.queue).as_str(),
        otel.kind = "consumer",
        messaging.system = names::MESSAGING_SYSTEM,
        messaging.operation.type = names::OPERATION_PROCESS,
        messaging.destination.name = attempt.queue,
        messaging.message.id = attempt.id.to_string().as_str(),
        job.id = attempt.id,
        job.queue = attempt.queue,
        job.kind = attempt.kind,
        job.attempt = attempt.attempt,
        job.max_attempts = attempt.max_attempts,
        job.outcome = Empty,
        job.code = Empty,
        job.classification = Empty,
    );
    if let Some(parent) = parent {
        let context = opentelemetry::Context::new().with_remote_span_context(parent.span_context());
        let _ = span.set_parent(context);
    }
    if let Some(link) = replayed_from {
        span.add_link_with_attributes(link.span_context(), vec![attr::replay_link()]);
    }
    span
}

/// Close a `process` span's attributes. The status is `Error` only when
/// the host or a dependency failed (transient failures, timeouts, panics,
/// lost leases); domain rejections leave it unset (TE §1.1).
pub(crate) fn finish(
    span: &Span,
    outcome: Outcome,
    code: Option<&str>,
    classification: Option<Classification>,
) {
    span.record(names::OUTCOME, outcome.as_str());
    if let Some(code) = code {
        span.record(names::CODE, code);
    }
    if let Some(classification) = classification {
        span.record(names::CLASSIFICATION, classification.as_str());
    }
    let host_failed = classification == Some(Classification::Transient)
        || code == Some("handler_panic")
        || outcome == Outcome::LeaseLost;
    if host_failed {
        span.set_status(Status::error(""));
    }
}

/// `relay outbox`, a root, opened only once a batch holds records.
pub(crate) fn relay_span() -> Span {
    tracing::info_span!(
        parent: None,
        "relay",
        otel.name = names::RELAY_SPAN,
        messaging.system = names::MESSAGING_SYSTEM,
    )
}

/// Mark a span `Error` for a systemic failure.
pub(crate) fn failed(span: &Span) {
    span.set_status(Status::error(""));
}

/// `admin job.replay`, the operator action that replays dead jobs: a child
/// of the admin request's span, in a trace of its own (TE §2.3).
pub(crate) fn replay_span(job: Option<i64>, queue: Option<&str>) -> Span {
    tracing::info_span!(
        "admin",
        otel.name = names::REPLAY_SPAN,
        job.id = job,
        job.queue = queue,
    )
}

/// Attributes of `merezha.outbox.quarantined`, for tests of this module.
#[cfg(test)]
fn quarantine_attributes(module: &str, code: &str) -> Vec<opentelemetry::KeyValue> {
    vec![attr::module(module), attr::error_type(code)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use merezha_telemetry::names::{Instrument, METRICS};

    /// AP-B2 defines no instrument of its own: the crate exports exactly the
    /// catalog's jobs and outbox rows.
    #[test]
    fn the_instruments_are_the_catalog_jobs_and_outbox_rows() {
        let mut ours: Vec<&str> = INSTRUMENTS.iter().map(|m| m.name).collect();
        ours.sort_unstable();
        let mut catalog: Vec<&str> = METRICS
            .iter()
            .filter(|m| m.name.starts_with("merezha.job.") || m.name.starts_with("merezha.outbox."))
            .map(|m| m.name)
            .collect();
        catalog.sort_unstable();
        assert_eq!(ours, catalog);
        let gauges: Vec<&str> = INSTRUMENTS
            .iter()
            .filter(|m| m.instrument == Instrument::Gauge)
            .map(|m| m.name)
            .collect();
        assert_eq!(
            gauges,
            [
                "merezha.job.count",
                "merezha.job.lag",
                "merezha.outbox.pending",
                "merezha.outbox.lag"
            ]
        );
        let keys: Vec<String> = quarantine_attributes("tasks", "unroutable")
            .iter()
            .map(|kv| kv.key.to_string())
            .collect();
        assert_eq!(keys, metric::OUTBOX_QUARANTINED.attributes);
    }

    #[test]
    fn tallies_count_each_value() {
        let counts = tally(["a", "b", "a"]);
        assert_eq!(counts.get("a"), Some(&2));
        assert_eq!(counts.get("b"), Some(&1));
    }
}
