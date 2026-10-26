//! Dispatch and projection telemetry (TE §1.2, §1.3, §2.2): `command {tag}`,
//! `query {tag}` and `projection {name}` spans, the outcome mapping, the
//! `merezha.command.*`, `merezha.query.*`, `merezha.events.*` and
//! `merezha.projection.*` instruments, and the lag cache their gauges read.
//!
//! Gauges never do I/O in their callbacks: dispatch and catch-up update an
//! in-memory cache, and the lag sampler refreshes it from the store every
//! `telemetry.metrics.sample_interval_ms`, so writes and runs of other
//! instances count.
use merezha_domain::{Classification, Failure, Metadata};
use merezha_telemetry::{
    Meters, instruments,
    names::{Outcome, attr, dispatch, dispatch::Trigger, metric},
    propagation::StoredContext,
};
use opentelemetry::{
    metrics::{Counter, Histogram, Meter, ObservableCounter, ObservableGauge},
    trace::TraceId,
};
use std::{
    cell::Cell,
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{Span, field::Empty};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// The application's instrumentation scope.
pub const SCOPE: &str = "merezha.application";

/// Codes that mean the host failed its own contract whatever their
/// classification (TE §1.3 "failed"): guest traps and limits, malformed
/// guest replies and broken storage. `validation_failed` is not one: it is
/// the domain's code for a payload the guest refused.
const FAILED: &[&str] = &[
    "trap",
    "fuel_exhausted",
    "memory_limit",
    "output_limit",
    "internal",
    "invalid_response",
    "protocol_version",
    "serialization_failed",
    "storage_invalid",
    "storage_schema",
    "storage_configuration",
    "invalid_event_feed",
];
/// Codes the guest call path produces itself (the runtime's codes and the
/// protocol's): any other code out of a guest call is the guest's own
/// reply, recorded only as the span attribute `merezha.guest.error_code`
/// (TE §1.4 rule 3).
const GUEST_PATH: &[&str] = &[
    "invalid_input",
    "not_found",
    "guest_error",
    "input_limit",
    "output_limit",
    "fuel_exhausted",
    "memory_limit",
    "timeout",
    "trap",
    "overloaded",
    "internal",
    "invalid_response",
    "protocol_version",
    "validation_failed",
    "serialization_failed",
];

/// The dispatch outcome of a failure (TE §1.3): `denied` for
/// authorization, `failed` for host failures and transient ones,
/// `conflict`, otherwise `rejected`. A guest's own reply counts by its
/// classification only.
pub(crate) fn outcome(failure: &Failure, from_guest: bool) -> Outcome {
    let code = failure.code.as_str();
    if !from_guest {
        if matches!(code, "unauthorized" | "unauthenticated") {
            return Outcome::Denied;
        }
        if FAILED.contains(&code) {
            return Outcome::Failed;
        }
    }
    match failure.classification {
        Classification::Permanent => Outcome::Rejected,
        Classification::Conflict => Outcome::Conflict,
        Classification::Transient => Outcome::Failed,
    }
}

/// A failed projection run, and whether the failure is the guest's own
/// reply to `project` (its code then stays off logs and metrics).
#[derive(Debug)]
pub(crate) struct RunFailure {
    pub failure: Failure,
    pub from_guest: bool,
}
impl From<Failure> for RunFailure {
    fn from(failure: Failure) -> Self {
        Self {
            failure,
            from_guest: false,
        }
    }
}
impl From<RunFailure> for Failure {
    fn from(run: RunFailure) -> Self {
        run.failure
    }
}
impl RunFailure {
    /// A failure out of a guest call: the guest's own reply unless the
    /// guest call path produces the code itself.
    pub(crate) fn from_guest_call(failure: Failure) -> Self {
        Self {
            from_guest: from_guest(&failure),
            failure,
        }
    }
    /// The code for logs and `error.type`: `guest_error` for a guest's
    /// own code.
    pub(crate) fn log_code(&self) -> &str {
        if self.from_guest {
            "guest_error"
        } else {
            &self.failure.code
        }
    }
}
/// Whether a failure out of a guest call is the guest's own reply.
fn from_guest(failure: &Failure) -> bool {
    !GUEST_PATH.contains(&failure.code.as_str())
}

/// A configured tag or projection name with its span name, built once from
/// the configuration so dispatch formats nothing.
#[derive(Clone, Debug)]
pub(crate) struct Label {
    name: Arc<str>,
    span: Arc<str>,
}
impl Label {
    fn new(kind: &str, name: &str) -> Self {
        Self {
            name: Arc::from(name),
            span: Arc::from(format!("{kind} {name}")),
        }
    }
}
/// The labels of every configured command, query and projection.
#[derive(Debug)]
struct Labels {
    commands: BTreeMap<String, Label>,
    queries: BTreeMap<String, Label>,
    projections: BTreeMap<String, Label>,
    #[cfg_attr(
        not(feature = "postgres"),
        expect(dead_code, reason = "reactors run on PostgreSQL only")
    )]
    reactors: BTreeMap<String, Label>,
    unknown_command: Label,
    unknown_query: Label,
    unknown_projection: Label,
}
impl Labels {
    fn new(config: &crate::config::Config) -> Self {
        fn build<'a>(kind: &str, names: impl Iterator<Item = &'a str>) -> BTreeMap<String, Label> {
            names
                .map(|name| (name.to_owned(), Label::new(kind, name)))
                .collect()
        }
        Self {
            commands: build("command", config.commands.iter().map(|r| r.tag.as_str())),
            queries: build("query", config.queries.iter().map(|r| r.tag.as_str())),
            projections: build(
                "projection",
                config.projections.iter().map(|p| p.name.as_str()),
            ),
            reactors: build(
                "react",
                config.reactors.iter().flatten().map(|r| r.name.as_str()),
            ),
            unknown_command: Label::new("command", dispatch::UNKNOWN),
            unknown_query: Label::new("query", dispatch::UNKNOWN),
            unknown_projection: Label::new("projection", dispatch::UNKNOWN),
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// What one projection's lag gauges read.
#[derive(Clone, Debug, Default, PartialEq)]
struct ProjectionLag {
    /// `None` until a sample, a catch-up or the open state told us.
    checkpoint: Option<u64>,
    /// `accepted_unix_ms` of the first unprocessed event, when known.
    oldest_ms: Option<u64>,
    warm: Option<bool>,
    rebuild_remaining: Option<u64>,
}
/// The cache behind every lag gauge.
#[derive(Debug, Default)]
pub(crate) struct LagState {
    head: Option<u64>,
    projections: BTreeMap<String, ProjectionLag>,
    /// Reactor lag, keyed like projections; `warm` and `rebuild_remaining`
    /// stay unset, `halted` is the reactor's.
    reactors: BTreeMap<String, ProjectionLag>,
    halted: BTreeMap<String, bool>,
}
/// A read of the store's lag, by the sampler.
pub(crate) struct Sample {
    pub head: u64,
    pub projections: Vec<SampledProjection>,
    /// Reactors as projections, plus whether each is halted.
    pub reactors: Vec<(SampledProjection, bool)>,
}
pub(crate) struct SampledProjection {
    pub name: String,
    pub checkpoint: u64,
    pub oldest_ms: Option<u64>,
    pub warm: Option<bool>,
    pub rebuild_remaining: Option<u64>,
}
impl LagState {
    fn caught_up(&self, lag: &ProjectionLag) -> bool {
        matches!((self.head, lag.checkpoint), (Some(h), Some(c)) if c >= h)
    }
    /// A position this instance saw committed: the head at that moment.
    fn head(&mut self, position: u64) {
        let previous = self.head;
        if previous.is_some_and(|h| position <= h) {
            return;
        }
        self.head = Some(position);
        let now = now_ms();
        for lag in self
            .projections
            .values_mut()
            .chain(self.reactors.values_mut())
        {
            // Events just accepted are the oldest unprocessed ones of a
            // projection or reactor that was caught up.
            if previous.is_some() && lag.checkpoint == previous && lag.oldest_ms.is_none() {
                lag.oldest_ms = Some(now);
            }
        }
    }
    /// A feed read from `position`, accepted at `accepted_ms`, by a run of
    /// `name`: the first unprocessed event.
    fn feed(&mut self, name: &str, position: u64, accepted_ms: u64) {
        let lag = self.projections.entry(name.to_owned()).or_default();
        lag.checkpoint = Some(position.saturating_sub(1).max(lag.checkpoint.unwrap_or(0)));
        lag.oldest_ms = Some(accepted_ms);
        if self.head.is_none_or(|h| h < position) {
            self.head = Some(position);
        }
    }
    /// A published checkpoint.
    fn checkpoint(&mut self, name: &str, checkpoint: u64) {
        let lag = self.projections.entry(name.to_owned()).or_default();
        lag.checkpoint = Some(lag.checkpoint.map_or(checkpoint, |c| c.max(checkpoint)));
        if self.head.is_none_or(|h| h < checkpoint) {
            self.head = Some(checkpoint);
        }
        let lag = &self.projections[name];
        if self.caught_up(lag) {
            self.projections
                .get_mut(name)
                .expect("inserted above")
                .oldest_ms = None;
        }
    }
    /// The sampler's read replaces older knowledge; a local commit or run
    /// that got further meanwhile wins.
    fn apply(&mut self, sample: Sample) {
        if self.head.is_none_or(|h| h < sample.head) {
            self.head = Some(sample.head);
        }
        for row in sample.projections {
            let lag = self.projections.entry(row.name).or_default();
            if lag.checkpoint.is_none_or(|c| c <= row.checkpoint) {
                lag.checkpoint = Some(row.checkpoint);
                lag.oldest_ms = if row.checkpoint < sample.head {
                    row.oldest_ms
                } else {
                    None
                };
            }
            lag.warm = row.warm;
            lag.rebuild_remaining = row.rebuild_remaining;
        }
        for (row, halted) in sample.reactors {
            self.halted.insert(row.name.clone(), halted);
            let lag = self.reactors.entry(row.name).or_default();
            if lag.checkpoint.is_none_or(|c| c <= row.checkpoint) {
                lag.checkpoint = Some(row.checkpoint);
                lag.oldest_ms = if row.checkpoint < sample.head {
                    row.oldest_ms
                } else {
                    None
                };
            }
        }
    }
    /// A reactor's committed checkpoint and halted state.
    #[cfg(feature = "postgres")]
    fn reactor(&mut self, name: &str, checkpoint: u64, halted: bool) {
        self.halted.insert(name.to_owned(), halted);
        if self.head.is_none_or(|h| h < checkpoint) {
            self.head = Some(checkpoint);
        }
        let head = self.head;
        let lag = self.reactors.entry(name.to_owned()).or_default();
        lag.checkpoint = Some(lag.checkpoint.map_or(checkpoint, |c| c.max(checkpoint)));
        if head.is_some_and(|h| lag.checkpoint.is_some_and(|c| c >= h)) {
            lag.oldest_ms = None;
        }
    }
    /// A reactor's feed read from `position`, accepted at `accepted_ms`.
    #[cfg(feature = "postgres")]
    fn reactor_feed(&mut self, name: &str, position: u64, accepted_ms: u64) {
        let lag = self.reactors.entry(name.to_owned()).or_default();
        lag.checkpoint = Some(position.saturating_sub(1).max(lag.checkpoint.unwrap_or(0)));
        lag.oldest_ms = Some(accepted_ms);
        if self.head.is_none_or(|h| h < position) {
            self.head = Some(position);
        }
    }
    /// `(events, age in seconds)` of `lag`, when known.
    fn lag(&self, lag: &ProjectionLag) -> Option<(u64, Option<f64>)> {
        let (head, checkpoint) = (self.head?, lag.checkpoint?);
        let events = head.saturating_sub(checkpoint);
        let age = if events == 0 {
            Some(0.0)
        } else {
            lag.oldest_ms
                .map(|oldest| now_ms().saturating_sub(oldest) as f64 / 1000.0)
        };
        Some((events, age))
    }
}

/// The instruments built from the meter given to
/// [`crate::Application::with_meters`].
struct Metrics {
    command: Histogram<f64>,
    query: Histogram<f64>,
    appended: Counter<u64>,
    replayed: Counter<u64>,
    catch_up: Histogram<f64>,
    processed: Counter<u64>,
    _head: ObservableGauge<u64>,
    _lag_events: ObservableGauge<u64>,
    _lag_age: ObservableGauge<f64>,
    _warm: ObservableGauge<u64>,
    _rebuild: ObservableGauge<u64>,
    _locks: Option<(ObservableGauge<f64>, ObservableCounter<u64>)>,
    #[cfg_attr(
        not(feature = "postgres"),
        expect(dead_code, reason = "reactors run on PostgreSQL only")
    )]
    reactor_batch: Histogram<f64>,
    #[cfg_attr(
        not(feature = "postgres"),
        expect(dead_code, reason = "reactors run on PostgreSQL only")
    )]
    reactor_effects: Counter<u64>,
    #[cfg_attr(
        not(feature = "postgres"),
        expect(dead_code, reason = "reactors run on PostgreSQL only")
    )]
    reactor_skipped: Counter<u64>,
    _reactor_lag_events: ObservableGauge<u64>,
    _reactor_lag_age: ObservableGauge<f64>,
    _reactor_halted: ObservableGauge<u64>,
    _reactor_locks: Option<(ObservableGauge<f64>, ObservableCounter<u64>)>,
}

/// The per-revision lock sources a PostgreSQL store exposes (AP-A3):
/// held lock ages and contended attempts, read from memory.
pub(crate) trait LockSource: Send + Sync {
    /// `(projection, seconds held)`, the longest per projection.
    fn held(&self) -> Vec<(String, f64)>;
    /// `(projection, contended attempts)`, live and rebuild together.
    fn contended(&self) -> Vec<(String, u64)>;
    /// `(reactor, seconds held)` of the `r:` locks held here.
    fn reactors_held(&self) -> Vec<(String, f64)> {
        Vec::new()
    }
    /// `(reactor, contended attempts)`.
    fn reactors_contended(&self) -> Vec<(String, u64)> {
        Vec::new()
    }
}

/// Telemetry state of one application: its instruments, once a meter is
/// given, and the lag cache.
pub(crate) struct Instruments {
    metrics: OnceLock<Metrics>,
    lag: Arc<Mutex<LagState>>,
    labels: Labels,
}
impl Instruments {
    /// `projections` start with `known` checkpoints when the store gave
    /// them at open (memory and SQLite), else unknown until sampled.
    pub(crate) fn new(
        config: &crate::config::Config,
        head: Option<u64>,
        projections: Vec<(String, Option<u64>)>,
    ) -> Self {
        let state = LagState {
            head,
            projections: projections
                .into_iter()
                .map(|(name, checkpoint)| {
                    (
                        name,
                        ProjectionLag {
                            checkpoint,
                            ..ProjectionLag::default()
                        },
                    )
                })
                .collect(),
            ..LagState::default()
        };
        Self {
            metrics: OnceLock::new(),
            lag: Arc::new(Mutex::new(state)),
            labels: Labels::new(config),
        }
    }
    /// Build the instruments once when `meters` export; later calls keep
    /// the first meter. Disabled meters build nothing, so dispatch records
    /// no metric and builds no metric attribute.
    pub(crate) fn install(&self, meters: &Meters, locks: Option<Arc<dyn LockSource>>) {
        if meters.enabled() {
            let meter = meters.meter(SCOPE);
            self.metrics
                .get_or_init(|| Metrics::new(&meter, &self.lag, locks));
        }
    }
    fn lag(&self) -> std::sync::MutexGuard<'_, LagState> {
        self.lag.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub(crate) fn observe_head(&self, position: u64) {
        self.lag().head(position);
    }
    pub(crate) fn observe_feed(&self, name: &str, position: u64, accepted_ms: u64) {
        self.lag().feed(name, position, accepted_ms);
    }
    pub(crate) fn observe_checkpoint(&self, name: &str, checkpoint: u64) {
        self.lag().checkpoint(name, checkpoint);
    }
    pub(crate) fn apply(&self, sample: Sample) {
        self.lag().apply(sample);
    }
    #[cfg(feature = "postgres")]
    pub(crate) fn observe_reactor(&self, name: &str, checkpoint: u64, halted: bool) {
        self.lag().reactor(name, checkpoint, halted);
    }
    #[cfg(feature = "postgres")]
    pub(crate) fn observe_reactor_feed(&self, name: &str, position: u64, accepted_ms: u64) {
        self.lag().reactor_feed(name, position, accepted_ms);
    }
    #[cfg(feature = "postgres")]
    /// One event of `reactor` skipped (`merezha.reactor.skipped`).
    pub(crate) fn reactor_skipped(&self, reactor: &str, reason: merezha_telemetry::names::Reason) {
        if let Some(metrics) = self.metrics.get() {
            metrics.reactor_skipped.add(
                1,
                &[
                    dispatch::reactor_name(self.reactor_label(reactor).name.clone()),
                    attr::reason(reason),
                ],
            );
        }
    }
    #[cfg(feature = "postgres")]
    fn reactor_label(&self, name: &str) -> &Label {
        self.labels
            .reactors
            .get(name)
            .unwrap_or(&self.labels.unknown_projection)
    }
    #[cfg(feature = "postgres")]
    /// The tracker of one batch of configured reactor `name`.
    pub(crate) fn react(&self, name: &str) -> ReactorRun<'_> {
        let label = self.reactor_label(name);
        let span = tracing::info_span!(
            parent: None,
            "react",
            otel.name = &*label.span,
            otel.status_code = Empty,
            merezha.reactor.name = &*label.name,
            merezha.outcome = Empty,
            error.type = Empty,
            merezha.guest.error_code = Empty,
            merezha.events.count = Empty,
            merezha.event.position = Empty,
        );
        ReactorRun {
            instruments: self,
            label,
            span,
            started: Instant::now(),
            links: Vec::new(),
        }
    }
    /// The tracker of one command dispatch of `tag`, recorded as
    /// `_unknown` unless configured.
    pub(crate) fn command(&self, tag: &str, metadata: &Metadata) -> Dispatch<'_> {
        let labels = &self.labels;
        let label = labels.commands.get(tag).unwrap_or(&labels.unknown_command);
        let span = tracing::info_span!(
            "command",
            otel.name = &*label.span,
            otel.status_code = Empty,
            merezha.command.tag = &*label.name,
            merezha.outcome = Empty,
            error.type = Empty,
            merezha.guest.error_code = Empty,
            merezha.message_id = metadata.message_id.as_str(),
            merezha.correlation_id = metadata.correlation_id.as_str(),
            merezha.causation_id = Empty,
            merezha.stream.id = Empty,
            merezha.command.expected_version = Empty,
            merezha.event.position = Empty,
            merezha.events.count = Empty,
        );
        if let Some(causation) = &metadata.causation_id {
            span.record("merezha.causation_id", causation.as_str());
        }
        Dispatch::new(self, Kind::Command, label, span)
    }
    /// The tracker of one query dispatch of `tag`.
    pub(crate) fn query(&self, tag: &str, metadata: &Metadata) -> Dispatch<'_> {
        let labels = &self.labels;
        let label = labels.queries.get(tag).unwrap_or(&labels.unknown_query);
        let span = tracing::info_span!(
            "query",
            otel.name = &*label.span,
            otel.status_code = Empty,
            merezha.query.tag = &*label.name,
            merezha.outcome = Empty,
            error.type = Empty,
            merezha.guest.error_code = Empty,
            merezha.message_id = metadata.message_id.as_str(),
            merezha.correlation_id = metadata.correlation_id.as_str(),
            merezha.causation_id = Empty,
            merezha.event.position = Empty,
        );
        if let Some(causation) = &metadata.causation_id {
            span.record("merezha.causation_id", causation.as_str());
        }
        Dispatch::new(self, Kind::Query, label, span)
    }
    /// The tracker of one catch-up of configured projection `name`.
    pub(crate) fn catch_up(&self, name: &str, trigger: Trigger) -> ProjectionRun<'_> {
        let labels = &self.labels;
        let label = labels
            .projections
            .get(name)
            .unwrap_or(&labels.unknown_projection);
        ProjectionRun {
            instruments: self,
            label,
            trigger,
            span: None,
            started: Instant::now(),
            links: Vec::new(),
            events: 0,
        }
    }
}
impl Metrics {
    fn new(meter: &Meter, lag: &Arc<Mutex<LagState>>, locks: Option<Arc<dyn LockSource>>) -> Self {
        let read = |lag: &Arc<Mutex<LagState>>| {
            let lag = lag.clone();
            move || {
                let state = lag.lock().unwrap_or_else(|e| e.into_inner());
                let head = state.head;
                let rows: Vec<_> = state
                    .projections
                    .iter()
                    .map(|(name, p)| (name.clone(), state.lag(p), p.warm, p.rebuild_remaining))
                    .collect();
                (head, rows)
            }
        };
        let reactors = |lag: &Arc<Mutex<LagState>>| {
            let lag = lag.clone();
            move || {
                let state = lag.lock().unwrap_or_else(|e| e.into_inner());
                state
                    .reactors
                    .iter()
                    .map(|(name, r)| (name.clone(), state.lag(r)))
                    .collect::<Vec<_>>()
            }
        };
        let reactor_events = reactors(lag);
        let reactor_age = reactors(lag);
        let halted = {
            let lag = lag.clone();
            move || {
                let state = lag.lock().unwrap_or_else(|e| e.into_inner());
                state
                    .halted
                    .iter()
                    .map(|(name, halted)| (name.clone(), *halted))
                    .collect::<Vec<_>>()
            }
        };
        let reactor_locks = locks.clone();
        let head = read(lag);
        let events = read(lag);
        let age = read(lag);
        let warm = read(lag);
        let rebuild = read(lag);
        Self {
            command: instruments::histogram(meter, &metric::COMMAND_DURATION),
            query: instruments::histogram(meter, &metric::QUERY_DURATION),
            appended: instruments::counter(meter, &metric::EVENTS_APPENDED),
            replayed: instruments::counter(meter, &metric::COMMAND_RECEIPTS_REPLAYED),
            catch_up: instruments::histogram(meter, &metric::PROJECTION_CATCH_UP_DURATION),
            processed: instruments::counter(meter, &metric::PROJECTION_EVENTS_PROCESSED),
            _head: instruments::u64_gauge(meter, &metric::EVENTS_HEAD, move |o| {
                if let (Some(head), _) = head() {
                    o.observe(head, &[]);
                }
            }),
            _lag_events: instruments::u64_gauge(meter, &metric::PROJECTION_LAG_EVENTS, move |o| {
                for (name, lag, _, _) in events().1 {
                    if let Some((events, _)) = lag {
                        o.observe(events, &[dispatch::projection_name(name.as_str())]);
                    }
                }
            }),
            _lag_age: instruments::f64_gauge(meter, &metric::PROJECTION_LAG_AGE, move |o| {
                for (name, lag, _, _) in age().1 {
                    if let Some((_, Some(age))) = lag {
                        o.observe(age, &[dispatch::projection_name(name.as_str())]);
                    }
                }
            }),
            _warm: instruments::u64_gauge(meter, &metric::PROJECTION_WARM, move |o| {
                for (name, _, warm, _) in warm().1 {
                    if let Some(warm) = warm {
                        o.observe(u64::from(warm), &[dispatch::projection_name(name.as_str())]);
                    }
                }
            }),
            _rebuild: instruments::u64_gauge(
                meter,
                &metric::PROJECTION_REBUILD_REMAINING,
                move |o| {
                    for (name, _, _, remaining) in rebuild().1 {
                        if let Some(remaining) = remaining {
                            o.observe(remaining, &[dispatch::projection_name(name.as_str())]);
                        }
                    }
                },
            ),
            _locks: locks.map(|locks| {
                let held = locks.clone();
                (
                    instruments::f64_gauge(meter, &metric::PROJECTION_LOCK_HELD, move |o| {
                        for (name, seconds) in held.held() {
                            o.observe(seconds, &[dispatch::projection_name(name.as_str())]);
                        }
                    }),
                    instruments::observable_counter(
                        meter,
                        &metric::PROJECTION_LOCK_CONTENDED,
                        move |o| {
                            for (name, attempts) in locks.contended() {
                                o.observe(attempts, &[dispatch::projection_name(name.as_str())]);
                            }
                        },
                    ),
                )
            }),
            reactor_batch: instruments::histogram(meter, &metric::REACTOR_BATCH_DURATION),
            reactor_effects: instruments::counter(meter, &metric::REACTOR_EFFECTS),
            reactor_skipped: instruments::counter(meter, &metric::REACTOR_SKIPPED),
            _reactor_lag_events: instruments::u64_gauge(
                meter,
                &metric::REACTOR_LAG_EVENTS,
                move |o| {
                    for (name, lag) in reactor_events() {
                        if let Some((events, _)) = lag {
                            o.observe(events, &[dispatch::reactor_name(name.as_str())]);
                        }
                    }
                },
            ),
            _reactor_lag_age: instruments::f64_gauge(meter, &metric::REACTOR_LAG_AGE, move |o| {
                for (name, lag) in reactor_age() {
                    if let Some((_, Some(age))) = lag {
                        o.observe(age, &[dispatch::reactor_name(name.as_str())]);
                    }
                }
            }),
            _reactor_halted: instruments::u64_gauge(meter, &metric::REACTOR_HALTED, move |o| {
                for (name, halted) in halted() {
                    o.observe(u64::from(halted), &[dispatch::reactor_name(name.as_str())]);
                }
            }),
            _reactor_locks: reactor_locks.map(|locks| {
                let held = locks.clone();
                (
                    instruments::f64_gauge(meter, &metric::REACTOR_LOCK_HELD, move |o| {
                        for (name, seconds) in held.reactors_held() {
                            o.observe(seconds, &[dispatch::reactor_name(name.as_str())]);
                        }
                    }),
                    instruments::observable_counter(
                        meter,
                        &metric::REACTOR_LOCK_CONTENDED,
                        move |o| {
                            for (name, attempts) in locks.reactors_contended() {
                                o.observe(attempts, &[dispatch::reactor_name(name.as_str())]);
                            }
                        },
                    ),
                )
            }),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Command,
    Query,
}

/// One command or query in flight: its span, its clock and whether its
/// failure came from the guest's own reply.
pub(crate) struct Dispatch<'a> {
    instruments: &'a Instruments,
    kind: Kind,
    label: &'a Label,
    span: Span,
    started: Instant,
    guest: Cell<bool>,
}
impl<'a> Dispatch<'a> {
    fn new(instruments: &'a Instruments, kind: Kind, label: &'a Label, span: Span) -> Self {
        Self {
            instruments,
            kind,
            label,
            span,
            started: Instant::now(),
            guest: Cell::new(false),
        }
    }
    pub(crate) fn span(&self) -> &Span {
        &self.span
    }
    /// A failure out of a guest call: a code the guest path does not
    /// produce itself is the guest's reply, kept as
    /// `merezha.guest.error_code` on this span only.
    pub(crate) fn guest_failed(&self, failure: &Failure) {
        if from_guest(failure) {
            self.guest.set(true);
            self.span
                .record("merezha.guest.error_code", failure.code.as_str());
        }
    }
    pub(crate) fn stream(&self, stream: &str, expected: Option<u64>) {
        self.span.record("merezha.stream.id", stream);
        if let Some(expected) = expected {
            self.span
                .record("merezha.command.expected_version", expected);
        }
    }
    /// End the dispatch: `Ok(replayed)` or the failure.
    pub(crate) fn finish(self, result: Result<Settled, &Failure>) {
        let elapsed = self.started.elapsed();
        let (outcome, code) = match result {
            Ok(settled) => {
                match settled {
                    Settled::Committed { position, events } => {
                        self.span.record("merezha.event.position", position);
                        self.span.record("merezha.events.count", events as u64);
                        if events > 0 {
                            self.instruments.observe_head(position);
                        }
                    }
                    Settled::Replayed { position } => {
                        self.span.record("merezha.event.position", position);
                    }
                    Settled::Answered { checkpoint } => {
                        self.span.record("merezha.event.position", checkpoint);
                    }
                }
                let outcome = match settled {
                    Settled::Committed { .. } => Outcome::Accepted,
                    Settled::Replayed { .. } => Outcome::Replayed,
                    Settled::Answered { .. } => Outcome::Ok,
                };
                (outcome, None)
            }
            Err(failure) => {
                let guest = self.guest.get();
                let code = if guest {
                    "guest_error"
                } else {
                    failure.code.as_str()
                };
                (outcome(failure, guest), Some(code))
            }
        };
        self.span.record("merezha.outcome", outcome.as_str());
        if let Some(code) = code {
            self.span.record("error.type", code);
        }
        if outcome == Outcome::Failed {
            self.span.record("otel.status_code", "error");
        }
        let duration_ms = elapsed.as_millis() as u64;
        let (tag, outcome_text) = (&*self.label.name, outcome.as_str());
        let _entered = self.span.enter();
        match (self.kind, outcome == Outcome::Failed, code) {
            (Kind::Command, true, Some(code)) => merezha_telemetry::event!(
                WARN,
                "command.completed",
                tag,
                outcome = outcome_text,
                code,
                duration_ms
            ),
            (Kind::Command, _, Some(code)) => merezha_telemetry::event!(
                DEBUG,
                "command.completed",
                tag,
                outcome = outcome_text,
                code,
                duration_ms
            ),
            (Kind::Command, _, None) => merezha_telemetry::event!(
                DEBUG,
                "command.completed",
                tag,
                outcome = outcome_text,
                duration_ms
            ),
            (Kind::Query, true, Some(code)) => merezha_telemetry::event!(
                WARN,
                "query.completed",
                tag,
                outcome = outcome_text,
                code,
                duration_ms
            ),
            (Kind::Query, _, Some(code)) => merezha_telemetry::event!(
                DEBUG,
                "query.completed",
                tag,
                outcome = outcome_text,
                code,
                duration_ms
            ),
            (Kind::Query, _, None) => merezha_telemetry::event!(
                DEBUG,
                "query.completed",
                tag,
                outcome = outcome_text,
                duration_ms
            ),
        }
        let Some(metrics) = self.instruments.metrics.get() else {
            return;
        };
        let seconds = elapsed.as_secs_f64();
        match self.kind {
            Kind::Command => {
                let tag = dispatch::command_tag(self.label.name.clone());
                metrics
                    .command
                    .record(seconds, &[tag.clone(), attr::outcome(outcome)]);
                match result {
                    Ok(Settled::Committed { events, .. }) if events > 0 => {
                        metrics.appended.add(events as u64, &[tag])
                    }
                    Ok(Settled::Replayed { .. }) => metrics.replayed.add(1, &[tag]),
                    _ => {}
                }
            }
            Kind::Query => metrics.query.record(
                seconds,
                &[
                    dispatch::query_tag(self.label.name.clone()),
                    attr::outcome(outcome),
                ],
            ),
        }
    }
}
/// How a dispatch ended well.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Settled {
    /// A command appended `events` events up to `position`.
    Committed { position: u64, events: usize },
    /// A command's stored receipt.
    Replayed { position: u64 },
    /// A query answered from this checkpoint.
    Answered { checkpoint: u64 },
}

/// One catch-up (or rebuild) of a projection: a root `projection {name}`
/// span, created with the first batch so runs with no new events leave no
/// span (TE §1.2), linked to at most 8 distinct source traces.
pub(crate) struct ProjectionRun<'a> {
    instruments: &'a Instruments,
    label: &'a Label,
    trigger: Trigger,
    span: Option<Span>,
    started: Instant,
    links: Vec<TraceId>,
    events: u64,
}
impl ProjectionRun<'_> {
    /// The span, created on first use as a root.
    pub(crate) fn span(&mut self) -> &Span {
        let (label, trigger) = (self.label, self.trigger);
        self.span.get_or_insert_with(|| {
            tracing::info_span!(
                parent: None,
                "projection",
                otel.name = &*label.span,
                otel.status_code = Empty,
                merezha.projection.name = &*label.name,
                merezha.projection.trigger = trigger.as_str(),
                merezha.outcome = Empty,
                error.type = Empty,
                merezha.guest.error_code = Empty,
                merezha.events.count = Empty,
                merezha.event.position = Empty,
            )
        })
    }
    /// A batch from the feed: the first event is the oldest unprocessed one,
    /// and each source trace becomes a link (`source_event`), at most 8
    /// distinct traces per span.
    pub(crate) fn batch<'e>(
        &mut self,
        first: Option<(u64, u64)>,
        traces: impl IntoIterator<Item = &'e Option<StoredContext>>,
    ) {
        // A rebuild reads history from the start: not this projection's lag.
        if let Some((position, accepted_ms)) = first
            && self.trigger != Trigger::Rebuild
        {
            self.instruments
                .observe_feed(&self.label.name, position, accepted_ms);
        }
        let span = self.span().clone();
        for context in traces.into_iter().flatten() {
            let context = context.span_context();
            let trace = context.trace_id();
            if !context.is_valid()
                || self.links.contains(&trace)
                || self.links.len() >= dispatch::MAX_SOURCE_LINKS
            {
                continue;
            }
            self.links.push(trace);
            span.add_link_with_attributes(context, vec![dispatch::source_event_link()]);
        }
    }
    /// A batch of `events` processed, and the served checkpoint when it
    /// moved (a rebuild publishes only at its swap).
    pub(crate) fn published(&mut self, events: usize, checkpoint: Option<u64>) {
        self.events += events as u64;
        let Some(checkpoint) = checkpoint else {
            if let Some(metrics) = self.instruments.metrics.get() {
                metrics.processed.add(
                    events as u64,
                    &[dispatch::projection_name(self.label.name.clone())],
                );
            }
            return;
        };
        self.instruments
            .observe_checkpoint(&self.label.name, checkpoint);
        if let Some(metrics) = self.instruments.metrics.get() {
            metrics.processed.add(
                events as u64,
                &[dispatch::projection_name(self.label.name.clone())],
            );
        }
        if let Some(span) = &self.span {
            span.record("merezha.event.position", checkpoint);
        }
    }
    /// End the catch-up. Nothing is recorded when no batch ran.
    /// A guest's own failure code is recorded as `merezha.guest.error_code`
    /// only, with `error.type = guest_error`.
    pub(crate) fn finish(self, result: Result<(), &RunFailure>) {
        let Some(span) = self.span else {
            return;
        };
        let outcome = match result {
            Ok(()) => Outcome::Accepted,
            Err(run) => {
                if run.from_guest {
                    span.record("merezha.guest.error_code", run.failure.code.as_str());
                }
                span.record("error.type", run.log_code());
                outcome(&run.failure, run.from_guest)
            }
        };
        span.record("merezha.outcome", outcome.as_str());
        span.record("merezha.events.count", self.events);
        if outcome == Outcome::Failed {
            span.record("otel.status_code", "error");
        }
        if let Some(metrics) = self.instruments.metrics.get() {
            metrics.catch_up.record(
                self.started.elapsed().as_secs_f64(),
                &[
                    dispatch::projection_name(self.label.name.clone()),
                    dispatch::projection_trigger(self.trigger),
                    attr::outcome(outcome),
                ],
            );
        }
    }
}

#[cfg(feature = "postgres")]
/// One reactor batch with matching events: a root `react {reactor}` span
/// linked to at most 8 distinct source traces, and on finish
/// `merezha.reactor.batch.duration` and `merezha.reactor.effects`.
pub(crate) struct ReactorRun<'a> {
    instruments: &'a Instruments,
    label: &'a Label,
    span: Span,
    started: Instant,
    links: Vec<TraceId>,
}
#[cfg(feature = "postgres")]
impl ReactorRun<'_> {
    pub(crate) fn span(&self) -> &Span {
        &self.span
    }
    /// The batch's source traces become links (`source_event`).
    pub(crate) fn batch<'e>(
        &mut self,
        traces: impl IntoIterator<Item = &'e Option<StoredContext>>,
    ) {
        for context in traces.into_iter().flatten() {
            let context = context.span_context();
            let trace = context.trace_id();
            if !context.is_valid()
                || self.links.contains(&trace)
                || self.links.len() >= dispatch::MAX_SOURCE_LINKS
            {
                continue;
            }
            self.links.push(trace);
            self.span
                .add_link_with_attributes(context, vec![dispatch::source_event_link()]);
        }
    }
    fn record(&self, outcome: Outcome) {
        self.span.record("merezha.outcome", outcome.as_str());
        if let Some(metrics) = self.instruments.metrics.get() {
            metrics.reactor_batch.record(
                self.started.elapsed().as_secs_f64(),
                &[
                    dispatch::reactor_name(self.label.name.clone()),
                    attr::outcome(outcome),
                ],
            );
        }
    }
    /// The batch of `events` committed through `position`, with its
    /// accepted effect types and its rejections.
    pub(crate) fn finish_accepted(
        self,
        events: usize,
        position: u64,
        accepted: &[&'static str],
        rejected: &[&'static str],
    ) {
        self.span.record("merezha.events.count", events as u64);
        self.span.record("merezha.event.position", position);
        self.record(Outcome::Accepted);
        let Some(metrics) = self.instruments.metrics.get() else {
            return;
        };
        let effect = |kind: &str| match kind {
            "notify" => dispatch::ReactorEffect::Notify,
            "webhook" => dispatch::ReactorEffect::Webhook,
            _ => dispatch::ReactorEffect::Command,
        };
        for (kinds, outcome) in [(accepted, Outcome::Accepted), (rejected, Outcome::Rejected)] {
            for kind in kinds {
                metrics.reactor_effects.add(
                    1,
                    &[
                        dispatch::reactor_name(self.label.name.clone()),
                        dispatch::reactor_effect(effect(kind)),
                        attr::outcome(outcome),
                    ],
                );
            }
        }
    }
    /// The batch failed; `halted` when it halted the reactor. A guest's
    /// own code is recorded as `merezha.guest.error_code` only.
    pub(crate) fn finish_failed(self, run: &RunFailure, halted: bool) {
        if run.from_guest {
            self.span
                .record("merezha.guest.error_code", run.failure.code.as_str());
        }
        self.span.record("error.type", run.log_code());
        self.span.record("otel.status_code", "error");
        self.record(if halted {
            Outcome::Halted
        } else {
            Outcome::Failed
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_map_to_the_dispatch_outcomes() {
        let cases = [
            (
                Failure::permanent("validation_failed"),
                false,
                Outcome::Rejected,
            ),
            (
                Failure::permanent("invalid_response"),
                false,
                Outcome::Failed,
            ),
            (
                Failure::permanent("handler_not_found"),
                false,
                Outcome::Rejected,
            ),
            (
                Failure::permanent("payload_limit"),
                false,
                Outcome::Rejected,
            ),
            (Failure::permanent("unauthorized"), false, Outcome::Denied),
            (
                Failure::conflict("concurrency_conflict"),
                false,
                Outcome::Conflict,
            ),
            (Failure::transient("storage_busy"), false, Outcome::Failed),
            (Failure::transient("timeout"), false, Outcome::Failed),
            (Failure::permanent("trap"), false, Outcome::Failed),
            (Failure::permanent("fuel_exhausted"), false, Outcome::Failed),
            (Failure::permanent("memory_limit"), false, Outcome::Failed),
            // A guest's own reply counts by its classification only.
            (Failure::permanent("internal"), true, Outcome::Rejected),
            (Failure::permanent("unauthorized"), true, Outcome::Rejected),
            (Failure::conflict("taken"), true, Outcome::Conflict),
            (Failure::transient("later"), true, Outcome::Failed),
        ];
        for (failure, guest, expected) in cases {
            assert_eq!(outcome(&failure, guest), expected, "{failure:?}");
        }
    }

    #[test]
    fn the_lag_cache_follows_commits_runs_and_samples() {
        let mut state = LagState {
            head: Some(0),
            projections: [(
                "p".to_owned(),
                ProjectionLag {
                    checkpoint: Some(0),
                    ..ProjectionLag::default()
                },
            )]
            .into(),
            ..LagState::default()
        };
        let p = |s: &LagState| s.lag(&s.projections["p"]);
        assert_eq!(p(&state), Some((0, Some(0.0))));
        state.head(3);
        let (events, age) = p(&state).unwrap();
        assert_eq!(events, 3);
        assert!(age.unwrap() >= 0.0);
        state.feed("p", 1, now_ms() - 5_000);
        assert!(p(&state).unwrap().1.unwrap() >= 5.0);
        state.checkpoint("p", 2);
        assert_eq!(p(&state).unwrap().0, 1);
        state.checkpoint("p", 3);
        assert_eq!(p(&state), Some((0, Some(0.0))));
        // Another instance appended and the sampler saw it.
        state.apply(Sample {
            head: 10,
            projections: vec![SampledProjection {
                name: "p".into(),
                checkpoint: 3,
                oldest_ms: Some(now_ms() - 2_000),
                warm: Some(true),
                rebuild_remaining: None,
            }],
            reactors: Vec::new(),
        });
        let (events, age) = p(&state).unwrap();
        assert_eq!(events, 7);
        assert!(age.unwrap() >= 2.0);
        // A stale sample never moves a checkpoint back.
        state.checkpoint("p", 10);
        state.apply(Sample {
            head: 10,
            projections: vec![SampledProjection {
                name: "p".into(),
                checkpoint: 4,
                oldest_ms: Some(1),
                warm: Some(true),
                rebuild_remaining: Some(6),
            }],
            reactors: Vec::new(),
        });
        assert_eq!(p(&state), Some((0, Some(0.0))));
        assert_eq!(state.projections["p"].rebuild_remaining, Some(6));
        // Unknown until told.
        let unknown = LagState::default();
        assert_eq!(unknown.lag(&ProjectionLag::default()), None);
    }

    #[test]
    fn a_rebuild_reading_history_leaves_the_lag_alone() {
        let config = crate::config::Config::parse(
            "version=1\n[[projections]]\nname=\"p\"\nservice=\"view\"\ninitial=0\n",
        )
        .unwrap();
        let instruments = Instruments::new(&config, Some(5), vec![("p".to_owned(), Some(4))]);
        let before = instruments.lag().projections["p"].clone();
        let mut run = instruments.catch_up("p", Trigger::Rebuild);
        run.batch(Some((1, 7)), &[]);
        run.published(3, None);
        assert_eq!(instruments.lag().projections["p"], before);
        run.finish(Ok(()));
        let mut live = instruments.catch_up("p", Trigger::Admin);
        live.batch(Some((5, 7)), &[]);
        assert_eq!(instruments.lag().projections["p"].oldest_ms, Some(7));
    }
}
