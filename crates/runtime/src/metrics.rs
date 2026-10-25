//! Guest spans and instruments (TE §1.2, §1.3, §2.4). The runtime only
//! uses the `tracing` and OpenTelemetry APIs: spans reach an exporter
//! through whatever subscriber the host installed, instruments through the
//! host's [`Meters`]. Span names and attribute values are built once per
//! service and pipeline at catalog load; without exported metrics no
//! instrument exists and no metric attribute is built.
use crate::{Error, GuestOperation, manifest::Limits};
use merezha_telemetry::{
    Meters, instruments,
    names::{dispatch, metric},
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, ObservableGauge, UpDownCounter},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tracing::{Span, field::Empty};

/// The runtime's instrumentation scope.
pub const SCOPE: &str = "merezha.runtime";

/// Whether a failed call broke the host's own contract (span status
/// `Error`, TE §1.1): limits, traps, overload, internal failures. Guest
/// replies, bad input and unknown services leave the status unset.
fn host_failure(error: Error) -> bool {
    !matches!(
        error,
        Error::GuestError | Error::InvalidInput | Error::InputLimit | Error::NotFound
    )
}

/// One service's span names (one per operation) and attribute values,
/// built once at catalog load.
pub(crate) struct Labels {
    id: Arc<str>,
    service: KeyValue,
    spans: [Arc<str>; GuestOperation::ALL.len()],
}
impl Labels {
    pub(crate) fn service(id: &str) -> Self {
        let id: Arc<str> = Arc::from(id);
        Self {
            service: dispatch::guest_service(id.clone()),
            spans: GuestOperation::ALL.map(|op| Arc::from(format!("guest {id} {}", op.as_str()))),
            id,
        }
    }
    fn span_name(&self, operation: GuestOperation) -> &str {
        let index = GuestOperation::ALL
            .iter()
            .position(|op| *op == operation)
            .unwrap_or(0);
        &self.spans[index]
    }
}
/// A raw pipeline's id and span name, built once at catalog load.
#[derive(Clone)]
pub(crate) struct PipelineLabels {
    id: Arc<str>,
    span: Arc<str>,
}
impl PipelineLabels {
    pub(crate) fn new(id: &str) -> Self {
        Self {
            id: Arc::from(id),
            span: Arc::from(format!("pipeline {id}")),
        }
    }
}

/// The instruments, when metrics are exported.
struct Instruments {
    duration: Histogram<f64>,
    fuel: Histogram<u64>,
    utilization: Histogram<f64>,
    memory: Histogram<u64>,
    active: UpDownCounter<i64>,
    reloads: Counter<u64>,
    _generation: ObservableGauge<u64>,
}
pub(crate) struct Metrics {
    instruments: Option<Instruments>,
    /// The labels of services the catalog does not declare.
    unknown: Labels,
}
impl Metrics {
    pub(crate) fn new(meters: &Meters, generation: Arc<AtomicU64>) -> Self {
        let instruments = meters.enabled().then(|| {
            let meter = meters.meter(SCOPE);
            Instruments {
                duration: instruments::histogram(&meter, &metric::GUEST_INVOCATION_DURATION),
                fuel: instruments::u64_histogram(&meter, &metric::GUEST_FUEL_CONSUMED),
                utilization: instruments::histogram(&meter, &metric::GUEST_FUEL_UTILIZATION),
                memory: instruments::u64_histogram(&meter, &metric::GUEST_MEMORY_PEAK),
                active: instruments::up_down_counter(&meter, &metric::GUEST_INVOCATIONS_ACTIVE),
                reloads: instruments::counter(&meter, &metric::CATALOG_RELOADS),
                _generation: instruments::u64_gauge(
                    &meter,
                    &metric::CATALOG_GENERATION,
                    move |o| o.observe(generation.load(Ordering::SeqCst), &[]),
                ),
            }
        });
        Self {
            instruments,
            unknown: Labels::service(dispatch::UNKNOWN),
        }
    }
    pub(crate) fn active(&self, delta: i64) {
        if let Some(m) = &self.instruments {
            m.active.add(delta, &[]);
        }
    }
    pub(crate) fn reloaded(&self, ok: bool) {
        use merezha_telemetry::names::{Outcome, attr};
        if let Some(m) = &self.instruments {
            let outcome = if ok {
                Outcome::Ok
            } else {
                Outcome::InvalidManifest
            };
            m.reloads.add(1, &[attr::outcome(outcome)]);
        }
    }
    /// A call refused before it ran (`not_found`, `overloaded`): a span
    /// that ends at once and a duration of zero, so every outcome counts.
    /// Without `labels` (an unknown service) it records `_unknown`: raw ids
    /// come from request paths and never reach a span or a metric.
    pub(crate) fn refused(
        &self,
        labels: Option<&Labels>,
        generation: u64,
        operation: GuestOperation,
        error: Error,
    ) {
        let labels = labels.unwrap_or(&self.unknown);
        let span = guest_span(labels, operation, generation);
        settle_span(&span, Err(error));
        if let Some(m) = &self.instruments {
            m.duration
                .record(0.0, &series(labels, operation, error.code()));
        }
    }
    /// The span and the bookkeeping of one guest call with `fuel` left.
    pub(crate) fn call<'a>(
        &'a self,
        labels: &'a Labels,
        operation: GuestOperation,
        generation: u64,
        limits: &Limits,
        fuel: u64,
        input_bytes: usize,
    ) -> Call<'a> {
        let span = guest_span(labels, operation, generation);
        span.record(dispatch::GUEST_FUEL_LIMIT, fuel);
        span.record(dispatch::GUEST_MEMORY_LIMIT, limits.memory_bytes as u64);
        span.record(dispatch::GUEST_INPUT_BYTES, input_bytes as u64);
        Call {
            metrics: self,
            span,
            labels,
            operation,
            started: Instant::now(),
            fuel,
            ran: None,
        }
    }
}

fn series(labels: &Labels, operation: GuestOperation, outcome: &'static str) -> [KeyValue; 3] {
    [
        labels.service.clone(),
        dispatch::guest_operation(operation),
        dispatch::guest_outcome(outcome),
    ]
}

/// `guest {service} {operation}`, a child of the current span (the
/// dispatch, HTTP or pipeline span).
fn guest_span(labels: &Labels, operation: GuestOperation, generation: u64) -> Span {
    tracing::info_span!(
        "guest",
        otel.name = labels.span_name(operation),
        otel.status_code = Empty,
        merezha.guest.service = &*labels.id,
        merezha.guest.operation = operation.as_str(),
        merezha.guest.outcome = Empty,
        merezha.catalog.generation = generation,
        merezha.guest.fuel.limit = Empty,
        merezha.guest.fuel.consumed = Empty,
        merezha.guest.memory.limit = Empty,
        merezha.guest.memory.peak = Empty,
        merezha.guest.input.bytes = Empty,
        merezha.guest.output.bytes = Empty,
    )
}
/// `pipeline {id}`: the parent of a raw pipeline's step spans.
pub(crate) fn pipeline_span(labels: &PipelineLabels, generation: u64) -> Span {
    tracing::info_span!(
        "pipeline",
        otel.name = &*labels.span,
        otel.status_code = Empty,
        merezha.guest.pipeline = &*labels.id,
        merezha.guest.outcome = Empty,
        merezha.catalog.generation = generation,
    )
}
/// Record the outcome, and status `Error` for host failures.
pub(crate) fn settle_span(span: &Span, result: Result<(), Error>) {
    if let Err(error) = result
        && host_failure(error)
    {
        span.record("otel.status_code", "error");
    }
    let outcome = result.map_or_else(Error::code, |()| "ok");
    span.record("merezha.guest.outcome", outcome);
}

/// One guest call in flight.
pub(crate) struct Call<'a> {
    metrics: &'a Metrics,
    span: Span,
    labels: &'a Labels,
    operation: GuestOperation,
    started: Instant,
    fuel: u64,
    /// Fuel consumed and peak memory, once a store existed.
    ran: Option<(u64, usize)>,
}
impl Call<'_> {
    pub(crate) fn span(&self) -> &Span {
        &self.span
    }
    pub(crate) fn ran(&mut self, consumed: u64, peak_memory: usize) {
        self.ran = Some((consumed, peak_memory));
    }
    pub(crate) fn consumed(&self) -> u64 {
        self.ran.map_or(0, |(consumed, _)| consumed)
    }
    /// End the call with the output length or the failure.
    pub(crate) fn finish(self, result: Result<usize, Error>) {
        let span = &self.span;
        if let Ok(bytes) = result {
            span.record(dispatch::GUEST_OUTPUT_BYTES, bytes as u64);
        }
        settle_span(span, result.map(|_| ()));
        if let Some((consumed, peak)) = self.ran {
            span.record(dispatch::GUEST_FUEL_CONSUMED, consumed);
            span.record(dispatch::GUEST_MEMORY_PEAK, peak as u64);
        }
        let Some(m) = &self.metrics.instruments else {
            return;
        };
        let outcome = result.map_or_else(Error::code, |_| "ok");
        m.duration.record(
            self.started.elapsed().as_secs_f64(),
            &series(self.labels, self.operation, outcome),
        );
        if let Some((consumed, peak)) = self.ran {
            let call = [
                self.labels.service.clone(),
                dispatch::guest_operation(self.operation),
            ];
            m.fuel.record(consumed, &call);
            if self.fuel > 0 {
                m.utilization
                    .record(consumed as f64 / self.fuel as f64, &call);
            }
            m.memory
                .record(peak as u64, std::slice::from_ref(&self.labels.service));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_host_failures_set_error_status_and_every_code_is_an_outcome() {
        use Error::*;
        for (error, failure) in [
            (InvalidInput, false),
            (NotFound, false),
            (GuestError, false),
            (InputLimit, false),
            (OutputLimit, true),
            (FuelExhausted, true),
            (MemoryLimit, true),
            (Timeout, true),
            (Trap, true),
            (Overloaded, true),
            (Internal, true),
        ] {
            assert_eq!(host_failure(error), failure, "{error}");
            assert!(dispatch::GUEST_OUTCOMES.contains(&error.code()), "{error}");
        }
    }
}
