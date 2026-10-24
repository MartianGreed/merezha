//! Catalog instruments from an OpenTelemetry `Meter` (TE §0, §1.3).
//!
//! Libraries take a [`Meter`] (or the host's [`crate::Meters`]) and build their
//! instruments once, from the [`names::metric`](mod@crate::names::metric) rows,
//! so name, unit and histogram boundaries always come from the catalog.
//! Only the `opentelemetry` API is used: without the `pipeline` feature
//! every instrument is the API's no-op.
use crate::names::{Instrument, Metric};
use opentelemetry::metrics::{
    AsyncInstrument, Counter, Histogram, Meter, MeterProvider, ObservableCounter, ObservableGauge,
    UpDownCounter, noop::NoopMeterProvider,
};

/// A meter that records nothing, the default of every library builder.
pub fn noop_meter(module: &'static str) -> Meter {
    NoopMeterProvider::new().meter(module)
}

fn check(metric: &Metric, instrument: Instrument) {
    debug_assert_eq!(
        metric.instrument, instrument,
        "{} is not a {instrument:?}",
        metric.name
    );
}

/// The histogram of `metric` in seconds or another `f64` unit, with the
/// catalog boundaries.
pub fn histogram(meter: &Meter, metric: &Metric) -> Histogram<f64> {
    check(metric, Instrument::Histogram);
    meter
        .f64_histogram(metric.name)
        .with_unit(metric.unit)
        .with_boundaries(metric.buckets.to_vec())
        .build()
}
/// The histogram of `metric` for integral values (bytes, fuel).
pub fn u64_histogram(meter: &Meter, metric: &Metric) -> Histogram<u64> {
    check(metric, Instrument::Histogram);
    meter
        .u64_histogram(metric.name)
        .with_unit(metric.unit)
        .with_boundaries(metric.buckets.to_vec())
        .build()
}
pub fn counter(meter: &Meter, metric: &Metric) -> Counter<u64> {
    check(metric, Instrument::Counter);
    meter
        .u64_counter(metric.name)
        .with_unit(metric.unit)
        .build()
}
pub fn up_down_counter(meter: &Meter, metric: &Metric) -> UpDownCounter<i64> {
    check(metric, Instrument::UpDownCounter);
    meter
        .i64_up_down_counter(metric.name)
        .with_unit(metric.unit)
        .build()
}
/// A counter whose cumulative value a callback reads from a source that
/// already counts (such as a lock-attempt counter). Callbacks must only
/// read in-memory state: no I/O, no locks held across work.
pub fn observable_counter<F>(meter: &Meter, metric: &Metric, callback: F) -> ObservableCounter<u64>
where
    F: Fn(&dyn AsyncInstrument<u64>) + Send + Sync + 'static,
{
    check(metric, Instrument::Counter);
    meter
        .u64_observable_counter(metric.name)
        .with_unit(metric.unit)
        .with_callback(callback)
        .build()
}
/// An observable gauge of integral values. Callbacks read cached state only.
pub fn u64_gauge<F>(meter: &Meter, metric: &Metric, callback: F) -> ObservableGauge<u64>
where
    F: Fn(&dyn AsyncInstrument<u64>) + Send + Sync + 'static,
{
    check(metric, Instrument::Gauge);
    meter
        .u64_observable_gauge(metric.name)
        .with_unit(metric.unit)
        .with_callback(callback)
        .build()
}
/// An observable gauge of `f64` values (ages in seconds). Callbacks read
/// cached state only.
pub fn f64_gauge<F>(meter: &Meter, metric: &Metric, callback: F) -> ObservableGauge<f64>
where
    F: Fn(&dyn AsyncInstrument<f64>) + Send + Sync + 'static,
{
    check(metric, Instrument::Gauge);
    meter
        .f64_observable_gauge(metric.name)
        .with_unit(metric.unit)
        .with_callback(callback)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::metric;

    #[test]
    fn a_noop_meter_builds_every_kind() {
        let meter = noop_meter("merezha.test");
        histogram(&meter, &metric::COMMAND_DURATION).record(0.1, &[]);
        u64_histogram(&meter, &metric::GUEST_FUEL_CONSUMED).record(10, &[]);
        counter(&meter, &metric::EVENTS_APPENDED).add(1, &[]);
        up_down_counter(&meter, &metric::GUEST_INVOCATIONS_ACTIVE).add(1, &[]);
        let _gauge = u64_gauge(&meter, &metric::EVENTS_HEAD, |o| o.observe(1, &[]));
        let _age = f64_gauge(&meter, &metric::PROJECTION_LAG_AGE, |o| o.observe(0.0, &[]));
        let _locks = observable_counter(&meter, &metric::PROJECTION_LOCK_CONTENDED, |o| {
            o.observe(0, &[])
        });
    }
}
