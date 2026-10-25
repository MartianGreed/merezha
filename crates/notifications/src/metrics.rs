//! The notification rows of TE §1.3 this slice records (NT §9, TE-T6):
//! requests, deliveries, attempts, attempt duration, latency, render
//! failures and purged data. Attribute values come from closed sets
//! (sources and topics are declared, channels, statuses, reasons, providers
//! and outcomes are enums); ids, addresses and templates never become
//! attributes.
use merezha_platform::ports::Channel;
use merezha_telemetry::{instruments, names::metric};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};
use std::{sync::Arc, time::Duration};

struct Instruments {
    requests: Counter<u64>,
    deliveries: Counter<u64>,
    attempts: Counter<u64>,
    duration: Histogram<f64>,
    latency: Histogram<f64>,
    render_failures: Counter<u64>,
    data_purged: Counter<u64>,
}

/// Cheap to clone; [`Metrics::disabled`] records nothing.
#[derive(Clone, Default)]
pub struct Metrics(Option<Arc<Instruments>>);
impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field("enabled", &self.0.is_some())
            .finish()
    }
}
impl Metrics {
    pub fn disabled() -> Self {
        Self(None)
    }
    pub fn new(meter: &Meter) -> Self {
        Self(Some(Arc::new(Instruments {
            requests: instruments::counter(meter, &metric::NOTIFICATION_REQUESTS),
            deliveries: instruments::counter(meter, &metric::NOTIFICATION_DELIVERIES),
            attempts: instruments::counter(meter, &metric::NOTIFICATION_ATTEMPTS),
            duration: instruments::histogram(meter, &metric::NOTIFICATION_DELIVERY_DURATION),
            latency: instruments::histogram(meter, &metric::NOTIFICATION_LATENCY),
            render_failures: instruments::counter(meter, &metric::NOTIFICATION_RENDER_FAILURES),
            data_purged: instruments::counter(meter, &metric::NOTIFICATION_DATA_PURGED),
        })))
    }
    /// `outcome`: `accepted`, `duplicate` or `invalid`.
    pub fn request(&self, source: &str, topic: &str, outcome: &'static str) {
        if let Some(i) = &self.0 {
            i.requests.add(
                1,
                &[
                    KeyValue::new("notify.source", source.to_owned()),
                    KeyValue::new("notify.topic", topic.to_owned()),
                    KeyValue::new("notify.outcome", outcome),
                ],
            );
        }
    }
    /// A delivery reaching a terminal status or `accepted`.
    pub fn delivery(&self, channel: Channel, topic: &str, status: &'static str, reason: &str) {
        if let Some(i) = &self.0 {
            i.deliveries.add(
                1,
                &[
                    KeyValue::new("notify.channel", channel.as_str()),
                    KeyValue::new("notify.topic", topic.to_owned()),
                    KeyValue::new("notify.status", status),
                    KeyValue::new("notify.reason", reason.to_owned()),
                ],
            );
        }
    }
    /// One provider attempt and how long it took.
    pub fn attempt(
        &self,
        channel: Channel,
        provider: &'static str,
        outcome: &'static str,
        took: Duration,
    ) {
        if let Some(i) = &self.0 {
            let attributes = [
                KeyValue::new("notify.channel", channel.as_str()),
                KeyValue::new("notify.provider", provider),
                KeyValue::new("notify.outcome", outcome),
            ];
            i.attempts.add(1, &attributes);
            i.duration.record(took.as_secs_f64(), &attributes);
        }
    }
    /// From the request until the provider accepted.
    pub fn latency(&self, channel: Channel, took: Duration) {
        if let Some(i) = &self.0 {
            i.latency.record(
                took.as_secs_f64(),
                &[KeyValue::new("notify.channel", channel.as_str())],
            );
        }
    }
    pub fn render_failure(&self, channel: Channel, topic: &str) {
        if let Some(i) = &self.0 {
            i.render_failures.add(
                1,
                &[
                    KeyValue::new("notify.channel", channel.as_str()),
                    KeyValue::new("notify.topic", topic.to_owned()),
                ],
            );
        }
    }
    pub fn data_purged(&self, rows: u64) {
        if let Some(i) = &self.0
            && rows > 0
        {
            i.data_purged.add(rows, &[]);
        }
    }
}
