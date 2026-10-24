//! Exporter accounting (TE §3 "Bounded queues, exact drop counters") and
//! the `telemetry` section of `/v1/admin/status` (TE §5; the route is T2).
//!
//! Per signal, at every quiescent point:
//! `exported + failed + dropped + pending == emitted`, where `dropped`
//! counts refusals for a full queue and items left at shutdown. The SDK
//! counts its drops privately, so the guarded processors admit items
//! against `pending` (queued plus in flight) and the counting exporters
//! settle each finished batch.
use crate::{
    config::{Sampler, TelemetryConfig},
    names::TelemetrySignal,
};
use opentelemetry_sdk::error::OTelSdkError;
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

/// At most one `telemetry.export_failed` warning per signal per window.
pub const WARNING_INTERVAL_MS: u64 = 60_000;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// The stable code of an export failure, never its `Display` text.
pub fn failure_code(error: &OTelSdkError) -> &'static str {
    match error {
        OTelSdkError::Timeout(_) => "timeout",
        OTelSdkError::AlreadyShutdown => "shutdown",
        _ => "export_failed",
    }
}
const CODES: [&str; 5] = ["none", "ok", "timeout", "shutdown", "export_failed"];
fn code_index(code: &str) -> u8 {
    CODES.iter().position(|c| *c == code).unwrap_or(4) as u8
}

/// Whether a guarded processor may enqueue an item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    Accepted,
    QueueFull,
    Closed,
}

/// Counters of one signal.
#[derive(Debug)]
pub struct SignalAccount {
    signal: TelemetrySignal,
    max_queue: u64,
    emitted: AtomicU64,
    exported: AtomicU64,
    failed: AtomicU64,
    dropped_queue_full: AtomicU64,
    dropped_shutdown: AtomicU64,
    pending: AtomicU64,
    attributes_dropped: AtomicU64,
    last_success_ms: AtomicU64,
    last_code: AtomicU8,
    last_warning_ms: AtomicU64,
    closed: AtomicBool,
    /// Held while settling a batch or finalizing, so a batch that finishes
    /// after shutdown is not counted twice. `true` once finalized.
    finalized: Mutex<bool>,
}

/// A copy of a signal's counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SignalCounts {
    pub emitted: u64,
    pub exported: u64,
    pub failed: u64,
    pub dropped_queue_full: u64,
    pub dropped_shutdown: u64,
    pub pending: u64,
    pub attributes_dropped: u64,
    /// Unix milliseconds of the last successful export, 0 when none.
    pub last_success_unix_ms: u64,
    pub last_code: &'static str,
}
impl SignalCounts {
    pub fn dropped(&self) -> u64 {
        self.dropped_queue_full + self.dropped_shutdown
    }
    /// `exported + failed + dropped + pending == emitted`.
    pub fn balanced(&self) -> bool {
        self.exported + self.failed + self.dropped() + self.pending == self.emitted
    }
}

impl SignalAccount {
    pub fn new(signal: TelemetrySignal, max_queue: u32) -> Self {
        Self {
            signal,
            max_queue: u64::from(max_queue),
            emitted: AtomicU64::new(0),
            exported: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            dropped_queue_full: AtomicU64::new(0),
            dropped_shutdown: AtomicU64::new(0),
            pending: AtomicU64::new(0),
            attributes_dropped: AtomicU64::new(0),
            last_success_ms: AtomicU64::new(0),
            last_code: AtomicU8::new(0),
            last_warning_ms: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            finalized: Mutex::new(false),
        }
    }
    pub fn signal(&self) -> TelemetrySignal {
        self.signal
    }
    /// Count one emitted item and admit it unless the queue is full or the
    /// pipeline is shutting down. Never blocks.
    pub fn admit(&self) -> Admission {
        self.emitted.fetch_add(1, Ordering::Relaxed);
        if self.closed.load(Ordering::Acquire) {
            self.dropped_shutdown.fetch_add(1, Ordering::Relaxed);
            return Admission::Closed;
        }
        if self.pending.fetch_add(1, Ordering::AcqRel) >= self.max_queue {
            self.pending.fetch_sub(1, Ordering::AcqRel);
            self.dropped_queue_full.fetch_add(1, Ordering::Relaxed);
            return Admission::QueueFull;
        }
        Admission::Accepted
    }
    /// Settle a finished batch of `items` admitted items. Returns whether a
    /// failure warning is due (at most one per [`WARNING_INTERVAL_MS`]).
    pub fn settle(&self, items: u64, result: Result<(), &'static str>) -> bool {
        let Ok(finalized) = self.finalized.lock() else {
            return false;
        };
        if *finalized {
            // Already counted as dropped at shutdown.
            return false;
        }
        let _ = self
            .pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |p| {
                Some(p.saturating_sub(items))
            });
        drop(finalized);
        self.record(items, result)
    }
    /// Count an export without a queue (metrics): emitted and settled at once.
    pub fn record_export(&self, items: u64, result: Result<(), &'static str>) -> bool {
        if self.finalized.lock().map(|g| *g).unwrap_or(true) {
            return false;
        }
        self.emitted.fetch_add(items, Ordering::Relaxed);
        self.record(items, result)
    }
    fn record(&self, items: u64, result: Result<(), &'static str>) -> bool {
        match result {
            Ok(()) => {
                self.exported.fetch_add(items, Ordering::Relaxed);
                self.last_success_ms.store(now_ms(), Ordering::Relaxed);
                self.last_code.store(1, Ordering::Relaxed);
                false
            }
            Err(code) => {
                self.failed.fetch_add(items, Ordering::Relaxed);
                self.last_code.store(code_index(code), Ordering::Relaxed);
                self.warning_due(now_ms())
            }
        }
    }
    fn warning_due(&self, now: u64) -> bool {
        let last = self.last_warning_ms.load(Ordering::Acquire);
        (last == 0 || now.saturating_sub(last) >= WARNING_INTERVAL_MS)
            && self
                .last_warning_ms
                .compare_exchange(last, now.max(1), Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }
    /// Count attributes the allowlist removed.
    pub fn attributes_dropped(&self, count: u64) {
        if count > 0 {
            self.attributes_dropped.fetch_add(count, Ordering::Relaxed);
        }
    }
    /// Refuse new items from now on (shutdown started).
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
    /// After the shutdown flush: whatever is still pending counts as
    /// `dropped{reason=shutdown}`, and late batch results are ignored.
    pub fn finalize(&self) {
        self.close();
        if let Ok(mut finalized) = self.finalized.lock()
            && !*finalized
        {
            *finalized = true;
            let left = self.pending.swap(0, Ordering::AcqRel);
            self.dropped_shutdown.fetch_add(left, Ordering::Relaxed);
        }
    }
    pub fn counts(&self) -> SignalCounts {
        SignalCounts {
            emitted: self.emitted.load(Ordering::Acquire),
            exported: self.exported.load(Ordering::Acquire),
            failed: self.failed.load(Ordering::Acquire),
            dropped_queue_full: self.dropped_queue_full.load(Ordering::Acquire),
            dropped_shutdown: self.dropped_shutdown.load(Ordering::Acquire),
            pending: self.pending.load(Ordering::Acquire),
            attributes_dropped: self.attributes_dropped.load(Ordering::Acquire),
            last_success_unix_ms: self.last_success_ms.load(Ordering::Acquire),
            last_code: CODES[usize::from(self.last_code.load(Ordering::Acquire)).min(4)],
        }
    }
}

/// The `telemetry` section of `/v1/admin/status`: codes, counts,
/// configured names and timestamps only. The endpoint shows scheme, host
/// and port; header values never appear.
#[derive(Clone, Debug, PartialEq)]
pub struct TelemetryStatus {
    pub enabled: bool,
    pub protocol: &'static str,
    pub endpoint: String,
    pub sampling_ratio: f64,
    pub signals: BTreeMap<&'static str, SignalCounts>,
    /// Lines the lossy stderr writer dropped.
    pub stderr_dropped: u64,
}
impl TelemetryStatus {
    pub(crate) fn summary(config: &TelemetryConfig) -> (bool, &'static str, String, f64) {
        let ratio = match config.traces.sampler {
            Sampler::AlwaysOff => 0.0,
            Sampler::ParentBasedRatio => config.traces.ratio,
        };
        (
            !config.signals.is_empty(),
            config.protocol.as_str(),
            origin(&config.endpoint),
            ratio,
        )
    }
    /// The JSON document embedded in the status response.
    pub fn to_json(&self) -> serde_json::Value {
        let signals: serde_json::Map<String, serde_json::Value> = self
            .signals
            .iter()
            .map(|(name, c)| {
                let last_success = (c.last_success_unix_ms > 0)
                    .then(|| {
                        jiff::Timestamp::from_millisecond(c.last_success_unix_ms as i64)
                            .ok()
                            .map(|t| t.to_string())
                    })
                    .flatten();
                (
                    (*name).to_owned(),
                    serde_json::json!({
                        "exported": c.exported,
                        "failed": c.failed,
                        "dropped": c.dropped(),
                        "pending": c.pending,
                        "attributes_dropped": c.attributes_dropped,
                        "last_success_at": last_success,
                        "last_code": c.last_code,
                    }),
                )
            })
            .collect();
        serde_json::json!({
            "enabled": self.enabled,
            "protocol": self.protocol,
            "endpoint": self.endpoint,
            "sampling_ratio": self.sampling_ratio,
            "signals": signals,
            "stderr": {"dropped": self.stderr_dropped},
        })
    }
}

/// `scheme://host:port` of an endpoint, nothing else.
pub fn origin(endpoint: &str) -> String {
    match reqwest::Url::parse(endpoint) {
        Ok(url) => format!(
            "{}://{}:{}",
            url.scheme(),
            url.host_str().unwrap_or_default(),
            url.port_or_known_default().unwrap_or_default()
        ),
        Err(_) => "invalid".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounting_balances_through_refusal_settlement_and_shutdown() {
        let account = SignalAccount::new(TelemetrySignal::Traces, 3);
        let admitted: Vec<_> = (0..5).map(|_| account.admit()).collect();
        assert_eq!(
            admitted,
            [
                Admission::Accepted,
                Admission::Accepted,
                Admission::Accepted,
                Admission::QueueFull,
                Admission::QueueFull
            ]
        );
        assert!(account.counts().balanced());
        assert!(!account.settle(1, Ok(())));
        assert!(account.settle(1, Err("timeout")), "first failure warns");
        assert!(
            !account.settle(0, Err("timeout")),
            "warnings are rate limited"
        );
        assert_eq!(account.admit(), Admission::Accepted);
        account.finalize();
        // A batch finishing after shutdown was already counted as dropped.
        account.settle(2, Ok(()));
        assert_eq!(account.admit(), Admission::Closed);
        let c = account.counts();
        assert_eq!(
            (
                c.emitted,
                c.exported,
                c.failed,
                c.dropped_queue_full,
                c.dropped_shutdown,
                c.pending
            ),
            (7, 1, 1, 2, 3, 0)
        );
        assert!(c.balanced());
        assert_eq!(c.last_code, "timeout");
    }

    #[test]
    fn status_shows_only_the_endpoint_origin() {
        let config = TelemetryConfig {
            endpoint: "https://otlp.example.com/v1/otlp".into(),
            signals: crate::config::Signal::ALL.to_vec(),
            ..TelemetryConfig::default()
        };
        let (enabled, protocol, endpoint, ratio) = TelemetryStatus::summary(&config);
        assert!(enabled);
        assert_eq!(protocol, "http/protobuf");
        assert_eq!(endpoint, "https://otlp.example.com:443");
        assert_eq!(ratio, 1.0);
        let status = TelemetryStatus {
            enabled,
            protocol,
            endpoint,
            sampling_ratio: ratio,
            signals: BTreeMap::from([("traces", SignalCounts::default())]),
            stderr_dropped: 0,
        };
        let json = status.to_json();
        assert_eq!(
            json["signals"]["traces"]["last_success_at"],
            serde_json::Value::Null
        );
        assert_eq!(json["endpoint"], "https://otlp.example.com:443");
    }
}
