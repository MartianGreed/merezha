//! The identity rows of TE §1.3 (TE-T6 for ID PR 1). Instruments come from
//! the catalog; attribute values are closed sets (methods, outcomes,
//! purposes, policy names). No user, session, address or token ever
//! becomes an attribute.
use merezha_telemetry::names::metric;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

struct Instruments {
    login_attempts: Counter<u64>,
    authentications: Counter<u64>,
    sessions_created: Counter<u64>,
    sessions_revoked: Counter<u64>,
    tokens_issued: Counter<u64>,
    rate_limited: Counter<u64>,
    hash_duration: Histogram<f64>,
    hash_queue_wait: Histogram<f64>,
    audit_failures: Counter<u64>,
    // Keeps the observable gauge registered.
    _active: opentelemetry::metrics::ObservableGauge<u64>,
}

/// Cheap to clone; a disabled set records nothing.
#[derive(Clone, Default)]
pub struct Metrics {
    instruments: Option<Arc<Instruments>>,
    /// Live sessions as the cleanup job last counted them.
    active: Arc<AtomicU64>,
}
impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field("enabled", &self.instruments.is_some())
            .finish()
    }
}

fn counter(meter: &Meter, row: merezha_telemetry::names::Metric) -> Counter<u64> {
    meter.u64_counter(row.name).with_unit(row.unit).build()
}
fn histogram(meter: &Meter, row: merezha_telemetry::names::Metric) -> Histogram<f64> {
    meter
        .f64_histogram(row.name)
        .with_unit(row.unit)
        .with_boundaries(row.buckets.to_vec())
        .build()
}

impl Metrics {
    pub fn disabled() -> Self {
        Self::default()
    }
    /// The instruments on `meter`.
    pub fn new(meter: &Meter) -> Self {
        let active = Arc::new(AtomicU64::new(0));
        let observed = active.clone();
        let gauge = meter
            .u64_observable_gauge(metric::IDENTITY_SESSIONS_ACTIVE.name)
            .with_unit(metric::IDENTITY_SESSIONS_ACTIVE.unit)
            .with_callback(move |observer| observer.observe(observed.load(Ordering::Relaxed), &[]))
            .build();
        Self {
            instruments: Some(Arc::new(Instruments {
                login_attempts: counter(meter, metric::IDENTITY_LOGIN_ATTEMPTS),
                authentications: counter(meter, metric::IDENTITY_AUTHENTICATIONS),
                sessions_created: counter(meter, metric::IDENTITY_SESSIONS_CREATED),
                sessions_revoked: counter(meter, metric::IDENTITY_SESSIONS_REVOKED),
                tokens_issued: counter(meter, metric::IDENTITY_TOKENS_ISSUED),
                rate_limited: counter(meter, metric::IDENTITY_RATE_LIMITED),
                hash_duration: histogram(meter, metric::IDENTITY_PASSWORD_HASH_DURATION),
                hash_queue_wait: histogram(meter, metric::IDENTITY_PASSWORD_HASH_QUEUE_WAIT),
                audit_failures: counter(meter, metric::IDENTITY_AUDIT_WRITE_FAILURES),
                _active: gauge,
            })),
            active,
        }
    }
    /// `method`: `password`, `magic_link`; `outcome`: TE's closed set.
    pub fn login(&self, method: &'static str, outcome: &'static str) {
        if let Some(i) = &self.instruments {
            i.login_attempts.add(
                1,
                &[
                    KeyValue::new("identity.method", method),
                    KeyValue::new("identity.outcome", outcome),
                    KeyValue::new("identity.provider", "local"),
                ],
            );
        }
    }
    /// `credential`: `session`, `api_key`, `none`; `outcome`: `ok`,
    /// `invalid`, `expired`, `revoked`.
    pub fn authentication(&self, credential: &'static str, outcome: &'static str) {
        if let Some(i) = &self.instruments {
            i.authentications.add(
                1,
                &[
                    KeyValue::new("identity.credential", credential),
                    KeyValue::new("identity.outcome", outcome),
                ],
            );
        }
    }
    pub fn session_created(&self, method: &'static str) {
        if let Some(i) = &self.instruments {
            i.sessions_created
                .add(1, &[KeyValue::new("identity.method", method)]);
        }
    }
    /// `reason`: `logout`, `expired`, `admin`, `password_change`.
    pub fn sessions_revoked(&self, reason: &'static str, count: u64) {
        if let Some(i) = &self.instruments
            && count > 0
        {
            i.sessions_revoked
                .add(count, &[KeyValue::new("identity.reason", reason)]);
        }
    }
    pub fn token_issued(&self, purpose: &'static str) {
        if let Some(i) = &self.instruments {
            i.tokens_issued
                .add(1, &[KeyValue::new("identity.purpose", purpose)]);
        }
    }
    pub fn rate_limited(&self, policy: &'static str) {
        if let Some(i) = &self.instruments {
            i.rate_limited
                .add(1, &[KeyValue::new("identity.policy", policy)]);
        }
    }
    pub fn hash_duration(&self, took: Duration) {
        if let Some(i) = &self.instruments {
            i.hash_duration.record(took.as_secs_f64(), &[]);
        }
    }
    pub fn hash_queue_wait(&self, took: Duration) {
        if let Some(i) = &self.instruments {
            i.hash_queue_wait.record(took.as_secs_f64(), &[]);
        }
    }
    pub fn audit_failure(&self) {
        if let Some(i) = &self.instruments {
            i.audit_failures.add(1, &[]);
        }
    }
    /// Set by the cleanup job.
    pub fn set_active_sessions(&self, count: u64) {
        self.active.store(count, Ordering::Relaxed);
    }
}
