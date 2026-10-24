//! The meter source the host hands to libraries (TE §0): no OpenTelemetry
//! globals, so each library takes its `Meter` from the host's provider, or a
//! no-op meter when metrics are not exported.
//!
//! [`Meters`] is the one meter API of the workspace: the binary gets it from
//! `Telemetry::meters()` and passes it to the host (`HostOptions::meters`),
//! which derives each library's meter from it by instrumentation scope
//! (`merezha.edge`, `merezha-postgres`, and the blocks' own).
use opentelemetry::metrics::{Meter, MeterProvider, noop::NoopMeterProvider};
use std::sync::Arc;

/// A cloneable handle on the host's meter provider. [`Meters::default`] (or
/// [`Meters::noop`]) records nothing; [`Meters::new`] wraps a provider.
///
/// ```
/// let meters = merezha_telemetry::Meters::default();
/// assert!(!meters.enabled());
/// let meter = meters.meter("merezha-postgres");
/// meter.u64_counter("merezha.example").build().add(1, &[]);
/// assert_eq!(format!("{meters:?}"), "Meters { enabled: false }");
/// ```
#[derive(Clone, Default)]
pub struct Meters(Option<Arc<dyn MeterProvider + Send + Sync>>);
impl Meters {
    /// Meters from `provider`, enabled. `Telemetry::meters()` passes the
    /// SDK provider when metrics are exported and [`Meters::noop`] otherwise.
    pub fn new(provider: impl MeterProvider + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(provider)))
    }
    /// The no-op meters, the same as [`Meters::default`].
    pub fn noop() -> Self {
        Self(None)
    }
    /// Whether these meters record: built from a provider rather than
    /// no-op. Libraries can skip work that only feeds instruments.
    pub fn enabled(&self) -> bool {
        self.0.is_some()
    }
    /// The meter of instrumentation scope `scope`, the library's name.
    pub fn meter(&self, scope: &'static str) -> Meter {
        match &self.0 {
            Some(provider) => provider.meter(scope),
            None => NoopMeterProvider::new().meter(scope),
        }
    }
}
impl std::fmt::Debug for Meters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Meters")
            .field("enabled", &self.enabled())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_and_provider_meters_report_whether_they_record() {
        assert!(!Meters::noop().enabled());
        assert!(!Meters::default().enabled());
        let meters = Meters::new(NoopMeterProvider::new());
        assert!(meters.enabled());
        assert_eq!(format!("{meters:?}"), "Meters { enabled: true }");
    }
}
