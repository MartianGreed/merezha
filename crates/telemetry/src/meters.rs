//! The meter source the host hands to libraries (TE §0): no OpenTelemetry
//! globals, so each library takes its `Meter` from the host's provider, or a
//! no-op meter when metrics are not exported.
use opentelemetry::metrics::{Meter, MeterProvider, noop::NoopMeterProvider};
use std::sync::Arc;

/// A cloneable handle on the host's meter provider. [`Meters::default`] is
/// the no-op provider every library uses unless the host passes its own.
///
/// ```
/// let meters = merezha_telemetry::Meters::default();
/// let meter = meters.meter("merezha-postgres");
/// meter.u64_counter("merezha.example").build().add(1, &[]);
/// ```
#[derive(Clone, Default)]
pub struct Meters(Option<Arc<dyn MeterProvider + Send + Sync>>);
impl Meters {
    /// Meters from `provider` (the binary passes the SDK provider).
    pub fn new(provider: impl MeterProvider + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(provider)))
    }
    /// The meter of instrumentation scope `scope`, the library's crate name.
    pub fn meter(&self, scope: &'static str) -> Meter {
        match &self.0 {
            Some(provider) => provider.meter(scope),
            None => NoopMeterProvider::new().meter(scope),
        }
    }
}
impl std::fmt::Debug for Meters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() {
            "Meters(provider)"
        } else {
            "Meters(noop)"
        })
    }
}
