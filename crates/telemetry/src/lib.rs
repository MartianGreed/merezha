//! Merezha telemetry: the name catalog every block uses, and the host's
//! OpenTelemetry pipeline (TE, `docs/design/telemetry.md`).
//!
//! Features:
//!
//! - default: [`names`] (the R9 catalog and typed attribute constructors),
//!   the [`event!`] macro, [`config`] (the `[telemetry]` model the kernel
//!   parses), [`blocking`] and [`Meters`], the meter source libraries
//!   take. Only `tracing` and the `opentelemetry` API; every library may
//!   depend on it.
//! - `propagation`: the `propagation` module, with the stored trace context and
//!   span parents and links through `tracing-opentelemetry`. No SDK, no
//!   network code.
//! - `pipeline`: the `merezha` binary only. Providers, guarded and
//!   allowlist processors, counting OTLP exporters, the stderr JSON
//!   formatter, the subscriber and shutdown flush.
//! - `testing`: an in-memory `testing::Harness` for tests.
//!
//! There are no OpenTelemetry globals: providers live in the
//! `pipeline::Telemetry` handle the host owns. The `tracing` subscriber is
//! the one process global, installed by the binary.
pub mod blocking;
pub mod config;
pub mod meters;
pub mod names;

#[cfg(feature = "propagation")]
pub mod propagation;

#[cfg(feature = "pipeline")]
pub mod allowlist;
#[cfg(feature = "pipeline")]
pub mod json;
#[cfg(feature = "pipeline")]
pub mod pipeline;
#[cfg(feature = "pipeline")]
pub mod status;
#[cfg(feature = "pipeline")]
pub mod subscriber;

#[cfg(feature = "testing")]
pub mod testing;

pub use blocking::spawn_blocking_in_span;
pub use config::{Service, Signal, TelemetryConfig};
pub use meters::Meters;
#[cfg(feature = "pipeline")]
pub use pipeline::Telemetry;

#[doc(hidden)]
pub mod __private {
    pub use tracing;
}

/// The `tracing` target of every catalog event, whatever crate emits it.
pub const TARGET: &str = "merezha";

/// Emit a catalog log event: `event!(LEVEL, "area.noun_verb", field = …)`.
///
/// Expands to `tracing::event!(name: "…", target: "merezha", Level::LEVEL,
/// …, "…")`: the event name is the `tracing` metadata name and the constant
/// message, so JSON lines and OTLP log bodies are never empty. Fields must be the ones
/// the [`names::EVENTS`] row lists; the catalog test fails any other. Never
/// pass `%err` or `?value` of foreign types: field values are codes, counts,
/// configured names and validated identifiers only.
///
/// ```
/// merezha_telemetry::event!(INFO, "listener.ready", address = "127.0.0.1:8080");
/// merezha_telemetry::event!(INFO, "listener.draining");
/// ```
#[macro_export]
macro_rules! event {
    ($level:ident, $name:literal $(,)?) => {
        $crate::__private::tracing::event!(
            name: $name,
            target: $crate::TARGET,
            $crate::__private::tracing::Level::$level,
            $name
        )
    };
    ($level:ident, $name:literal, $($fields:tt)+) => {
        $crate::__private::tracing::event!(
            name: $name,
            target: $crate::TARGET,
            $crate::__private::tracing::Level::$level,
            $($fields)+,
            $name
        )
    };
}
