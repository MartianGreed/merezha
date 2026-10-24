//! Blocking work that keeps its telemetry context (TE §2.4).
//!
//! `tokio::task::spawn_blocking` runs the closure on a pool thread, where
//! the caller's thread-local `tracing` dispatcher (scoped in tests) and the
//! OpenTelemetry context (activated on span enter) are both missing.
//! [`spawn_blocking_in_span`] captures `Dispatch::current()` and
//! `Span::current()` and re-enters both. T3 moves every CLI and application
//! `spawn_blocking` to it; `clippy.toml` bans the raw call
//! (`disallowed-methods`) everywhere but here.

/// `tokio::task::spawn_blocking` that runs `f` under the caller's
/// dispatcher and inside the caller's current span. Must be called from a
/// Tokio runtime, like `spawn_blocking`.
pub fn spawn_blocking_in_span<F, R>(f: F) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    let span = tracing::Span::current();
    #[expect(
        clippy::disallowed_methods,
        reason = "the one sanctioned spawn_blocking (TE §2.4)"
    )]
    tokio::task::spawn_blocking(move || {
        tracing::dispatcher::with_default(&dispatch, || span.in_scope(f))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tracing::{Subscriber, span};

    /// Counts events and remembers the last entered span id.
    #[derive(Default, Clone)]
    struct Probe {
        events: Arc<AtomicUsize>,
        entered: Arc<AtomicUsize>,
    }
    impl Subscriber for Probe {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(7)
        }
        fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
        fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {
            self.events.fetch_add(1, Ordering::SeqCst);
        }
        fn enter(&self, id: &span::Id) {
            self.entered.store(id.into_u64() as usize, Ordering::SeqCst);
        }
        fn exit(&self, _: &span::Id) {}
    }

    #[tokio::test]
    async fn blocking_work_keeps_the_scoped_dispatcher_and_span() {
        let probe = Probe::default();
        let dispatch = tracing::Dispatch::new(probe.clone());
        let handle = tracing::dispatcher::with_default(&dispatch, || {
            let span = tracing::info_span!("work");
            let _entered = span.enter();
            spawn_blocking_in_span(|| {
                tracing::info!("inside");
                42
            })
        });
        assert_eq!(handle.await.unwrap(), 42);
        assert_eq!(probe.events.load(Ordering::SeqCst), 1);
        assert_eq!(probe.entered.load(Ordering::SeqCst), 7);
    }
}
