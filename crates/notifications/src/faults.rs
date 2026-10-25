//! Named crash points (NT §8 `Faults`). With the `testkit` feature a test
//! arms a point to pause the handler that reaches it for a while, ignoring
//! cancellation, as a dead worker would: its lease expires and another run
//! takes the job over. Without the feature every point is a no-op.

/// After the expansion transaction committed.
pub const AFTER_EXPANSION_COMMIT: &str = "after_expansion_commit";
/// After Tx A committed the `started` attempt.
pub const AFTER_ATTEMPT_COMMITTED: &str = "after_attempt_committed";
/// After the provider call, before Tx B.
pub const AFTER_PROVIDER_CALL: &str = "after_provider_call";
/// After Tx B committed the outcome.
pub const AFTER_TX_B: &str = "after_tx_b";

/// The armed points. Cheap to clone.
#[derive(Clone, Debug, Default)]
pub struct Faults {
    #[cfg(feature = "testkit")]
    armed: std::sync::Arc<
        std::sync::Mutex<std::collections::BTreeMap<&'static str, std::time::Duration>>,
    >,
}
impl Faults {
    /// Pause the next handler that reaches `point` for `pause`.
    #[cfg(feature = "testkit")]
    pub fn pause_once(&self, point: &'static str, pause: std::time::Duration) {
        if let Ok(mut armed) = self.armed.lock() {
            armed.insert(point, pause);
        }
    }
    /// Reached `point`: pause when armed.
    pub(crate) async fn hit(&self, point: &'static str) {
        #[cfg(feature = "testkit")]
        {
            let pause = self.armed.lock().ok().and_then(|mut a| a.remove(point));
            if let Some(pause) = pause {
                tokio::time::sleep(pause).await;
            }
        }
        #[cfg(not(feature = "testkit"))]
        let _ = point;
    }
}
