//! Named crash points (NT §8 `Faults`). With the `testkit` feature a test
//! arms a point once: `pause_once` holds the handler that reaches it for a
//! while, ignoring cancellation, and `crash_once` never lets it go on, as
//! a dead worker would. Either way its lease expires and another run takes
//! the job over; a crashed run is aborted at the job's deadline. Without
//! the feature every point is a no-op.

/// After the expansion transaction committed.
pub const AFTER_EXPANSION_COMMIT: &str = "after_expansion_commit";
/// After Tx A committed the `started` attempt.
pub const AFTER_ATTEMPT_COMMITTED: &str = "after_attempt_committed";
/// After the provider call, before Tx B.
pub const AFTER_PROVIDER_CALL: &str = "after_provider_call";
/// After Tx B committed the outcome.
pub const AFTER_TX_B: &str = "after_tx_b";

#[cfg(feature = "testkit")]
#[derive(Clone, Copy, Debug)]
enum Fault {
    Pause(std::time::Duration),
    Crash,
}

/// The armed points. Cheap to clone.
#[derive(Clone, Debug, Default)]
pub struct Faults {
    #[cfg(feature = "testkit")]
    armed: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<&'static str, Fault>>>,
}
impl Faults {
    /// Pause the next handler that reaches `point` for `pause`.
    #[cfg(feature = "testkit")]
    pub fn pause_once(&self, point: &'static str, pause: std::time::Duration) {
        if let Ok(mut armed) = self.armed.lock() {
            armed.insert(point, Fault::Pause(pause));
        }
    }
    /// The next handler that reaches `point` never returns.
    #[cfg(feature = "testkit")]
    pub fn crash_once(&self, point: &'static str) {
        if let Ok(mut armed) = self.armed.lock() {
            armed.insert(point, Fault::Crash);
        }
    }
    /// Reached `point`: pause or hang when armed.
    pub(crate) async fn hit(&self, point: &'static str) {
        #[cfg(feature = "testkit")]
        {
            let fault = self.armed.lock().ok().and_then(|mut a| a.remove(point));
            match fault {
                Some(Fault::Pause(pause)) => tokio::time::sleep(pause).await,
                Some(Fault::Crash) => std::future::pending::<()>().await,
                None => {}
            }
        }
        #[cfg(not(feature = "testkit"))]
        let _ = point;
    }
}
