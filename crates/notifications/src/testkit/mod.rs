//! The test kit (NT §8, feature `testkit`, dev-dependencies only). It adds
//! no mock-server dependency.
//!
//! - [`FakeBrevo`]: an in-process Brevo API on `127.0.0.1:0`.
//!
//! The named crash points live in [`crate::faults`].
mod brevo;

pub use brevo::{FakeBrevo, IDEMPOTENCY_WINDOW, Processed, Recorded};
