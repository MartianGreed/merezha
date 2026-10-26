//! The test kit (NT §8, feature `testkit`, dev-dependencies only). It adds
//! no mock-server dependency.
//!
//! - [`FakeBrevo`]: an in-process Brevo API on `127.0.0.1:0`.
//! - [`EventStreamParser`] and [`EventReader`]: the `text/event-stream`
//!   grammar and a reader over a response body, for the inbox stream
//!   (NT-N5).
//!
//! The named crash points live in [`crate::faults`].
mod brevo;
mod sse;

pub use brevo::{FakeBrevo, IDEMPOTENCY_WINDOW, Processed, Recorded};
pub use sse::{EventReader, EventStreamParser, SseEvent};
