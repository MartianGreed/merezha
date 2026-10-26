//! The application admin documents (AP §D.1): the status document TE's
//! `/v1/admin/status` embeds (R10) and, on PostgreSQL, the projection
//! overview, the rebuild request and the reactor documents.
use merezha_domain::Identity;
use serde::Serialize;

/// `GET /v1/application/status` and [`crate::Application::status`].
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ApplicationStatus {
    /// The manifest identity; `None` for memory storage.
    pub application: Option<Identity>,
    pub head: u64,
    pub projections: Vec<ProjectionStatus>,
    /// Configured reactors (PostgreSQL only; empty otherwise).
    pub reactors: Vec<ReactorStatus>,
    /// PostgreSQL only.
    pub instance: Option<InstanceStatus>,
    /// `connected`, `reconnecting`, or `off` (manual catch-up or no
    /// scheduler); `None` without PostgreSQL.
    pub listener: Option<&'static str>,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ProjectionStatus {
    pub name: Identity,
    pub checkpoint: u64,
    pub lag_events: u64,
    /// Age of the first unprocessed event; 0 when caught up.
    pub lag_seconds: f64,
    /// `active` or `failing`.
    pub status: String,
    pub failure_code: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InstanceStatus {
    pub id: String,
    pub background_runs: usize,
}
/// `GET /v1/application/projections`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ProjectionsOverview {
    pub head: u64,
    pub projections: Vec<ProjectionEntry>,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ProjectionEntry {
    pub name: Identity,
    pub revision: u32,
    pub partitioned: bool,
    pub checkpoint: u64,
    pub lag: u64,
    pub warm: bool,
    pub status: String,
    pub failure_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rebuild: Option<RebuildEntry>,
    /// RFC 3339, UTC.
    pub updated_at: String,
    /// Every revision row of this name, for `GET projections/{name}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revisions: Option<Vec<RevisionEntry>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RebuildEntry {
    pub checkpoint: u64,
    pub target: u64,
    /// `running` or `failed`.
    pub status: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RevisionEntry {
    pub revision: u32,
    pub checkpoint: u64,
    pub warm: bool,
}
/// `202` body of `POST .../rebuild` on PostgreSQL.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RebuildRequested {
    pub name: Identity,
    pub revision: u32,
    pub target: u64,
}
/// One reactor in [`ApplicationStatus`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReactorStatus {
    pub name: Identity,
    /// `active`, `paused` or `halted`.
    pub status: String,
    pub lag_events: u64,
}
/// `GET /v1/application/reactors`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReactorsOverview {
    pub head: u64,
    pub reactors: Vec<ReactorEntry>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReactorEntry {
    pub name: Identity,
    pub checkpoint: u64,
    /// `head - checkpoint`: the gap a reactor removed and re-added shows.
    pub lag: u64,
    /// `active`, `paused` or `halted`.
    pub status: String,
    pub halted_position: Option<u64>,
    pub failure_code: Option<String>,
    pub failures: u32,
    /// RFC 3339, UTC; the next attempt after a failure.
    pub retry_at: Option<String>,
    /// How many effects were rejected so far, counted up to 10,000.
    pub rejected: u64,
}
/// `GET /v1/application/reactors/{name}/rejections`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RejectionsPage {
    pub rejections: Vec<RejectionEntry>,
    /// The cursor of the next page, `{position}.{index}`; absent at the end.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RejectionEntry {
    pub position: u64,
    pub index: u8,
    /// `notify`, `webhook` or `command`.
    pub effect: String,
    pub code: String,
    /// RFC 3339, UTC.
    pub rejected_at: String,
}
/// The answer of `POST reactors/{name}/pause`, `/resume` and `/retry`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReactorState {
    pub name: Identity,
    pub status: String,
}
/// The answer of `POST reactors/{name}/skip`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReactorSkipped {
    pub name: Identity,
    pub checkpoint: u64,
}
/// What `POST reactors/{name}/skip` skips.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkipTo {
    /// Exactly the stuck event: it must be `checkpoint + 1`.
    Position(u64),
    /// Everything up to the head.
    Head,
}
