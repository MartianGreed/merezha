//! The application admin documents (AP §D.1): the status document TE's
//! `/v1/admin/status` embeds (R10) and, on PostgreSQL, the projection
//! overview and the rebuild request.
use merezha_domain::Identity;
use serde::Serialize;

/// `GET /v1/application/status` and [`crate::Application::status`].
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ApplicationStatus {
    /// The manifest identity; `None` for memory storage.
    pub application: Option<Identity>,
    pub head: u64,
    pub projections: Vec<ProjectionStatus>,
    /// Reactors arrive with AP-C3; always empty here.
    pub reactors: Vec<serde_json::Value>,
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
