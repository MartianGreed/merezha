//! The internal seam between `Application` and its durable stores (SQLite
//! and PostgreSQL). Every method runs SQL only, never guest code, and is
//! bounded by the admitted deadline.
use crate::application::{Key, ProjectionState};
use merezha_domain::{Event, Failure, Identity, Metadata, Receipt, StoredEvent};
use std::time::Instant;

/// One command's commit: the decided batch under its expected version,
/// with the idempotency claim, if any, and its canonical fingerprint.
pub(crate) struct CommitCommand<'a> {
    pub stream: &'a Identity,
    /// The stream's tenant; the stream is then `"{tenant}:{aggregate}/{id}"`
    /// (AP-C2). `None` for application streams.
    pub tenant: Option<&'a Identity>,
    pub expected: u64,
    pub events: Vec<Event>,
    pub metadata: &'a Metadata,
    pub claim: Option<(&'a Key, &'a str)>,
}
/// A configured projection at its configured revision. Tenant partitions
/// join here in AP-C2.
pub(crate) struct ProjectionRef<'a> {
    pub name: &'a Identity,
    #[cfg_attr(
        not(feature = "postgres"),
        expect(
            dead_code,
            reason = "SQLite fingerprints revisions; PostgreSQL keys rows by (name, revision)"
        )
    )]
    pub revision: u32,
}

pub(crate) trait DurableStore: Send + Sync {
    /// The stream's history in version order.
    fn load(&self, stream: &Identity, deadline: Instant) -> Result<Vec<StoredEvent>, Failure>;
    /// The highest committed position, 0 when empty.
    fn head(&self, deadline: Instant) -> Result<u64, Failure>;
    /// At most `limit` events after position `after`, in position order.
    fn read_all(
        &self,
        after: u64,
        limit: usize,
        deadline: Instant,
    ) -> Result<Vec<StoredEvent>, Failure>;
    /// Fast-path replay before the guest decides: the stored receipt, `None`
    /// when absent, `idempotency_mismatch` when the fingerprint differs.
    fn receipt(
        &self,
        key: &Key,
        fingerprint: &str,
        deadline: Instant,
    ) -> Result<Option<Receipt>, Failure>;
    /// Commit the batch and the receipt atomically, rechecking the stream
    /// version and the key inside the store's transaction. `true` when the
    /// key's stored receipt was replayed instead.
    fn commit(&self, c: CommitCommand<'_>, deadline: Instant) -> Result<(Receipt, bool), Failure>;
    /// [`DurableStore::commit`] without the replay flag.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the store contract tests use it")
    )]
    fn commit_command(&self, c: CommitCommand<'_>, deadline: Instant) -> Result<Receipt, Failure> {
        self.commit(c, deadline).map(|(receipt, _)| receipt)
    }
    /// The persisted pair and its row revision.
    fn projection(
        &self,
        p: &ProjectionRef<'_>,
        deadline: Instant,
    ) -> Result<(ProjectionState, i64), Failure>;
    /// Publish state and checkpoint together when the row revision still
    /// matches, else `projection_conflict`. Returns the new row revision.
    fn commit_projection(
        &self,
        p: &ProjectionRef<'_>,
        row_revision: i64,
        state: &ProjectionState,
        deadline: Instant,
    ) -> Result<i64, Failure>;
    /// A second instance over the same data with its own connections and
    /// what it recovered at open, for the store contract tests.
    #[cfg(feature = "test-support")]
    fn independent(
        &self,
        config: &crate::config::Config,
    ) -> Result<(std::sync::Arc<dyn DurableStore>, crate::sqlite::Recovered), Failure>;
}
