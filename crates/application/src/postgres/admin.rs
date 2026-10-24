//! The PostgreSQL reads behind the admin documents of [`crate::status`].
use super::{PostgresStore, invalid, read_error, sql, timeout, unsigned};
use crate::{
    config::Projection,
    status::{
        ApplicationStatus, InstanceStatus, ProjectionEntry, ProjectionStatus, RebuildEntry,
        RevisionEntry,
    },
};
use merezha_domain::{Failure, Identity};
use std::{
    sync::atomic::Ordering,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

/// One configured revision's overview row.
pub(crate) struct Overview {
    pub entry: ProjectionEntry,
    /// `accepted_unix_ms` of the first unprocessed event.
    pub oldest: Option<i64>,
}

impl PostgresStore {
    /// The configured revisions' rows, their rebuilds and the head, read in
    /// one snapshot.
    pub(crate) fn overview(
        &self,
        projections: &[&Projection],
        deadline: Instant,
    ) -> Result<(u64, Vec<Overview>), Failure> {
        let names: Vec<String> = projections
            .iter()
            .map(|p| p.name.as_str().to_owned())
            .collect();
        let revisions = projections
            .iter()
            .map(|p| i32::try_from(p.revision).map_err(|_| invalid()))
            .collect::<Result<Vec<i32>, _>>()?;
        let handle = self.db.clone();
        let (head, rows) = self.block_on(
            deadline,
            self.db.transaction(deadline, move |tx| {
                Box::pin(async move {
                    handle
                        .batch(tx, &sql::ADMIN_SNAPSHOT)
                        .await
                        .map_err(|e| read_error(&e))?;
                    let head: i64 = handle
                        .query_one(tx, &sql::HEAD, &[])
                        .await
                        .map_err(|e| read_error(&e))?
                        .get(0);
                    let rows = handle
                        .query(tx, &sql::OVERVIEW, &[&names, &revisions])
                        .await
                        .map_err(|e| read_error(&e))?;
                    Ok((head, rows))
                })
            }),
        )?;
        let head = unsigned(head)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let checkpoint = unsigned(row.get(3))?;
            let warm_position = unsigned(row.get(4))?;
            let rebuild = match row.get::<_, Option<i64>>(8) {
                Some(rebuild_checkpoint) => Some(RebuildEntry {
                    checkpoint: unsigned(rebuild_checkpoint)?,
                    target: unsigned(row.get::<_, Option<i64>>(9).ok_or_else(invalid)?)?,
                    status: row.get::<_, Option<String>>(10).ok_or_else(invalid)?,
                }),
                None => None,
            };
            out.push(Overview {
                entry: ProjectionEntry {
                    name: Identity::new(row.get::<_, &str>(0)).map_err(|_| invalid())?,
                    revision: u32::try_from(row.get::<_, i32>(1)).map_err(|_| invalid())?,
                    partitioned: row.get(2),
                    checkpoint,
                    lag: head.saturating_sub(checkpoint),
                    warm: checkpoint >= warm_position,
                    status: row.get(5),
                    failure_code: row.get(6),
                    rebuild,
                    updated_at: row.get(7),
                    revisions: None,
                },
                oldest: row.get(11),
            });
        }
        Ok((head, out))
    }
    /// Every revision row of `name`.
    pub(crate) fn revisions(
        &self,
        name: &Identity,
        deadline: Instant,
    ) -> Result<Vec<RevisionEntry>, Failure> {
        let text = name.as_str().to_owned();
        let rows = self.block_on(
            deadline,
            self.session(deadline, timeout, async |client| {
                self.db
                    .query(client, &sql::REVISIONS, &[&text])
                    .await
                    .map_err(|e| read_error(&e))
            }),
        )?;
        rows.iter()
            .map(|row| {
                let checkpoint = unsigned(row.get(1))?;
                Ok(RevisionEntry {
                    revision: u32::try_from(row.get::<_, i32>(0)).map_err(|_| invalid())?,
                    checkpoint,
                    warm: checkpoint >= unsigned(row.get(2))?,
                })
            })
            .collect()
    }
    /// The status document (§D.1) from one overview read.
    pub(crate) fn status(
        &self,
        projections: &[&Projection],
        deadline: Instant,
    ) -> Result<ApplicationStatus, Failure> {
        let (head, rows) = self.overview(projections, deadline)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let shared = self.shared();
        Ok(ApplicationStatus {
            application: Some(self.application.clone()),
            head,
            projections: rows
                .into_iter()
                .map(|row| ProjectionStatus {
                    lag_seconds: match row.oldest {
                        Some(accepted) if row.entry.lag > 0 => {
                            now.saturating_sub(accepted).max(0) as f64 / 1000.0
                        }
                        _ => 0.0,
                    },
                    name: row.entry.name,
                    checkpoint: row.entry.checkpoint,
                    lag_events: row.entry.lag,
                    status: row.entry.status,
                    failure_code: row.entry.failure_code,
                })
                .collect(),
            reactors: Vec::new(),
            instance: Some(InstanceStatus {
                id: shared.options.instance_id.clone(),
                background_runs: shared.runs.load(Ordering::SeqCst),
            }),
            listener: Some(
                ["off", "connected", "reconnecting"]
                    .get(usize::from(shared.listener.load(Ordering::SeqCst)))
                    .copied()
                    .unwrap_or("off"),
            ),
        })
    }
}
