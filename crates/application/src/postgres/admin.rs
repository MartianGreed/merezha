//! The PostgreSQL reads behind the admin documents of [`crate::status`].
use super::{PostgresStore, invalid, read_error, signed, sql, timeout, unsigned};
use crate::{
    config::{Projection, Reactor},
    status::{
        ApplicationStatus, InstanceStatus, ProjectionEntry, ProjectionStatus, ReactorEntry,
        ReactorSkipped, ReactorState, ReactorStatus, RebuildEntry, RejectionEntry, RejectionsPage,
        RevisionEntry, SkipTo,
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
    /// The status document (§D.1) from the projection and reactor
    /// overviews.
    pub(crate) fn status(
        &self,
        projections: &[&Projection],
        reactors: &[&Reactor],
        deadline: Instant,
    ) -> Result<ApplicationStatus, Failure> {
        let (head, rows) = self.overview(projections, deadline)?;
        let reactors = if reactors.is_empty() {
            Vec::new()
        } else {
            self.reactors_overview(reactors, false, deadline)?
                .1
                .into_iter()
                .map(|(entry, _)| ReactorStatus {
                    name: entry.name,
                    status: entry.status,
                    lag_events: entry.lag,
                })
                .collect()
        };
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
            reactors,
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

/// `GET reactors` counts each reactor's rejections up to this many.
pub const REJECTIONS_COUNTED: u64 = 10_000;

/// Configured reactors' admin entries with the accepted time of each one's
/// first unprocessed event, and the head they were read at.
pub(crate) type ReactorRows = (u64, Vec<(ReactorEntry, Option<i64>)>);

/// The reactor admin operations (AP §D.1). Rows exist for every reactor
/// ever configured; a reactor no longer configured can still be skipped,
/// paused, resumed or retried before it is re-added.
impl PostgresStore {
    /// Configured reactors' rows and the head, in one snapshot, each with
    /// the accepted time of its first unprocessed event. With
    /// `count_rejections`, each entry's `rejected` counts its rejections up
    /// to [`REJECTIONS_COUNTED`]; otherwise it is 0, so the lag sampler and
    /// the status document never scan the rejections.
    pub(crate) fn reactors_overview(
        &self,
        reactors: &[&Reactor],
        count_rejections: bool,
        deadline: Instant,
    ) -> Result<ReactorRows, Failure> {
        let cap = if count_rejections {
            REJECTIONS_COUNTED as i64
        } else {
            0
        };
        let names: Vec<String> = reactors
            .iter()
            .map(|r| r.name.as_str().to_owned())
            .collect();
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
                        .query(tx, &sql::REACTORS_OVERVIEW, &[&names, &cap])
                        .await
                        .map_err(|e| read_error(&e))?;
                    Ok((head, rows))
                })
            }),
        )?;
        let head = unsigned(head)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let checkpoint = unsigned(row.get(1))?;
            out.push((
                ReactorEntry {
                    name: Identity::new(row.get::<_, &str>(0)).map_err(|_| invalid())?,
                    checkpoint,
                    lag: head.saturating_sub(checkpoint),
                    status: row.get(2),
                    halted_position: row.get::<_, Option<i64>>(3).map(unsigned).transpose()?,
                    failure_code: row.get(4),
                    failures: u32::try_from(row.get::<_, i32>(5)).map_err(|_| invalid())?,
                    retry_at: row.get(6),
                    rejected: row.get::<_, Option<i64>>(7).map_or(Ok(0), unsigned)?,
                },
                row.get(8),
            ));
        }
        Ok((head, out))
    }
    /// `reactor_not_found` unless `name` has a row.
    async fn reactor_exists(&self, name: &str, deadline: Instant) -> Result<(), Failure> {
        let row = self
            .session(deadline, timeout, async |client| {
                self.db
                    .query_opt(client, &sql::REACTOR, &[&name])
                    .await
                    .map_err(|e| read_error(&e))
            })
            .await?;
        row.map(|_| ())
            .ok_or_else(|| Failure::permanent("reactor_not_found"))
    }
    /// One page of `name`'s rejections after `after` (`(position,
    /// index)`), at most `limit`, oldest first.
    pub(crate) fn rejections(
        &self,
        name: &str,
        limit: usize,
        after: Option<(u64, u8)>,
        deadline: Instant,
    ) -> Result<RejectionsPage, Failure> {
        let (position, index) = match after {
            Some((position, index)) => (signed(position)?, i16::from(index)),
            None => (-1, -1),
        };
        let count = i64::try_from(limit).map_err(|_| invalid())? + 1;
        let rows = self.block_on(deadline, async {
            self.reactor_exists(name, deadline).await?;
            self.session(deadline, timeout, async |client| {
                self.db
                    .query(
                        client,
                        &sql::REJECTIONS,
                        &[&name, &position, &index, &count],
                    )
                    .await
                    .map_err(|e| read_error(&e))
            })
            .await
        })?;
        let mut rejections = rows
            .iter()
            .map(|row| {
                Ok(RejectionEntry {
                    position: unsigned(row.get(0))?,
                    index: u8::try_from(row.get::<_, i16>(1)).map_err(|_| invalid())?,
                    effect: row.get(2),
                    code: row.get(3),
                    rejected_at: row.get(4),
                })
            })
            .collect::<Result<Vec<_>, Failure>>()?;
        let next = if rejections.len() > limit {
            rejections.truncate(limit);
            rejections
                .last()
                .map(|last| format!("{}.{}", last.position, last.index))
        } else {
            None
        };
        Ok(RejectionsPage { rejections, next })
    }
    /// Pause (`active = false`) or resume a reactor; resuming makes it due
    /// now and wakes its lane here.
    pub(crate) fn set_reactor_status(
        &self,
        name: &Identity,
        active: bool,
        deadline: Instant,
    ) -> Result<ReactorState, Failure> {
        let status = if active { "active" } else { "paused" };
        let text = name.as_str();
        let row = self.block_on(
            deadline,
            self.session(deadline, timeout, async |client| {
                self.db
                    .query_opt(client, &sql::SET_REACTOR_STATUS, &[&text, &status])
                    .await
                    .map_err(|e| read_error(&e))
            }),
        )?;
        let row = row.ok_or_else(|| Failure::permanent("reactor_not_found"))?;
        self.wake_reactor(text);
        Ok(ReactorState {
            name: name.clone(),
            status: row.get(0),
        })
    }
    /// Clear a reactor's backoff so its next run is now, and wake it here.
    pub(crate) fn retry_reactor(
        &self,
        name: &Identity,
        deadline: Instant,
    ) -> Result<ReactorState, Failure> {
        let text = name.as_str();
        let row = self.block_on(
            deadline,
            self.session(deadline, timeout, async |client| {
                self.db
                    .query_opt(client, &sql::RETRY_REACTOR, &[&text])
                    .await
                    .map_err(|e| read_error(&e))
            }),
        )?;
        let row = row.ok_or_else(|| Failure::permanent("reactor_not_found"))?;
        self.wake_reactor(text);
        Ok(ReactorState {
            name: name.clone(),
            status: row.get(0),
        })
    }
    /// An operator's skip (§D.1): exactly the stuck event
    /// (`checkpoint + 1`, else `invalid_skip`), or everything to the head.
    /// Recorded in `reactor_skips` with `principal`, under the row lock,
    /// so it orders against a running batch, whose CAS then fails. A skip
    /// to the head with nothing left to skip changes nothing.
    pub(crate) fn skip_reactor(
        &self,
        name: &Identity,
        to: SkipTo,
        principal: Option<&str>,
        deadline: Instant,
    ) -> Result<(ReactorSkipped, bool), Failure> {
        let text = name.as_str().to_owned();
        let principal = principal.map(str::to_owned);
        let db = self.db.clone();
        let (checkpoint, skipped) = self.block_on(
            deadline,
            self.db.transaction(deadline, move |tx| {
                Box::pin(async move {
                    let row = db
                        .query_opt(tx, &sql::LOCK_REACTOR, &[&text])
                        .await
                        .map_err(|e| read_error(&e))?
                        .ok_or_else(|| Failure::permanent("reactor_not_found"))?;
                    let current: i64 = row.get(0);
                    let head: i64 = db
                        .query_one(tx, &sql::HEAD, &[])
                        .await
                        .map_err(|e| read_error(&e))?
                        .get(0);
                    let (target, reason) = match to {
                        SkipTo::Position(position) => {
                            let position = signed(position)?;
                            if position != current + 1 || position > head {
                                return Err(Failure::conflict("invalid_skip"));
                            }
                            (position, "admin")
                        }
                        SkipTo::Head if head <= current => return Ok((current, false)),
                        SkipTo::Head => (head, "skip_to_head"),
                    };
                    // The row covers the whole skipped range.
                    let first = current + 1;
                    db.execute(
                        tx,
                        &sql::INSERT_SKIP,
                        &[&text, &target, &reason, &None::<String>, &principal, &first],
                    )
                    .await
                    .map_err(|e| read_error(&e))?;
                    let moved: i64 = db
                        .query_opt(tx, &sql::SKIP_REACTOR, &[&text, &target])
                        .await
                        .map_err(|e| read_error(&e))?
                        .ok_or_else(|| Failure::conflict("invalid_skip"))?
                        .get(0);
                    Ok((moved, true))
                })
            }),
        )?;
        self.wake_reactor(name.as_str());
        Ok((
            ReactorSkipped {
                name: name.clone(),
                checkpoint: unsigned(checkpoint)?,
            },
            skipped,
        ))
    }
    fn wake_reactor(&self, name: &str) {
        if let Some(lane) = self.shared().reactors.get(name) {
            lane.wake.notify_one();
        }
    }
}
