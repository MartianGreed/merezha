//! `merezha application verify` (AP §A.11): read-only invariant checks
//! over `merezha_app`, in bounded chunks inside one repeatable-read,
//! read-only transaction, so concurrent appends cannot produce false
//! violations. It is O(history) and never runs at startup.
use super::{MIGRATIONS, MODULE, read_error, unsigned};
use merezha_domain::Failure;
use merezha_postgres::{Database, MigrationMode, ModuleMigrations, Statement, Tx, run_migrations};
use serde::Serialize;
use std::time::Instant;

/// Positions per chunk.
const POSITION_CHUNK: i64 = 10_000;
/// Streams, receipts or projection rows per chunk.
const ROW_CHUNK: i64 = 1_000;
/// Missing positions listed per failing chunk.
const MISSING_PER_CHUNK: i64 = 10;
/// Violations listed in a report; later ones are only counted.
pub const MAX_VIOLATIONS: usize = 100;

pub(crate) const SNAPSHOT: Statement = Statement::new(
    "app.verify.snapshot",
    "SET",
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
);
pub(crate) const POSITIONS: Statement = Statement::new(
    "app.verify.positions",
    "SELECT merezha_app.events",
    "SELECT (SELECT position FROM merezha_app.head WHERE id = 1), \
            (SELECT coalesce(max(position), 0) FROM merezha_app.events)",
);
pub(crate) const GAP_COUNT: Statement = Statement::new(
    "app.verify.gap_count",
    "SELECT merezha_app.events",
    "SELECT count(*) FROM merezha_app.events WHERE position > $1 AND position <= $2",
);
pub(crate) const GAPS: Statement = Statement::new(
    "app.verify.gaps",
    "SELECT merezha_app.events",
    "SELECT s.p FROM generate_series($1::bigint + 1, $2::bigint) AS s (p) \
      WHERE NOT EXISTS (SELECT 1 FROM merezha_app.events e WHERE e.position = s.p) \
      ORDER BY s.p LIMIT $3",
);
pub(crate) const STREAMS: Statement = Statement::new(
    "app.verify.streams",
    "SELECT merezha_app.streams",
    "SELECT s.stream, s.version, s.first_position, s.last_position, count(e.position), \
            coalesce(min(e.version), 0), coalesce(max(e.version), 0), \
            coalesce(min(e.position), 0), coalesce(max(e.position), 0) \
       FROM (SELECT * FROM merezha_app.streams WHERE stream > $1 ORDER BY stream LIMIT $2) s \
       LEFT JOIN merezha_app.events e ON e.stream = s.stream \
      GROUP BY s.stream, s.version, s.first_position, s.last_position \
      ORDER BY s.stream",
);
pub(crate) const RECEIPTS: Statement = Statement::new(
    "app.verify.receipts",
    "SELECT merezha_app.receipts",
    "SELECT r.tag, r.tenant, r.actor, r.key, r.events, \
            (SELECT count(*) FROM merezha_app.events e \
              WHERE e.position > r.position - r.events AND e.position <= r.position \
                AND e.stream = r.stream AND e.version = r.version - (r.position - e.position)) \
       FROM merezha_app.receipts r \
      WHERE (r.tag, r.tenant, r.actor, r.key) > ($1, $2, $3, $4) \
      ORDER BY r.tag, r.tenant, r.actor, r.key LIMIT $5",
);
pub(crate) const CHECKPOINTS: Statement = Statement::new(
    "app.verify.checkpoints",
    "SELECT merezha_app.projections",
    "SELECT 'projection', name, revision, checkpoint > (SELECT position FROM merezha_app.head WHERE id = 1) \
       FROM merezha_app.projections \
     UNION ALL \
     SELECT 'rebuild', name, revision, checkpoint > (SELECT position FROM merezha_app.head WHERE id = 1) \
       FROM merezha_app.projection_rebuilds \
     ORDER BY 1, 2, 3",
);

/// What a violation concerns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Subject {
    Head,
    Position(u64),
    Stream(String),
    Receipt {
        tag: String,
        tenant: String,
        actor: String,
        key: String,
    },
    Projection {
        name: String,
        revision: i32,
    },
    Rebuild {
        name: String,
        revision: i32,
    },
}
/// One broken invariant. `check` is one of `positions_contiguous`,
/// `head_matches_events`, `stream_versions_contiguous`,
/// `stream_matches_events`, `receipt_matches_events` and
/// `checkpoint_within_head`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Violation {
    pub check: &'static str,
    pub subject: Subject,
}
/// The JSON report `merezha application verify` prints.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct VerifyReport {
    pub ok: bool,
    pub head: u64,
    pub events: u64,
    pub streams: u64,
    pub receipts: u64,
    pub projections: u64,
    /// Every violation found, of which at most [`MAX_VIOLATIONS`] are listed.
    pub violation_count: u64,
    pub violations: Vec<Violation>,
}
impl VerifyReport {
    fn violate(&mut self, check: &'static str, subject: Subject) {
        self.violation_count += 1;
        if self.violations.len() < MAX_VIOLATIONS {
            self.violations.push(Violation { check, subject });
        }
    }
}

/// Check the invariants of §A.11 before `deadline`. Fails `storage_schema`
/// when the application migrations are not applied; a broken invariant is a
/// report with `ok = false`, not a failure.
pub async fn verify(db: &Database, deadline: Instant) -> Result<VerifyReport, Failure> {
    let report = run_migrations(
        db,
        &[ModuleMigrations {
            module: MODULE,
            migrations: MIGRATIONS,
        }],
        MigrationMode::Verify,
        deadline,
    )
    .await
    .map_err(|error| error.failure)?;
    if !report.is_current() {
        return Err(Failure::permanent("storage_schema"));
    }
    let handle = db.clone();
    db.transaction(deadline, move |tx| {
        Box::pin(async move {
            handle
                .batch(tx, &SNAPSHOT)
                .await
                .map_err(|e| read_error(&e))?;
            let mut report = VerifyReport::default();
            positions(&handle, tx, &mut report).await?;
            streams(&handle, tx, &mut report).await?;
            receipts(&handle, tx, &mut report).await?;
            checkpoints(&handle, tx, &mut report).await?;
            report.ok = report.violation_count == 0;
            Ok(report)
        })
    })
    .await
}

async fn positions(db: &Database, tx: &Tx<'_>, report: &mut VerifyReport) -> Result<(), Failure> {
    let row = db
        .query_one(tx, &POSITIONS, &[])
        .await
        .map_err(|e| read_error(&e))?;
    let head: i64 = row.get(0);
    let max: i64 = row.get(1);
    report.head = unsigned(head)?;
    if head != max {
        report.violate("head_matches_events", Subject::Head);
    }
    let mut low = 0;
    while low < max {
        let high = (low + POSITION_CHUNK).min(max);
        let count: i64 = db
            .query_one(tx, &GAP_COUNT, &[&low, &high])
            .await
            .map_err(|e| read_error(&e))?
            .get(0);
        report.events += unsigned(count)?;
        if count != high - low {
            for row in db
                .query(tx, &GAPS, &[&low, &high, &MISSING_PER_CHUNK])
                .await
                .map_err(|e| read_error(&e))?
            {
                report.violate(
                    "positions_contiguous",
                    Subject::Position(unsigned(row.get(0))?),
                );
            }
            // Missing positions beyond the listed ones are counted only.
            let listed = MISSING_PER_CHUNK.min(high - low - count);
            report.violation_count += unsigned(high - low - count - listed)?;
        }
        low = high;
    }
    Ok(())
}

async fn streams(db: &Database, tx: &Tx<'_>, report: &mut VerifyReport) -> Result<(), Failure> {
    let mut after = String::new();
    loop {
        let rows = db
            .query(tx, &STREAMS, &[&after, &ROW_CHUNK])
            .await
            .map_err(|e| read_error(&e))?;
        for row in &rows {
            let stream: String = row.get(0);
            let (version, first, last): (i64, i64, i64) = (row.get(1), row.get(2), row.get(3));
            let count: i64 = row.get(4);
            let (min_version, max_version): (i64, i64) = (row.get(5), row.get(6));
            let (min_position, max_position): (i64, i64) = (row.get(7), row.get(8));
            // Versions are unique per stream, so 1..=n with n rows is contiguous.
            if count == 0 || min_version != 1 || max_version != count {
                report.violate(
                    "stream_versions_contiguous",
                    Subject::Stream(stream.clone()),
                );
            }
            if version != max_version || first != min_position || last != max_position {
                report.violate("stream_matches_events", Subject::Stream(stream.clone()));
            }
            report.streams += 1;
            after = stream;
        }
        if (rows.len() as i64) < ROW_CHUNK {
            return Ok(());
        }
    }
}

async fn receipts(db: &Database, tx: &Tx<'_>, report: &mut VerifyReport) -> Result<(), Failure> {
    let mut after = [String::new(), String::new(), String::new(), String::new()];
    loop {
        let rows = db
            .query(
                tx,
                &RECEIPTS,
                &[&after[0], &after[1], &after[2], &after[3], &ROW_CHUNK],
            )
            .await
            .map_err(|e| read_error(&e))?;
        for row in &rows {
            let key = [row.get(0), row.get(1), row.get(2), row.get(3)];
            let events: i32 = row.get(4);
            let matched: i64 = row.get(5);
            if i64::from(events) != matched {
                let [tag, tenant, actor, key] = key.clone();
                report.violate(
                    "receipt_matches_events",
                    Subject::Receipt {
                        tag,
                        tenant,
                        actor,
                        key,
                    },
                );
            }
            report.receipts += 1;
            after = key;
        }
        if (rows.len() as i64) < ROW_CHUNK {
            return Ok(());
        }
    }
}

async fn checkpoints(db: &Database, tx: &Tx<'_>, report: &mut VerifyReport) -> Result<(), Failure> {
    let rows = db
        .query(tx, &CHECKPOINTS, &[])
        .await
        .map_err(|e| read_error(&e))?;
    for row in &rows {
        let (kind, name, revision, beyond): (&str, String, i32, bool) =
            (row.get(0), row.get(1), row.get(2), row.get(3));
        if kind == "projection" {
            report.projections += 1;
        }
        if beyond {
            report.violate(
                "checkpoint_within_head",
                if kind == "projection" {
                    Subject::Projection { name, revision }
                } else {
                    Subject::Rebuild { name, revision }
                },
            );
        }
    }
    Ok(())
}
