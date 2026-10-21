//! Owned local SQLite storage. SQL transactions never span guest execution.
use crate::{
    application::{Key, ProjectionState, Record},
    config::{Config, Limits, StorageConfig},
};
use merezha_domain::{Event, Failure, Identity, Metadata, Receipt, StoredEvent, compatible_json};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    path::Path,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const SCHEMA: &[&str] = &[
    "CREATE TABLE manifest (id INTEGER PRIMARY KEY CHECK(id=1), application TEXT NOT NULL, configuration TEXT NOT NULL, fingerprint INTEGER NOT NULL CHECK(fingerprint=1)) STRICT",
    "CREATE TABLE events (position INTEGER PRIMARY KEY CHECK(position>0), stream TEXT NOT NULL, version INTEGER NOT NULL CHECK(version>0), data TEXT NOT NULL, UNIQUE(stream,version)) STRICT",
    "CREATE TABLE receipts (key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, receipt TEXT NOT NULL) STRICT",
    "CREATE TABLE projections (name TEXT PRIMARY KEY, state TEXT NOT NULL, checkpoint INTEGER NOT NULL CHECK(checkpoint>=0), revision INTEGER NOT NULL CHECK(revision>=0)) STRICT",
];
const APPLICATION_ID: i64 = 0x4d525a41;

pub(crate) struct SqliteStore {
    // Connection closes before the OS owner lock is released.
    connection: Mutex<Connection>,
    _owner: File,
    limits: Limits,
    poisoned: AtomicBool,
}
pub(crate) type Recovered = (BTreeMap<Key, Record>, BTreeMap<Identity, ProjectionState>);

fn invalid() -> Failure {
    Failure::permanent("storage_invalid")
}
fn sql(error: rusqlite::Error) -> Failure {
    use rusqlite::ErrorCode;
    match error.sqlite_error_code() {
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => {
            Failure::transient("storage_busy")
        }
        Some(ErrorCode::OperationInterrupted) => Failure::transient("timeout"),
        Some(ErrorCode::DiskFull) => Failure::transient("storage_full"),
        Some(ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase) => invalid(),
        _ => Failure::transient("storage_unavailable"),
    }
}
fn encode(value: &impl Serialize) -> Result<String, Failure> {
    serde_json::to_string(value).map_err(|_| invalid())
}
fn decode<T: DeserializeOwned>(text: &str, max: usize) -> Result<T, Failure> {
    if text.len() > max {
        return Err(invalid());
    }
    serde_json::from_str(text).map_err(|_| invalid())
}
fn check_json(value: &serde_json::Value, max: usize) -> Result<(), Failure> {
    compatible_json(value)?;
    if encode(value)?.len() > max {
        return Err(invalid());
    }
    Ok(())
}
fn configuration(config: &Config) -> Result<String, Failure> {
    let mut copy = config.clone();
    copy.storage = StorageConfig::Memory;
    encode(&copy)
}
/// Conservative page/index/row overhead, including retained fingerprints and
/// every projection. The rollback journal needs a further main-file budget.
fn file_budget(config: &Config) -> usize {
    let l = &config.limits;
    4 * (l.max_store_bytes
        + l.max_idempotency * (l.max_payload_bytes + 2048)
        + config.projections.len() * (l.max_state_bytes + 1024)
        + 1_048_576)
}
fn schema(connection: &Connection) -> Result<Vec<String>, Failure> {
    let mut stmt = connection
        .prepare("SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name LIMIT 5")
        .map_err(sql)?;
    stmt.query_map([], |r| r.get(0))
        .map_err(sql)?
        .collect::<Result<_, _>>()
        .map_err(sql)
}
fn expected_schema() -> Vec<String> {
    // SQL text in sqlite_schema preserves the definition supplied at bootstrap.
    let mut definitions: Vec<_> = SCHEMA.iter().map(|s| s.to_string()).collect();
    definitions.sort_by_key(|s| s.split_whitespace().nth(2).unwrap_or_default().to_owned());
    definitions
}
impl SqliteStore {
    pub(crate) fn open(
        config: &Config,
        path: &Path,
        application: &Identity,
        initialize: bool,
    ) -> Result<(Self, Recovered), Failure> {
        config.validate()?;
        if !path.is_absolute() {
            return Err(Failure::permanent("storage_path"));
        }
        let database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(initialize)
            .open(path)
            .map_err(|_| Failure::permanent("storage_open"))?;
        if !database_file.metadata().map_err(|_| invalid())?.is_file() {
            return Err(Failure::permanent("storage_path"));
        }
        let canonical = path.canonicalize().map_err(|_| invalid())?;
        let mut lock_path = canonical.as_os_str().to_os_string();
        lock_path.push(".owner");
        let lock_path = std::path::PathBuf::from(lock_path);
        if lock_path
            .symlink_metadata()
            .is_ok_and(|m| m.file_type().is_symlink())
        {
            return Err(Failure::permanent("storage_path"));
        }
        // A sidecar avoids interference with SQLite's own platform file locks.
        // Never unlink it, including during shutdown or after a crash.
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|_| Failure::permanent("storage_lock"))?;
        if !owner.metadata().map_err(|_| invalid())?.is_file() {
            return Err(Failure::permanent("storage_path"));
        }
        owner.try_lock().map_err(|e| match e {
            std::fs::TryLockError::WouldBlock => Failure::conflict("storage_owned"),
            _ => Failure::permanent("storage_lock"),
        })?;
        let metadata = database_file.metadata().map_err(|_| invalid())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let target = canonical.metadata().map_err(|_| invalid())?;
            if metadata.nlink() != 1
                || owner.metadata().map_err(|_| invalid())?.nlink() != 1
                || metadata.dev() != target.dev()
                || metadata.ino() != target.ino()
            {
                return Err(Failure::permanent("storage_path"));
            }
        }
        let budget = file_budget(config);
        if metadata.len() > budget as u64 {
            return Err(invalid());
        }
        let mut connection = Connection::open_with_flags(
            &canonical,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(sql)?;
        connection.busy_timeout(Duration::ZERO).map_err(sql)?;
        connection
            .set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH, 1_048_576)
            .map_err(sql)?;
        connection
            .set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_SQL_LENGTH, 65_536)
            .map_err(sql)?;
        let deadline = Instant::now() + Duration::from_millis(config.limits.timeout_ms);
        connection
            .progress_handler(1000, Some(move || Instant::now() >= deadline))
            .map_err(sql)?;
        if !initialize {
            let version: i64 = connection
                .pragma_query_value(None, "user_version", |r| r.get(0))
                .map_err(sql)?;
            let id: i64 = connection
                .pragma_query_value(None, "application_id", |r| r.get(0))
                .map_err(sql)?;
            if version != 1 || id != APPLICATION_ID || schema(&connection)? != expected_schema() {
                return Err(Failure::permanent("storage_schema"));
            }
        }
        // DELETE keeps recovery/closed-file backup simple and avoids unbounded WAL.
        let journal: String = connection
            .pragma_update_and_check(None, "journal_mode", "DELETE", |r| r.get(0))
            .map_err(sql)?;
        connection.execute_batch("PRAGMA synchronous=EXTRA; PRAGMA cache_size=-1024; PRAGMA temp_store=MEMORY; PRAGMA mmap_size=0; PRAGMA journal_size_limit=0; PRAGMA trusted_schema=OFF;").map_err(sql)?;
        let page_size: usize = connection
            .pragma_query_value(None, "page_size", |r| r.get(0))
            .map_err(sql)?;
        if page_size != 4096 {
            return Err(Failure::permanent("storage_schema"));
        }
        let pages = budget / page_size;
        let actual: usize = connection
            .pragma_update_and_check(None, "max_page_count", pages as i64, |r| r.get(0))
            .map_err(sql)?;
        let sync: i64 = connection
            .pragma_query_value(None, "synchronous", |r| r.get(0))
            .map_err(sql)?;
        if journal != "delete" || sync != 3 || actual != pages {
            return Err(invalid());
        }
        if initialize {
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql)?;
            for statement in SCHEMA {
                tx.execute_batch(statement).map_err(sql)?;
            }
            tx.execute(
                "INSERT INTO manifest VALUES (1,?1,?2,1)",
                params![application.as_str(), configuration(config)?],
            )
            .map_err(sql)?;
            for p in &config.projections {
                check_json(&p.initial, config.limits.max_state_bytes)?;
                tx.execute(
                    "INSERT INTO projections VALUES (?1,?2,0,0)",
                    params![p.name.as_str(), encode(&p.initial)?],
                )
                .map_err(sql)?;
            }
            tx.pragma_update(None, "user_version", 1).map_err(sql)?;
            tx.pragma_update(None, "application_id", APPLICATION_ID)
                .map_err(sql)?;
            tx.commit().map_err(sql)?;
            // Make the newly created directory entry durable too.
            File::open(canonical.parent().ok_or_else(invalid)?)
                .and_then(|f| f.sync_all())
                .map_err(|_| Failure::transient("storage_sync"))?;
        }
        let store = Self {
            connection: Mutex::new(connection),
            _owner: owner,
            limits: config.limits.clone(),
            poisoned: AtomicBool::new(false),
        };
        let recovered = store.recover(config, application, deadline)?;
        Ok((store, recovered))
    }
    fn lock(&self, deadline: Instant) -> Result<MutexGuard<'_, Connection>, Failure> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(Failure::transient("storage_reopen_required"));
        }
        if Instant::now() >= deadline {
            return Err(Failure::transient("timeout"));
        }
        let connection = self
            .connection
            .try_lock()
            .map_err(|_| Failure::transient("storage_busy"))?;
        if !connection.is_autocommit() {
            self.poisoned.store(true, Ordering::Release);
            return Err(Failure::transient("storage_reopen_required"));
        }
        connection
            .progress_handler(1000, Some(move || Instant::now() >= deadline))
            .map_err(sql)?;
        Ok(connection)
    }
    fn recover(
        &self,
        config: &Config,
        application: &Identity,
        deadline: Instant,
    ) -> Result<Recovered, Failure> {
        let mut conn = self.lock(deadline)?;
        let tx = conn.transaction().map_err(sql)?;
        let integrity: String = tx
            .pragma_query_value(None, "quick_check", |r| r.get(0))
            .map_err(sql)?;
        if integrity != "ok" {
            return Err(invalid());
        }
        let (app, configuration_text, fingerprint): (String, String, i64) = tx
            .query_row(
                "SELECT application,configuration,fingerprint FROM manifest WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(sql)?;
        if app != application.as_str()
            || configuration_text != configuration(config)?
            || fingerprint != 1
        {
            return Err(Failure::permanent("storage_configuration"));
        }
        let (count, bytes): (usize, usize) = tx
            .query_row(
                "SELECT count(*),coalesce(sum(length(CAST(data AS BLOB))),0) FROM events",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sql)?;
        if count > self.limits.max_events || bytes > self.limits.max_store_bytes {
            return Err(invalid());
        }
        let mut versions = BTreeMap::new();
        let mut history = Vec::with_capacity(count);
        {
            let mut stmt = tx
                .prepare("SELECT position,stream,version,data FROM events ORDER BY position")
                .map_err(sql)?;
            let mut rows = stmt.query([]).map_err(sql)?;
            while let Some(row) = rows.next().map_err(sql)? {
                let text: String = row.get(3).map_err(sql)?;
                let event: StoredEvent = decode(&text, self.limits.max_payload_bytes + 4096)?;
                let position: u64 = row.get(0).map_err(sql)?;
                let stream: String = row.get(1).map_err(sql)?;
                let version: u64 = row.get(2).map_err(sql)?;
                let previous = versions.get(&event.stream).copied().unwrap_or(0_u64);
                if event.position != history.len() as u64 + 1
                    || event.position != position
                    || event.stream.as_str() != stream
                    || event.version != previous + 1
                    || event.version != version
                    || event.event.schema_version == 0
                    || event.event_id.as_str() != format!("event/{position}")
                {
                    return Err(invalid());
                }
                check_json(&event.event.payload, self.limits.max_payload_bytes)?;
                versions.insert(event.stream.clone(), version);
                history.push(event);
            }
        }
        if versions.len() > self.limits.max_streams {
            return Err(invalid());
        }
        let receipt_count: usize = tx
            .query_row("SELECT count(*) FROM receipts", [], |r| r.get(0))
            .map_err(sql)?;
        if receipt_count > self.limits.max_idempotency {
            return Err(invalid());
        }
        let mut records = BTreeMap::new();
        {
            let mut stmt = tx
                .prepare("SELECT key,fingerprint,receipt FROM receipts")
                .map_err(sql)?;
            let mut rows = stmt.query([]).map_err(sql)?;
            while let Some(row) = rows.next().map_err(sql)? {
                let key_text: String = row.get(0).map_err(sql)?;
                let key: Key = decode(&key_text, 1024)?;
                let payload: String = row.get(1).map_err(sql)?;
                let fingerprint: serde_json::Value =
                    decode(&payload, self.limits.max_payload_bytes + 1024)?;
                let receipt_text: String = row.get(2).map_err(sql)?;
                let receipt: Receipt = decode(&receipt_text, 1024)?;
                if encode(&key)? != key_text
                    || !config.commands.iter().any(|r| r.tag == key.tag)
                    || crate::application::canonical(&fingerprint)? != payload
                    || fingerprint.as_object().is_none_or(|o| o.len() != 3)
                    || !(fingerprint["expected_version"].is_null()
                        || fingerprint["expected_version"].as_u64().is_some())
                {
                    return Err(invalid());
                }
                let version = history
                    .iter()
                    .take(receipt.position as usize)
                    .filter(|e| e.stream == receipt.stream)
                    .count() as u64;
                if fingerprint.get("stream") != Some(&serde_json::json!(receipt.stream))
                    || fingerprint.get("expected_version").is_none()
                    || fingerprint.get("payload").is_none()
                    || receipt.position > count as u64
                    || receipt.version != version
                    || receipt.events > self.limits.batch_size
                    || receipt.events as u64 > receipt.version
                    || (receipt.events > 0
                        && history
                            .get(receipt.position.saturating_sub(1) as usize)
                            .is_none_or(|e| e.stream != receipt.stream))
                {
                    return Err(invalid());
                }
                check_json(&fingerprint["payload"], self.limits.max_payload_bytes)?;
                if fingerprint["expected_version"]
                    .as_u64()
                    .is_some_and(|v| v != receipt.version - receipt.events as u64)
                    || history
                        .iter()
                        .take(receipt.position as usize)
                        .rev()
                        .take(receipt.events)
                        .any(|e| e.stream != receipt.stream || e.metadata.actor != key.actor)
                {
                    return Err(invalid());
                }
                records.insert(
                    key,
                    Record {
                        payload,
                        result: Some(receipt),
                    },
                );
            }
        }
        let mut projections = BTreeMap::new();
        {
            let mut stmt = tx
                .prepare("SELECT name,state,checkpoint,revision FROM projections")
                .map_err(sql)?;
            let mut rows = stmt.query([]).map_err(sql)?;
            while let Some(row) = rows.next().map_err(sql)? {
                if projections.len() >= config.projections.len() {
                    return Err(invalid());
                }
                let name = Identity::new(row.get::<_, String>(0).map_err(sql)?)?;
                if !config.projections.iter().any(|p| p.name == name) {
                    return Err(invalid());
                }
                let text: String = row.get(1).map_err(sql)?;
                let state = decode(&text, self.limits.max_state_bytes)?;
                check_json(&state, self.limits.max_state_bytes)?;
                let checkpoint: u64 = row.get(2).map_err(sql)?;
                let revision: u64 = row.get(3).map_err(sql)?;
                if checkpoint > count as u64 || revision > i64::MAX as u64 {
                    return Err(invalid());
                }
                projections.insert(name, ProjectionState { state, checkpoint });
            }
        }
        if projections.len() != config.projections.len() {
            return Err(invalid());
        }
        tx.commit().map_err(sql)?;
        Ok((records, projections))
    }
    pub(crate) fn load(
        &self,
        stream: &Identity,
        deadline: Instant,
    ) -> Result<Vec<StoredEvent>, Failure> {
        let conn = self.lock(deadline)?;
        self.events(
            &conn,
            "SELECT data FROM events WHERE stream=?1 ORDER BY version",
            stream.as_str(),
            self.limits.max_events,
        )
    }
    fn events(
        &self,
        conn: &Connection,
        statement: &str,
        parameter: impl rusqlite::ToSql,
        max: usize,
    ) -> Result<Vec<StoredEvent>, Failure> {
        let mut stmt = conn.prepare(statement).map_err(sql)?;
        let mut rows = stmt.query([parameter]).map_err(sql)?;
        let mut events = Vec::new();
        while let Some(row) = rows.next().map_err(sql)? {
            if events.len() >= max {
                return Err(invalid());
            }
            let text: String = row.get(0).map_err(sql)?;
            let event: StoredEvent = decode(&text, self.limits.max_payload_bytes + 4096)?;
            check_json(&event.event.payload, self.limits.max_payload_bytes)?;
            events.push(event);
        }
        Ok(events)
    }
    pub(crate) fn head(&self, deadline: Instant) -> Result<u64, Failure> {
        let conn = self.lock(deadline)?;
        head(&conn)
    }
    pub(crate) fn read_all(
        &self,
        after: u64,
        limit: usize,
        deadline: Instant,
    ) -> Result<Vec<StoredEvent>, Failure> {
        if limit == 0 || limit > 64 {
            return Err(Failure::permanent("invalid_batch"));
        }
        let conn = self.lock(deadline)?;
        self.events(
            &conn,
            &format!("SELECT data FROM events WHERE position>?1 ORDER BY position LIMIT {limit}"),
            i64::try_from(after).map_err(|_| invalid())?,
            limit,
        )
    }
    pub(crate) fn commit_command(
        &self,
        stream: &Identity,
        expected: u64,
        events: Vec<Event>,
        metadata: &Metadata,
        claim: Option<(&Key, &str)>,
        deadline: Instant,
    ) -> Result<Receipt, Failure> {
        let mut conn = self.lock(deadline)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        if let Some((key, fingerprint)) = claim {
            if let Some((stored, receipt)) = tx
                .query_row(
                    "SELECT fingerprint,receipt FROM receipts WHERE key=?1",
                    [encode(key)?],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(sql)?
            {
                if stored != fingerprint {
                    return Err(Failure::conflict("idempotency_mismatch"));
                }
                return decode(&receipt, 1024);
            }
            let count: usize = tx
                .query_row("SELECT count(*) FROM receipts", [], |r| r.get(0))
                .map_err(sql)?;
            if count >= self.limits.max_idempotency {
                return Err(Failure::transient("idempotency_capacity"));
            }
        }
        let version: u64 = tx
            .query_row(
                "SELECT coalesce(max(version),0) FROM events WHERE stream=?1",
                [stream.as_str()],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if version != expected {
            return Err(Failure::conflict("concurrency_conflict"));
        }
        let position = head(&tx)?;
        if events.len() > self.limits.batch_size {
            return Err(Failure::permanent("event_batch_limit"));
        }
        let streams: usize = tx
            .query_row("SELECT count(DISTINCT stream) FROM events", [], |r| {
                r.get(0)
            })
            .map_err(sql)?;
        if events.len() > self.limits.max_events.saturating_sub(position as usize)
            || (!events.is_empty() && version == 0 && streams >= self.limits.max_streams)
        {
            return Err(Failure::transient("store_capacity"));
        }
        let used: usize = tx
            .query_row(
                "SELECT coalesce(sum(length(CAST(data AS BLOB))),0) FROM events",
                [],
                |r| r.get(0),
            )
            .map_err(sql)?;
        let accepted_unix_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| invalid())?
                .as_millis(),
        )
        .map_err(|_| invalid())?;
        let mut batch = Vec::with_capacity(events.len());
        let mut bytes = 0;
        for (i, event) in events.into_iter().enumerate() {
            if event.schema_version == 0 {
                return Err(Failure::permanent("invalid_event"));
            }
            compatible_json(&event.payload)?;
            if encode(&event.payload)?.len() > self.limits.max_payload_bytes {
                return Err(Failure::permanent("payload_limit"));
            }
            let stored = StoredEvent {
                event_id: Identity::new(format!("event/{}", position + i as u64 + 1))?,
                stream: stream.clone(),
                version: expected + i as u64 + 1,
                position: position + i as u64 + 1,
                event,
                metadata: metadata.clone(),
                accepted_unix_ms,
            };
            let data = encode(&stored)?;
            bytes += data.len();
            batch.push((stored, data));
        }
        if bytes > self.limits.max_store_bytes.saturating_sub(used) {
            return Err(Failure::transient("store_capacity"));
        }
        let receipt = Receipt {
            stream: stream.clone(),
            version: expected + batch.len() as u64,
            position: position + batch.len() as u64,
            events: batch.len(),
        };
        for (event, data) in batch {
            tx.execute(
                "INSERT INTO events VALUES (?1,?2,?3,?4)",
                params![event.position, event.stream.as_str(), event.version, data],
            )
            .map_err(sql)?;
        }
        #[cfg(test)]
        crash_cut("after_events");
        if let Some((key, fingerprint)) = claim {
            tx.execute(
                "INSERT INTO receipts VALUES (?1,?2,?3)",
                params![encode(key)?, fingerprint, encode(&receipt)?],
            )
            .map_err(sql)?;
        }
        #[cfg(test)]
        crash_cut("after_receipt");
        self.commit(tx, deadline)?;
        #[cfg(test)]
        crash_cut("after_commit");
        Ok(receipt)
    }
    fn commit(&self, tx: rusqlite::Transaction<'_>, deadline: Instant) -> Result<(), Failure> {
        if Instant::now() >= deadline {
            return Err(Failure::transient("timeout"));
        }
        // Commit errors can have uncertain outcomes. Drop rolls back any live
        // transaction; block further use until verified reopen reconciles it.
        tx.commit().map_err(|_| {
            self.poisoned.store(true, Ordering::Release);
            Failure::transient("commit_outcome_unknown")
        })
    }
    pub(crate) fn projection(
        &self,
        name: &Identity,
        deadline: Instant,
    ) -> Result<(ProjectionState, i64), Failure> {
        let conn = self.lock(deadline)?;
        let (text, checkpoint, revision): (String, u64, i64) = conn
            .query_row(
                "SELECT state,checkpoint,revision FROM projections WHERE name=?1",
                [name.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(sql)?;
        let state = decode(&text, self.limits.max_state_bytes)?;
        check_json(&state, self.limits.max_state_bytes)?;
        Ok((ProjectionState { state, checkpoint }, revision))
    }
    pub(crate) fn commit_projection(
        &self,
        name: &Identity,
        revision: i64,
        state: &ProjectionState,
        deadline: Instant,
    ) -> Result<i64, Failure> {
        check_json(&state.state, self.limits.max_state_bytes)?;
        let next = revision.checked_add(1).ok_or_else(invalid)?;
        let mut conn = self.lock(deadline)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        if state.checkpoint > head(&tx)? {
            return Err(invalid());
        }
        let changed = tx.execute("UPDATE projections SET state=?1,checkpoint=?2,revision=?3 WHERE name=?4 AND revision=?5", params![encode(&state.state)?,state.checkpoint,next,name.as_str(),revision]).map_err(sql)?;
        if changed != 1 {
            return Err(Failure::conflict("projection_conflict"));
        }
        #[cfg(test)]
        crash_cut("projection_before_commit");
        self.commit(tx, deadline)?;
        #[cfg(test)]
        crash_cut("projection_after_commit");
        Ok(next)
    }
}
#[cfg(test)]
fn crash_cut(point: &str) {
    if std::env::var("MEREZHA_SQLITE_CRASH_CUT").as_deref() == Ok(point) {
        let marker = std::env::var_os("MEREZHA_SQLITE_CRASH_MARKER").expect("test marker");
        std::fs::write(marker, point).expect("write crash cut marker");
        loop {
            std::thread::park_timeout(Duration::from_secs(1));
        }
    }
}
fn head(conn: &Connection) -> Result<u64, Failure> {
    conn.query_row("SELECT coalesce(max(position),0) FROM events", [], |r| {
        r.get(0)
    })
    .map_err(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AllowAll, Application, Guest, GuestSession};
    use merezha_domain::{Command, Operation, Reply};
    use serde_json::json;
    use std::{
        process::{Child, Command as Process, Stdio},
        sync::Arc,
    };

    fn id(s: &str) -> Identity {
        Identity::new(s).unwrap()
    }
    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }
    fn config(path: &Path, initialize: bool) -> Config {
        let mut c = Config::parse(
            r#"version=1
[[commands]]
tag="Add"
service="counter"
aggregate="Counter"
[[projections]]
name="total"
service="counter"
initial=0
"#,
        )
        .unwrap();
        c.storage = StorageConfig::Sqlite {
            path: path.into(),
            application: id("counter-v1"),
            initialize,
        };
        c
    }
    fn command(key: &str) -> Command {
        serde_json::from_value(json!({"tag":"Add","stream":"one","payload":3,"idempotency_key":key,"metadata":{"message_id":"m1","correlation_id":"c1","actor":"alice"}})).unwrap()
    }
    #[derive(Clone)]
    struct Counter;
    impl Guest for Counter {
        fn services(&self) -> Result<Vec<String>, Failure> {
            Ok(vec!["counter".into()])
        }
        fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
            Ok(Box::new(self.clone()))
        }
    }
    impl GuestSession for Counter {
        fn invoke(&self, _: &str, op: Operation) -> Result<Reply, Failure> {
            Ok(match op {
                Operation::Validate { payload, .. } => Reply::Validated { payload },
                Operation::Decide { payload, .. } => Reply::Decided {
                    events: vec![Event {
                        tag: id("Added"),
                        schema_version: 1,
                        payload,
                    }],
                },
                Operation::Project { state, events, .. } => Reply::Projected {
                    state: json!(
                        state.as_i64().unwrap()
                            + events
                                .iter()
                                .map(|e| e.event.payload.as_i64().unwrap())
                                .sum::<i64>()
                    ),
                },
                Operation::Query { state, .. } => Reply::Queried { value: state },
            })
        }
    }
    fn open_app(path: &Path, initialize: bool) -> Application {
        Application::open(
            config(path, initialize),
            Arc::new(Counter),
            Arc::new(AllowAll),
        )
        .unwrap()
    }
    fn open_store(path: &Path, initialize: bool) -> (SqliteStore, Recovered) {
        SqliteStore::open(
            &config(path, initialize),
            path,
            &id("counter-v1"),
            initialize,
        )
        .unwrap()
    }
    fn event() -> Event {
        Event {
            tag: id("Added"),
            schema_version: 1,
            payload: json!(3),
        }
    }
    fn metadata() -> Metadata {
        command("k").metadata
    }
    fn key() -> Key {
        Key {
            tag: id("Add"),
            actor: Some(id("alice")),
            key: id("k"),
        }
    }
    fn fingerprint() -> String {
        crate::application::canonical(
            &json!({"stream":"Counter/one","payload":3,"expected_version":null}),
        )
        .unwrap()
    }
    // Deliberate adapter-level injection bypasses the production owner gate to
    // prove the SQL CAS against an independently opened connection.
    fn independent(store: &SqliteStore, path: &Path) -> SqliteStore {
        let connection = Connection::open(path).unwrap();
        connection.busy_timeout(Duration::ZERO).unwrap();
        SqliteStore {
            connection: Mutex::new(connection),
            _owner: store._owner.try_clone().unwrap(),
            limits: store.limits.clone(),
            poisoned: AtomicBool::new(false),
        }
    }
    #[test]
    fn independent_connections_recheck_stream_key_and_projection_row_revision() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (store, _) = open_store(&path, true);
        let other = independent(&store, &path);
        let stream = id("Counter/one");
        let key = key();
        let fp = fingerprint();
        let first = store
            .commit_command(
                &stream,
                0,
                vec![event()],
                &metadata(),
                Some((&key, &fp)),
                deadline(),
            )
            .unwrap();
        assert_eq!(
            other
                .commit_command(&stream, 0, vec![event()], &metadata(), None, deadline())
                .unwrap_err()
                .code,
            "concurrency_conflict"
        );
        assert_eq!(
            other
                .commit_command(
                    &stream,
                    0,
                    vec![event()],
                    &metadata(),
                    Some((&key, &fp)),
                    deadline()
                )
                .unwrap(),
            first
        );
        assert_eq!(other.head(deadline()).unwrap(), 1);
        let name = id("total");
        let (_, stale) = other.projection(&name, deadline()).unwrap();
        let state = ProjectionState {
            state: json!(3),
            checkpoint: 1,
        };
        let revision = store
            .commit_projection(&name, 0, &state, deadline())
            .unwrap();
        assert_eq!(
            other
                .commit_projection(&name, stale, &state, deadline())
                .unwrap_err()
                .code,
            "projection_conflict"
        );
        // A successful rebuild can publish at the SAME checkpoint. Revision
        // still changes, so a candidate computed before it cannot overwrite it.
        store
            .commit_projection(&name, revision, &state, deadline())
            .unwrap();
        assert_eq!(
            other
                .commit_projection(
                    &name,
                    revision,
                    &ProjectionState {
                        state: json!(99),
                        checkpoint: 1
                    },
                    deadline()
                )
                .unwrap_err()
                .code,
            "projection_conflict"
        );
        assert_eq!(store.projection(&name, deadline()).unwrap().0, state);
    }
    #[test]
    fn statement_failure_rolls_back_events_receipt_position_and_projection_pair() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (store, _) = open_store(&path, true);
        let external = Connection::open(&path).unwrap();
        external.execute_batch("CREATE TRIGGER reject_receipt BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT,'injected'); END").unwrap();
        assert!(
            store
                .commit_command(
                    &id("Counter/one"),
                    0,
                    vec![event()],
                    &metadata(),
                    Some((&key(), &fingerprint())),
                    deadline()
                )
                .is_err()
        );
        assert_eq!(store.head(deadline()).unwrap(), 0);
        assert_eq!(
            external
                .query_row("SELECT count(*) FROM receipts", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        external
            .execute_batch("DROP TRIGGER reject_receipt")
            .unwrap();
        let receipt = store
            .commit_command(
                &id("Counter/one"),
                0,
                vec![event()],
                &metadata(),
                Some((&key(), &fingerprint())),
                deadline(),
            )
            .unwrap();
        assert_eq!((receipt.position, receipt.version), (1, 1));
        external.execute_batch("CREATE TRIGGER reject_checkpoint BEFORE UPDATE OF checkpoint ON projections BEGIN SELECT RAISE(ABORT,'injected'); END").unwrap();
        assert!(
            store
                .commit_projection(
                    &id("total"),
                    0,
                    &ProjectionState {
                        state: json!(3),
                        checkpoint: 1
                    },
                    deadline()
                )
                .is_err()
        );
        assert_eq!(
            store.projection(&id("total"), deadline()).unwrap().0,
            ProjectionState {
                state: json!(0),
                checkpoint: 0
            }
        );
        external
            .execute_batch("DROP TRIGGER reject_checkpoint")
            .unwrap();
        drop(store);
        let (reopened, records) = open_store(&path, false);
        assert_eq!(records.0.len(), 1);
        assert_eq!(
            reopened
                .projection(&id("total"), deadline())
                .unwrap()
                .0
                .checkpoint,
            0
        );
        reopened
            .commit_projection(
                &id("total"),
                0,
                &ProjectionState {
                    state: json!(3),
                    checkpoint: 1,
                },
                deadline(),
            )
            .unwrap();
    }
    #[test]
    fn busy_deadline_capacity_and_commit_failure_do_not_strand_a_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (store, _) = open_store(&path, true);
        let external = Connection::open(&path).unwrap();
        external.execute_batch("BEGIN IMMEDIATE").unwrap();
        let started = Instant::now();
        assert_eq!(
            store
                .commit_command(
                    &id("Counter/one"),
                    0,
                    vec![event()],
                    &metadata(),
                    None,
                    deadline()
                )
                .unwrap_err()
                .code,
            "storage_busy"
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        external.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            store
                .load(&id("Counter/one"), Instant::now())
                .unwrap_err()
                .code,
            "timeout"
        );
        store
            .connection
            .lock()
            .unwrap()
            .commit_hook(Some(|| true))
            .unwrap();
        assert_eq!(
            store
                .commit_command(
                    &id("Counter/one"),
                    0,
                    vec![event()],
                    &metadata(),
                    None,
                    deadline()
                )
                .unwrap_err()
                .code,
            "commit_outcome_unknown"
        );
        assert!(store.connection.lock().unwrap().is_autocommit());
        assert_eq!(
            store.head(deadline()).unwrap_err().code,
            "storage_reopen_required"
        );
        drop(store);
        let (reopened, _) = open_store(&path, false);
        assert_eq!(reopened.head(deadline()).unwrap(), 0);
        let conn = reopened.connection.lock().unwrap();
        let pages: i64 = conn
            .pragma_query_value(None, "page_count", |r| r.get(0))
            .unwrap();
        conn.pragma_update(None, "max_page_count", pages).unwrap();
        drop(conn);
        let large = Event {
            payload: json!("x".repeat(16000)),
            ..event()
        };
        assert_eq!(
            reopened
                .commit_command(
                    &id("Counter/one"),
                    0,
                    vec![large],
                    &metadata(),
                    None,
                    deadline()
                )
                .unwrap_err()
                .code,
            "storage_full"
        );
        assert_eq!(reopened.head(deadline()).unwrap(), 0);
    }
    #[test]
    fn reopen_rejects_schema_configuration_and_recovered_bound_drift_without_reset() {
        for mutation in [
            "PRAGMA user_version=2",
            "CREATE TABLE alien (x TEXT)",
            "UPDATE manifest SET fingerprint=2",
            "UPDATE projections SET checkpoint=1",
            "UPDATE projections SET state='9999999999999999999999999999'",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            drop(open_store(&path, true));
            Connection::open(&path)
                .unwrap()
                .execute_batch(mutation)
                .unwrap_or_else(|_| {
                    // The schema CHECK may itself reject a corrupt fingerprint.
                    Connection::open(&path)
                        .unwrap()
                        .execute_batch(
                            "PRAGMA ignore_check_constraints=ON; UPDATE manifest SET fingerprint=2",
                        )
                        .unwrap();
                });
            let before = std::fs::read(&path).unwrap();
            assert!(
                SqliteStore::open(&config(&path, false), &path, &id("counter-v1"), false).is_err(),
                "{mutation}"
            );
            assert_eq!(std::fs::read(&path).unwrap(), before, "{mutation}");
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        drop(open_store(&path, true));
        let before = std::fs::read(&path).unwrap();
        let mut changed = config(&path, false);
        changed.projections[0].revision = 2;
        assert!(SqliteStore::open(&changed, &path, &id("counter-v1"), false).is_err());
        assert!(
            SqliteStore::open(
                &config(&path, false),
                &path,
                &id("other-application"),
                false
            )
            .is_err()
        );
        assert!(SqliteStore::open(&config(&path, true), &path, &id("counter-v1"), true).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let empty = dir.path().join("empty");
        File::create(&empty).unwrap();
        assert!(
            SqliteStore::open(&config(&empty, false), &empty, &id("counter-v1"), false).is_err()
        );
        assert_eq!(empty.metadata().unwrap().len(), 0);
    }
    struct OwnedChild(Child);
    impl Drop for OwnedChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    #[test]
    #[ignore = "subprocess entry point, invoked only by crash recovery test"]
    fn crash_worker() {
        let path = std::env::var_os("MEREZHA_SQLITE_CRASH_DATABASE").expect("child database");
        let app = open_app(Path::new(&path), false);
        if std::env::var("MEREZHA_SQLITE_CRASH_ACTION").as_deref() == Ok("rebuild") {
            app.rebuild("total").unwrap();
        } else {
            app.command(command("k")).unwrap();
        }
        panic!("crash cut was not reached");
    }
    #[test]
    fn killed_process_recovers_each_command_transaction_cut_and_releases_owner() {
        for cut in ["after_events", "after_receipt", "after_commit"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let marker = dir.path().join("cut");
            drop(open_app(&path, true));
            let mut child = OwnedChild(
                Process::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "sqlite::tests::crash_worker",
                        "--ignored",
                        "--nocapture",
                    ])
                    .env("MEREZHA_SQLITE_CRASH_DATABASE", &path)
                    .env("MEREZHA_SQLITE_CRASH_CUT", cut)
                    .env("MEREZHA_SQLITE_CRASH_MARKER", &marker)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap(),
            );
            let until = Instant::now() + Duration::from_secs(10);
            while !marker.exists() {
                assert!(Instant::now() < until, "child failed to reach {cut}");
                assert!(
                    child.0.try_wait().unwrap().is_none(),
                    "child exited before {cut}"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            let owned =
                Application::open(config(&path, false), Arc::new(Counter), Arc::new(AllowAll))
                    .err()
                    .unwrap();
            assert_eq!(owned.code, "storage_owned");
            child.0.kill().unwrap();
            let status = child.0.wait().unwrap();
            assert!(!status.success());
            let (store, (records, _)) = open_store(&path, false);
            let committed = cut == "after_commit";
            assert_eq!(
                store.head(deadline()).unwrap(),
                u64::from(committed),
                "{cut}"
            );
            assert_eq!(records.len(), usize::from(committed), "{cut}");
            drop(store);
            let app = open_app(&path, false);
            let receipt = app.command(command("k")).unwrap();
            assert_eq!((receipt.position, receipt.version), (1, 1), "{cut}");
            assert_eq!(app.command(command("k")).unwrap(), receipt);
            assert_eq!(
                app.catch_up("total").unwrap(),
                ProjectionState {
                    state: json!(3),
                    checkpoint: 1
                }
            );
            drop(app);
            let (store, _) = open_store(&path, false);
            assert_eq!(store.head(deadline()).unwrap(), 1);
            eprintln!(
                "crash cut {cut}: child={status}; recovered events/receipts={}; retry position=1",
                u8::from(committed)
            );
        }
    }
    #[test]
    fn killed_rebuild_publishes_both_state_and_checkpoint_or_preserves_both() {
        for cut in ["projection_before_commit", "projection_after_commit"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let marker = dir.path().join("cut");
            let app = open_app(&path, true);
            app.command(command("k")).unwrap();
            drop(app);
            let mut child = OwnedChild(
                Process::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "sqlite::tests::crash_worker",
                        "--ignored",
                        "--nocapture",
                    ])
                    .env("MEREZHA_SQLITE_CRASH_DATABASE", &path)
                    .env("MEREZHA_SQLITE_CRASH_CUT", cut)
                    .env("MEREZHA_SQLITE_CRASH_MARKER", &marker)
                    .env("MEREZHA_SQLITE_CRASH_ACTION", "rebuild")
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap(),
            );
            let until = Instant::now() + Duration::from_secs(10);
            while !marker.exists() {
                assert!(Instant::now() < until, "child failed to reach {cut}");
                assert!(
                    child.0.try_wait().unwrap().is_none(),
                    "child exited before {cut}"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            child.0.kill().unwrap();
            let status = child.0.wait().unwrap();
            let (store, _) = open_store(&path, false);
            let pair = store.projection(&id("total"), deadline()).unwrap().0;
            let committed = cut == "projection_after_commit";
            assert_eq!(
                pair,
                ProjectionState {
                    state: json!(if committed { 3 } else { 0 }),
                    checkpoint: u64::from(committed)
                },
                "{cut}"
            );
            drop(store);
            assert_eq!(
                open_app(&path, false).rebuild("total").unwrap(),
                ProjectionState {
                    state: json!(3),
                    checkpoint: 1
                }
            );
            eprintln!(
                "rebuild cut {cut}: child={status}; recovered checkpoint={}",
                u64::from(committed)
            );
        }
    }
    #[test]
    fn recovered_event_invariants_and_receipt_counts_are_checked_before_readiness() {
        for mutation in ["version", "payload", "schema", "position", "receipt_count"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut c = config(&path, true);
            c.limits.max_payload_bytes = 8;
            c.limits.max_idempotency = 1;
            let app = Application::open(c.clone(), Arc::new(Counter), Arc::new(AllowAll)).unwrap();
            app.command(command("k")).unwrap();
            drop(app);
            let conn = Connection::open(&path).unwrap();
            match mutation {
                "version" => {
                    conn.execute("UPDATE events SET version=2", []).unwrap();
                }
                "position" => {
                    conn.execute("UPDATE events SET position=2", []).unwrap();
                }
                "receipt_count" => {
                    conn.execute("INSERT INTO receipts SELECT replace(key,'\"k\"','\"other\"'),fingerprint,receipt FROM receipts",[]).unwrap();
                }
                _ => {
                    let text: String = conn
                        .query_row("SELECT data FROM events", [], |r| r.get(0))
                        .unwrap();
                    let mut value: serde_json::Value = serde_json::from_str(&text).unwrap();
                    if mutation == "schema" {
                        value["event"]["schema_version"] = json!(0);
                    } else {
                        value["event"]["payload"] = json!("x".repeat(9));
                    }
                    conn.execute("UPDATE events SET data=?1", [value.to_string()])
                        .unwrap();
                }
            }
            drop(conn);
            let before = std::fs::read(&path).unwrap();
            c.storage = StorageConfig::Sqlite {
                path: path.clone(),
                application: id("counter-v1"),
                initialize: false,
            };
            assert!(
                Application::open(c, Arc::new(Counter), Arc::new(AllowAll)).is_err(),
                "{mutation}"
            );
            assert_eq!(std::fs::read(&path).unwrap(), before, "{mutation}");
        }
    }
    #[test]
    fn closed_backup_restore_retains_exact_events_metadata_and_projection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let restored = dir.path().join("restore");
        let app = open_app(&path, true);
        let receipt = app.command(command("k")).unwrap();
        app.catch_up("total").unwrap();
        drop(app);
        let (store, _) = open_store(&path, false);
        let events = store.load(&id("Counter/one"), deadline()).unwrap();
        drop(store);
        std::fs::copy(&path, &restored).unwrap();
        let (store, (_, models)) = open_store(&restored, false);
        assert_eq!(store.load(&id("Counter/one"), deadline()).unwrap(), events);
        assert_eq!(
            models[&id("total")],
            ProjectionState {
                state: json!(3),
                checkpoint: 1
            }
        );
        drop(store);
        let app = open_app(&restored, false);
        assert_eq!(app.command(command("k")).unwrap(), receipt);
        assert_eq!(app.command(command("second")).unwrap().position, 2);
    }
    #[cfg(unix)]
    #[test]
    fn canonical_aliases_share_owner_and_hardlinks_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let alias = dir.path().join("alias");
        let (store, _) = open_store(&path, true);
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        let error = SqliteStore::open(&config(&alias, false), &alias, &id("counter-v1"), false)
            .err()
            .unwrap();
        assert_eq!(error.code, "storage_owned");
        drop(store);
        drop(open_store(&alias, false));
        let linked = dir.path().join("hardlink");
        std::fs::hard_link(&path, &linked).unwrap();
        let error = SqliteStore::open(&config(&linked, false), &linked, &id("counter-v1"), false)
            .err()
            .unwrap();
        assert_eq!(error.code, "storage_path");
    }
}
