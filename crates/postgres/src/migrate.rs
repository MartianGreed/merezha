//! Forward-only module migrations recorded in `merezha.migrations`.
//!
//! Apply mode takes a session advisory lock on a connection removed from the
//! pool, polling for it so a waiting runner holds no snapshot, so concurrent
//! hosts serialize and the lock is released when that
//! connection closes, even if the caller is cancelled. Each pending
//! transactional migration runs with its ledger row in one transaction. A
//! non-transactional step runs its single statement outside any transaction
//! and is recorded only after it succeeds. Verify mode takes no lock and reads
//! in one read-only transaction on a pooled session.
use crate::{Database, classify};
use merezha_domain::Failure;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt::Write as _, time::Instant};

/// Advisory lock key for the migration ledger: the ASCII bytes of `merezha1`.
pub const MIGRATIONS_LOCK: i64 = 0x6d65_7265_7a68_6131;
const MAX_MIGRATIONS: usize = 1024;
const MAX_SQL_BYTES: usize = 1_048_576;
const MAX_MODULES: usize = 64;

/// How the runner executes a migration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MigrationKind {
    /// The SQL and its ledger row commit together in one transaction. The
    /// SQL may contain several statements and must not manage transactions.
    #[default]
    Transactional,
    /// One statement run outside any transaction, for statements PostgreSQL
    /// refuses inside one, such as `CREATE INDEX CONCURRENTLY`. The ledger
    /// row is written only after the statement succeeded, so a failed or
    /// interrupted step runs again on the next apply and must be safe to
    /// repeat: `CREATE INDEX CONCURRENTLY` must say `IF NOT EXISTS`, and an
    /// invalid index left by an earlier failed build is dropped and rebuilt.
    NonTransactional,
}

/// One forward-only schema change. `version` orders a module's migrations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub sql: &'static str,
    pub kind: MigrationKind,
}
impl Migration {
    /// A transactional migration: `sql` and the ledger row commit together.
    pub const fn new(version: u32, name: &'static str, sql: &'static str) -> Self {
        Self {
            version,
            name,
            sql,
            kind: MigrationKind::Transactional,
        }
    }
    /// A declared non-transactional step: exactly one statement, run outside
    /// a transaction under the run's advisory lock, recorded after success.
    ///
    /// ```
    /// use merezha_postgres::Migration;
    /// const BY_TITLE: Migration = Migration::non_transactional(
    ///     3,
    ///     "entries_by_title",
    ///     "CREATE INDEX CONCURRENTLY IF NOT EXISTS entries_by_title ON content.entries (title)",
    /// );
    /// ```
    pub const fn non_transactional(version: u32, name: &'static str, sql: &'static str) -> Self {
        Self {
            version,
            name,
            sql,
            kind: MigrationKind::NonTransactional,
        }
    }
}
/// A module's declared migrations in strictly increasing version order.
#[derive(Clone, Copy, Debug)]
pub struct ModuleMigrations<'a> {
    pub module: &'a str,
    pub migrations: &'a [Migration],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationMode {
    /// Create the ledger if needed and apply every pending migration.
    Apply,
    /// Check applied migrations and report pending ones without writing.
    Verify,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingMigration {
    pub module: String,
    pub version: u32,
    pub name: &'static str,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// Migrations this run applied, in order.
    pub applied: Vec<PendingMigration>,
    /// Declared migrations not applied yet. Always empty after `Apply`.
    pub pending: Vec<PendingMigration>,
}
impl MigrationReport {
    pub fn is_current(&self) -> bool {
        self.pending.is_empty()
    }
}
/// A classified failure plus the module and version it concerns. Never
/// carries SQL or server text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationError {
    pub failure: Failure,
    pub module: Option<String>,
    pub version: Option<u32>,
}
impl MigrationError {
    fn new(failure: Failure) -> Self {
        Self {
            failure,
            module: None,
            version: None,
        }
    }
    fn at(code: &str, module: &str, version: Option<u32>) -> Self {
        Self {
            failure: Failure::permanent(code),
            module: Some(module.to_owned()),
            version,
        }
    }
}
impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.failure.code)?;
        if let Some(module) = &self.module {
            write!(f, " ({module}")?;
            if let Some(version) = self.version {
                write!(f, " v{version}")?;
            }
            f.write_str(")")?;
        }
        Ok(())
    }
}
impl std::error::Error for MigrationError {}

/// Lowercase hex SHA-256 of the migration SQL, as stored in the ledger.
pub fn checksum(sql: &str) -> String {
    let digest = Sha256::digest(sql.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn valid_label(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn validate(modules: &[ModuleMigrations<'_>]) -> Result<(), MigrationError> {
    if modules.len() > MAX_MODULES {
        return Err(MigrationError::new(Failure::permanent(
            "invalid_migrations",
        )));
    }
    let mut seen = std::collections::BTreeSet::new();
    for module in modules {
        if !valid_label(module.module, 64) || !seen.insert(module.module) {
            return Err(MigrationError::at(
                "invalid_migrations",
                module.module,
                None,
            ));
        }
        if module.migrations.len() > MAX_MIGRATIONS {
            return Err(MigrationError::at(
                "invalid_migrations",
                module.module,
                None,
            ));
        }
        let mut previous = 0;
        for migration in module.migrations {
            let step_ok = match migration.kind {
                MigrationKind::Transactional => true,
                MigrationKind::NonTransactional => single_statement(migration.sql)
                    .is_some_and(|statement| concurrent_index(statement).is_ok()),
            };
            if migration.version <= previous
                || migration.version > i32::MAX as u32
                || !valid_label(migration.name, 128)
                || migration.sql.trim().is_empty()
                || migration.sql.len() > MAX_SQL_BYTES
                || !step_ok
            {
                return Err(MigrationError::at(
                    "invalid_migrations",
                    module.module,
                    Some(migration.version),
                ));
            }
            previous = migration.version;
        }
    }
    Ok(())
}

/// The one statement of `sql`, without its trailing semicolons, or `None`
/// when `sql` holds no statement or more than one. Quotes, dollar quotes and
/// comments are skipped, so a `;` inside them does not split the statement.
fn single_statement(sql: &str) -> Option<&str> {
    let bytes = sql.as_bytes();
    let mut end = None;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        let next = bytes.get(index + 1).copied();
        let comment = matches!((byte, next), (b'-', Some(b'-')) | (b'/', Some(b'*')));
        if end.is_some() && !(byte.is_ascii_whitespace() || byte == b';' || comment) {
            return None;
        }
        match byte {
            b'\'' | b'"' => {
                index += 1;
                while index < bytes.len() && bytes[index] != byte {
                    index += 1;
                }
                if index == bytes.len() {
                    return None;
                }
            }
            b'-' if next == Some(b'-') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
                continue;
            }
            b'/' if next == Some(b'*') => {
                // PostgreSQL block comments nest.
                let mut depth = 0usize;
                while index < bytes.len() {
                    if bytes[index..].starts_with(b"/*") {
                        depth += 1;
                        index += 2;
                    } else if bytes[index..].starts_with(b"*/") {
                        depth -= 1;
                        index += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        index += 1;
                    }
                }
                if depth != 0 {
                    return None;
                }
                continue;
            }
            b'$' => {
                let tag_end = bytes[index + 1..]
                    .iter()
                    .position(|b| !(b.is_ascii_alphanumeric() || *b == b'_'))
                    .map(|p| index + 1 + p);
                if let Some(tag_end) = tag_end
                    && bytes[tag_end] == b'$'
                    && !bytes.get(index + 1).is_some_and(u8::is_ascii_digit)
                {
                    let tag = &bytes[index..=tag_end];
                    let body = tag_end + 1;
                    let close = bytes[body..].windows(tag.len()).position(|w| w == tag)?;
                    index = body + close + tag.len();
                    continue;
                }
            }
            b';' => {
                end.get_or_insert(index);
            }
            _ => {}
        }
        index += 1;
    }
    let statement = sql[..end.unwrap_or(sql.len())].trim();
    (!statement.is_empty()).then_some(statement)
}

/// The index a `CREATE [UNIQUE] INDEX CONCURRENTLY` statement builds.
#[derive(Debug, PartialEq, Eq)]
struct ConcurrentIndex {
    /// The index name as stored in `pg_class`.
    name: String,
    /// The table as written, for `to_regclass`.
    table: String,
}
/// `Ok(None)` for any statement other than a concurrent index build, and
/// `Err` for a concurrent build that is not safe to retry: it must say
/// `IF NOT EXISTS` and name its index.
fn concurrent_index(statement: &str) -> Result<Option<ConcurrentIndex>, ()> {
    let mut words = statement.split_whitespace().peekable();
    let mut keyword = |expected: &str| {
        let matched = words
            .peek()
            .is_some_and(|w| w.eq_ignore_ascii_case(expected));
        if matched {
            words.next();
        }
        matched
    };
    if !keyword("create") {
        return Ok(None);
    }
    keyword("unique");
    if !keyword("index") || !keyword("concurrently") {
        return Ok(None);
    }
    if !(keyword("if") && keyword("not") && keyword("exists")) {
        return Err(());
    }
    let name = words.next().ok_or(())?;
    if name.eq_ignore_ascii_case("on") {
        return Err(());
    }
    if name.matches('"').count() % 2 != 0 {
        return Err(());
    }
    let name = match name.strip_prefix('"').and_then(|n| n.strip_suffix('"')) {
        Some(quoted) if !quoted.is_empty() => quoted.replace("\"\"", "\""),
        Some(_) => return Err(()),
        None if name.contains(['"', '.', '(']) => return Err(()),
        None => name.to_ascii_lowercase(),
    };
    if !words.next().is_some_and(|w| w.eq_ignore_ascii_case("on")) {
        return Err(());
    }
    let mut table = words.next().ok_or(())?;
    if table.eq_ignore_ascii_case("only") {
        table = words.next().ok_or(())?;
    }
    let table = table.split('(').next().unwrap_or_default();
    // Quoted names with spaces would split into several words: refuse them
    // rather than guess, so the invalid-index lookup always finds its table.
    if table.is_empty() || table.matches('"').count() % 2 != 0 {
        return Err(());
    }
    Ok(Some(ConcurrentIndex {
        name,
        table: table.to_owned(),
    }))
}

struct Applied {
    name: String,
    checksum: String,
}

/// Compare the ledger with the declarations; return what is pending.
fn plan(
    modules: &[ModuleMigrations<'_>],
    applied: &BTreeMap<(String, u32), Applied>,
) -> Result<Vec<(usize, Migration)>, MigrationError> {
    let mut pending = Vec::new();
    for (index, module) in modules.iter().enumerate() {
        let mut highest = 0;
        for ((name, version), row) in applied.range((module.module.to_owned(), 0)..) {
            if name != module.module {
                break;
            }
            let Some(declared) = module.migrations.iter().find(|m| m.version == *version) else {
                return Err(MigrationError::at(
                    "unknown_applied_migration",
                    module.module,
                    Some(*version),
                ));
            };
            if row.checksum != checksum(declared.sql) {
                return Err(MigrationError::at(
                    "migration_checksum_mismatch",
                    module.module,
                    Some(*version),
                ));
            }
            if row.name != declared.name {
                return Err(MigrationError::at(
                    "migration_name_mismatch",
                    module.module,
                    Some(*version),
                ));
            }
            highest = highest.max(*version);
        }
        for migration in module.migrations {
            if applied.contains_key(&(module.module.to_owned(), migration.version)) {
                continue;
            }
            if migration.version < highest {
                return Err(MigrationError::at(
                    "migration_out_of_order",
                    module.module,
                    Some(migration.version),
                ));
            }
            pending.push((index, *migration));
        }
    }
    Ok(pending)
}

fn database(error: &tokio_postgres::Error) -> MigrationError {
    MigrationError::new(classify(error))
}

const LEDGER: &str = "CREATE SCHEMA IF NOT EXISTS merezha;
CREATE TABLE IF NOT EXISTS merezha.migrations (
    module text NOT NULL,
    version integer NOT NULL CHECK (version > 0),
    name text NOT NULL,
    checksum text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (module, version)
);";

/// Verify or apply `modules` before `deadline`.
///
/// Refuses with a permanent failure, before applying anything, when a
/// declaration is invalid (`invalid_migrations`), an applied version is not
/// declared (`unknown_applied_migration`), an applied checksum or name differs
/// (`migration_checksum_mismatch`, `migration_name_mismatch`) or a pending
/// version sorts below an applied one (`migration_out_of_order`). A failing
/// migration rolls back alone and reports `migration_failed`; earlier ones stay
/// applied. Ledger rows of modules absent from `modules` are left untouched.
pub async fn run_migrations(
    db: &Database,
    modules: &[ModuleMigrations<'_>],
    mode: MigrationMode,
    deadline: Instant,
) -> Result<MigrationReport, MigrationError> {
    validate(modules)?;
    let at = tokio::time::Instant::from_std(deadline);
    match tokio::time::timeout_at(at, run(db, modules, mode, deadline)).await {
        Ok(result) => result,
        Err(_) => Err(MigrationError::new(Failure::transient("migration_timeout"))),
    }
}

async fn run(
    db: &Database,
    modules: &[ModuleMigrations<'_>],
    mode: MigrationMode,
    deadline: Instant,
) -> Result<MigrationReport, MigrationError> {
    let mut pooled = db.acquire(deadline).await.map_err(MigrationError::new)?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    let budget = remaining.as_millis().clamp(1, i32::MAX as u128);
    if mode == MigrationMode::Verify {
        // Readiness verifies repeatedly, so stay on a pooled session: the
        // bound is `SET LOCAL` and ends with this read-only transaction.
        let transaction = pooled.transaction().await.map_err(|e| database(&e))?;
        transaction
            .batch_execute(&format!(
                "SET LOCAL statement_timeout = {budget}; SET TRANSACTION READ ONLY"
            ))
            .await
            .map_err(|e| database(&e))?;
        let applied = read_ledger(&*transaction, modules).await?;
        transaction.commit().await.map_err(|e| database(&e))?;
        return Ok(MigrationReport {
            applied: Vec::new(),
            pending: report(modules, plan(modules, &applied.unwrap_or_default())?),
        });
    }
    // Detach from the pool: session settings and the advisory lock die with
    // this connection instead of leaking into later borrowers.
    let mut client = deadpool_postgres::Object::take(pooled);
    client
        .batch_execute(&format!("SET statement_timeout = {budget}"))
        .await
        .map_err(|e| database(&e))?;
    lock(&client, deadline).await?;
    let transaction = client.transaction().await.map_err(|e| database(&e))?;
    transaction
        .batch_execute(LEDGER)
        .await
        .map_err(|e| database(&e))?;
    transaction.commit().await.map_err(|e| database(&e))?;
    let applied = read_ledger(&*client, modules).await?.unwrap_or_default();
    let pending = plan(modules, &applied)?;
    let mut done = Vec::new();
    for (index, migration) in pending {
        let module = modules[index].module;
        let fail = |e: &tokio_postgres::Error| {
            let failure = classify(e);
            MigrationError {
                failure: if failure.classification == merezha_domain::Classification::Transient {
                    failure
                } else {
                    Failure::permanent("migration_failed")
                },
                module: Some(module.to_owned()),
                version: Some(migration.version),
            }
        };
        let version = migration.version as i32;
        let digest = checksum(migration.sql);
        let row: [&(dyn tokio_postgres::types::ToSql + Sync); 4] =
            [&module, &version, &migration.name, &digest];
        match migration.kind {
            MigrationKind::Transactional => {
                let transaction = client.transaction().await.map_err(|e| fail(&e))?;
                transaction
                    .batch_execute(migration.sql)
                    .await
                    .map_err(|e| fail(&e))?;
                transaction
                    .execute(RECORD, &row)
                    .await
                    .map_err(|e| fail(&e))?;
                transaction.commit().await.map_err(|e| MigrationError {
                    failure: if e.as_db_error().is_some() {
                        Failure::permanent("migration_failed")
                    } else {
                        Failure::transient("commit_outcome_unknown")
                    },
                    module: Some(module.to_owned()),
                    version: Some(migration.version),
                })?;
            }
            MigrationKind::NonTransactional => {
                let invalid =
                    || MigrationError::at("invalid_migrations", module, Some(migration.version));
                let statement = single_statement(migration.sql).ok_or_else(invalid)?;
                let index = concurrent_index(statement).map_err(|()| invalid())?;
                if let Some(index) = &index {
                    drop_invalid_index(&client, index, module, migration.version)
                        .await
                        .map_err(|e| fail(&e))?;
                }
                // A single statement in the simple query protocol runs
                // outside any explicit transaction block. Until the ledger
                // row below exists, the next apply runs it again.
                client
                    .batch_execute(statement)
                    .await
                    .map_err(|e| fail(&e))?;
                client.execute(RECORD, &row).await.map_err(|e| fail(&e))?;
            }
        }
        tracing::info!(
            event = "migration.applied",
            module,
            version = migration.version,
            name = migration.name
        );
        done.push(PendingMigration {
            module: module.to_owned(),
            version: migration.version,
            name: migration.name,
        });
    }
    // Dropping the detached client closes the session and releases the lock.
    Ok(MigrationReport {
        applied: done,
        pending: Vec::new(),
    })
}

const RECORD: &str =
    "INSERT INTO merezha.migrations (module, version, name, checksum) VALUES ($1, $2, $3, $4)";

/// Drop the invalid index a failed `CREATE INDEX CONCURRENTLY` left behind,
/// so the retried `IF NOT EXISTS` build creates it again instead of keeping
/// an index PostgreSQL never uses.
async fn drop_invalid_index(
    client: &tokio_postgres::Client,
    index: &ConcurrentIndex,
    module: &str,
    version: u32,
) -> Result<(), tokio_postgres::Error> {
    let found = client
        .query_opt(
            "SELECT format('%I.%I', n.nspname, c.relname)
             FROM pg_index i
             JOIN pg_class c ON c.oid = i.indexrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE i.indrelid = to_regclass($1)::oid AND c.relname = $2 AND NOT i.indisvalid",
            &[&index.table, &index.name],
        )
        .await?;
    if let Some(row) = found {
        let qualified: String = row.get(0);
        client
            .batch_execute(&format!("DROP INDEX CONCURRENTLY IF EXISTS {qualified}"))
            .await?;
        tracing::warn!(event = "migration.invalid_index_dropped", module, version);
    }
    Ok(())
}

/// Pause between attempts to take the advisory lock.
const LOCK_POLL: std::time::Duration = std::time::Duration::from_millis(50);

/// Take the session advisory lock before `deadline`.
///
/// A blocking `pg_advisory_lock` would hold its statement's snapshot while it
/// waits, and the holder's `CREATE INDEX CONCURRENTLY` waits for every older
/// snapshot to end: the two would deadlock. Each `pg_try_advisory_lock`
/// attempt ends at once, so a waiting runner holds no snapshot.
async fn lock(client: &tokio_postgres::Client, deadline: Instant) -> Result<(), MigrationError> {
    loop {
        let locked: bool = client
            .query_one("SELECT pg_try_advisory_lock($1)", &[&MIGRATIONS_LOCK])
            .await
            .map_err(|e| match classify(&e).code.as_str() {
                "database_timeout" => {
                    MigrationError::new(Failure::transient("migration_lock_timeout"))
                }
                _ => database(&e),
            })?
            .get(0);
        if locked {
            return Ok(());
        }
        if Instant::now() + LOCK_POLL >= deadline {
            return Err(MigrationError::new(Failure::transient(
                "migration_lock_timeout",
            )));
        }
        tokio::time::sleep(LOCK_POLL).await;
    }
}

/// Applied rows for `modules`, or `None` when the ledger does not exist.
async fn read_ledger(
    client: &impl tokio_postgres::GenericClient,
    modules: &[ModuleMigrations<'_>],
) -> Result<Option<BTreeMap<(String, u32), Applied>>, MigrationError> {
    let exists: bool = client
        .query_one("SELECT to_regclass('merezha.migrations') IS NOT NULL", &[])
        .await
        .map_err(|e| database(&e))?
        .get(0);
    if !exists {
        return Ok(None);
    }
    let names: Vec<&str> = modules.iter().map(|m| m.module).collect();
    let rows = client
        .query(
            "SELECT module, version, name, checksum FROM merezha.migrations WHERE module = ANY($1)",
            &[&names],
        )
        .await
        .map_err(|e| database(&e))?;
    let mut applied = BTreeMap::new();
    for row in rows {
        let version: i32 = row.get(1);
        applied.insert(
            (row.get::<_, String>(0), version.max(0) as u32),
            Applied {
                name: row.get(2),
                checksum: row.get(3),
            },
        );
    }
    Ok(Some(applied))
}

fn report(
    modules: &[ModuleMigrations<'_>],
    pending: Vec<(usize, Migration)>,
) -> Vec<PendingMigration> {
    pending
        .into_iter()
        .map(|(index, m)| PendingMigration {
            module: modules[index].module.to_owned(),
            version: m.version,
            name: m.name,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE: Migration = Migration::new(1, "create", "CREATE TABLE t (id int)");
    const TWO: Migration = Migration::new(2, "index", "CREATE INDEX ON t (id)");

    #[test]
    fn checksum_is_sha256_hex() {
        assert_eq!(
            checksum(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
    #[test]
    fn declarations_must_be_ordered_named_and_unique() {
        let ok = [ONE, TWO];
        assert!(
            validate(&[ModuleMigrations {
                module: "kernel",
                migrations: &ok
            }])
            .is_ok()
        );
        for (module, migrations) in [
            ("Kernel", &[ONE][..]),
            ("kernel", &[TWO, ONE][..]),
            ("kernel", &[ONE, ONE][..]),
            ("kernel", &[Migration { version: 0, ..ONE }][..]),
            ("kernel", &[Migration { sql: " ", ..ONE }][..]),
            (
                "kernel",
                &[Migration {
                    name: "bad name",
                    ..ONE
                }][..],
            ),
        ] {
            let error = validate(&[ModuleMigrations { module, migrations }]).unwrap_err();
            assert_eq!(error.failure.code, "invalid_migrations");
        }
        let twice = [
            ModuleMigrations {
                module: "a",
                migrations: &ok,
            },
            ModuleMigrations {
                module: "a",
                migrations: &ok,
            },
        ];
        assert!(validate(&twice).is_err());
    }
    #[test]
    fn plan_refuses_drift_and_out_of_order_versions() {
        let declared = [ONE, TWO];
        let modules = [ModuleMigrations {
            module: "kernel",
            migrations: &declared,
        }];
        let row = |m: &Migration| Applied {
            name: m.name.into(),
            checksum: checksum(m.sql),
        };
        let mut applied = BTreeMap::new();
        assert_eq!(plan(&modules, &applied).unwrap().len(), 2);
        applied.insert(("kernel".into(), 1), row(&ONE));
        assert_eq!(plan(&modules, &applied).unwrap(), vec![(0, TWO)]);
        applied.insert(("other".into(), 9), row(&ONE));
        assert_eq!(plan(&modules, &applied).unwrap(), vec![(0, TWO)]);
        applied.insert(("kernel".into(), 3), row(&ONE));
        assert_eq!(
            plan(&modules, &applied).unwrap_err().failure.code,
            "unknown_applied_migration"
        );
        applied.remove(&("kernel".into(), 3));
        applied.insert(
            ("kernel".into(), 2),
            Applied {
                name: "index".into(),
                checksum: checksum("CREATE INDEX ON t (other)"),
            },
        );
        let error = plan(&modules, &applied).unwrap_err();
        assert_eq!(
            (error.failure.code.as_str(), error.version),
            ("migration_checksum_mismatch", Some(2))
        );
        applied.insert(
            ("kernel".into(), 2),
            Applied {
                name: "renamed".into(),
                checksum: checksum(TWO.sql),
            },
        );
        assert_eq!(
            plan(&modules, &applied).unwrap_err().failure.code,
            "migration_name_mismatch"
        );
        let mut applied = BTreeMap::new();
        applied.insert(("kernel".into(), 2), row(&TWO));
        assert_eq!(
            plan(&modules, &applied).unwrap_err().failure.code,
            "migration_out_of_order"
        );
    }
    #[test]
    fn non_transactional_steps_hold_exactly_one_statement() {
        assert_eq!(single_statement(" SELECT 1 ;; \n"), Some("SELECT 1"));
        assert_eq!(
            single_statement("SELECT ';', \"a;b\", $$;$$, $t$;$t$ -- ; x\n /* ; /* ; */ */"),
            Some("SELECT ';', \"a;b\", $$;$$, $t$;$t$ -- ; x\n /* ; /* ; */ */")
        );
        assert_eq!(single_statement("SELECT 1; -- done\n"), Some("SELECT 1"));
        for bad in [
            "",
            " ; ",
            "SELECT 1; SELECT 2",
            "SELECT 1; 'x'",
            "SELECT 'open",
            "SELECT /* open",
            "SELECT $$open",
        ] {
            assert_eq!(single_statement(bad), None, "{bad}");
        }
        let step = |sql| Migration::non_transactional(1, "step", sql);
        let declare = |migration: Migration| {
            validate(&[ModuleMigrations {
                module: "content",
                migrations: &[migration],
            }])
        };
        assert!(declare(step("CREATE INDEX CONCURRENTLY IF NOT EXISTS i ON t (a)")).is_ok());
        assert!(declare(step("ALTER TYPE mood ADD VALUE IF NOT EXISTS 'ok'")).is_ok());
        for bad in [
            "CREATE INDEX CONCURRENTLY i ON t (a)",
            "CREATE INDEX CONCURRENTLY ON t (a)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ON t (a)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS i ON t (a); SELECT 1",
        ] {
            assert_eq!(
                declare(step(bad)).unwrap_err().failure.code,
                "invalid_migrations",
                "{bad}"
            );
        }
        // Several statements stay fine in a transactional migration.
        assert!(declare(Migration::new(1, "m", "SELECT 1; SELECT 2")).is_ok());
    }
    #[test]
    fn concurrent_index_targets_name_the_index_and_its_table() {
        assert_eq!(
            concurrent_index(
                "create unique index concurrently if not exists Entries_Title on only content.entries(title)"
            ),
            Ok(Some(ConcurrentIndex {
                name: "entries_title".into(),
                table: "content.entries".into()
            }))
        );
        assert_eq!(
            concurrent_index(
                r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS "By""Title""" ON "Entries" (t)"#
            ),
            Ok(Some(ConcurrentIndex {
                name: r#"By"Title""#.into(),
                table: r#""Entries""#.into()
            }))
        );
        for unsupported in [
            r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS "By Title" ON t (a)"#,
            r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS i ON "My Table" (a)"#,
        ] {
            assert_eq!(concurrent_index(unsupported), Err(()), "{unsupported}");
        }
        assert_eq!(concurrent_index("CREATE INDEX i ON t (a)"), Ok(None));
        assert_eq!(
            concurrent_index("DROP INDEX CONCURRENTLY IF EXISTS i"),
            Ok(None)
        );
    }
}
