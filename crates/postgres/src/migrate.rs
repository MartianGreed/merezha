//! Forward-only module migrations recorded in `merezha.migrations`.
//!
//! Apply mode takes a session advisory lock on a connection removed from the
//! pool, so concurrent hosts serialize and the lock is released when that
//! connection closes, even if the caller is cancelled. Each pending migration
//! runs with its ledger row in one transaction. Verify mode is read-only and
//! takes no lock.
use crate::{Database, classify};
use merezha_domain::Failure;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt::Write as _, time::Instant};

/// Advisory lock key for the migration ledger: the ASCII bytes of `merezha1`.
pub const MIGRATIONS_LOCK: i64 = 0x6d65_7265_7a68_6131;
const MAX_MIGRATIONS: usize = 1024;
const MAX_SQL_BYTES: usize = 1_048_576;
const MAX_MODULES: usize = 64;

/// One forward-only schema change. `version` orders a module's migrations;
/// `sql` may contain several statements and must not manage transactions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub sql: &'static str,
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
            if migration.version <= previous
                || migration.version > i32::MAX as u32
                || !valid_label(migration.name, 128)
                || migration.sql.trim().is_empty()
                || migration.sql.len() > MAX_SQL_BYTES
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
    let pooled = db.acquire(deadline).await.map_err(MigrationError::new)?;
    // Detach from the pool: session settings and the advisory lock die with
    // this connection instead of leaking into later borrowers.
    let mut client = deadpool_postgres::Object::take(pooled);
    let remaining = deadline.saturating_duration_since(Instant::now());
    let budget = remaining.as_millis().clamp(1, i32::MAX as u128);
    client
        .batch_execute(&format!("SET statement_timeout = {budget}"))
        .await
        .map_err(|e| database(&e))?;
    if mode == MigrationMode::Apply {
        client
            .execute("SELECT pg_advisory_lock($1)", &[&MIGRATIONS_LOCK])
            .await
            .map_err(|e| match classify(&e).code.as_str() {
                "database_timeout" => {
                    MigrationError::new(Failure::transient("migration_lock_timeout"))
                }
                _ => database(&e),
            })?;
        let transaction = client.transaction().await.map_err(|e| database(&e))?;
        transaction
            .batch_execute(LEDGER)
            .await
            .map_err(|e| database(&e))?;
        transaction.commit().await.map_err(|e| database(&e))?;
    } else {
        let exists: bool = client
            .query_one("SELECT to_regclass('merezha.migrations') IS NOT NULL", &[])
            .await
            .map_err(|e| database(&e))?
            .get(0);
        if !exists {
            let applied = BTreeMap::new();
            return Ok(MigrationReport {
                applied: Vec::new(),
                pending: report(modules, plan(modules, &applied)?),
            });
        }
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
    let pending = plan(modules, &applied)?;
    if mode == MigrationMode::Verify {
        return Ok(MigrationReport {
            applied: Vec::new(),
            pending: report(modules, pending),
        });
    }
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
        let transaction = client.transaction().await.map_err(|e| fail(&e))?;
        transaction
            .batch_execute(migration.sql)
            .await
            .map_err(|e| fail(&e))?;
        transaction
            .execute(
                "INSERT INTO merezha.migrations (module, version, name, checksum) VALUES ($1, $2, $3, $4)",
                &[
                    &module,
                    &(migration.version as i32),
                    &migration.name,
                    &checksum(migration.sql),
                ],
            )
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

    const ONE: Migration = Migration {
        version: 1,
        name: "create",
        sql: "CREATE TABLE t (id int)",
    };
    const TWO: Migration = Migration {
        version: 2,
        name: "index",
        sql: "CREATE INDEX ON t (id)",
    };

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
}
