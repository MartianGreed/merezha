//! Disposable databases for tests against a real PostgreSQL server.
use crate::{DatabaseConfig, Secret};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

/// Administrative URL of the server tests may create databases on. It must
/// be a `postgres://` URL whose role has `CREATEDB`; connections use no TLS.
pub const TEST_DATABASE_ENV: &str = "MEREZHA_TEST_DATABASE_URL";
/// When set to `1`, a missing [`TEST_DATABASE_ENV`] fails the test instead
/// of skipping it. CI sets both.
pub const REQUIRE_DATABASE_ENV: &str = "MEREZHA_TEST_REQUIRE_DATABASE";

static NEXT: AtomicU64 = AtomicU64::new(0);

/// A uniquely named database, dropped (with `FORCE`) when this value drops.
///
/// ```no_run
/// let Some(db) = merezha_postgres::TestDatabase::create() else { return };
/// let config = db.config();
/// ```
pub struct TestDatabase {
    name: String,
    admin: String,
    url: String,
}
impl std::fmt::Debug for TestDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestDatabase")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}
impl TestDatabase {
    /// Create a fresh database, or return `None` after printing a skip
    /// message when [`TEST_DATABASE_ENV`] is unset.
    ///
    /// # Panics
    /// When the variable is set but unusable, or when it is unset and
    /// [`REQUIRE_DATABASE_ENV`] is `1`. Safe to call from sync or async tests.
    pub fn create() -> Option<Self> {
        let Ok(admin) = std::env::var(TEST_DATABASE_ENV) else {
            assert!(
                std::env::var(REQUIRE_DATABASE_ENV).as_deref() != Ok("1"),
                "{REQUIRE_DATABASE_ENV}=1 but {TEST_DATABASE_ENV} is unset"
            );
            eprintln!("skipped: set {TEST_DATABASE_ENV} to run PostgreSQL tests");
            return None;
        };
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        let name = format!(
            "merezha_test_{}_{}_{nanos}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let url = with_database(&admin, &name)
            .unwrap_or_else(|| panic!("{TEST_DATABASE_ENV} must be a postgres:// URL"));
        let db = Self { name, admin, url };
        db.admin_execute(&format!("CREATE DATABASE \"{}\"", db.name));
        Some(db)
    }
    /// URL of the created database, for `{ env = ... }` secrets or [`Self::config`].
    pub fn url(&self) -> &str {
        &self.url
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Default settings with short timeouts suited to tests.
    pub fn config(&self) -> DatabaseConfig {
        let mut config = DatabaseConfig::new(Secret::new(self.url.clone()));
        config.pool_size = 4;
        config.connect_timeout = std::time::Duration::from_secs(2);
        config.acquire_timeout = std::time::Duration::from_secs(2);
        config
    }
    /// Run statements on the administrative connection, e.g. to make the
    /// created database refuse connections.
    ///
    /// # Panics
    /// When the statements fail.
    pub fn admin_execute(&self, sql: &str) {
        if let Err(message) = admin(&self.admin, sql) {
            panic!("test database administration failed: {message}");
        }
    }
}
impl Drop for TestDatabase {
    fn drop(&mut self) {
        let sql = format!("DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)", self.name);
        if let Err(message) = admin(&self.admin, &sql) {
            eprintln!("could not drop test database {}: {message}", self.name);
        }
    }
}

/// Run `sql` on a dedicated thread and runtime so callers may be inside or
/// outside an async runtime.
fn admin(url: &str, sql: &str) -> Result<(), String> {
    let url = url.to_owned();
    let sql = sql.to_owned();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        runtime.block_on(async move {
            let (client, connection) = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                tokio_postgres::connect(&url, tokio_postgres::NoTls),
            )
            .await
            .map_err(|_| "connect timeout".to_owned())?
            .map_err(|e| e.to_string())?;
            let task = tokio::spawn(connection);
            let result = client.batch_execute(&sql).await.map_err(|e| e.to_string());
            drop(client);
            let _ = task.await;
            result
        })
    })
    .join()
    .map_err(|_| "administration thread panicked".to_owned())?
}

/// Replace the database path of a `postgres://` URL, keeping its query.
fn with_database(url: &str, name: &str) -> Option<String> {
    let rest = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))?;
    let scheme = &url[..url.len() - rest.len()];
    let (base, query) = match rest.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (rest, None),
    };
    let authority = base
        .split_once('/')
        .map_or(base, |(authority, _)| authority);
    let mut out = format!("{scheme}{authority}/{name}");
    if let Some(query) = query {
        out.push('?');
        out.push_str(query);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    #[test]
    fn database_path_is_replaced_and_query_kept() {
        use super::with_database;
        assert_eq!(
            with_database("postgres://u:p@h:5432/postgres?sslmode=disable", "x").unwrap(),
            "postgres://u:p@h:5432/x?sslmode=disable"
        );
        assert_eq!(
            with_database("postgresql://h", "x").unwrap(),
            "postgresql://h/x"
        );
        assert!(with_database("host=h dbname=y", "x").is_none());
    }
}
