//! PostgreSQL kernel shared by every Merezha block: one bounded connection
//! pool, deadline-scoped transactions, the module migration runner and a test
//! harness. Errors are classified [`Failure`] values with stable codes; no
//! error carries SQL text, server messages or connection strings.
mod migrate;
mod testing;
mod tls;

pub use deadpool_postgres::Transaction;
pub use migrate::{
    MIGRATIONS_LOCK, Migration, MigrationError, MigrationMode, MigrationReport, ModuleMigrations,
    PendingMigration, checksum, run_migrations,
};
pub use testing::{REQUIRE_DATABASE_ENV, TEST_DATABASE_ENV, TestDatabase};
pub use tokio_postgres;

use deadpool_postgres::{Manager, ManagerConfig, Pool, PoolError, RecyclingMethod, Runtime};
use merezha_domain::Failure;
use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_postgres::error::SqlState;

/// A boxed `Send` future, used where trait objects or higher-ranked closures
/// cannot name their future type.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A resolved secret value. `Debug` never prints the value; call
/// [`Secret::expose`] only at the point of use.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(Arc<str>);
impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(Arc::from(value.into()))
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// Transport security for database connections. `Require` verifies the
/// server certificate chain and host name against the platform trust store
/// plus an optional extra CA file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TlsMode {
    #[default]
    Disable,
    Require,
}

/// Typed `[database]` settings. Ranges are enforced by [`DatabaseConfig::validate`].
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    /// libpq-style URL or key/value string. Always a secret: it may embed a password.
    pub url: Secret,
    pub pool_size: usize,
    pub connect_timeout: Duration,
    pub acquire_timeout: Duration,
    /// Default per-statement bound for pooled sessions and the ceiling a
    /// deadline-scoped transaction can request.
    pub statement_timeout: Duration,
    /// Apply pending migrations at startup instead of only verifying them.
    pub migrate: bool,
    pub tls: TlsMode,
    pub tls_ca_file: Option<PathBuf>,
}
/// Inclusive bounds for numeric database settings.
pub mod limits {
    use std::time::Duration;
    pub const POOL_SIZE: (usize, usize) = (1, 256);
    pub const CONNECT_TIMEOUT: (Duration, Duration) =
        (Duration::from_millis(100), Duration::from_secs(60));
    pub const ACQUIRE_TIMEOUT: (Duration, Duration) =
        (Duration::from_millis(10), Duration::from_secs(60));
    pub const STATEMENT_TIMEOUT: (Duration, Duration) =
        (Duration::from_millis(10), Duration::from_secs(300));
}
impl DatabaseConfig {
    pub const DEFAULT_POOL_SIZE: usize = 16;
    pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
    pub const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(2);
    pub const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Defaults for everything except the required URL.
    pub fn new(url: Secret) -> Self {
        Self {
            url,
            pool_size: Self::DEFAULT_POOL_SIZE,
            connect_timeout: Self::DEFAULT_CONNECT_TIMEOUT,
            acquire_timeout: Self::DEFAULT_ACQUIRE_TIMEOUT,
            statement_timeout: Self::DEFAULT_STATEMENT_TIMEOUT,
            migrate: false,
            tls: TlsMode::Disable,
            tls_ca_file: None,
        }
    }
    /// Every violated rule as `(setting, code)`, empty when valid.
    pub fn validate(&self) -> Vec<(&'static str, &'static str)> {
        let mut issues = Vec::new();
        if tokio_postgres::Config::from_str(self.url.expose()).is_err() {
            issues.push(("url", "invalid_database_url"));
        }
        let (min, max) = limits::POOL_SIZE;
        if !(min..=max).contains(&self.pool_size) {
            issues.push(("pool_size", "out_of_range"));
        }
        for (name, value, (min, max)) in [
            (
                "connect_timeout_ms",
                self.connect_timeout,
                limits::CONNECT_TIMEOUT,
            ),
            (
                "acquire_timeout_ms",
                self.acquire_timeout,
                limits::ACQUIRE_TIMEOUT,
            ),
            (
                "statement_timeout_ms",
                self.statement_timeout,
                limits::STATEMENT_TIMEOUT,
            ),
        ] {
            if !(min..=max).contains(&value) {
                issues.push((name, "out_of_range"));
            }
        }
        if self.tls == TlsMode::Disable && self.tls_ca_file.is_some() {
            issues.push(("tls_ca_file", "tls_ca_without_tls"));
        }
        issues
    }
}

/// Pool snapshot for diagnostics; counts only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolStatus {
    pub max_size: usize,
    pub size: usize,
    pub available: usize,
    pub waiting: usize,
}

/// A cloneable handle to the bounded pool.
#[derive(Clone)]
pub struct Database {
    pool: Pool,
    acquire_timeout: Duration,
    statement_timeout: Duration,
}
impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}
/// A pooled client. Dropping it returns the connection to the pool.
pub type PooledClient = deadpool_postgres::Object;

impl Database {
    /// Build the pool. Connections open lazily; call [`Database::ping`] to
    /// prove reachability. Fails `invalid_database_config` or `tls_config`.
    pub fn connect(config: &DatabaseConfig) -> Result<Self, Failure> {
        if !config.validate().is_empty() {
            return Err(Failure::permanent("invalid_database_config"));
        }
        let mut pg = tokio_postgres::Config::from_str(config.url.expose())
            .map_err(|_| Failure::permanent("invalid_database_config"))?;
        pg.connect_timeout(config.connect_timeout)
            .application_name("merezha")
            .options(format!(
                "-c statement_timeout={}",
                config.statement_timeout.as_millis()
            ))
            .keepalives(true);
        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = match config.tls {
            TlsMode::Disable => {
                pg.ssl_mode(tokio_postgres::config::SslMode::Disable);
                Manager::from_config(pg, tokio_postgres::NoTls, manager_config)
            }
            TlsMode::Require => {
                pg.ssl_mode(tokio_postgres::config::SslMode::Require);
                let tls = tls::connector(config.tls_ca_file.as_deref())?;
                Manager::from_config(pg, tls, manager_config)
            }
        };
        let pool = Pool::builder(manager)
            .max_size(config.pool_size)
            .wait_timeout(Some(config.acquire_timeout))
            .create_timeout(Some(config.connect_timeout))
            .recycle_timeout(Some(config.connect_timeout))
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|_| Failure::permanent("invalid_database_config"))?;
        Ok(Self {
            pool,
            acquire_timeout: config.acquire_timeout,
            statement_timeout: config.statement_timeout,
        })
    }
    /// Acquire a pooled client before `deadline` and within the configured
    /// acquisition timeout.
    pub async fn acquire(&self, deadline: Instant) -> Result<PooledClient, Failure> {
        let bound = deadline.min(Instant::now() + self.acquire_timeout);
        match tokio::time::timeout_at(bound.into(), self.pool.get()).await {
            Ok(Ok(client)) => Ok(client),
            Ok(Err(error)) => Err(pool_failure(&error)),
            Err(_) => Err(Failure::transient("database_busy")),
        }
    }
    /// Run `SELECT 1` on a pooled connection before `deadline`.
    pub async fn ping(&self, deadline: Instant) -> Result<(), Failure> {
        let client = self.acquire(deadline).await?;
        match tokio::time::timeout_at(deadline.into(), client.simple_query("SELECT 1")).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(classify(&error)),
            Err(_) => Err(Failure::transient("database_timeout")),
        }
    }
    /// Run `work` in one transaction whose `statement_timeout` is the time
    /// left until `deadline`, capped by the configured statement timeout.
    ///
    /// The transaction commits when `work` returns `Ok` and rolls back
    /// otherwise, including when the deadline expires first. A failed or
    /// interrupted commit returns `commit_outcome_unknown`: the caller must
    /// not assume a rollback.
    ///
    /// ```no_run
    /// # async fn example(db: merezha_postgres::Database) -> Result<(), merezha_domain::Failure> {
    /// let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    /// let total: i64 = db
    ///     .transaction(deadline, |tx| {
    ///         Box::pin(async move {
    ///             let row = tx
    ///                 .query_one("SELECT 40 + 2", &[])
    ///                 .await
    ///                 .map_err(|e| merezha_postgres::classify(&e))?;
    ///             Ok(row.get::<_, i32>(0) as i64)
    ///         })
    ///     })
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub async fn transaction<T, F>(&self, deadline: Instant, work: F) -> Result<T, Failure>
    where
        T: Send,
        F: for<'t> FnOnce(&'t Transaction<'t>) -> BoxFuture<'t, Result<T, Failure>> + Send,
    {
        let mut client = self.acquire(deadline).await?;
        let budget = statement_budget(deadline, self.statement_timeout)?;
        let at = tokio::time::Instant::from_std(deadline);
        let transaction = match tokio::time::timeout_at(at, client.transaction()).await {
            Ok(Ok(transaction)) => transaction,
            Ok(Err(error)) => return Err(classify(&error)),
            Err(_) => return Err(Failure::transient("database_timeout")),
        };
        let set = format!("SET LOCAL statement_timeout = {}", budget.as_millis());
        match tokio::time::timeout_at(at, transaction.batch_execute(&set)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(classify(&error)),
            Err(_) => return Err(Failure::transient("database_timeout")),
        }
        // Dropping the transaction on any early return rolls it back.
        let value = match tokio::time::timeout_at(at, work(&transaction)).await {
            Ok(Ok(value)) => value,
            Ok(Err(failure)) => return Err(failure),
            Err(_) => return Err(Failure::transient("database_timeout")),
        };
        match tokio::time::timeout_at(at, transaction.commit()).await {
            Ok(Ok(())) => Ok(value),
            Ok(Err(error)) if error.as_db_error().is_some() => Err(classify(&error)),
            _ => Err(Failure::transient("commit_outcome_unknown")),
        }
    }
    /// The configured per-statement ceiling.
    pub fn statement_timeout(&self) -> Duration {
        self.statement_timeout
    }
    pub fn status(&self) -> PoolStatus {
        let status = self.pool.status();
        PoolStatus {
            max_size: status.max_size,
            size: status.size,
            available: status.available,
            waiting: status.waiting,
        }
    }
    /// Close the pool: idle connections drop and later acquisitions fail
    /// `database_closed`. Checked-out clients close when returned.
    pub fn close(&self) {
        self.pool.close();
    }
}

/// Remaining time before `deadline`, capped at `ceiling`; at least 1 ms.
fn statement_budget(deadline: Instant, ceiling: Duration) -> Result<Duration, Failure> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining < Duration::from_millis(1) {
        return Err(Failure::transient("database_timeout"));
    }
    Ok(remaining.min(ceiling))
}

fn pool_failure(error: &PoolError) -> Failure {
    match error {
        PoolError::Timeout(deadpool_postgres::TimeoutType::Wait) => {
            Failure::transient("database_busy")
        }
        PoolError::Closed => Failure::transient("database_closed"),
        // Connection setup failures, including refused logins, make the
        // database unavailable to this host rather than the caller's fault.
        _ => Failure::transient("database_unavailable"),
    }
}

/// Map a driver error to a stable classified failure. Server messages,
/// details and SQL never leave this function.
pub fn classify(error: &tokio_postgres::Error) -> Failure {
    let Some(code) = error.code() else {
        return Failure::transient("database_unavailable");
    };
    if *code == SqlState::T_R_SERIALIZATION_FAILURE || *code == SqlState::T_R_DEADLOCK_DETECTED {
        Failure::conflict("database_conflict")
    } else if *code == SqlState::UNIQUE_VIOLATION {
        Failure::conflict("unique_violation")
    } else if *code == SqlState::QUERY_CANCELED {
        Failure::transient("database_timeout")
    } else if *code == SqlState::LOCK_NOT_AVAILABLE {
        Failure::transient("database_busy")
    } else if code.code().starts_with("08")
        || code.code().starts_with("53")
        || code.code().starts_with("57P")
    {
        // Connection exceptions, insufficient resources, operator intervention.
        Failure::transient("database_unavailable")
    } else if code.code().starts_with("23") {
        Failure::permanent("constraint_violation")
    } else {
        Failure::permanent("database_error")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_debug_is_redacted_and_config_debug_never_prints_url() {
        let config = DatabaseConfig::new(Secret::new("postgres://u:hunter2@db/app"));
        let printed = format!("{config:?}");
        assert!(printed.contains("Secret(<redacted>)"));
        assert!(!printed.contains("hunter2"));
    }
    #[test]
    fn validation_reports_every_violation() {
        let mut config = DatabaseConfig::new(Secret::new("postgres://db/app"));
        assert!(config.validate().is_empty());
        config.pool_size = 0;
        config.acquire_timeout = Duration::ZERO;
        config.statement_timeout = Duration::from_secs(301);
        config.tls_ca_file = Some("/ca.pem".into());
        config.url = Secret::new("postgres://db:notaport/app");
        assert_eq!(
            config.validate(),
            vec![
                ("url", "invalid_database_url"),
                ("pool_size", "out_of_range"),
                ("acquire_timeout_ms", "out_of_range"),
                ("statement_timeout_ms", "out_of_range"),
                ("tls_ca_file", "tls_ca_without_tls"),
            ]
        );
        assert_eq!(
            Database::connect(&config).unwrap_err().code,
            "invalid_database_config"
        );
    }
    #[test]
    fn statement_budget_is_capped_and_refuses_expired_deadlines() {
        let ceiling = Duration::from_millis(50);
        let far = Instant::now() + Duration::from_secs(10);
        assert_eq!(statement_budget(far, ceiling).unwrap(), ceiling);
        let past = Instant::now() - Duration::from_millis(1);
        assert_eq!(
            statement_budget(past, ceiling).unwrap_err().code,
            "database_timeout"
        );
    }
}
