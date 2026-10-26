//! The statement registry (TE §1.3 "PostgreSQL"). Every statement the
//! kernel runs is a [`Statement`] constant with a registered name and a
//! low-cardinality summary, and runs through [`Database::query`] and its
//! siblings. They time it in `db.client.operation.duration` and, inside a
//! caller's span, open a CLIENT span named after the summary. Spans and
//! metrics never carry SQL text, parameters or server messages: a failure
//! records only its SQLSTATE and its [`crate::classify`] code.
//!
//! Blocks declare their statements the same way, one constant per
//! statement, and list them for [`check_registry`] in a test.
use crate::{Database, instruments::Telemetry};
use tokio_postgres::{Row, types::ToSql};

/// Longest registered name or summary, in bytes.
pub const MAX_STATEMENT_LABEL: usize = 64;
/// The name no registered statement may take: TE §1.3 reserves it for SQL
/// that bypassed the registry.
pub const ADHOC: &str = "adhoc";

/// One registered statement: `name` (`app.events.append`) becomes
/// `merezha.db.statement` on its span; `summary` (`INSERT merezha_app.events`)
/// becomes the span name and `db.query.summary`, and its words give
/// `db.operation.name` and `db.collection.name`; `sql` is what runs, or the
/// shape of the text for [`Database::batch_text`].
///
/// [`Statement::new`] checks the labels at compile time when used in a
/// constant:
///
/// - `name`: two or more dot-separated segments of `[a-z][a-z0-9_]*`, at
///   most 64 bytes, never `adhoc`;
/// - `summary`: one to three upper-case words (the operation), then at most
///   one target of `[a-z_][a-z0-9_.]*` (the collection), or a function
///   written with `()` (no collection), at most 64 bytes;
/// - `sql`: not empty.
///
/// ```
/// use merezha_postgres::Statement;
/// const INSERT_NOTE: Statement = Statement::new(
///     "notes.insert",
///     "INSERT notes.notes",
///     "INSERT INTO notes.notes (id, body) VALUES ($1, $2)",
/// );
/// assert_eq!(INSERT_NOTE.operation(), "INSERT");
/// assert_eq!(INSERT_NOTE.collection(), Some("notes.notes"));
/// ```
///
/// An invalid label does not compile:
///
/// ```compile_fail
/// const BAD: merezha_postgres::Statement =
///     merezha_postgres::Statement::new("adhoc", "SELECT", "SELECT 1");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Statement {
    pub name: &'static str,
    pub summary: &'static str,
    pub sql: &'static str,
}
impl Statement {
    /// A registered statement. Panics (at compile time in a constant) when
    /// a label breaks the rules above.
    pub const fn new(name: &'static str, summary: &'static str, sql: &'static str) -> Self {
        assert!(valid_name(name.as_bytes()), "invalid statement name");
        assert!(
            valid_summary(summary.as_bytes()),
            "invalid statement summary"
        );
        assert!(!sql.is_empty(), "empty statement sql");
        Self { name, summary, sql }
    }
    /// `db.operation.name`: the upper-case words of the summary.
    pub fn operation(&self) -> &'static str {
        match self.target_start() {
            Some(start) => &self.summary[..start - 1],
            None => self.summary,
        }
    }
    /// `db.collection.name`: the summary's target, unless it names a
    /// function (`pg_try_advisory_lock()`).
    pub fn collection(&self) -> Option<&'static str> {
        let target = &self.summary[self.target_start()?..];
        (!target.ends_with("()")).then_some(target)
    }
    fn target_start(&self) -> Option<usize> {
        let last = self.summary.rfind(' ')? + 1;
        let first = self.summary.as_bytes()[last];
        (first.is_ascii_lowercase() || first == b'_').then_some(last)
    }
}

const fn valid_name(name: &[u8]) -> bool {
    if name.is_empty() || name.len() > MAX_STATEMENT_LABEL {
        return false;
    }
    let adhoc = ADHOC.as_bytes();
    if name.len() == adhoc.len() {
        let mut i = 0;
        let mut same = true;
        while i < name.len() {
            same = same && name[i] == adhoc[i];
            i += 1;
        }
        if same {
            return false;
        }
    }
    let mut segments = 0;
    let mut i = 0;
    while i < name.len() {
        if !name[i].is_ascii_lowercase() {
            return false;
        }
        i += 1;
        while i < name.len() && name[i] != b'.' {
            let b = name[i];
            if !(b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_') {
                return false;
            }
            i += 1;
        }
        segments += 1;
        if i < name.len() {
            // Skip the dot; a trailing dot leaves an empty segment.
            i += 1;
            if i == name.len() {
                return false;
            }
        }
    }
    segments >= 2
}

const fn upper_word(s: &[u8], start: usize, end: usize) -> bool {
    let mut i = start;
    while i < end {
        if !s[i].is_ascii_uppercase() {
            return false;
        }
        i += 1;
    }
    true
}

const fn target_word(s: &[u8], start: usize, end: usize) -> bool {
    let first = s[start];
    if !(first.is_ascii_lowercase() || first == b'_') {
        return false;
    }
    // A function target ends with `()`.
    let body_end = if end - start > 2 && s[end - 2] == b'(' && s[end - 1] == b')' {
        end - 2
    } else {
        end
    };
    let mut i = start;
    while i < body_end {
        let b = s[i];
        if !(b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'.') {
            return false;
        }
        i += 1;
    }
    s[body_end - 1] != b'.'
}

const fn valid_summary(s: &[u8]) -> bool {
    if s.is_empty() || s.len() > MAX_STATEMENT_LABEL {
        return false;
    }
    let mut words = 0;
    let mut target = false;
    let mut i = 0;
    while i < s.len() {
        let start = i;
        while i < s.len() && s[i] != b' ' {
            i += 1;
        }
        if i == start || target {
            // An empty word, or anything after the target.
            return false;
        }
        if upper_word(s, start, i) {
            if words == 3 {
                return false;
            }
            words += 1;
        } else if words > 0 && target_word(s, start, i) {
            target = true;
        } else {
            return false;
        }
        if i < s.len() {
            i += 1;
            if i == s.len() {
                return false;
            }
        }
    }
    words > 0
}

/// Check a registry, the kernel's [`KERNEL_STATEMENTS`] plus every block's
/// list: names are unique across all of them. Returns the duplicated names.
pub fn check_registry(sets: &[&[Statement]]) -> Result<(), Vec<&'static str>> {
    let mut seen = std::collections::BTreeSet::new();
    let mut duplicates = Vec::new();
    for statement in sets.iter().flat_map(|set| set.iter()) {
        if !seen.insert(statement.name) {
            duplicates.push(statement.name);
        }
    }
    if duplicates.is_empty() {
        Ok(())
    } else {
        Err(duplicates)
    }
}

/// A connection or transaction statements can run on: the driver's
/// `Client` and `Transaction`, the pool's [`crate::PooledClient`],
/// [`crate::Tx`] and [`crate::DetachedClient`].
pub trait Session: private::Sealed + Sync {}
impl<T: private::Sealed + Sync> Session for T {}

mod private {
    /// What a session runs statements on.
    pub enum Driver<'a> {
        Client(&'a tokio_postgres::Client),
        Transaction(&'a tokio_postgres::Transaction<'a>),
    }
    pub trait Sealed {
        fn driver(&self) -> Driver<'_>;
    }
    impl Sealed for tokio_postgres::Client {
        fn driver(&self) -> Driver<'_> {
            Driver::Client(self)
        }
    }
    impl Sealed for tokio_postgres::Transaction<'_> {
        fn driver(&self) -> Driver<'_> {
            Driver::Transaction(self)
        }
    }
    impl Sealed for deadpool_postgres::ClientWrapper {
        fn driver(&self) -> Driver<'_> {
            Driver::Client(self)
        }
    }
    impl Sealed for deadpool_postgres::Object {
        fn driver(&self) -> Driver<'_> {
            Driver::Client(self)
        }
    }
    impl Sealed for deadpool_postgres::Transaction<'_> {
        fn driver(&self) -> Driver<'_> {
            Driver::Transaction(self)
        }
    }
    impl Sealed for crate::DetachedClient {
        fn driver(&self) -> Driver<'_> {
            Driver::Client(self)
        }
    }
}
use private::Driver;

/// Run one driver method on whichever client a session holds.
macro_rules! on_driver {
    ($session:expr, |$client:ident| $call:expr) => {{
        let driver = $session.driver();
        async move {
            match driver {
                Driver::Client($client) => $call.await,
                Driver::Transaction($client) => $call.await,
            }
        }
    }};
}

/// Instrumented execution of registered statements. Each method has the
/// driver method's contract and error; map errors with [`crate::classify`].
///
/// ```no_run
/// # async fn example(db: merezha_postgres::Database) -> Result<(), merezha_domain::Failure> {
/// use merezha_postgres::{Statement, classify};
/// const COUNT: Statement = Statement::new(
///     "notes.count",
///     "SELECT notes.notes",
///     "SELECT count(*) FROM notes.notes WHERE body = $1",
/// );
/// let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
/// let total: i64 = db
///     .transaction(deadline, |tx| {
///         let db = db.clone();
///         Box::pin(async move {
///             let row = db
///                 .query_one(tx, &COUNT, &[&"draft"])
///                 .await
///                 .map_err(|e| classify(&e))?;
///             Ok(row.get(0))
///         })
///     })
///     .await?;
/// # Ok(()) }
/// ```
impl Database {
    pub async fn query(
        &self,
        session: &impl Session,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, tokio_postgres::Error> {
        self.telemetry.rows(session, statement, params).await
    }
    pub async fn query_one(
        &self,
        session: &impl Session,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, tokio_postgres::Error> {
        self.telemetry.row(session, statement, params).await
    }
    pub async fn query_opt(
        &self,
        session: &impl Session,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, tokio_postgres::Error> {
        self.telemetry.maybe_row(session, statement, params).await
    }
    /// `statement.sql` prepared on `client` through the pool's per-session
    /// statement cache, for [`Database::query_one_prepared`]. Not timed: it
    /// is a cache lookup after the session's first use.
    pub async fn prepare_cached(
        &self,
        client: &crate::PooledClient,
        statement: &Statement,
    ) -> Result<tokio_postgres::Statement, tokio_postgres::Error> {
        // The registry's own dispatch; the scan skips this file.
        client.prepare_cached(statement.sql).await
    }
    /// [`Database::query_one`] with `prepared`, the driver statement
    /// prepared from `statement.sql` (for example with the pool's
    /// `prepare_cached`), so a hot statement skips its parse round trip.
    pub async fn query_one_prepared(
        &self,
        session: &impl Session,
        statement: &Statement,
        prepared: &tokio_postgres::Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, tokio_postgres::Error> {
        let fut = on_driver!(session, |c| c.query_one(prepared, params));
        self.telemetry.observe(statement, fut).await
    }
    /// The number of rows affected.
    pub async fn execute(
        &self,
        session: &impl Session,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error> {
        self.telemetry.run(session, statement, params).await
    }
    /// `statement.sql` in the simple query protocol, which may hold several
    /// statements and takes no parameters.
    pub async fn batch(
        &self,
        session: &impl Session,
        statement: &Statement,
    ) -> Result<(), tokio_postgres::Error> {
        self.telemetry.batch(session, statement).await
    }
    /// Text built at run time (DDL naming a derived index, a `SET` of a
    /// computed bound, a migration body) under a registered name; `sql` has
    /// the shape `statement.sql` documents. The text is never recorded.
    /// Values belong in parameters: prefer [`Database::execute`] whenever
    /// the statement can take them.
    pub async fn batch_text(
        &self,
        session: &impl Session,
        statement: &Statement,
        sql: &str,
    ) -> Result<(), tokio_postgres::Error> {
        self.telemetry.batch_text(session, statement, sql).await
    }
    /// [`Database::query`] for text built at run time under a registered
    /// name, with parameters: a list whose filters, sort keys and cursor
    /// condition vary per request, its shape documented in
    /// `statement.sql`. Every value travels as a parameter; the text holds
    /// only identifiers and placeholders the caller built from validated
    /// input. The text is never recorded.
    pub async fn query_text(
        &self,
        session: &impl Session,
        statement: &Statement,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, tokio_postgres::Error> {
        let fut = on_driver!(session, |c| c.query(sql, params));
        self.telemetry.observe(statement, fut).await
    }
    /// [`Database::query_text`] expecting exactly one row, with the
    /// driver's `query_one` contract.
    pub async fn query_one_text(
        &self,
        session: &impl Session,
        statement: &Statement,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, tokio_postgres::Error> {
        let fut = on_driver!(session, |c| c.query_one(sql, params));
        self.telemetry.observe(statement, fut).await
    }
    /// [`Database::query_text`] expecting at most one row, with the
    /// driver's `query_opt` contract.
    pub async fn query_opt_text(
        &self,
        session: &impl Session,
        statement: &Statement,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, tokio_postgres::Error> {
        let fut = on_driver!(session, |c| c.query_opt(sql, params));
        self.telemetry.observe(statement, fut).await
    }
    /// [`Database::execute`] for text built at run time under a registered
    /// name, with parameters: the number of rows affected. The text is
    /// never recorded.
    pub async fn execute_text(
        &self,
        session: &impl Session,
        statement: &Statement,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error> {
        let fut = on_driver!(session, |c| c.execute(sql, params));
        self.telemetry.observe(statement, fut).await
    }
}

impl Telemetry {
    pub(crate) async fn batch(
        &self,
        session: &impl Session,
        statement: &Statement,
    ) -> Result<(), tokio_postgres::Error> {
        let fut = on_driver!(session, |c| c.batch_execute(statement.sql));
        self.observe(statement, fut).await
    }
    pub(crate) async fn batch_text(
        &self,
        session: &impl Session,
        statement: &Statement,
        sql: &str,
    ) -> Result<(), tokio_postgres::Error> {
        let fut = on_driver!(session, |c| c.batch_execute(sql));
        self.observe(statement, fut).await
    }
    pub(crate) async fn row(
        &self,
        session: &impl Session,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, tokio_postgres::Error> {
        let fut = on_driver!(session, |c| c.query_one(statement.sql, params));
        self.observe(statement, fut).await
    }
    pub(crate) async fn maybe_row(
        &self,
        session: &impl Session,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, tokio_postgres::Error> {
        let fut = on_driver!(session, |c| c.query_opt(statement.sql, params));
        self.observe(statement, fut).await
    }
    pub(crate) async fn rows(
        &self,
        session: &impl Session,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, tokio_postgres::Error> {
        let fut = on_driver!(session, |c| c.query(statement.sql, params));
        self.observe(statement, fut).await
    }
    pub(crate) async fn run(
        &self,
        session: &impl Session,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error> {
        let fut = on_driver!(session, |c| c.execute(statement.sql, params));
        self.observe(statement, fut).await
    }
    /// `SELECT 1` in the simple query protocol.
    pub(crate) async fn probe(
        &self,
        session: &impl Session,
        statement: &Statement,
    ) -> Result<(), tokio_postgres::Error> {
        let fut = on_driver!(session, |c| c.simple_query(statement.sql));
        self.observe(statement, fut).await.map(drop)
    }
}

/// The kernel's own statements. [`KERNEL_STATEMENTS`] lists them all.
pub mod kernel {
    use super::Statement;

    /// `Database::ping` and readiness.
    pub const PING: Statement = Statement::new("kernel.ping", "SELECT", "SELECT 1");
    /// Every transaction's `BEGIN` (issued by the driver).
    pub const BEGIN: Statement = Statement::new("kernel.transaction.begin", "BEGIN", "BEGIN");
    /// Every transaction's `COMMIT` (issued by the driver).
    pub const COMMIT: Statement = Statement::new("kernel.transaction.commit", "COMMIT", "COMMIT");
    /// The deadline bound of a transaction; the milliseconds are computed.
    pub const TRANSACTION_TIMEOUT: Statement = Statement::new(
        "kernel.transaction.timeout",
        "SET",
        "SET LOCAL statement_timeout = <milliseconds>",
    );
    /// The session setting of every detached client.
    pub const CONNECTION_CHECK: Statement = Statement::new(
        "kernel.session.connection_check",
        "SET",
        "SET client_connection_check_interval = 500",
    );
    /// A listener session's `LISTEN` of its validated channels.
    pub const LISTEN: Statement = Statement::new("kernel.listen", "LISTEN", "LISTEN <channel>;");
    /// `Listener::check`.
    pub const LISTENER_CHECK: Statement =
        Statement::new("kernel.listener.check", "SELECT", "SELECT 1");
    /// The migration runner's session bound; the milliseconds are computed.
    pub const MIGRATION_TIMEOUT: Statement = Statement::new(
        "kernel.migration.timeout",
        "SET",
        "SET statement_timeout = <milliseconds>",
    );
    /// The verify run's read-only transaction and its bound.
    pub const MIGRATION_READ_ONLY: Statement = Statement::new(
        "kernel.migration.read_only",
        "SET",
        "SET LOCAL statement_timeout = <milliseconds>; SET TRANSACTION READ ONLY",
    );
    /// One attempt at the migration advisory lock.
    pub const MIGRATION_LOCK: Statement = Statement::new(
        "kernel.migration.lock",
        "SELECT pg_try_advisory_lock()",
        "SELECT pg_try_advisory_lock($1)",
    );
    /// The ledger schema and table.
    pub const MIGRATION_LEDGER: Statement = Statement::new(
        "kernel.migration.ledger",
        "CREATE merezha.migrations",
        crate::migrate::LEDGER,
    );
    pub const MIGRATION_LEDGER_EXISTS: Statement = Statement::new(
        "kernel.migration.ledger_exists",
        "SELECT to_regclass()",
        "SELECT to_regclass('merezha.migrations') IS NOT NULL",
    );
    pub const MIGRATION_LEDGER_READ: Statement = Statement::new(
        "kernel.migration.ledger_read",
        "SELECT merezha.migrations",
        "SELECT module, version, name, checksum FROM merezha.migrations WHERE module = ANY($1)",
    );
    pub const MIGRATION_RECORD: Statement = Statement::new(
        "kernel.migration.record",
        "INSERT merezha.migrations",
        "INSERT INTO merezha.migrations (module, version, name, checksum) VALUES ($1, $2, $3, $4)",
    );
    /// A transactional migration's SQL, declared by its module.
    pub const MIGRATION_APPLY: Statement =
        Statement::new("kernel.migration.apply", "MIGRATE", "<the migration's SQL>");
    /// A non-transactional step's one statement, declared by its module.
    pub const MIGRATION_STEP: Statement =
        Statement::new("kernel.migration.step", "MIGRATE", "<the step's statement>");
    /// The invalid index a failed concurrent build left behind.
    pub const MIGRATION_FIND_INVALID_INDEX: Statement = Statement::new(
        "kernel.migration.find_invalid_index",
        "SELECT pg_index",
        "SELECT format('%I.%I', n.nspname, c.relname)
             FROM pg_index i
             JOIN pg_class c ON c.oid = i.indexrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE i.indrelid = to_regclass($1)::oid AND c.relname = $2 AND NOT i.indisvalid",
    );
    pub const MIGRATION_DROP_INVALID_INDEX: Statement = Statement::new(
        "kernel.migration.drop_invalid_index",
        "DROP INDEX",
        "DROP INDEX CONCURRENTLY IF EXISTS <schema>.<index>",
    );
}

/// Every statement the kernel runs.
pub const KERNEL_STATEMENTS: &[Statement] = &[
    kernel::PING,
    kernel::BEGIN,
    kernel::COMMIT,
    kernel::TRANSACTION_TIMEOUT,
    kernel::CONNECTION_CHECK,
    kernel::LISTEN,
    kernel::LISTENER_CHECK,
    kernel::MIGRATION_TIMEOUT,
    kernel::MIGRATION_READ_ONLY,
    kernel::MIGRATION_LOCK,
    kernel::MIGRATION_LEDGER,
    kernel::MIGRATION_LEDGER_EXISTS,
    kernel::MIGRATION_LEDGER_READ,
    kernel::MIGRATION_RECORD,
    kernel::MIGRATION_APPLY,
    kernel::MIGRATION_STEP,
    kernel::MIGRATION_FIND_INVALID_INDEX,
    kernel::MIGRATION_DROP_INVALID_INDEX,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_follow_the_registry_rules() {
        for name in ["app.events.append", "kernel.ping", "a1.b_2.c"] {
            assert!(valid_name(name.as_bytes()), "{name}");
        }
        let long = format!("a.{}", "b".repeat(63));
        for name in [
            "",
            "adhoc",
            "ping",
            "Kernel.ping",
            "kernel..ping",
            "kernel.ping.",
            ".kernel",
            "kernel.1ping",
            "kernel.pi-ng",
            &long,
        ] {
            assert!(!valid_name(name.as_bytes()), "{name}");
        }
        for summary in [
            "SELECT",
            "INSERT merezha_app.events",
            "CREATE INDEX",
            "DROP INDEX content.entries",
            "SELECT pg_try_advisory_lock()",
            "SELECT _private",
        ] {
            assert!(valid_summary(summary.as_bytes()), "{summary}");
        }
        for summary in [
            "",
            "select t",
            "SELECT  t",
            " SELECT",
            "SELECT ",
            "SELECT a b",
            "t SELECT",
            "SELECT t.",
            "SELECT 'x'",
            "SELECT t WHERE",
            "A B C D",
            "SELECT t(1)",
            "SELECT t;DROP",
        ] {
            assert!(!valid_summary(summary.as_bytes()), "{summary}");
        }
    }
    #[test]
    fn operation_and_collection_derive_from_the_summary() {
        let cases = [
            (
                kernel::MIGRATION_RECORD,
                "INSERT",
                Some("merezha.migrations"),
            ),
            (kernel::MIGRATION_LOCK, "SELECT", None),
            (kernel::PING, "SELECT", None),
            (kernel::MIGRATION_DROP_INVALID_INDEX, "DROP INDEX", None),
            (
                Statement::new("x.y", "CREATE INDEX content.entries", "x"),
                "CREATE INDEX",
                Some("content.entries"),
            ),
            (
                Statement::new("x.y", "SELECT _t", "x"),
                "SELECT",
                Some("_t"),
            ),
        ];
        for (statement, operation, collection) in cases {
            assert_eq!(statement.operation(), operation, "{}", statement.name);
            assert_eq!(statement.collection(), collection, "{}", statement.name);
        }
    }
    #[test]
    fn the_kernel_registry_is_unique_and_duplicates_are_reported() {
        assert_eq!(check_registry(&[KERNEL_STATEMENTS]), Ok(()));
        let again = [kernel::PING];
        assert_eq!(
            check_registry(&[KERNEL_STATEMENTS, &again]),
            Err(vec!["kernel.ping"])
        );
    }
    #[test]
    fn the_connection_check_statement_matches_its_constant() {
        assert_eq!(
            kernel::CONNECTION_CHECK.sql,
            format!(
                "SET client_connection_check_interval = {}",
                crate::CONNECTION_CHECK_INTERVAL.as_millis()
            )
        );
    }
}
