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

/// Driver methods that take SQL text, a driver statement or a portal
/// first: a call to one of them is unregistered unless its second argument
/// is a registered [`Statement`] (the registry's own methods take the
/// session first and the statement second).
pub const DRIVER_METHODS: &[&str] = &[
    "query",
    "query_one",
    "query_opt",
    "query_raw",
    "query_typed",
    "query_typed_raw",
    "query_portal",
    "execute",
    "execute_raw",
    "batch_execute",
    "simple_query",
    "prepare",
    "prepare_typed",
    "prepare_cached",
    "prepare_typed_cached",
    "bind",
    "copy_in",
    "copy_out",
];

/// `source` with comments blanked and string and character literal
/// contents replaced by `_`, newlines kept, so calls are found in code only
/// and arguments split on code commas only.
fn code_only(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    let blank = |b: u8| if b == b'\n' { b'\n' } else { b' ' };
    while i < bytes.len() {
        let b = bytes[i];
        if bytes[i..].starts_with(b"//") {
            while i < bytes.len() && bytes[i] != b'\n' {
                out.push(b' ');
                i += 1;
            }
        } else if bytes[i..].starts_with(b"/*") {
            let mut depth = 0usize;
            while i < bytes.len() {
                if bytes[i..].starts_with(b"/*") {
                    depth += 1;
                    out.extend_from_slice(b"  ");
                    i += 2;
                } else if bytes[i..].starts_with(b"*/") {
                    depth -= 1;
                    out.extend_from_slice(b"  ");
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    out.push(blank(bytes[i]));
                    i += 1;
                }
            }
        } else if b == b'r'
            && matches!(bytes.get(i + 1), Some(b'"' | b'#'))
            && !bytes
                .get(i.wrapping_sub(1))
                .is_some_and(|p| p.is_ascii_alphanumeric() || *p == b'_')
        {
            // Raw string: r"…", r#"…"#.
            let hashes = bytes[i + 1..].iter().take_while(|c| **c == b'#').count();
            if bytes.get(i + 1 + hashes) != Some(&b'"') {
                out.push(b);
                i += 1;
                continue;
            }
            out.push(b'"');
            i += 2 + hashes;
            let mut close = vec![b'"'];
            close.extend(std::iter::repeat_n(b'#', hashes));
            while i < bytes.len() && !bytes[i..].starts_with(&close) {
                out.push(if bytes[i] == b'\n' { b'\n' } else { b'_' });
                i += 1;
            }
            out.push(b'"');
            i += close.len();
        } else if b == b'"' {
            out.push(b'"');
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                if bytes[i] == b'\\' {
                    out.push(b'_');
                    i += 1;
                }
                if i < bytes.len() {
                    out.push(if bytes[i] == b'\n' { b'\n' } else { b'_' });
                    i += 1;
                }
            }
            out.push(b'"');
            i += 1;
        } else if b == b'\'' {
            // A character literal ('x', '\n', '"'); otherwise a lifetime.
            let end = if bytes.get(i + 1) == Some(&b'\\') {
                bytes[i + 2..]
                    .iter()
                    .position(|c| *c == b'\'')
                    .map(|p| i + 2 + p)
            } else {
                let width = source[i + 1..].chars().next().map_or(1, char::len_utf8);
                (bytes.get(i + 1 + width) == Some(&b'\'')).then_some(i + 1 + width)
            };
            match end {
                Some(end) => {
                    out.push(b'\'');
                    out.extend(std::iter::repeat_n(b'_', end - i - 1));
                    out.push(b'\'');
                    i = end + 1;
                }
                None => {
                    out.push(b);
                    i += 1;
                }
            }
        } else {
            out.push(b);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The top-level arguments of the call whose `(` ends at `start`.
fn arguments(code: &str, start: usize) -> Vec<&str> {
    let bytes = code.as_bytes();
    let mut depth = 0usize;
    let mut args = Vec::new();
    let mut from = start;
    for (offset, b) in bytes[start..].iter().enumerate() {
        let at = start + offset;
        match b {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' if depth > 0 => depth -= 1,
            b')' => {
                let last = code[from..at].trim();
                if !last.is_empty() {
                    args.push(last);
                }
                return args;
            }
            b',' if depth == 0 => {
                args.push(code[from..at].trim());
                from = at + 1;
            }
            _ => {}
        }
    }
    args
}

/// `&path::TO::A_CONSTANT`: a registered statement constant by convention.
fn statement_argument(argument: &str) -> bool {
    let Some(path) = argument.strip_prefix('&') else {
        return false;
    };
    let segments: Vec<&str> = path.trim().split("::").collect();
    segments.iter().all(|s| {
        !s.is_empty()
            && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            && !s.as_bytes()[0].is_ascii_digit()
    }) && segments.last().is_some_and(|last| {
        last.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    })
}

/// Calls in `source` that hand SQL to the driver instead of a registered
/// [`Statement`], as `(line, method)`: a [`DRIVER_METHODS`] call with
/// arguments whose second argument is not `&A_STATEMENT`. Comments and
/// string contents are ignored. A block lists its own violations in a test
/// with [`scan_sources`].
pub fn unregistered_calls(source: &str) -> Vec<(usize, &'static str)> {
    let code = code_only(source);
    let mut found = Vec::new();
    for method in DRIVER_METHODS {
        let needle = format!(".{method}(");
        for (at, _) in code.match_indices(&needle) {
            let args = arguments(&code, at + needle.len());
            if args.is_empty() {
                continue;
            }
            if args.get(1).is_some_and(|a| statement_argument(a)) {
                continue;
            }
            let line = code[..at].bytes().filter(|b| *b == b'\n').count() + 1;
            found.push((line, *method));
        }
    }
    found.sort_unstable();
    found
}

/// [`unregistered_calls`] over every `.rs` file under `dir`, recursively,
/// except the file names in `skip`, as `path:line: method`. A file that
/// cannot be read is an error.
pub fn scan_sources(dir: &std::path::Path, skip: &[&str]) -> std::io::Result<Vec<String>> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    let mut files = 0usize;
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path)? {
                pending.push(entry?.path());
            }
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if !name.ends_with(".rs") || skip.contains(&name) {
            continue;
        }
        files += 1;
        let source = std::fs::read_to_string(&path)?;
        for (line, method) in unregistered_calls(&source) {
            found.push(format!("{}:{line}: {method}", path.display()));
        }
    }
    if files == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no Rust source to scan",
        ));
    }
    found.sort();
    Ok(found)
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
    /// Every SQL call of the kernel's runtime goes through the registry:
    /// no source file of the crate hands SQL to the driver. `statement.rs`
    /// is the registry's own dispatch, and the test harness (`testing.rs`)
    /// runs administrative SQL on its own connection outside any pool.
    #[test]
    fn kernel_sources_call_no_driver_method_directly() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let found = scan_sources(&dir, &["statement.rs", "testing.rs"]).unwrap();
        assert!(found.is_empty(), "unregistered SQL: {found:?}");
    }
    #[test]
    fn the_scan_flags_every_bypass_form() {
        let bypasses = [
            "let sql = \"SELECT 1\";\nclient.query(sql, &[]).await",
            "client.query_one(\"SELECT 1\", &[])",
            "client\n    .execute(\n        \"INSERT INTO t VALUES ($1)\",\n        &[&a],\n    )",
            "tx.batch_execute(&set)",
            "client.simple_query(\"SELECT 1\")",
            "client.query_typed(SQL, &[])",
            "client.prepare_typed(SQL, &[])",
            "client.prepare_cached(sql::APPEND.sql)",
            "client.query_raw(&statement, params)",
            "client.query_one(&prepared, &params)",
            "client.query(kernel::PING.sql, &[])",
            "client.copy_in(\"COPY t FROM STDIN\")",
        ];
        for source in bypasses {
            assert_eq!(unregistered_calls(source).len(), 1, "{source}");
        }
        let fine = [
            "db.query(&client, &sql::HEAD, &[])",
            "db.execute(tx, &INSERT, &[&a, &b])",
            "self.db\n    .query_opt(\n        &self.client,\n        &sql::PROJECTION,\n        &[&name, &revision],\n    )",
            "db.prepare_cached(&client, &sql::APPEND)",
            "url.query()",
            "// client.query(\"SELECT 1\", &[])",
            "/* tx.batch_execute(\"x\") */",
            "let doc = \"call .query(\\\"x\\\", &[]) here\";",
            "let quote = b'\"'; let other = 'a';",
            "let raw = r#\"client.execute(\"x\", &[])\"#;",
        ];
        for source in fine {
            assert_eq!(unregistered_calls(source), vec![], "{source}");
        }
        assert_eq!(
            unregistered_calls("fn a<'t>(x: &'t str) {}\nclient.execute(sql, &[])"),
            vec![(2, "execute")]
        );
    }
}
