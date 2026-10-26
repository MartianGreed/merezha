//! The workspace's statement-registry scan (TE §1.3 "PostgreSQL"): SQL a
//! crate hands to the driver directly opens no DB span and no
//! `db.client.operation.duration` series, so every Rust source under
//! `crates/*/src` is parsed and scanned (`scan`, whose module
//! documentation gives the rules): method calls of a driver method
//! (`scan::DRIVER_METHODS`: `query`, `query_one`, `query_opt`, `execute`,
//! `batch_execute`, `query_typed`, `prepare`, `prepare_typed`,
//! `prepare_cached`, `simple_query`, `copy_in`, `copy_out` and the rest)
//! with the driver's argument count whose second argument is not a
//! registered `&STATEMENT`, turbofish included; path calls and method
//! references of those methods on a driver type (`Transaction::execute`,
//! `GenericClient::query`); and macros, whose arguments and `macro_rules!`
//! bodies are scanned too, a method named by a metavariable included.
//! Comments and string contents are ignored.
//!
//! [`ALLOWED`] lists the files that may still do so, with their exact call
//! count and the reason. It only shrinks: a new call fails the scan, and so
//! does an entry whose file now has fewer calls than listed.
mod scan;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

/// `(crate directory, file under it, unregistered calls, why)`.
const ALLOWED: &[(&str, &str, usize, &str)] = &[
    (
        "postgres",
        "src/statement.rs",
        13,
        "the registry's own dispatch: every registered statement reaches the driver here",
    ),
    (
        "postgres",
        "src/testing.rs",
        1,
        "the test harness's administrative connection (CREATE and DROP DATABASE), outside any pool",
    ),
    (
        "jobs",
        "src/outbox.rs",
        2,
        "`PgOutbox`, the unmetered public form tests build as a unit value, holds no Database; \
         the runtime's `MeteredOutbox` runs the same two statements through the registry",
    ),
    (
        "application",
        "src/sqlite.rs",
        18,
        "rusqlite's `prepare` and `execute` on the SQLite development store, not PostgreSQL",
    ),
    (
        "application",
        "src/application.rs",
        1,
        "`Instruments::query(tag, metadata)` opens a query span; it runs no SQL",
    ),
    (
        "application",
        "src/postgres/tests.rs",
        3,
        "crash-cut unit tests that plant and inspect rows behind the store on purpose",
    ),
];

fn workspace_crates() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/postgres sits in crates/")
        .to_path_buf()
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.expect("directory entry").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Every `crates/*/src` file, parsed, as `((crate, file under it), syntax)`.
fn sources() -> Vec<((String, String), syn::File)> {
    let crates = workspace_crates();
    let mut members: Vec<PathBuf> = std::fs::read_dir(&crates)
        .expect("crates/")
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| path.join("Cargo.toml").is_file())
        .collect();
    members.sort();
    let mut parsed = Vec::new();
    for member in members {
        let name = member
            .file_name()
            .and_then(|n| n.to_str())
            .expect("crate directory name")
            .to_owned();
        let mut files = Vec::new();
        rust_files(&member.join("src"), &mut files);
        for path in files {
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let file = scan::parse(&source)
                .unwrap_or_else(|e| panic!("{}: does not parse: {e}", path.display()));
            let relative = path
                .strip_prefix(&member)
                .expect("under the crate")
                .to_string_lossy()
                .replace('\\', "/");
            parsed.push(((name.clone(), relative), file));
        }
    }
    parsed
}

/// Every unregistered call under `crates/*/src`, per `(crate, file)`, as
/// `line: what`, with the number of files scanned.
fn unregistered() -> (usize, BTreeMap<(String, String), Vec<String>>) {
    let sources = sources();
    let statements: BTreeSet<String> = sources
        .iter()
        .flat_map(|(_, file)| scan::statement_constants(file))
        .collect();
    let mut found = BTreeMap::new();
    for (key, file) in &sources {
        let calls = scan::unregistered_calls(file, &statements);
        if !calls.is_empty() {
            found.insert(
                key.clone(),
                calls
                    .into_iter()
                    .map(|(line, what)| format!("{line}: {what}"))
                    .collect(),
            );
        }
    }
    (sources.len(), found)
}

#[test]
fn no_crate_hands_sql_to_the_driver_outside_the_allowlist() {
    let (files, mut found) = unregistered();
    // Every workspace crate is walked, not only this one.
    assert!(files > 100, "scanned only {files} files");
    let mut errors = Vec::new();
    for (krate, file, allowed, why) in ALLOWED {
        assert!(!why.is_empty(), "{krate}/{file} needs a reason");
        let calls = found
            .remove(&((*krate).to_owned(), (*file).to_owned()))
            .unwrap_or_default();
        if calls.len() != *allowed {
            errors.push(format!(
                "crates/{krate}/{file}: {} unregistered calls, {allowed} allowed \
                 (lower the entry when calls move to the registry): {calls:?}",
                calls.len()
            ));
        }
    }
    for ((krate, file), calls) in found {
        errors.push(format!("crates/{krate}/{file}: unregistered SQL {calls:?}"));
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn the_allowlist_has_one_entry_per_file() {
    let mut seen = std::collections::BTreeSet::new();
    for (krate, file, _, _) in ALLOWED {
        assert!(seen.insert((krate, file)), "{krate}/{file} listed twice");
    }
}

/// `snippet` as the body of a function, scanned with the statement
/// constants it declares itself. Lines count from the snippet's first.
fn calls(snippet: &str) -> Vec<(usize, String)> {
    let file = scan::parse(&format!("fn fixture() {{{snippet}\n}}"))
        .unwrap_or_else(|e| panic!("{snippet}: {e}"));
    scan::unregistered_calls(&file, &scan::statement_constants(&file))
}

#[test]
fn the_scan_flags_every_bypass_form() {
    let bypasses = [
        "let sql = \"SELECT 1\";\nclient.query(sql, &[]).await;",
        "client.query_one(\"SELECT 1\", &[]);",
        "client\n    .execute(\n        \"INSERT INTO t VALUES ($1)\",\n        &[&a],\n    );",
        "tx.batch_execute(&set);",
        "client.simple_query(\"SELECT 1\");",
        "client.query_typed(SQL, &[]);",
        "client.prepare_typed(SQL, &[]);",
        "client.prepare_cached(sql::APPEND.sql);",
        "client.query_raw(&statement, params);",
        "client.query_one(&prepared, &params);",
        "client.query(kernel::PING.sql, &[]);",
        "client.copy_in(\"COPY t FROM STDIN\");",
        // Inside macro arguments, parsed as code or read as tokens.
        "assert!(client.execute(sql, &[]).await.is_ok());",
        "tokio::select! { r = client.execute(sql, &[]) => r, _ = stop => 0 };",
    ];
    for source in bypasses {
        assert_eq!(calls(source).len(), 1, "{source}: {:?}", calls(source));
    }
    let fine = [
        "db.query(&client, &sql::HEAD, &[]);",
        "db.execute(tx, &INSERT, &[&a, &b]);",
        "self.db\n    .query_opt(\n        &self.client,\n        &sql::PROJECTION,\n        &[&name, &revision],\n    );",
        "db.prepare_cached(&client, &sql::APPEND);",
        "url.query();",
        "// client.query(\"SELECT 1\", &[])",
        "/* tx.batch_execute(\"x\") */",
        "let doc = \"call .query(\\\"x\\\", &[]) here\";",
        "let quote = b'\"'; let other = 'a';",
        "let raw = r#\"client.execute(\"x\", &[])\"#;",
        // Same-named methods of other arities are not the driver's.
        "app.query(query);",
        "self.client.execute(request);",
        "outbox.bind(&source);",
        "runtime.prepare(registry, ids, operation, None);",
        "client.batch_execute(sql, extra);",
        // Path calls on types that are not the driver's.
        "EarlyListener::bind(&config, None, &options);",
        "Database::query(&db, tx, &sql::HEAD, &[]);",
        "let f = Admission::query;",
        // A registered statement second: the registry's shape.
        "const PING: Statement = Statement::new(\"a.b\", \"SELECT\", \"SELECT 1\");\nother.bind(&a, &PING);",
        // A metavariable that is not a method.
        "macro_rules! level { ($l:ident) => { tracing::Level::$l }; }",
    ];
    for source in fine {
        assert_eq!(calls(source), vec![], "{source}");
    }
    assert_eq!(
        calls("fn a<'t>(x: &'t str) {}\nclient.execute(sql, &[]);"),
        vec![(2, "execute".to_owned())]
    );
}

/// Issue #53: each form the text scan missed is flagged.
#[test]
fn the_scan_flags_the_forms_the_text_scan_missed() {
    let blind_spots: [(&str, &str, &str); 9] = [
        (
            "path call",
            "Transaction::execute(tx, \"DELETE FROM t\", &[]);",
            "Transaction::execute",
        ),
        (
            "trait path call",
            "use tokio_postgres::GenericClient;\nGenericClient::query(&client, \"SELECT 1\", &[]);",
            "GenericClient::query",
        ),
        (
            "qualified path call",
            "<Transaction as GenericClient>::query(&tx, sql, &[]);",
            "GenericClient::query",
        ),
        (
            "renamed trait",
            "use tokio_postgres::GenericClient as Driver;\nDriver::execute(&client, sql, &[]);",
            "Driver::execute",
        ),
        (
            "method reference",
            "let f = Transaction::batch_execute;",
            "Transaction::batch_execute",
        ),
        (
            "turbofish",
            "client.query_raw::<_, &dyn ToSql, _>(sql, params);",
            "query_raw",
        ),
        (
            "macro taking the method name",
            "macro_rules! run { ($c:expr, $m:ident) => { $c.$m(\"DELETE FROM t\", &[]) }; }\nrun!(client, execute);",
            "$m",
        ),
        (
            "uppercase parameter constant",
            "const NONE: &[&(dyn ToSql + Sync)] = &[];\ntx.execute(sql, &NONE);",
            "execute",
        ),
        (
            "driver call in a macro body",
            "macro_rules! run { ($c:expr) => { $c.execute(\"DELETE FROM t\", &[]) }; }",
            "execute",
        ),
    ];
    for (form, source, what) in blind_spots {
        let found = calls(source);
        assert_eq!(found.len(), 1, "{form}: {source}: {found:?}");
        assert_eq!(found[0].1, what, "{form}");
    }
}
