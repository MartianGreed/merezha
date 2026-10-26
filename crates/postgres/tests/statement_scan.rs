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
//! Comments and string contents are ignored. The method list is checked
//! against the locked driver's sources, so an upgrade that adds a way to
//! run SQL fails until the list has it.
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
    calls_beside(snippet, &[])
}

/// [`calls`] with `elsewhere` declared as statements by another file.
fn calls_beside(snippet: &str, elsewhere: &[&str]) -> Vec<(usize, String)> {
    let file = scan::parse(&format!("fn fixture() {{{snippet}\n}}"))
        .unwrap_or_else(|e| panic!("{snippet}: {e}"));
    let mut statements = scan::statement_constants(&file);
    statements.extend(elsewhere.iter().map(|name| (*name).to_owned()));
    scan::unregistered_calls(&file, &statements)
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

/// The PR #70 review: driver methods the first list lacked, a parameter
/// constant named like a statement of another file, generic and
/// metavariable paths, and the rules no fixture pinned yet.
#[test]
fn the_scan_flags_the_forms_the_review_found() {
    let forms: [(&str, &str, &str); 12] = [
        (
            "scalar query with a turbofish",
            "client.query_scalar::<i64, _>(\"SELECT 1\", &[]);",
            "query_scalar",
        ),
        (
            "typed execute",
            "client.execute_typed(\"DELETE FROM t WHERE id = $1\", &[(&id, Type::INT8)]);",
            "execute_typed",
        ),
        (
            "raw simple query",
            "client.simple_query_raw(\"SELECT 1\");",
            "simple_query_raw",
        ),
        (
            "generic parameter bounded by the driver trait",
            "fn run<C: GenericClient>(c: &C) { C::execute(c, sql, &[]); }",
            "C::execute",
        ),
        (
            "generic parameter bounded in a where clause",
            "fn run<C>(c: &C) where C: tokio_postgres::GenericClient { C::query(c, sql, &[]); }",
            "C::query",
        ),
        (
            "metavariable type in a macro body",
            "macro_rules! run { ($t:ident) => { $t::execute(tx, \"DELETE FROM t\", &[]) }; }",
            "$t::execute",
        ),
        (
            "metavariable method on a qualified path in a macro body",
            "macro_rules! run { ($t:ty, $m:ident) => { <$t>::$m(tx, \"DELETE FROM t\", &[]) }; }",
            "$m",
        ),
        (
            "driver type on a qualified path in a macro body",
            "macro_rules! run { () => { <Transaction as Run>::execute(tx, \"DELETE FROM t\", &[]) }; }",
            "Transaction::execute",
        ),
        (
            "turbofish in a macro body",
            "macro_rules! run { ($c:expr) => { $c.query_raw::<_, &dyn ToSql, _>(\"SELECT 1\", p) }; }",
            "query_raw",
        ),
        (
            "driver type before `as` on a qualified path",
            "<tokio_postgres::Transaction<'_> as Run>::query(&tx, sql, &[]);",
            "Run::query",
        ),
        (
            "type alias",
            "type Conn = tokio_postgres::Client;\nConn::query(&c, sql, &[]);",
            "Conn::query",
        ),
        (
            "alias of an alias",
            "type Conn = tokio_postgres::Client;\ntype Again = Conn;\nAgain::execute(&c, sql, &[]);",
            "Again::execute",
        ),
    ];
    let mut missed = Vec::new();
    for (form, source, what) in forms {
        let found = calls(source);
        if found.len() != 1 || found[0].1 != what {
            missed.push(format!("{form}: {found:?}"));
        }
    }
    // Another file declares a statement `HEAD`; this file's `HEAD` is a
    // parameter list, so it is no statement here.
    for source in [
        "const HEAD: &[&(dyn ToSql + Sync)] = &[];\ntx.execute(sql, &HEAD);",
        "static HEAD: &[&(dyn ToSql + Sync)] = &[];\ntx.execute(sql, &HEAD);",
    ] {
        let found = calls_beside(source, &["HEAD"]);
        if found != vec![(2, "execute".to_owned())] {
            missed.push(format!(
                "parameter constant named like a statement: {found:?}"
            ));
        }
    }
    assert!(missed.is_empty(), "missed:\n{}", missed.join("\n"));
    // Still fine: the statement itself, and a generic bound off the driver.
    assert_eq!(calls_beside("other.bind(&a, &HEAD);", &["HEAD"]), vec![]);
    assert_eq!(
        calls("fn run<C: std::fmt::Display>(c: C) { C::execute(c, a, b); }"),
        vec![]
    );
}

/// The sources of a locked dependency, from `cargo metadata` (downloaded
/// already: the build compiled them).
fn dependency_sources(name: &str) -> (String, PathBuf) {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let host = std::process::Command::new(&cargo)
        .arg("-vV")
        .output()
        .expect("cargo -vV");
    let host = String::from_utf8(host.stdout).expect("utf-8");
    let host = host
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .expect("the host triple");
    let output = std::process::Command::new(&cargo)
        .args([
            "metadata",
            "--locked",
            "--format-version",
            "1",
            "--filter-platform",
            host,
        ])
        .current_dir(workspace_crates())
        .output()
        .expect("cargo metadata");
    assert!(
        output.status.success(),
        "cargo metadata: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).expect("metadata");
    let packages = metadata["packages"].as_array().expect("packages");
    let package = packages
        .iter()
        .find(|p| p["name"] == name)
        .unwrap_or_else(|| panic!("{name} is not in the lock"));
    let manifest = Path::new(package["manifest_path"].as_str().expect("manifest_path"));
    (
        package["version"].as_str().expect("version").to_owned(),
        manifest.parent().expect("crate directory").join("src"),
    )
}

/// The public methods of the driver's `Client`, `Transaction` and
/// `GenericClient` (`tokio_postgres` and `deadpool_postgres`, whose
/// `ClientWrapper` and `Transaction` add the cached prepares) that take a
/// query, statement or portal first, as `(name, arguments after self)`.
fn driver_methods(sources: &Path) -> BTreeSet<(String, usize)> {
    use syn::{FnArg, ImplItem, Item, Pat, Signature, TraitItem, Type, Visibility};
    fn takes_sql(signature: &Signature) -> Option<(String, usize)> {
        let mut inputs = signature.inputs.iter();
        if !matches!(inputs.next(), Some(FnArg::Receiver(_))) {
            return None;
        }
        let rest: Vec<&FnArg> = inputs.collect();
        let Some(FnArg::Typed(first)) = rest.first() else {
            return None;
        };
        let Pat::Ident(name) = &*first.pat else {
            return None;
        };
        ["query", "statement", "portal"]
            .contains(&name.ident.to_string().as_str())
            .then(|| (signature.ident.to_string(), rest.len()))
    }
    let mut files = Vec::new();
    rust_files(sources, &mut files);
    let mut methods = BTreeSet::new();
    for path in files {
        let source = std::fs::read_to_string(&path).expect("driver source");
        let file = scan::parse(&source)
            .unwrap_or_else(|e| panic!("{}: does not parse: {e}", path.display()));
        for item in &file.items {
            match item {
                Item::Impl(item) if item.trait_.is_none() => {
                    let Type::Path(ty) = &*item.self_ty else {
                        continue;
                    };
                    let Some(last) = ty.path.segments.last() else {
                        continue;
                    };
                    if !["Client", "Transaction", "ClientWrapper"]
                        .contains(&last.ident.to_string().as_str())
                    {
                        continue;
                    }
                    for member in &item.items {
                        if let ImplItem::Fn(f) = member
                            && matches!(f.vis, Visibility::Public(_))
                        {
                            methods.extend(takes_sql(&f.sig));
                        }
                    }
                }
                Item::Trait(item) if item.ident == "GenericClient" => {
                    for member in &item.items {
                        if let TraitItem::Fn(f) = member {
                            methods.extend(takes_sql(&f.sig));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    methods
}

/// The scan's method list is exactly the locked driver's, so a driver
/// upgrade that adds a way to run SQL fails here until the list has it.
#[test]
fn the_driver_method_list_is_the_locked_drivers() {
    let mut driver = BTreeSet::new();
    for (name, expected) in [
        ("tokio-postgres", "0.7.18"),
        ("deadpool-postgres", "0.14.2"),
    ] {
        let (version, sources) = dependency_sources(name);
        // Checked by hand for these versions: the methods above are every
        // way they run SQL. Re-check on an upgrade, then bump.
        assert_eq!(version, expected, "{name} moved: re-check DRIVER_METHODS");
        driver.extend(driver_methods(&sources));
    }
    let listed: BTreeSet<(String, usize)> = scan::DRIVER_METHODS
        .iter()
        .map(|(name, arity)| ((*name).to_owned(), *arity))
        .collect();
    assert_eq!(listed, driver);
}
