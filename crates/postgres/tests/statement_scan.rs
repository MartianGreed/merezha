//! The workspace's statement-registry scan (TE §1.3 "PostgreSQL"): SQL a
//! crate hands to the driver directly opens no DB span and no
//! `db.client.operation.duration` series, so every Rust source under
//! `crates/*/src` is scanned with `statement::unregistered_calls`, which
//! flags each driver method (`statement::DRIVER_METHODS`: `query`,
//! `query_one`, `query_opt`, `execute`, `batch_execute`, `query_typed`,
//! `prepare`, `prepare_typed`, `prepare_cached`, `simple_query`, `copy_in`,
//! `copy_out` and the rest) whose second argument is not a registered
//! `&STATEMENT`. Comments and string contents are ignored.
//!
//! [`ALLOWED`] lists the files that may still do so, with their exact call
//! count and the reason. It only shrinks: a new call fails the scan, and so
//! does an entry whose file now has fewer calls than listed.
use merezha_postgres::statement::unregistered_calls;
use std::{
    collections::BTreeMap,
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

/// Every unregistered call under `crates/*/src`, per `(crate, file)`, as
/// `line: method`.
fn unregistered() -> (usize, BTreeMap<(String, String), Vec<String>>) {
    let crates = workspace_crates();
    let mut found = BTreeMap::new();
    let mut files = 0;
    let mut members: Vec<PathBuf> = std::fs::read_dir(&crates)
        .expect("crates/")
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| path.join("Cargo.toml").is_file())
        .collect();
    members.sort();
    for member in members {
        let name = member
            .file_name()
            .and_then(|n| n.to_str())
            .expect("crate directory name")
            .to_owned();
        let mut sources = Vec::new();
        rust_files(&member.join("src"), &mut sources);
        for path in sources {
            files += 1;
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let calls = unregistered_calls(&source);
            if calls.is_empty() {
                continue;
            }
            let relative = path
                .strip_prefix(&member)
                .expect("under the crate")
                .to_string_lossy()
                .replace('\\', "/");
            found.insert(
                (name.clone(), relative),
                calls
                    .into_iter()
                    .map(|(line, method)| format!("{line}: {method}"))
                    .collect(),
            );
        }
    }
    (files, found)
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
