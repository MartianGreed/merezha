//! Every event the workspace's sources emit through `event!` is in the
//! catalog at the level of the call (TE §4, §8).
//!
//! The runtime catalog checks (`Harness::assert_catalogued`, and
//! `testing::check_stderr_catalogued` in the process tests) only see the
//! events a test happens to drive. `db.pool_undersized` is emitted inside
//! the `merezha` process at startup and was observed only by a process test
//! that read stderr without the check, so it shipped without a catalog row.
//! This scan reads every `event!(LEVEL, "name", …)` call in `crates/*/src`
//! instead, so a name missing from `names.rs` fails whether or not a test
//! reaches it. A call whose level is a macro metavariable has its name
//! checked and its level left to the runtime checks; comment lines, doc
//! examples included, are not calls.
use merezha_telemetry::names;
use std::{
    fs,
    path::{Path, PathBuf},
};

fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Source text without comment lines, so the examples in doc comments
/// (which name catalog events too) are not counted as calls.
fn code(text: &str) -> String {
    text.lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// `(level, name)` of each `event!(` call written with a level and a string
/// literal name, `parent: …` allowed first. The level is `None` when it is
/// a macro metavariable (`event!($level, "http.request.completed", …)`):
/// the name is still checked. Other shapes (the macro's own definition)
/// are not calls and are skipped.
fn calls(text: &str) -> Vec<(Option<String>, String)> {
    let text = code(text);
    let mut found = Vec::new();
    for (at, _) in text.match_indices("event!(") {
        let mut rest = text[at + "event!(".len()..].trim_start();
        if let Some(after) = rest.strip_prefix("parent:") {
            let Some(comma) = after.find(',') else {
                continue;
            };
            rest = after[comma + 1..].trim_start();
        }
        let word: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$')
            .collect();
        let level = if ["ERROR", "WARN", "INFO", "DEBUG", "TRACE"].contains(&word.as_str()) {
            Some(word.clone())
        } else if word.len() > 1 && word.starts_with('$') && !word[1..].contains('$') {
            None
        } else {
            continue;
        };
        let Some(rest) = rest[word.len()..].trim_start().strip_prefix(',') else {
            continue;
        };
        let Some(rest) = rest.trim_start().strip_prefix('"') else {
            continue;
        };
        let Some(end) = rest.find('"') else {
            continue;
        };
        found.push((level, rest[..end].to_owned()));
    }
    found
}

#[test]
fn every_event_call_in_the_sources_is_catalogued_at_its_level() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut files = Vec::new();
    for entry in fs::read_dir(&crates).unwrap() {
        let src = entry.unwrap().path().join("src");
        if src.is_dir() {
            sources(&src, &mut files);
        }
    }
    let mut problems = Vec::new();
    let mut seen = 0;
    for file in &files {
        for (level, name) in calls(&fs::read_to_string(file).unwrap()) {
            seen += 1;
            let at = file.strip_prefix(&crates).unwrap_or(file).display();
            match names::event(&name) {
                None => problems.push(format!("{at}: {name} is not in names::EVENTS")),
                Some(row)
                    if level
                        .as_ref()
                        .is_some_and(|level| !row.levels.iter().any(|l| l.as_str() == level)) =>
                {
                    problems.push(format!("{at}: {name} at {level:?} is not a catalog level"));
                }
                Some(_) => {}
            }
        }
    }
    assert!(seen > 100, "the scan found only {seen} event! calls");
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn the_scan_reads_levels_names_and_parents() {
    let text = r#"
        merezha_telemetry::event!(WARN, "db.pool_undersized", pool_size, recommended);
        event!(parent: None, ERROR, "startup.failed", code);
        event!(
            INFO,
            "listener.draining"
        );
        merezha_telemetry::event!($level, "http.request.completed", method);
        /// merezha_telemetry::event!(INFO, "doc.example_only");
        // merezha_telemetry::event!(INFO, "comment.example_only");
        $crate::__private::tracing::event!(name: $name, target: $crate::TARGET);
    "#;
    let level = |l: &str| Some(l.to_owned());
    assert_eq!(
        calls(text),
        [
            (level("WARN"), "db.pool_undersized".to_owned()),
            (level("ERROR"), "startup.failed".to_owned()),
            (level("INFO"), "listener.draining".to_owned()),
            (None, "http.request.completed".to_owned()),
        ]
    );
}
