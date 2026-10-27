//! Embeds `templates/` in the crate (scaffold §2.7): one sorted table of
//! `(relative path, include_bytes!)` entries in `$OUT_DIR/templates.rs`.
//!
//! The walk refuses, and so fails the build:
//!
//! - symbolic links and anything that is neither a file nor a directory;
//! - names that are not UTF-8, or not plain names (no `..`, no separators);
//! - files over 1 MiB;
//! - `.tmpl` files that are not UTF-8, or that hold a `%%` sequence other
//!   than the closed set of substitutions ([`PLACEHOLDERS`]), so a typo
//!   never reaches a user;
//! - any file named `Cargo.toml`: template manifests are stored as
//!   `Cargo.toml.tmpl`, so Cargo never takes a template for a package.
//!
//! It skips, with a `cargo::warning`, what is not meant to ship: names
//! starting with `.` (files and directories) other than [`ALLOWED_DOT_NAMES`],
//! and editor and system leftovers (`*~`, `*.swp`, `*.swo`, `#*#`, `.#*`,
//! `.DS_Store`, `Thumbs.db`). Store a generated dotfile without its dot
//! (`gitignore`) and name it in the index.
//!
//! `cargo::rerun-if-changed=templates` makes Cargo rescan the directory, so
//! an edited template rebuilds the crate.
//!
//! This file has no dependencies beyond `std`: the build-time tests copy it
//! into a probe crate with crafted templates.
use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

/// The substitutions a `.tmpl` file may use, as `%%name%%`. Keep in step
/// with `Values` in `src/render.rs`.
const PLACEHOLDERS: [&str; 5] = [
    "app_id",
    "app_name",
    "merezha_dependency",
    "merezha_rev",
    "merezha_version",
];
/// Dotted names that are embedded: directories and files the generated
/// projects keep under their own name.
const ALLOWED_DOT_NAMES: [&str; 2] = [".github", ".contremaitre.yaml"];
/// Largest embedded file.
const MAX_FILE_BYTES: u64 = 1024 * 1024;
/// Bounds on the walk.
const MAX_FILES: usize = 4096;
const MAX_DEPTH: usize = 16;

fn main() {
    println!("cargo::rerun-if-changed=templates");
    let manifest =
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let root = manifest.join("templates");
    let mut files = Vec::new();
    walk(&root, "", 0, &mut files);
    files.sort();
    let mut out = String::from(
        "/// Every template file: its path under `templates/` and its bytes, sorted by path.\n\
         pub static FILES: &[(&str, &[u8])] = &[\n",
    );
    for (relative, absolute) in &files {
        writeln!(
            out,
            "    ({relative:?}, include_bytes!({:?})),",
            absolute.to_str().expect("UTF-8 path")
        )
        .expect("write to a string");
    }
    out.push_str("];\n");
    let target = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR")).join("templates.rs");
    fs::write(target, out).expect("write templates.rs");
}

fn walk(directory: &Path, prefix: &str, depth: usize, files: &mut Vec<(String, PathBuf)>) {
    assert!(depth <= MAX_DEPTH, "templates: {prefix} is nested too deep");
    let entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("templates: cannot read {}: {error}", directory.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|error| panic!("templates: {prefix}: {error}"));
        let name = entry.file_name();
        let name = name
            .to_str()
            .unwrap_or_else(|| panic!("templates: a name under {prefix:?} is not UTF-8"));
        assert!(
            !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\']),
            "templates: {prefix}{name} is not a plain name"
        );
        let relative = format!("{prefix}{name}");
        if skipped(name) {
            println!("cargo::warning=templates: skipping {relative}");
            continue;
        }
        let path = entry.path();
        // `symlink_metadata` never follows a link.
        let metadata = fs::symlink_metadata(&path)
            .unwrap_or_else(|error| panic!("templates: {relative}: {error}"));
        let kind = metadata.file_type();
        assert!(
            !kind.is_symlink(),
            "templates: {relative} is a symbolic link"
        );
        if kind.is_dir() {
            walk(&path, &format!("{relative}/"), depth + 1, files);
            continue;
        }
        assert!(
            kind.is_file(),
            "templates: {relative} is not a regular file"
        );
        assert!(
            name != "Cargo.toml",
            "templates: {relative} must be stored as Cargo.toml.tmpl"
        );
        assert!(
            metadata.len() <= MAX_FILE_BYTES,
            "templates: {relative} is over 1 MiB"
        );
        if name.ends_with(".tmpl") {
            let bytes =
                fs::read(&path).unwrap_or_else(|error| panic!("templates: {relative}: {error}"));
            let text = std::str::from_utf8(&bytes).unwrap_or_else(|_| {
                panic!("templates: {relative} is a .tmpl file that is not UTF-8")
            });
            check_placeholders(&relative, text);
        }
        files.push((relative, path));
        assert!(
            files.len() <= MAX_FILES,
            "templates: more than {MAX_FILES} files"
        );
    }
}

/// Every `%%` must open one of [`PLACEHOLDERS`].
fn check_placeholders(relative: &str, text: &str) {
    let mut rest = text;
    while let Some(start) = rest.find("%%") {
        let after = &rest[start + 2..];
        let known = PLACEHOLDERS.iter().find(|name| {
            after
                .strip_prefix(**name)
                .is_some_and(|tail| tail.starts_with("%%"))
        });
        match known {
            Some(name) => rest = &after[name.len() + 2..],
            None => {
                let shown: String = after.chars().take(32).collect();
                panic!("templates: {relative} has an unknown substitution at %%{shown}");
            }
        }
    }
}

/// Hidden names and editor or system leftovers, which never ship.
fn skipped(name: &str) -> bool {
    let editor = name.ends_with('~')
        || name.ends_with(".swp")
        || name.ends_with(".swo")
        || (name.starts_with('#') && name.ends_with('#'))
        || name == "Thumbs.db";
    editor || (name.starts_with('.') && !ALLOWED_DOT_NAMES.contains(&name))
}
