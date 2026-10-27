//! Records the merezha revision this binary is built from (scaffold §2.9)
//! as `MEREZHA_BUILD_SOURCE_REV`, empty when unknown:
//!
//! 1. `MEREZHA_SOURCE_REV` from the build environment (release and CI
//!    builds), which must be a full commit id;
//! 2. else `git rev-parse HEAD` in this crate's directory, kept only when
//!    the repository is merezha's (`git rev-parse --show-prefix` is
//!    `crates/cli/` and `build.rs` is tracked) and `git --no-optional-locks
//!    status --porcelain --untracked-files=no` prints nothing. `--no-optional-locks` keeps `git status` from
//!    refreshing the index, which would make Cargo rerun this script on
//!    every build. Untracked files do not count: Cargo leaves an untracked
//!    `.cargo-ok` in every git checkout it builds;
//! 3. else none: a dirty or unknown tree records nothing rather than a
//!    revision that is not the source.
//!
//! The script reruns when `MEREZHA_SOURCE_REV` changes or when one of the
//! git files that move with a commit, a branch switch or a staged edit
//! changes: `HEAD`, the loose ref `HEAD` names, `packed-refs`, `index` and
//! the `reftable` directory, each only when it exists (Cargo treats a
//! missing path as always changed). A branch whose loose ref was packed has
//! no file to watch, so its parent directory is watched instead: the next
//! commit writes the loose ref there. Paths come from `git rev-parse
//! --git-path`, so linked worktrees work.
//!
//! The revision is advisory in a development checkout: an unstaged edit
//! touches none of these files, so the script keeps the clean revision it
//! saw last. Only builds with `MEREZHA_SOURCE_REV` set and fresh builds of a
//! clean checkout (`cargo install --git ... --rev`) are authoritative.
//!
//! This file has no dependencies beyond `std`: the source revision tests
//! copy it into `crates/cli/` of a temporary repository.
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

const OUTPUT: &str = "MEREZHA_BUILD_SOURCE_REV";

fn main() {
    println!("cargo::rerun-if-env-changed=MEREZHA_SOURCE_REV");
    let given = std::env::var("MEREZHA_SOURCE_REV").unwrap_or_default();
    let given = given.trim();
    let rev = if given.is_empty() {
        let directory = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
        from_git(&directory)
    } else {
        assert!(
            is_commit_id(given),
            "MEREZHA_SOURCE_REV must be a full lowercase commit id (40 or 64 hex digits)"
        );
        Some(given.to_owned())
    };
    println!("cargo::rustc-env={OUTPUT}={}", rev.unwrap_or_default());
}

/// Where this crate sits in a merezha checkout.
const PREFIX: &str = "crates/cli/";

/// The clean checkout's `HEAD`, after registering the files to watch. The
/// repository must be merezha's own: this crate at [`PREFIX`] with this
/// script tracked. A copy inside another repository (untracked, vendored or
/// a subtree elsewhere) records nothing rather than that repository's
/// commit, which does not exist in merezha.
fn from_git(directory: &Path) -> Option<String> {
    watch_git_files(directory);
    let prefix = git(directory, &["rev-parse", "--show-prefix"])?;
    if prefix.trim_end_matches(['\n', '\r']) != PREFIX {
        return None;
    }
    git(
        directory,
        &["ls-files", "--error-unmatch", "--", "build.rs"],
    )?;
    let head = git(directory, &["rev-parse", "--verify", "HEAD"])?;
    let head = head.trim();
    if !is_commit_id(head) {
        return None;
    }
    let status = git(
        directory,
        &["status", "--porcelain", "--untracked-files=no"],
    )?;
    status.trim().is_empty().then(|| head.to_owned())
}

fn watch_git_files(directory: &Path) {
    let Some(paths) = git(
        directory,
        &[
            "rev-parse",
            "--git-path",
            "HEAD",
            "--git-path",
            "index",
            "--git-path",
            "packed-refs",
            "--git-path",
            "reftable",
        ],
    ) else {
        return;
    };
    let resolve = |path: &str| {
        let path = Path::new(path);
        if path.is_absolute() {
            path.to_owned()
        } else {
            directory.join(path)
        }
    };
    let paths: Vec<PathBuf> = paths.lines().map(resolve).collect();
    let mut watched: Vec<PathBuf> = paths.iter().filter(|p| p.exists()).cloned().collect();
    // The loose ref `HEAD` names, as `refs/heads/main`.
    if let Some(head) = paths.first()
        && let Ok(text) = std::fs::read_to_string(head)
        && let Some(name) = text.trim().strip_prefix("ref: ")
        && name.starts_with("refs/")
        && !name.split('/').any(|part| part.is_empty() || part == "..")
        && let Some(loose) = git(directory, &["rev-parse", "--git-path", name])
    {
        let loose = resolve(loose.trim());
        if loose.is_file() {
            watched.push(loose);
        } else if let Some(parent) = loose
            .ancestors()
            .skip(1)
            .take_while(|p| p.components().any(|c| c.as_os_str() == "refs"))
            .find(|p| p.is_dir())
        {
            watched.push(parent.to_owned());
        }
    }
    for path in watched {
        if let Some(path) = path.to_str() {
            println!("cargo::rerun-if-changed={path}");
        }
    }
}

/// Runs git in `directory` without optional locks; `None` when git is
/// missing, fails or prints something other than UTF-8.
fn git(directory: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("--no-optional-locks")
        .args(args)
        .current_dir(directory)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

fn is_commit_id(text: &str) -> bool {
    matches!(text.len(), 40 | 64) && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}
