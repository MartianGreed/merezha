//! The source revision `build.rs` records (scaffold §2.9), on a temporary
//! git repository holding a probe crate with this crate's very build
//! script. Each case that asserts a recorded value builds into an empty
//! target directory, so the result never depends on an earlier run. The
//! `merezha version` process test checks the binary under test.
//!
//! Skipped with a message when `git` is not installed.
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

const BUILD_SCRIPT: &str = include_str!("../build.rs");

fn cargo() -> Command {
    let mut command = Command::new(env!("CARGO"));
    // The probe must see only what each case sets.
    command
        .env_remove("MEREZHA_SOURCE_REV")
        .env_remove("CARGO_TARGET_DIR")
        .env_remove("CARGO_BUILD_TARGET_DIR");
    command
}

/// git with no user or system configuration (no signing, no hooks).
fn git(directory: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args([
            "-c",
            "commit.gpgsign=false",
            "-c",
            "core.hooksPath=/dev/null",
        ])
        .args(args)
        .current_dir(directory)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "probe")
        .env("GIT_AUTHOR_EMAIL", "probe@example.invalid")
        .env("GIT_COMMITTER_NAME", "probe")
        .env("GIT_COMMITTER_EMAIL", "probe@example.invalid")
        .output()
        .expect("git runs");
    assert!(output.status.success(), "git {args:?}: {output:?}");
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn git_available() -> bool {
    let found = Command::new("git").arg("--version").output().is_ok();
    if !found {
        eprintln!("skipping: git is not installed");
    }
    found
}

/// Writes the probe crate into `dir`: `build.rs` is the cli's, `main`
/// prints what it recorded.
fn probe(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();
    fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"rev-probe\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n",
    )
    .unwrap();
    fs::write(dir.join("build.rs"), BUILD_SCRIPT).unwrap();
    fs::write(
        dir.join("src/main.rs"),
        "fn main() { print!(\"{}\", env!(\"MEREZHA_BUILD_SOURCE_REV\")); }\n",
    )
    .unwrap();
}

/// A fresh repository with one commit of `README` and `.gitignore`.
fn init(repo: &Path) -> String {
    fs::create_dir_all(repo).unwrap();
    fs::write(repo.join("README"), "probe\n").unwrap();
    fs::write(repo.join(".gitignore"), "/target/\n").unwrap();
    git(repo, &["init", "--quiet", "--initial-branch=main"]);
    git(repo, &["add", "."]);
    git(repo, &["commit", "--quiet", "-m", "probe"]);
    git(repo, &["rev-parse", "HEAD"])
}

/// A committed repository shaped like merezha's: the probe crate at
/// `crates/cli`, as the build script requires.
fn repository(root: &Path) -> (PathBuf, String) {
    let repo = root.join("repo");
    probe(&repo.join(CRATE));
    let head = init(&repo);
    (repo, head)
}

const CRATE: &str = "crates/cli";

/// `cargo build -v` of the probe in `repo` into `target`; the build's
/// stderr and what the binary prints.
fn build(repo: &Path, target: &Path, env: &[(&str, &str)]) -> (String, String) {
    let mut command = cargo();
    command
        .args(["build", "-v", "--manifest-path"])
        .arg(repo.join(CRATE).join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target)
        .envs(env.iter().copied());
    let output = command.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(output.status.success(), "{stderr}");
    let recorded = run(&target.join("debug/rev-probe"));
    (stderr, recorded)
}

fn run(binary: &Path) -> String {
    let Output { status, stdout, .. } = Command::new(binary).output().unwrap();
    assert!(status.success());
    String::from_utf8(stdout).unwrap()
}

fn reran(stderr: &str) -> bool {
    stderr.contains("build-script-build`")
}

#[test]
fn cargo_install_from_a_git_checkout_records_the_commit_despite_cargo_ok() {
    if !git_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let (repo, head) = repository(root.path());
    let cargo_home = root.path().join("cargo-home");
    let install = root.path().join("install");
    let output = cargo()
        .arg("install")
        .arg("--git")
        .arg(format!("file://{}", repo.display()))
        .args(["--rev", &head, "--root"])
        .arg(&install)
        .arg("rev-probe")
        .env("CARGO_HOME", &cargo_home)
        .env("CARGO_TARGET_DIR", root.path().join("target"))
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    // The premise: Cargo's checkout holds the untracked `.cargo-ok`.
    let checkouts = cargo_home.join("git/checkouts");
    let mut marked = false;
    for checkout in fs::read_dir(&checkouts).unwrap() {
        for revision in fs::read_dir(checkout.unwrap().path()).unwrap() {
            let revision = revision.unwrap().path();
            marked |= revision.join(".cargo-ok").is_file() && revision.join(".git").exists();
        }
    }
    assert!(marked, "no git checkout with .cargo-ok under {checkouts:?}");
    assert_eq!(run(&install.join("bin/rev-probe")), head);
}

#[test]
fn a_fresh_build_records_none_with_a_modified_tracked_file_and_ignores_untracked_ones() {
    if !git_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let (repo, head) = repository(root.path());
    fs::write(repo.join("scratch.txt"), "untracked\n").unwrap();
    let (_, recorded) = build(&repo, &root.path().join("untracked"), &[]);
    assert_eq!(recorded, head, "an untracked file must not count");

    fs::write(repo.join("README"), "edited\n").unwrap();
    let (_, recorded) = build(&repo, &root.path().join("modified"), &[]);
    assert_eq!(recorded, "", "a modified tracked file records none");

    // MEREZHA_SOURCE_REV wins over the tree, dirty or not.
    let given = "0123456789abcdef0123456789abcdef01234567";
    let (_, recorded) = build(
        &repo,
        &root.path().join("given"),
        &[("MEREZHA_SOURCE_REV", given)],
    );
    assert_eq!(recorded, given);
}

#[test]
fn a_malformed_merezha_source_rev_fails_the_build() {
    if !git_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let (repo, _) = repository(root.path());
    for given in [
        "main\"; injected = \"",
        // Short and uppercase hex: only full lowercase ids are pinned.
        "0123456789abcdef",
        "0123456789abcdef0123456789abcdef0123456",
        "0123456789ABCDEF0123456789ABCDEF01234567",
    ] {
        let output = cargo()
            .args(["build", "--manifest-path"])
            .arg(repo.join(CRATE).join("Cargo.toml"))
            .env("CARGO_TARGET_DIR", root.path().join("target"))
            .env("MEREZHA_SOURCE_REV", given)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{given} was accepted");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("full lowercase commit id"),
            "{given}: {output:?}"
        );
    }
    // A 64-digit (SHA-256 repository) id is accepted.
    let sha256 = "0123456789abcdef".repeat(4);
    let (_, recorded) = build(
        &repo,
        &root.path().join("sha256"),
        &[("MEREZHA_SOURCE_REV", &sha256)],
    );
    assert_eq!(recorded, sha256);
}

#[test]
fn a_copy_inside_another_repository_records_none() {
    if !git_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    // Untracked at the right place inside an unrelated clean repository:
    // `git status --untracked-files=no` is empty, but `build.rs` is not
    // tracked there.
    let other = root.path().join("other");
    init(&other);
    probe(&other.join(CRATE));
    let (_, recorded) = build(&other, &root.path().join("untracked"), &[]);
    assert_eq!(
        recorded, "",
        "an untracked copy recorded the other repository's HEAD"
    );

    // Vendored and committed elsewhere in a monorepo: tracked, clean, but
    // not at `crates/cli`.
    let monorepo = root.path().join("monorepo");
    init(&monorepo);
    probe(&monorepo.join("vendor/merezha-cli"));
    git(&monorepo, &["add", "."]);
    git(&monorepo, &["commit", "--quiet", "-m", "vendor"]);
    let mut command = cargo();
    let output = command
        .args(["build", "--manifest-path"])
        .arg(monorepo.join("vendor/merezha-cli/Cargo.toml"))
        .env("CARGO_TARGET_DIR", root.path().join("vendored"))
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(run(&root.path().join("vendored/debug/rev-probe")), "");
}

#[test]
fn a_second_build_without_changes_does_not_rerun_the_script() {
    if !git_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let (repo, head) = repository(root.path());
    let target = root.path().join("target");
    let (stderr, recorded) = build(&repo, &target, &[]);
    assert!(reran(&stderr), "the first build runs the script");
    assert_eq!(recorded, head);
    let index = repo.join(".git/index");
    let indexed = fs::metadata(&index).unwrap().modified().unwrap();
    // A tracked file with a new mtime and the same content: a plain `git
    // status` would refresh the index here, and Cargo would rerun.
    let file = fs::File::options()
        .write(true)
        .open(repo.join("README"))
        .unwrap();
    file.set_modified(std::time::SystemTime::now()).unwrap();
    drop(file);
    let (stderr, recorded) = build(&repo, &target, &[]);
    assert!(!reran(&stderr), "{stderr}");
    assert!(stderr.contains("Fresh rev-probe"), "{stderr}");
    assert_eq!(recorded, head);
    assert_eq!(fs::metadata(&index).unwrap().modified().unwrap(), indexed);

    // Packed refs: the loose ref is gone and must not be watched as a
    // missing path, which Cargo would treat as always changed.
    git(&repo, &["pack-refs", "--all"]);
    assert!(!repo.join(".git/refs/heads/main").exists());
    let (stderr, _) = build(&repo, &target, &[]);
    assert!(reran(&stderr), "packing the refs rewrites packed-refs");
    let (stderr, recorded) = build(&repo, &target, &[]);
    assert!(!reran(&stderr), "{stderr}");
    assert_eq!(recorded, head);
}

#[test]
fn a_new_commit_reruns_the_script_and_records_the_new_revision() {
    if !git_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let (repo, head) = repository(root.path());
    let target = root.path().join("target");
    assert_eq!(build(&repo, &target, &[]).1, head);

    // An unstaged edit touches no watched file: the recorded revision is
    // advisory in a development checkout (§2.9) and stays.
    fs::write(repo.join("README"), "second\n").unwrap();
    assert_eq!(build(&repo, &target, &[]).1, head);
    // Staging it rewrites the index: the script reruns and sees a dirty tree.
    git(&repo, &["add", "README"]);
    assert_eq!(build(&repo, &target, &[]).1, "");
    git(&repo, &["commit", "--quiet", "-m", "second"]);
    let second = git(&repo, &["rev-parse", "HEAD"]);
    assert_eq!(build(&repo, &target, &[]).1, second);

    // A packed branch has no loose ref to watch; moving it writes one,
    // here without touching the index (as `git reset --soft` would).
    git(&repo, &["pack-refs", "--all"]);
    assert_eq!(build(&repo, &target, &[]).1, second);
    let third = git(
        &repo,
        &["commit-tree", "HEAD^{tree}", "-p", "HEAD", "-m", "third"],
    );
    git(&repo, &["update-ref", "refs/heads/main", &third]);
    assert_eq!(build(&repo, &target, &[]).1, third);

    // A branch switch.
    git(&repo, &["checkout", "--quiet", "-b", "other", &head]);
    assert_eq!(build(&repo, &target, &[]).1, head);
}

#[test]
fn a_linked_worktree_records_and_follows_its_own_head() {
    if !git_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let (repo, head) = repository(root.path());
    let worktree = root.path().join("worktree");
    git(
        &repo,
        &[
            "worktree",
            "add",
            "--quiet",
            "-b",
            "linked",
            worktree.to_str().unwrap(),
        ],
    );
    let target = root.path().join("target");
    assert_eq!(build(&worktree, &target, &[]).1, head);
    git(
        &worktree,
        &["commit", "--quiet", "--allow-empty", "-m", "linked"],
    );
    let linked = git(&worktree, &["rev-parse", "HEAD"]);
    assert_ne!(linked, head);
    assert_eq!(build(&worktree, &target, &[]).1, linked);
}

#[test]
fn merezha_version_prints_the_recorded_revision() {
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .arg("version")
        .output()
        .unwrap();
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["version"], env!("CARGO_PKG_VERSION"));
    let recorded = env!("MEREZHA_BUILD_SOURCE_REV");
    if recorded.is_empty() {
        assert!(report["rev"].is_null(), "{report}");
    } else {
        assert_eq!(report["rev"], recorded);
    }
    // CI builds and tests with MEREZHA_SOURCE_REV set for the whole job.
    if let Some(given) = option_env!("MEREZHA_SOURCE_REV").filter(|v| !v.is_empty()) {
        assert_eq!(report["rev"], given);
    }

    let extra = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["version", "--json"])
        .output()
        .unwrap();
    assert_eq!(extra.status.code(), Some(2));
}
