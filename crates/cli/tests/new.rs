//! `merezha new` (scaffold §2, SC-S2): process tests of the command, its
//! JSON report, its refusals and the files it writes, and the generated
//! configuration loaded the way `merezha run` loads it for every block
//! selection §2.2 allows.
use merezha_platform::{ConfigInput, host};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

const REV: &str = "0123456789abcdef0123456789abcdef01234567";

/// `merezha new ARGS` in `cwd`: the exit code and the JSON report.
fn new(cwd: &Path, args: &[&str]) -> (i32, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .arg("new")
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    let report = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|_| panic!("{args:?}: not JSON: {output:?}"));
    (output.status.code().unwrap(), report)
}

/// Every path under `dir`, relative, sorted; directories end with `/`.
fn tree(dir: &Path) -> Vec<String> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<String>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            if entry.file_type().unwrap().is_dir() {
                out.push(format!("{prefix}{name}/"));
                walk(&entry.path(), &format!("{prefix}{name}/"), out);
            } else {
                out.push(format!("{prefix}{name}"));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, "", &mut out);
    out.sort();
    out
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
}

/// The `.env` assignments.
fn env_file(dir: &Path) -> Vec<(String, String)> {
    fs::read_to_string(dir.join(".env"))
        .unwrap()
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| {
            let (name, value) = line.split_once('=').unwrap();
            (name.to_owned(), value.to_owned())
        })
        .collect()
}

fn git_available() -> bool {
    let found = Command::new("git").arg("--version").output().is_ok();
    if !found {
        eprintln!("skipping the git checks: git is not installed");
    }
    found
}

fn git(dir: &Path, args: &[&str]) -> std::process::Output {
    Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap()
}

#[test]
fn creates_the_starter_reports_it_and_keeps_secrets_out_of_committed_files() {
    let root = tempfile::tempdir().unwrap();
    let cwd = fs::canonicalize(root.path()).unwrap();
    let (code, report) = new(&cwd, &["acme", "--merezha-rev", REV]);
    assert_eq!(code, 0, "{report}");
    let dir = cwd.join("acme");
    let files: Vec<String> = tree(&dir)
        .into_iter()
        .filter(|p| !p.ends_with('/'))
        .collect();
    assert_eq!(
        report,
        json!({
            "status": "created",
            "dir": dir.to_str().unwrap(),
            "template": "starter",
            "blocks": ["identity", "notifications", "content", "telemetry"],
            "merezha": {"rev": REV},
            "secrets": [
                "MEREZHA_IDENTITY_SECRET_KEY",
                "MEREZHA_NOTIFY_SECRET_KEY",
                "MEREZHA_VAPID_PRIVATE_KEY"
            ],
            "files": files.len(),
            "next": [format!("cd {}", dir.display())],
        })
    );
    assert_eq!(
        files,
        [
            ".env",
            ".env.example",
            ".gitignore",
            ".merezha-version",
            "application.toml",
            "content.toml",
            "merezha.production.toml",
            "merezha.toml",
            "notifications/templates/notes.shared/en/body.html",
            "notifications/templates/notes.shared/en/body.txt",
            "notifications/templates/notes.shared/en/inbox.toml",
            "notifications/templates/notes.shared/en/push.toml",
            "notifications/templates/notes.shared/en/subject.txt",
            "notifications/templates/notes.shared/template.toml",
            "runtime.toml",
        ]
    );
    // Only the project: the staging directory is gone.
    assert!(
        tree(&cwd).iter().all(|p| p.starts_with("acme/")),
        "{:?}",
        tree(&cwd)
    );

    // Modes: .env exactly 0600; the rest follows the umask from 0644/0755.
    assert_eq!(mode(&dir.join(".env")), 0o600);
    for path in &files {
        if path != ".env" {
            assert_eq!(mode(&dir.join(path)) & !0o644, 0, "{path}");
        }
    }
    assert_eq!(mode(&dir) & !0o755, 0);

    assert_eq!(
        fs::read_to_string(dir.join(".merezha-version")).unwrap(),
        format!(
            "# The merezha revision this project builds against. scripts/dev.sh and CI install and check it.\nrev = \"{REV}\"\n"
        )
    );

    // The generated values: the right shapes, and none of them anywhere
    // but .env.
    let secrets = env_file(&dir);
    let names: Vec<&str> = secrets.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        report["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>()
    );
    let value = |name: &str| secrets.iter().find(|(n, _)| n == name).unwrap().1.clone();
    let identity = value("MEREZHA_IDENTITY_SECRET_KEY");
    assert!(identity.len() == 64 && identity.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(
        value("MEREZHA_NOTIFY_SECRET_KEY").len(),
        44,
        "base64 of 32 bytes"
    );
    assert_eq!(
        value("MEREZHA_VAPID_PRIVATE_KEY").len(),
        43,
        "base64url of a P-256 scalar"
    );
    let stdout = report.to_string();
    for (name, secret) in &secrets {
        assert!(!stdout.contains(secret.as_str()), "{name} in the report");
        for path in files.iter().filter(|p| *p != ".env") {
            let text = fs::read(dir.join(path)).unwrap();
            assert!(
                !text.windows(secret.len()).any(|w| w == secret.as_bytes()),
                "{name}'s value is in {path}"
            );
        }
    }
    // A second project gets other values.
    let (code, _) = new(&cwd, &["other", "--merezha-rev", REV]);
    assert_eq!(code, 0);
    let other = env_file(&cwd.join("other"));
    for ((_, a), (_, b)) in secrets.iter().zip(&other) {
        assert_ne!(a, b);
    }

    if git_available() {
        assert!(git(&dir, &["init", "--quiet"]).status.success());
        let ignored = git(&dir, &["check-ignore", ".env"]);
        assert!(ignored.status.success(), "{ignored:?}");
        assert!(
            !git(&dir, &["check-ignore", ".env.example"])
                .status
                .success()
        );
        let status = git(&dir, &["status", "--porcelain", "--untracked-files=all"]);
        let status = String::from_utf8(status.stdout).unwrap();
        assert!(!status.contains(".env\n"), "{status}");
        assert!(status.contains(".env.example"), "{status}");
    }
}

#[test]
fn no_secrets_writes_only_the_example() {
    let root = tempfile::tempdir().unwrap();
    let (code, report) = new(
        root.path(),
        &[
            "acme",
            "--merezha-rev",
            REV,
            "--no-secrets",
            "--blocks",
            "none",
        ],
    );
    assert_eq!(code, 0, "{report}");
    assert_eq!(report["secrets"], json!([]));
    assert_eq!(report["blocks"], json!([]));
    let dir = root.path().join("acme");
    assert!(!dir.join(".env").exists());
    assert_eq!(
        tree(&dir),
        [
            ".env.example",
            ".gitignore",
            ".merezha-version",
            "application.toml",
            "merezha.production.toml",
            "merezha.toml",
            "runtime.toml",
        ]
    );
    assert_eq!(report["files"], 7);
}

/// One refusal: what to prepare in `root` and the arguments.
struct Refusal {
    code: &'static str,
    args: Vec<String>,
    /// Runs in the test's root before the command; returns the cwd.
    setup: fn(&Path) -> PathBuf,
}

fn at_root(root: &Path) -> PathBuf {
    root.to_owned()
}

fn refusals(root: &Path) -> Vec<Refusal> {
    let s = |args: &[&str]| args.iter().map(|a| (*a).to_owned()).collect::<Vec<_>>();
    let rev = |mut args: Vec<String>| {
        args.extend(["--merezha-rev".to_owned(), REV.to_owned()]);
        args
    };
    let r = |code, args, setup| Refusal { code, args, setup };
    let fake = root.join("fake-merezha");
    let other = root.join("other-checkout");
    vec![
        r("invalid_arguments", s(&[]), at_root),
        r("invalid_arguments", rev(s(&["acme", "--force"])), at_root),
        r(
            "invalid_arguments",
            rev(s(&["acme", "--dir", "a", "--dir", "b"])),
            at_root,
        ),
        r(
            "invalid_arguments",
            rev(s(&["acme", "--no-secrets", "--no-secrets"])),
            at_root,
        ),
        r("invalid_arguments", s(&["acme", "--dir"]), at_root),
        r("invalid_arguments", rev(s(&["acme", "beta"])), at_root),
        r(
            "invalid_arguments",
            rev(s(&["acme", "--blocks", "telemetry,telemetry"])),
            at_root,
        ),
        r(
            "invalid_arguments",
            rev(s(&["acme", "--blocks", "none,telemetry"])),
            at_root,
        ),
        r("invalid_name", rev(s(&["Acme"])), at_root),
        r("invalid_name", rev(s(&["merezha"])), at_root),
        r("invalid_name", rev(s(&["acme-"])), at_root),
        r("invalid_name", rev(vec!["a".repeat(41)]), at_root),
        r(
            "unknown_template",
            rev(s(&["acme", "--template", "nope"])),
            at_root,
        ),
        r(
            "unknown_block",
            rev(s(&["acme", "--blocks", "billing"])),
            at_root,
        ),
        r("unknown_block", rev(s(&["acme", "--blocks", ""])), at_root),
        r(
            "unknown_block",
            rev(s(&["acme", "--blocks", "telemetry,"])),
            at_root,
        ),
        r(
            "block_requires_notifications",
            rev(s(&["acme", "--blocks", "identity"])),
            at_root,
        ),
        r(
            "block_requires_notifications",
            rev(s(&["acme", "--blocks", "identity,content,telemetry"])),
            at_root,
        ),
        r(
            "block_requires_identity",
            rev(s(&["acme", "--blocks", "content"])),
            at_root,
        ),
        r(
            "block_requires_identity",
            rev(s(&["acme", "--blocks", "notifications,content"])),
            at_root,
        ),
        r(
            "conflicting_source",
            rev(vec![
                "acme".into(),
                "--merezha-path".into(),
                fake.display().to_string(),
            ]),
            |root| {
                checkout(&root.join("fake-merezha"), "merezha-domain");
                root.to_owned()
            },
        ),
        r(
            "invalid_rev",
            s(&["acme", "--merezha-rev", "abc123"]),
            at_root,
        ),
        r(
            "invalid_rev",
            s(&["acme", "--merezha-rev", &REV.to_uppercase()]),
            at_root,
        ),
        r(
            "invalid_rev",
            s(&["acme", "--merezha-rev", "main"]),
            at_root,
        ),
        r(
            "invalid_merezha_path",
            vec![
                "acme".into(),
                "--merezha-path".into(),
                other.display().to_string(),
            ],
            |root| {
                checkout(&root.join("other-checkout"), "not-merezha");
                root.to_owned()
            },
        ),
        r(
            "invalid_merezha_path",
            s(&["acme", "--merezha-path", "missing-checkout"]),
            at_root,
        ),
        r("target_not_empty", rev(s(&["acme"])), |root| {
            fs::create_dir(root.join("acme")).unwrap();
            fs::write(root.join("acme/.keep"), "").unwrap();
            root.to_owned()
        }),
        r("target_not_directory", rev(s(&["acme"])), |root| {
            fs::write(root.join("acme"), "a file").unwrap();
            root.to_owned()
        }),
        r("target_is_symlink", rev(s(&["acme"])), |root| {
            fs::create_dir(root.join("elsewhere")).unwrap();
            std::os::unix::fs::symlink(root.join("elsewhere"), root.join("acme")).unwrap();
            root.to_owned()
        }),
        r("target_is_symlink", rev(s(&["acme"])), |root| {
            std::os::unix::fs::symlink(root.join("dangling"), root.join("acme")).unwrap();
            root.to_owned()
        }),
        r(
            "target_is_working_directory",
            rev(s(&["acme", "--dir", "."])),
            |root| {
                fs::create_dir(root.join("empty")).unwrap();
                root.join("empty")
            },
        ),
        r(
            "target_is_working_directory",
            rev(s(&["acme", "--dir", "../empty"])),
            |root| {
                fs::create_dir(root.join("empty")).unwrap();
                root.join("empty")
            },
        ),
        r(
            "parent_missing",
            rev(s(&["acme", "--dir", "missing/acme"])),
            at_root,
        ),
        r(
            "parent_missing",
            rev(s(&["acme", "--dir", "file/acme"])),
            |root| {
                fs::write(root.join("file"), "").unwrap();
                root.to_owned()
            },
        ),
    ]
}

/// A directory shaped like a merezha checkout whose domain crate is `name`.
fn checkout(dir: &Path, name: &str) {
    fs::create_dir_all(dir.join("crates/domain")).unwrap();
    fs::write(
        dir.join("crates/domain/Cargo.toml"),
        format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\n"),
    )
    .unwrap();
}

#[test]
fn every_refusal_exits_2_and_writes_nothing() {
    let probe = tempfile::tempdir().unwrap();
    let count = refusals(probe.path()).len();
    for index in 0..count {
        let root = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(root.path()).unwrap();
        let refusal = refusals(&root).remove(index);
        let cwd = (refusal.setup)(&root);
        let before = tree(&root);
        let args: Vec<&str> = refusal.args.iter().map(String::as_str).collect();
        let (code, report) = new(&cwd, &args);
        assert_eq!(report["error"]["code"], refusal.code, "{args:?}: {report}");
        assert_eq!(report["status"], "invalid", "{args:?}");
        assert_eq!(code, 2, "{args:?}");
        assert_eq!(tree(&root), before, "{args:?} wrote something");
    }
}

#[test]
fn help_lists_the_templates_and_blocks() {
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["new", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.starts_with("usage: merezha new NAME"), "{text}");
    assert!(text.contains("starter (default)"), "{text}");
    assert!(
        text.contains("identity,notifications,content,telemetry"),
        "{text}"
    );
}

#[test]
fn a_parent_reached_through_a_symbolic_link_is_canonicalized() {
    let root = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(root.path()).unwrap();
    fs::create_dir(root.join("real")).unwrap();
    std::os::unix::fs::symlink(root.join("real"), root.join("link")).unwrap();
    let (code, report) = new(&root, &["acme", "--dir", "link/acme", "--merezha-rev", REV]);
    assert_eq!(code, 0, "{report}");
    assert_eq!(report["dir"], root.join("real/acme").to_str().unwrap());
    assert!(root.join("real/acme/merezha.toml").is_file());
    // An empty directory target is replaced, through a linked parent too.
    fs::create_dir(root.join("real/empty")).unwrap();
    let (code, report) = new(
        &root,
        &["beta", "--dir", "link/empty", "--merezha-rev", REV],
    );
    assert_eq!(code, 0, "{report}");
    assert!(root.join("real/empty/runtime.toml").is_file());
    let mut top: Vec<String> = fs::read_dir(root.join("real"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    top.sort();
    assert_eq!(top, ["acme", "empty"], "no staging directory left");
    // `/tmp` itself is a symbolic link on macOS.
    let tmp = Path::new("/tmp");
    if fs::symlink_metadata(tmp).is_ok_and(|m| m.file_type().is_symlink()) {
        let name = format!("merezha-new-{}", std::process::id());
        let (code, report) = new(tmp, &["acme", "--dir", &name, "--merezha-rev", REV]);
        let real = fs::canonicalize(tmp).unwrap().join(&name);
        let _ = fs::remove_dir_all(&real);
        assert_eq!(code, 0, "{report}");
        assert_eq!(report["dir"], real.to_str().unwrap());
    }
}

/// This checkout, which holds `crates/domain`.
fn workspace() -> PathBuf {
    fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

#[test]
fn merezha_path_pins_a_relative_path() {
    let root = tempfile::tempdir().unwrap();
    let checkout = workspace();
    let (code, report) = new(
        root.path(),
        &[
            "acme",
            "--merezha-path",
            checkout.to_str().unwrap(),
            "--no-secrets",
        ],
    );
    assert_eq!(code, 0, "{report}");
    let dir = fs::canonicalize(root.path()).unwrap().join("acme");
    let path = report["merezha"]["path"].as_str().unwrap();
    assert!(path.starts_with(".."), "{report}");
    assert_eq!(fs::canonicalize(dir.join(path)).unwrap(), checkout);
    let pinned: toml::Table = fs::read_to_string(dir.join(".merezha-version"))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(pinned["path"].as_str(), Some(path));
    assert!(pinned.get("rev").is_none());
}

#[test]
fn without_a_flag_the_binary_revision_is_pinned_or_refused() {
    let version = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .arg("version")
        .output()
        .unwrap();
    let version: Value = serde_json::from_slice(&version.stdout).unwrap();
    let root = tempfile::tempdir().unwrap();
    let (code, report) = new(root.path(), &["acme", "--no-secrets"]);
    match version["rev"].as_str() {
        Some(rev) => {
            assert_eq!(code, 0, "{report}");
            assert_eq!(report["merezha"], json!({"rev": rev}));
            let pinned = fs::read_to_string(root.path().join("acme/.merezha-version")).unwrap();
            assert!(pinned.contains(&format!("rev = \"{rev}\"")));
        }
        None => {
            assert_eq!(code, 2, "{report}");
            assert_eq!(
                report,
                json!({"status": "invalid", "error": {"code": "source_rev_unknown"}})
            );
            assert!(tree(root.path()).is_empty());
        }
    }
}

/// A selection's `--blocks` value.
fn blocks_arg(blocks: &[&str]) -> String {
    if blocks.is_empty() {
        "none".to_owned()
    } else {
        blocks.join(",")
    }
}

/// Throwaway values for the variables `.env` does not hold.
fn environment(dir: &Path) -> Vec<(String, String)> {
    let mut env = if dir.join(".env").exists() {
        env_file(dir)
    } else {
        Vec::new()
    };
    env.push((
        "DATABASE_URL".into(),
        "postgres://app:throwaway@127.0.0.1:1/app".into(),
    ));
    env.push(("BREVO_API_KEY".into(), "xkeysib-throwaway".into()));
    env.push((
        "BREVO_WEBHOOK_TOKEN".into(),
        "throwaway-webhook-token-of-at-least-32-characters".into(),
    ));
    env
}

#[test]
fn the_generated_configuration_loads_like_merezha_run_for_every_selection() {
    let selections: [&[&str]; 8] = [
        &[],
        &["telemetry"],
        &["notifications"],
        &["notifications", "telemetry"],
        &["identity", "notifications"],
        &["identity", "notifications", "telemetry"],
        &["identity", "notifications", "content"],
        &["identity", "notifications", "content", "telemetry"],
    ];
    let root = tempfile::tempdir().unwrap();
    for blocks in selections {
        let label = blocks_arg(blocks);
        let name = format!("app{}", label.len());
        let (code, report) = new(
            root.path(),
            &[&name, "--merezha-rev", REV, "--blocks", &label],
        );
        assert_eq!(code, 0, "{label}: {report}");
        let dir = root.path().join(&name);
        let env = environment(&dir);
        let has = |block: &str| blocks.contains(&block);
        // Only blocks with secrets get a `.env` (identity implies
        // notifications).
        assert_eq!(dir.join(".env").exists(), has("notifications"), "{label}");
        assert_eq!(
            report["secrets"].as_array().unwrap().len(),
            usize::from(has("identity")) + 2 * usize::from(has("notifications")),
            "{label}: {report}"
        );

        let manifest = fs::read_to_string(dir.join("runtime.toml")).unwrap();
        let manifest = merezha_runtime::manifest::Manifest::parse(&manifest).unwrap();
        let ids: Vec<&str> = manifest.services.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, ["domain", "view", "notifier"]);

        for file in ["merezha.toml", "merezha.production.toml"] {
            let input = ConfigInput {
                path: dir.join(file),
                env: env.clone(),
                overrides: Vec::new(),
            };
            let (modules, _) = merezha_cli::host_blocks(&input);
            let (config, enabled) = host::configure(&input, modules)
                .unwrap_or_else(|errors| panic!("{label}: {file}: {errors:?}"));
            let enabled: BTreeSet<&str> = enabled.iter().map(|m| m.name()).collect();
            let expected: BTreeSet<&str> = ["identity", "notifications", "content"]
                .into_iter()
                .filter(|block| has(block))
                .collect();
            assert_eq!(enabled, expected, "{label}: {file}");
            assert_eq!(config.app.id, name);
            assert_eq!(config.http.public, file == "merezha.production.toml");
            // OTLP export is on only with an endpoint (R12).
            assert_eq!(
                !config.telemetry.signals.is_empty(),
                has("telemetry") && file == "merezha.production.toml",
                "{label}: {file}"
            );

            let section = config.application.as_ref().unwrap();
            let application = merezha_cli::read_application_config(&section.path)
                .unwrap_or_else(|f| panic!("{label}: {f:?}"));
            merezha_cli::run::check_application(&config, &application, has("identity"))
                .unwrap_or_else(|code| panic!("{label}: {file}: {code}"));

            if has("notifications") {
                let (report, _) = merezha_cli::notifications::check(&input);
                assert_eq!(report["status"], "ok", "{label}: {file}: {report}");
                assert_eq!(report["push"], true);
                assert_eq!(report["webhooks"], true);
                // The process, as the generated CI runs it: without
                // MEREZHA_NOTIFY_NO_EGRESS, which the production file's
                // Brevo base_url would fail.
                let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
                    .args(["notifications", "check", "--config"])
                    .arg(dir.join(file))
                    .env_clear()
                    .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
                    .output()
                    .unwrap();
                let report: Value = serde_json::from_slice(&output.stdout).unwrap();
                assert_eq!(report["status"], "ok", "{label}: {file}: {report}");
                assert!(output.status.success());
            }
        }
    }
}
