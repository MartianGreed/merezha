//! Rendering (scaffold §2.8, §3.14): the starter for every block selection
//! §2.2 allows, the `template.toml` index rules on crafted templates, the
//! substitutions, and the build script's refusals on a probe crate that
//! carries it. Loading the rendered configuration through the host is
//! `crates/cli/tests/new.rs`, which has every block registered.
use merezha_scaffold::{
    Block, Catalog, Command, Context, Error, Pin, Rendered, SecretKind, SecretSource, Selection,
    Values, catalog, render,
};
use std::{collections::BTreeMap, fs, path::Path, process::Command as Process};

const REV: &str = "0123456789abcdef0123456789abcdef01234567";

fn values() -> Values {
    Values {
        app_id: "acme".to_owned(),
        pin: Pin::Rev(REV.to_owned()),
        merezha_version: "0.1.0".to_owned(),
    }
}

fn starter(selection: &Selection) -> BTreeMap<String, String> {
    let catalog = Catalog::embedded().unwrap();
    let template = catalog.get("starter").unwrap();
    render::render(template, selection, &values())
        .unwrap()
        .into_iter()
        .map(|Rendered { path, contents, .. }| (path, String::from_utf8(contents).unwrap()))
        .collect()
}

#[test]
fn every_allowed_selection_renders_what_3_14_says() {
    let selections = Selection::allowed();
    assert_eq!(selections.len(), 8, "{selections:?}");
    for selection in selections {
        let on = |block| selection.contains(block);
        let files = starter(&selection);
        let label = selection.names().join(",");
        let has = |path: &str| files.contains_key(path);
        for path in [
            "merezha.toml",
            "merezha.production.toml",
            "runtime.toml",
            "application.toml",
            ".gitignore",
            ".env.example",
        ] {
            assert!(has(path), "{label}: {path} missing");
        }
        assert_eq!(has("content.toml"), on(Block::Content), "{label}");
        assert_eq!(
            has("notifications/templates/notes.shared/template.toml"),
            on(Block::Notifications),
            "{label}"
        );
        for (path, text) in &files {
            assert!(!text.contains("%%"), "{label}: {path} keeps a placeholder");
            assert!(!path.ends_with(".tmpl"), "{label}: {path}");
            assert!(
                !path.contains("config/") && !path.starts_with("env/"),
                "{label}: {path}"
            );
            if path.ends_with(".toml") {
                text.parse::<toml::Table>()
                    .unwrap_or_else(|e| panic!("{label}: {path}: {e}"));
                assert!(
                    text.starts_with("# SPDX-License-Identifier: MIT OR MIT-0\n"),
                    "{label}: {path}"
                );
            }
        }
        for config in ["merezha.toml", "merezha.production.toml"] {
            let table: toml::Table = files[config].parse().unwrap();
            let modules = table.get("modules").and_then(|m| m.as_table());
            let module = |name: &str| modules.is_some_and(|m| m.contains_key(name));
            assert_eq!(module("identity"), on(Block::Identity), "{label}: {config}");
            assert_eq!(
                module("notifications"),
                on(Block::Notifications),
                "{label}: {config}"
            );
            assert_eq!(module("content"), on(Block::Content), "{label}: {config}");
            let telemetry = table["telemetry"].as_table().unwrap();
            assert_eq!(
                telemetry.contains_key("resource"),
                on(Block::Telemetry),
                "{label}: {config}"
            );
            let queues = table["jobs"]["queues"].as_table().unwrap();
            assert!(queues.contains_key("application"));
            assert_eq!(
                queues.contains_key("notify.email"),
                on(Block::Notifications)
            );
            assert_eq!(queues.contains_key("identity"), on(Block::Identity));
            assert_eq!(queues.contains_key("content"), on(Block::Content));
            if on(Block::Notifications) {
                let webhooks = &table["modules"]["notifications"]["webhooks"];
                // The operator role exists only with identity.
                assert_eq!(
                    webhooks.get("application_alert_roles").is_some(),
                    on(Block::Identity),
                    "{label}: {config}"
                );
            }
            assert_eq!(table["app"]["id"].as_str(), Some("acme"));
        }
        let application = &files["application.toml"];
        assert_eq!(
            application.contains("access = \"public\""),
            !on(Block::Identity),
            "{label}"
        );
        assert_eq!(application.contains("[[permissions]]"), on(Block::Identity));
        let example = &files[".env.example"];
        assert_eq!(
            example.contains("MEREZHA_IDENTITY_SECRET_KEY="),
            on(Block::Identity),
            "{label}"
        );
        for variable in [
            "MEREZHA_NOTIFY_SECRET_KEY=",
            "MEREZHA_VAPID_PRIVATE_KEY=",
            "BREVO_API_KEY=",
            "BREVO_WEBHOOK_TOKEN=",
        ] {
            assert_eq!(
                example.contains(variable),
                on(Block::Notifications),
                "{label}"
            );
        }
        for line in example.lines().filter(|l| !l.starts_with('#')) {
            assert!(
                line.ends_with('='),
                "{label}: .env.example has a value: {line}"
            );
        }
        assert!(files[".gitignore"].contains("\n/.env\n"));
    }
}

#[test]
fn development_switches_stay_out_of_the_production_file() {
    let files = starter(&Selection::all());
    let production: toml::Table = files["merezha.production.toml"].parse().unwrap();
    assert_eq!(production["http"]["public"].as_bool(), Some(true));
    assert_eq!(production["database"]["tls"].as_str(), Some("require"));
    // A documentation range: a real private network would let any of its
    // peers set the client address `webhook_allowed_cidrs` checks.
    assert_eq!(
        production["http"]["trusted_proxies"],
        toml::Value::Array(vec!["192.0.2.0/24".into()])
    );
    let notifications = &production["modules"]["notifications"];
    assert!(notifications.get("egress").is_none());
    assert_eq!(notifications["email"]["driver"].as_str(), Some("brevo"));
    assert_eq!(notifications["templates"]["preview"].as_bool(), Some(false));
    assert!(notifications["templates"].get("allow_http_links").is_none());
    assert!(notifications["webhooks"].get("allow_http").is_none());
    assert_eq!(
        production["telemetry"]["resource"]["environment"].as_str(),
        Some("production")
    );
    let development: toml::Table = files["merezha.toml"].parse().unwrap();
    assert!(development["http"].get("public").is_none());
    assert!(development["http"].get("cors").is_none());
    assert_eq!(
        development["modules"]["notifications"]["email"]["driver"].as_str(),
        Some("capture")
    );
    // Every secret is an `{ env }` reference in both files.
    for text in [&files["merezha.toml"], &files["merezha.production.toml"]] {
        for key in [
            "secret_key",
            "vapid_private_key",
            "api_key",
            "webhook_token",
            "url",
        ] {
            for line in text
                .lines()
                .filter(|l| l.trim_start().starts_with(&format!("{key} =")))
            {
                assert!(line.contains("{ env = \""), "{line}");
            }
        }
    }
}

#[test]
fn the_embedded_table_is_the_templates_directory() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("templates");
    let mut found = Vec::new();
    walk(&root, "", &mut found);
    found.sort();
    let embedded: Vec<(String, Vec<u8>)> = catalog::embedded_files()
        .iter()
        .map(|(path, bytes)| ((*path).to_owned(), bytes.to_vec()))
        .collect();
    assert_eq!(found, embedded);
    for (path, _) in &found {
        assert_ne!(
            path.rsplit('/').next(),
            Some("Cargo.toml"),
            "{path}: store manifests as Cargo.toml.tmpl"
        );
    }
}

fn walk(dir: &Path, prefix: &str, out: &mut Vec<(String, Vec<u8>)>) {
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        let path = entry.path();
        // What the build script skips (local editor and system leftovers).
        let leftover = name.ends_with('~')
            || name.ends_with(".swp")
            || name.ends_with(".swo")
            || (name.starts_with('#') && name.ends_with('#'))
            || name == "Thumbs.db"
            || (name.starts_with('.') && name != ".github" && name != ".contremaitre.yaml");
        if leftover {
            continue;
        }
        if path.is_dir() {
            walk(&path, &format!("{prefix}{name}/"), out);
        } else {
            out.push((format!("{prefix}{name}"), fs::read(&path).unwrap()));
        }
    }
}

/// A crafted template for the index rules.
fn crafted(index: &str, files: &[(&str, &str)]) -> Result<Catalog, Error> {
    let mut all: Vec<(&'static str, &'static [u8])> = vec![(
        "demo/template.toml",
        Box::leak(index.to_owned().into_bytes().into_boxed_slice()),
    )];
    for (path, text) in files {
        let path: &'static str = Box::leak(format!("demo/{path}").into_boxed_str());
        all.push((path, Box::leak(text.as_bytes().to_vec().into_boxed_slice())));
    }
    Catalog::from_files(all)
}

fn render_demo(catalog: &Catalog, blocks: &[Block]) -> Result<BTreeMap<String, String>, Error> {
    let selection = blocks.iter().copied().collect::<Selection>();
    Ok(
        render::render(catalog.get("demo").unwrap(), &selection, &values())?
            .into_iter()
            .map(|file| (file.path, String::from_utf8(file.contents).unwrap()))
            .collect(),
    )
}

const DEMO_INDEX: &str = r#"
name = "demo"
description = "index rules"
blocks = ["identity", "notifications", "content", "telemetry"]

[[file]]
path = "joined.txt"
fragments = ["parts/00-kernel.txt", "parts/10-identity.txt.tmpl", "parts/20-kernel.txt", "parts/30-telemetry.txt"]

[[file]]
path = "renamed.txt"
source = "only-with-content.txt"
when = ["content"]

[[file]]
path = "renamed.txt"
source = "without-content.txt.tmpl"
unless = ["content"]

[[tree]]
source = "extra"
when = ["notifications"]
"#;

fn demo_files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("parts/00-kernel.txt", "kernel\n"),
        ("parts/10-identity.txt.tmpl", "identity of %%app_id%%\n"),
        ("parts/20-kernel.txt", "kernel again\n"),
        ("parts/30-telemetry.txt", "telemetry\n"),
        ("only-with-content.txt", "content\n"),
        ("without-content.txt.tmpl", "no content in %%app_name%%\n"),
        ("extra/notes.txt", "with notifications\n"),
        ("copied.toml.tmpl", "app = \"%%app_id%%\"\n"),
        (
            "Cargo.toml.tmpl",
            "[package]\nname = \"%%app_id%%\"\n%%merezha_dependency%%\n",
        ),
        ("plain.txt", "100%% literal outside .tmpl files\n"),
    ]
}

#[test]
fn the_index_rules() {
    let catalog = crafted(DEMO_INDEX, &demo_files()).unwrap();
    let all = render_demo(
        &catalog,
        &[
            Block::Identity,
            Block::Notifications,
            Block::Content,
            Block::Telemetry,
        ],
    )
    .unwrap();
    assert_eq!(
        all.keys().map(String::as_str).collect::<Vec<_>>(),
        [
            "Cargo.toml",
            "copied.toml",
            "extra/notes.txt",
            "joined.txt",
            "plain.txt",
            "renamed.txt",
        ],
        "sources and fragments are not copied; .tmpl is dropped"
    );
    assert_eq!(
        all["joined.txt"],
        "kernel\nidentity of acme\nkernel again\ntelemetry\n"
    );
    assert_eq!(all["renamed.txt"], "content\n");
    assert_eq!(
        all["copied.toml"],
        "# SPDX-License-Identifier: MIT OR MIT-0\napp = \"acme\"\n"
    );
    assert_eq!(
        all["Cargo.toml"],
        format!(
            "# SPDX-License-Identifier: MIT OR MIT-0\n[package]\nname = \"acme\"\n\
             merezha-domain = {{ git = \"https://github.com/MartianGreed/merezha\", rev = \"{REV}\" }}\n"
        )
    );
    assert_eq!(all["plain.txt"], "100%% literal outside .tmpl files\n");

    let none = render_demo(&catalog, &[]).unwrap();
    assert_eq!(none["joined.txt"], "kernel\nkernel again\n");
    assert_eq!(none["renamed.txt"], "no content in acme\n");
    assert!(!none.contains_key("extra/notes.txt"));
}

#[test]
fn index_defects_are_template_invalid() {
    let defect = |index: &str, files: &[(&str, &str)]| {
        let error = crafted(index, files)
            .and_then(|catalog| render_demo(&catalog, &[Block::Identity]))
            .unwrap_err();
        assert_eq!(error.code, "template_invalid", "{index}");
    };
    let head = "name = \"demo\"\ndescription = \"d\"\nblocks = [\"identity\"]\n";
    // Two entries for one output.
    defect(
        &format!(
            "{head}[[file]]\npath = \"a\"\nsource = \"x\"\n[[file]]\npath = \"a\"\nsource = \"y\"\n"
        ),
        &[("x", ""), ("y", "")],
    );
    // A copied file colliding with a generated one.
    defect(
        &format!("{head}[[file]]\npath = \"b\"\nsource = \"x\"\n"),
        &[("x", ""), ("b", "")],
    );
    // Outputs the command writes itself.
    defect(
        &format!("{head}[[file]]\npath = \".env\"\nsource = \"x\"\n"),
        &[("x", "")],
    );
    defect(
        &format!("{head}[[file]]\npath = \"../out\"\nsource = \"x\"\n"),
        &[("x", "")],
    );
    // A missing source, a block the template does not know, a fragment of
    // no block, both source and fragments, an unknown key.
    defect(
        &format!("{head}[[file]]\npath = \"a\"\nsource = \"missing\"\n"),
        &[],
    );
    defect(
        &format!("{head}[[file]]\npath = \"a\"\nsource = \"x\"\nwhen = [\"content\"]\n"),
        &[("x", "")],
    );
    defect(
        &format!("{head}[[file]]\npath = \"a\"\nfragments = [\"part.txt\"]\n"),
        &[("part.txt", "")],
    );
    defect(
        &format!("{head}[[file]]\npath = \"a\"\nsource = \"x\"\nfragments = [\"x\"]\n"),
        &[("x", "")],
    );
    defect(&format!("{head}colour = \"blue\"\n"), &[]);
    // The directory and the name disagree, an unknown template block.
    defect("name = \"other\"\ndescription = \"d\"\nblocks = []\n", &[]);
    defect(
        "name = \"demo\"\ndescription = \"d\"\nblocks = [\"billing\"]\n",
        &[],
    );
}

#[test]
fn substitutions_are_a_closed_set() {
    let values = values();
    assert_eq!(
        render::substitute(
            "%%app_id%%/%%app_name%%@%%merezha_version%%#%%merezha_rev%%",
            &values
        )
        .unwrap(),
        format!("acme/acme@0.1.0#{REV}")
    );
    for text in ["%%app_idd%%", "%%app_id", "%%%%", "50%% off"] {
        assert_eq!(
            render::substitute(text, &values).unwrap_err().code,
            "template_invalid",
            "{text}"
        );
    }
    let path = Pin::Path("../..".to_owned());
    assert_eq!(
        path.dependency(),
        "merezha-domain = { path = \"../../crates/domain\" }"
    );
    assert_eq!(
        Pin::Path(".".to_owned()).dependency(),
        "merezha-domain = { path = \"crates/domain\" }"
    );
    assert!(path.version_file().ends_with("path = \"../..\"\n"));
    assert!(
        Pin::Rev(REV.to_owned())
            .version_file()
            .ends_with(&format!("rev = \"{REV}\"\n"))
    );
    assert_eq!(path.rev(), "");
    // A quote or backslash in a path stays inside the TOML string.
    let odd = Pin::Path("../we\"ird\\dir".to_owned()).dependency();
    let parsed: toml::Table = odd.parse().unwrap();
    assert_eq!(
        parsed["merezha-domain"]["path"].as_str(),
        Some("../we\"ird\\dir/crates/domain")
    );
}

struct Fixed;
impl SecretSource for Fixed {
    fn generate(&mut self, _: SecretKind) -> Result<String, Error> {
        Ok("c2VjcmV0".to_owned())
    }
}

#[test]
fn a_fixed_template_refuses_blocks() {
    let index = "name = \"demo\"\ndescription = \"d\"\nblocks = [\"identity\", \"notifications\"]\nfixed = true\n";
    let catalog = crafted(index, &[("a.txt", "a\n")]).unwrap();
    let root = tempfile::tempdir().unwrap();
    let context = Context {
        catalog: &catalog,
        cwd: root.path(),
        built_from: Some(REV),
        merezha_version: "0.1.0",
    };
    let parse = |args: &[&str]| match Command::parse(args.iter().map(Into::into)).unwrap() {
        Command::New(request) => request,
        Command::Help => panic!("help"),
    };
    let refused = merezha_scaffold::prepare(
        &parse(&[
            "acme",
            "--template",
            "demo",
            "--blocks",
            "identity,notifications",
        ]),
        &context,
        &mut Fixed,
    )
    .unwrap_err();
    assert_eq!(refused.code, "blocks_fixed_by_template");
    let created = merezha_scaffold::prepare(
        &parse(&["acme", "--template", "demo"]),
        &context,
        &mut Fixed,
    )
    .unwrap()
    .write()
    .unwrap();
    assert_eq!(created.blocks, ["identity", "notifications"]);
    assert!(root.path().join("acme/a.txt").is_file());
}

const BUILD_SCRIPT: &str = include_str!("../build.rs");

/// Fills a probe's `templates/starter`.
type Setup = Box<dyn FnOnce(&Path)>;

/// Builds a probe crate whose `templates/` `setup` fills, with this crate's
/// build script; the build's success and stderr.
fn probe_build(root: &Path, case: &str, setup: impl FnOnce(&Path)) -> (bool, String) {
    let dir = root.join(case);
    fs::create_dir_all(dir.join("src")).unwrap();
    fs::create_dir_all(dir.join("templates/starter")).unwrap();
    fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"templates-probe\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n",
    )
    .unwrap();
    fs::write(dir.join("build.rs"), BUILD_SCRIPT).unwrap();
    fs::write(
        dir.join("src/main.rs"),
        "include!(concat!(env!(\"OUT_DIR\"), \"/templates.rs\"));\n\
         fn main() { for (path, bytes) in FILES { println!(\"{path} {}\", bytes.len()); } }\n",
    )
    .unwrap();
    setup(&dir.join("templates/starter"));
    let output = Process::new(env!("CARGO"))
        .args(["run", "--quiet", "--manifest-path"])
        .arg(dir.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", root.join("target"))
        .env_remove("CARGO_BUILD_TARGET_DIR")
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), text)
}

#[test]
fn the_build_script_refuses_unsafe_or_mistyped_templates() {
    let root = tempfile::tempdir().unwrap();
    let (ok, out) = probe_build(root.path(), "good", |t| {
        fs::create_dir(t.join("nested")).unwrap();
        fs::write(
            t.join("nested/b.toml.tmpl"),
            "id = \"%%app_id%%\" # %%merezha_rev%%\n",
        )
        .unwrap();
        fs::write(t.join("a.txt"), "50%% plain files are not checked\n").unwrap();
        fs::write(t.join("Cargo.toml.tmpl"), "[package]\n").unwrap();
        // Leftovers that must never ship, and the dotted names that do.
        for junk in [
            ".DS_Store",
            "a.txt.swp",
            "a.txt.swo",
            "a.txt~",
            "#a.txt#",
            ".#a.txt",
            "Thumbs.db",
        ] {
            fs::write(t.join(junk), "junk %%nope%%\n").unwrap();
        }
        fs::create_dir(t.join(".idea")).unwrap();
        fs::write(t.join(".idea/workspace.xml"), "<x/>\n").unwrap();
        fs::write(t.join("nested/.secret.tmpl"), "%%nope%%\n").unwrap();
        fs::create_dir_all(t.join(".github/workflows")).unwrap();
        fs::write(t.join(".github/workflows/ci.yml"), "on: push\n").unwrap();
        fs::write(t.join(".contremaitre.yaml"), "version: 1\n").unwrap();
    });
    assert!(ok, "{out}");
    let listed: Vec<&str> = out.lines().filter(|l| l.starts_with("starter/")).collect();
    assert_eq!(
        listed,
        [
            "starter/.contremaitre.yaml 11",
            "starter/.github/workflows/ci.yml 9",
            "starter/Cargo.toml.tmpl 10",
            "starter/a.txt 33",
            "starter/nested/b.toml.tmpl 36",
        ],
        "sorted, every file but the leftovers"
    );

    let cases: [(&str, &str, Setup); 6] = [
        (
            "placeholder",
            "unknown substitution at %%app_idd%%",
            Box::new(|t: &Path| fs::write(t.join("x.tmpl"), "a %%app_idd%% b\n").unwrap()),
        ),
        (
            "unterminated",
            "unknown substitution",
            Box::new(|t: &Path| fs::write(t.join("x.tmpl"), "a %%app_id b\n").unwrap()),
        ),
        (
            "symlink",
            "is a symbolic link",
            Box::new(|t: &Path| std::os::unix::fs::symlink("/etc/hosts", t.join("hosts")).unwrap()),
        ),
        (
            "manifest",
            "must be stored as Cargo.toml.tmpl",
            Box::new(|t: &Path| {
                fs::create_dir(t.join("guest")).unwrap();
                fs::write(t.join("guest/Cargo.toml"), "[package]\n").unwrap();
            }),
        ),
        (
            "large",
            "is over 1 MiB",
            Box::new(|t: &Path| fs::write(t.join("big.bin"), vec![b'a'; 1024 * 1024 + 1]).unwrap()),
        ),
        (
            "binary",
            "is a .tmpl file that is not UTF-8",
            Box::new(|t: &Path| fs::write(t.join("x.tmpl"), [0xff, 0xfe, b'a']).unwrap()),
        ),
    ];
    for (case, message, setup) in cases {
        let (ok, out) = probe_build(root.path(), case, setup);
        assert!(!ok, "{case} was accepted");
        assert!(out.contains(message), "{case}: {out}");
    }
}

/// Hands out `value` and counts the calls.
struct Counting {
    value: &'static str,
    calls: Vec<SecretKind>,
}
impl SecretSource for Counting {
    fn generate(&mut self, kind: SecretKind) -> Result<String, Error> {
        self.calls.push(kind);
        Ok(format!("{}{}", self.value, self.calls.len()))
    }
}

#[test]
fn prepare_writes_env_last_with_only_the_selected_secrets() {
    let catalog = Catalog::embedded().unwrap();
    let root = tempfile::tempdir().unwrap();
    let context = Context {
        catalog: &catalog,
        cwd: root.path(),
        built_from: Some(REV),
        merezha_version: "0.1.0",
    };
    let request = |args: &[&str]| match Command::parse(args.iter().map(Into::into)).unwrap() {
        Command::New(request) => request,
        Command::Help => panic!("help"),
    };
    let mut source = Counting {
        value: "c2VjcmV0",
        calls: Vec::new(),
    };
    let plan = merezha_scaffold::prepare(&request(&["acme"]), &context, &mut source).unwrap();
    let paths: Vec<&str> = plan.paths().collect();
    let gitignore = paths.iter().position(|p| *p == ".gitignore").unwrap();
    assert_eq!(paths.last(), Some(&".env"));
    assert!(gitignore < paths.len() - 1);
    assert_eq!(
        source.calls,
        [
            SecretKind::Token,
            SecretKind::Key,
            SecretKind::VapidPrivateKey
        ]
    );
    let shown = format!("{plan:?}");
    assert!(!shown.contains("c2VjcmV0"), "{shown}");
    let created = plan.write().unwrap();
    let env = fs::read_to_string(root.path().join("acme/.env")).unwrap();
    let lines: Vec<&str> = env.lines().filter(|l| !l.starts_with('#')).collect();
    assert_eq!(
        lines,
        [
            "MEREZHA_IDENTITY_SECRET_KEY=c2VjcmV01",
            "MEREZHA_NOTIFY_SECRET_KEY=c2VjcmV02",
            "MEREZHA_VAPID_PRIVATE_KEY=c2VjcmV03",
        ]
    );
    assert_eq!(created.secrets.len(), 3);

    // Without secret-holding blocks, or with --no-secrets, no `.env` and
    // no generation at all.
    for args in [
        &["plain", "--blocks", "telemetry"][..],
        &["quiet", "--no-secrets"][..],
    ] {
        let mut source = Counting {
            value: "x",
            calls: Vec::new(),
        };
        let plan = merezha_scaffold::prepare(&request(args), &context, &mut source).unwrap();
        assert!(!plan.paths().any(|p| p == ".env"), "{args:?}");
        assert!(source.calls.is_empty(), "{args:?}");
        assert!(plan.write().unwrap().secrets.is_empty());
    }

    // A value a shell would interpret is refused before anything is written.
    let mut unsafe_source = Counting {
        value: "$(id) ",
        calls: Vec::new(),
    };
    let error =
        merezha_scaffold::prepare(&request(&["shell"]), &context, &mut unsafe_source).unwrap_err();
    assert_eq!(error.code, "secret_invalid");
    assert!(!root.path().join("shell").exists());
}

/// The embedded starter with `template.toml` edited by `edit`.
fn edited_starter(edit: impl Fn(&str) -> String) -> Catalog {
    let files = catalog::embedded_files().iter().map(|(path, bytes)| {
        if *path == "starter/template.toml" {
            let text = edit(std::str::from_utf8(bytes).unwrap());
            let leaked: &'static [u8] = Box::leak(text.into_bytes().into_boxed_slice());
            (*path, leaked)
        } else {
            (*path, *bytes)
        }
    });
    Catalog::from_files(files).unwrap()
}

#[test]
fn the_sc_q7_switches_turn_push_and_webhooks_off_as_a_whole() {
    for (push, webhooks) in [(false, true), (true, false), (false, false)] {
        let catalog = edited_starter(|text| {
            text.replace("push = true", &format!("push = {push}"))
                .replace("webhooks = true", &format!("webhooks = {webhooks}"))
        });
        let template = catalog.get("starter").unwrap();
        for selection in Selection::allowed() {
            let label = format!("push={push} webhooks={webhooks} {:?}", selection.names());
            let files: BTreeMap<String, String> = render::render(template, &selection, &values())
                .unwrap()
                .into_iter()
                .map(|f| (f.path, String::from_utf8(f.contents).unwrap()))
                .collect();
            for config in ["merezha.toml", "merezha.production.toml"] {
                let table: toml::Table = files[config].parse().unwrap();
                let Some(notifications) = table.get("modules").and_then(|m| m.get("notifications"))
                else {
                    continue;
                };
                assert_eq!(notifications.get("push").is_some(), push, "{label}");
                assert_eq!(notifications.get("webhooks").is_some(), webhooks, "{label}");
                let channels = notifications["topics"]["notes.shared"]["channels"]
                    .as_array()
                    .unwrap();
                assert_eq!(
                    channels.iter().any(|c| c.as_str() == Some("push")),
                    push,
                    "{label}"
                );
                // No key landed in another table.
                assert!(
                    notifications["sms"].as_table().unwrap().len() == 1,
                    "{label}"
                );
                if !webhooks {
                    assert!(!files[config].contains("event_types"), "{label}");
                }
            }
            if selection.contains(Block::Notifications) {
                assert_eq!(
                    files[".env.example"].contains("MEREZHA_VAPID_PRIVATE_KEY="),
                    push,
                    "{label}"
                );
            }
        }
        // The VAPID secret is generated only with push.
        let root = tempfile::tempdir().unwrap();
        let context = Context {
            catalog: &catalog,
            cwd: root.path(),
            built_from: Some(REV),
            merezha_version: "0.1.0",
        };
        let Command::New(request) = Command::parse(["acme".into()]).unwrap() else {
            panic!("help");
        };
        let mut source = Counting {
            value: "c2VjcmV0",
            calls: Vec::new(),
        };
        merezha_scaffold::prepare(&request, &context, &mut source).unwrap();
        assert_eq!(source.calls.contains(&SecretKind::VapidPrivateKey), push);
    }
    // A switch the index does not declare is a template defect.
    let catalog = edited_starter(|text| text.replace("push = true\n", ""));
    let error = render::render(
        catalog.get("starter").unwrap(),
        &Selection::all(),
        &values(),
    )
    .unwrap_err();
    assert_eq!(error.code, "template_invalid");
}
