//! `merezha build` and `merezha componentize` as processes (scaffold §4.1,
//! SC-S1) on a fixture workspace the test generates: `domain`, `renamed`
//! (`[lib] name = "renamed_lib"`), `plain` (no `cdylib`), `example-only`
//! (an rlib library and a `cdylib` example) and three guests
//! that break the contract: `importer` (a WASI import), `exporter` (a second
//! export) and `oversized` (a core module over 16 MiB).
//!
//! The Wasm builds are skipped with a message when the
//! `wasm32-unknown-unknown` target is missing, unless
//! `MEREZHA_TEST_REQUIRE_WASM_TARGET=1` (the services workflow) turns that
//! into a failure. The manifest checks, which come before the target check,
//! run everywhere.
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

const DOMAIN_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../domain");
const WIT_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../wit");

const HANDLER_WIT: &str =
    "interface handler { handle: func(input: string) -> result<string, string>; }";

/// A guest crate's `lib.rs`: the SDK's `handle` behind `world`, plus
/// `extra` items.
fn guest_source(bindings: &str, extra: &str) -> String {
    format!(
        r#"pub fn handle(input: &str) -> Result<String, String> {{
    merezha_domain::handle(input, |_| Err(merezha_domain::Failure::permanent("unsupported_operation")))
}}

#[cfg(target_arch = "wasm32")]
mod component {{
    wit_bindgen::generate!({bindings});

    pub struct Component;

    impl exports::merezha::service::handler::Guest for Component {{
        fn handle(input: String) -> Result<String, String> {{
            super::handle(&input)
        }}
    }}
{extra}
    export!(Component);
}}
"#
    )
}

fn package(root: &Path, name: &str, lib: &str, source: &str) {
    let dir = root.join(name);
    fs::create_dir_all(dir.join("src")).unwrap();
    fs::write(
        dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2024"
publish = false

{lib}

[dependencies]
merezha-domain = {{ path = "{DOMAIN_ROOT}" }}

[target.'cfg(target_arch = "wasm32")'.dependencies]
wit-bindgen = {{ version = "=0.63.0", default-features = false, features = ["macros", "realloc"] }}
"#
        ),
    )
    .unwrap();
    fs::write(dir.join("src/lib.rs"), source).unwrap();
}

/// The fixture workspace under `root`, without `Cargo.lock`.
fn workspace(root: &Path) -> PathBuf {
    fs::create_dir_all(root).unwrap();
    let canonical = format!(r#"{{ path: "{WIT_ROOT}", world: "service" }}"#);
    let cdylib = "[lib]\ncrate-type = [\"cdylib\"]";
    package(root, "domain", cdylib, &guest_source(&canonical, ""));
    package(
        root,
        "renamed",
        "[lib]\nname = \"renamed_lib\"\ncrate-type = [\"cdylib\"]",
        &guest_source(&canonical, ""),
    );
    package(root, "plain", "", "pub fn nothing() {}\n");
    // An rlib library and a cdylib example: the example is not the library.
    package(
        root,
        "example-only",
        "[[example]]\nname = \"component\"\ncrate-type = [\"cdylib\"]",
        "pub fn nothing() {}\n",
    );
    fs::create_dir_all(root.join("example-only/examples")).unwrap();
    fs::write(
        root.join("example-only/examples/component.rs"),
        "pub fn x() {}\n",
    )
    .unwrap();
    let importer = format!(
        r##"{{ inline: r#"
package merezha:service@0.1.0;
{HANDLER_WIT}
world service {{ import wasi:clocks/wall-clock@0.2.0; export handler; }}
package wasi:clocks@0.2.0 {{ interface wall-clock {{ now: func() -> u64; }} }}
"#, world: "merezha:service/service", generate_all }}"##
    );
    package(
        root,
        "importer",
        cdylib,
        &guest_source(
            &importer,
            "    #[unsafe(no_mangle)]\n    pub extern \"C\" fn clock() -> u64 { wasi::clocks::wall_clock::now() }\n",
        ),
    );
    let exporter = format!(
        r##"{{ inline: r#"
package merezha:service@0.1.0;
{HANDLER_WIT}
interface admin {{ reset: func(); }}
world service {{ export handler; export admin; }}
"#, world: "service" }}"##
    );
    package(
        root,
        "exporter",
        cdylib,
        &guest_source(
            &exporter,
            "    impl exports::merezha::service::admin::Guest for Component {\n        fn reset() {}\n    }\n",
        ),
    );
    package(
        root,
        "oversized",
        cdylib,
        &guest_source(
            &canonical,
            "    #[unsafe(no_mangle)]\n    pub static OVERSIZED: [u8; 17 * 1024 * 1024] = [7; 17 * 1024 * 1024];\n",
        ),
    );
    fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nresolver = \"3\"\nmembers = [\"domain\", \"renamed\", \"plain\", \"example-only\", \"importer\", \"exporter\", \"oversized\"]\n",
    )
    .unwrap();
    root.join("Cargo.toml")
}

/// A runtime manifest in `dir` with `components/<id>.wasm` for each id.
fn manifest(dir: &Path, ids: &[&str]) -> PathBuf {
    fs::create_dir_all(dir).unwrap();
    let mut text = String::from(
        "version = 1\n\n[limits]\nmemory_bytes = 16777216\nfuel = 200000000\nmax_input_bytes = 65536\n\
         max_output_bytes = 65536\ntimeout_ms = 2000\nmax_concurrent_invocations = 4\n",
    );
    for id in ids {
        text.push_str(&format!(
            "\n[[services]]\nid = \"{id}\"\npath = \"components/{id}.wasm\"\napplication_protocol = 2\n"
        ));
    }
    let path = dir.join("runtime.toml");
    fs::write(&path, text).unwrap();
    path
}

fn merezha(args: &[&str], target: &Path, env: &[(&str, &Path)]) -> (Option<i32>, Value) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_merezha"));
    command
        .args(args)
        .env("CARGO_TARGET_DIR", target)
        .env_remove("CARGO_BUILD_TARGET_DIR");
    for (key, value) in env {
        command.env(key, value);
    }
    let Output {
        status,
        stdout,
        stderr,
    } = command.output().unwrap();
    if !status.success() {
        // Cargo's diagnostics, shown when a test fails.
        eprintln!("{}", String::from_utf8_lossy(&stderr));
    }
    let report = serde_json::from_slice(&stdout).unwrap_or_else(|error| {
        panic!(
            "stdout is not one JSON report ({error}): {}\nstderr: {}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        )
    });
    (status.code(), report)
}

fn build(manifest: &Path, workspace: &Path, target: &Path) -> (Option<i32>, Value) {
    merezha(
        &[
            "build",
            "--manifest",
            manifest.to_str().unwrap(),
            "--workspace",
            workspace.to_str().unwrap(),
        ],
        target,
        &[],
    )
}

fn files(dir: &Path) -> Vec<String> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn wasm_target_installed() -> bool {
    let output = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
        .args([
            "--print",
            "target-libdir",
            "--target",
            "wasm32-unknown-unknown",
        ])
        .output();
    let installed = output.is_ok_and(|o| {
        o.status.success() && Path::new(String::from_utf8_lossy(&o.stdout).trim()).is_dir()
    });
    if !installed {
        assert!(
            std::env::var_os("MEREZHA_TEST_REQUIRE_WASM_TARGET").is_none_or(|v| v != "1"),
            "MEREZHA_TEST_REQUIRE_WASM_TARGET=1 but wasm32-unknown-unknown is not installed"
        );
        eprintln!("skipping: rustup target add wasm32-unknown-unknown to run the Wasm builds");
    }
    installed
}

/// Kept between runs, so only the fixture's own crates rebuild.
fn fixture_target() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("guest-build-fixture")
}

#[test]
fn builds_the_fixture_and_refuses_contract_breaches_without_writing() {
    if !wasm_target_installed() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let workspace = workspace(&root.path().join("workspace"));
    let target = fixture_target();

    let good = manifest(&root.path().join("good"), &["domain", "renamed"]);
    let (code, report) = build(&good, &workspace, &target);
    assert_eq!(code, Some(0), "{report}");
    assert_eq!(report["status"], "built");
    assert_eq!(report["lockfile"], "created", "{report}");
    let components = report["components"].as_array().unwrap();
    assert_eq!(components.len(), 2);
    for (component, id) in components.iter().zip(["domain", "renamed"]) {
        assert_eq!(component["id"], id);
        let bytes = fs::metadata(root.path().join(format!("good/components/{id}.wasm")))
            .unwrap()
            .len();
        assert_eq!(component["bytes"], bytes);
    }
    assert_eq!(
        files(&root.path().join("good/components")),
        ["domain.wasm", "renamed.wasm"]
    );
    assert!(root.path().join("workspace/Cargo.lock").is_file());

    // The components load in the runtime and answer version 2 `describe`.
    let runtime = merezha_runtime::Runtime::load(&good).expect("the components load");
    for id in ["domain", "renamed"] {
        let output = runtime
            .invoke_service(id, r#"{"version":2,"body":{"op":"describe"}}"#)
            .unwrap();
        let reply: Value = serde_json::from_str(&output.output).unwrap();
        assert_eq!(reply["version"], 2, "{reply}");
        assert_eq!(reply["outcome"]["status"], "ok", "{reply}");
        assert_eq!(reply["outcome"]["reply"]["kind"], "described", "{reply}");
        assert_eq!(reply["outcome"]["reply"]["protocol"], 2, "{reply}");
    }

    // Now with the lockfile Cargo wrote: `--locked`.
    let (code, report) = build(&good, &workspace, &target);
    assert_eq!(code, Some(0), "{report}");
    assert_eq!(report["lockfile"], "locked");

    for (breach, reason) in [
        ("importer", "it imports wasi:clocks/wall-clock@0.2.0"),
        ("exporter", "services must export exactly"),
        ("oversized", "at most 16777216"),
    ] {
        let dir = root.path().join(breach);
        let manifest = manifest(&dir, &["domain", breach]);
        let (code, report) = build(&manifest, &workspace, &target);
        assert_eq!(code, Some(1), "{breach}: {report}");
        assert_eq!(report["status"], "failed");
        assert_eq!(report["error"]["code"], "component_rejected", "{report}");
        assert_eq!(report["error"]["service"], breach, "{report}");
        let message = report["error"]["reason"].as_str().unwrap();
        assert!(message.contains(reason), "{breach}: {message}");
        assert_eq!(
            files(&dir.join("components")),
            Vec::<String>::new(),
            "{breach} left files behind"
        );
    }

    // `--locked`: a workspace that no longer matches its lockfile fails
    // instead of being re-resolved.
    package(
        &root.path().join("workspace"),
        "late",
        "",
        "pub fn late() {}\n",
    );
    let members = fs::read_to_string(&workspace).unwrap();
    fs::write(
        &workspace,
        members.replace("\"oversized\"]", "\"oversized\", \"late\"]"),
    )
    .unwrap();
    let (code, report) = build(&good, &workspace, &target);
    assert_eq!(code, Some(1), "{report}");
    assert_eq!(report["error"]["code"], "cargo_failed", "{report}");
    fs::write(&workspace, members).unwrap();

    // `merezha componentize` is the last step alone.
    let core = target.join("wasm32-unknown-unknown/release/renamed_lib.wasm");
    let out = root.path().join("renamed.wasm");
    let (code, report) = merezha(
        &[
            "componentize",
            core.to_str().unwrap(),
            out.to_str().unwrap(),
        ],
        &target,
        &[],
    );
    assert_eq!(code, Some(0), "{report}");
    assert_eq!(report["status"], "componentized");
    assert_eq!(report["bytes"], fs::metadata(&out).unwrap().len());
    let core = target.join("wasm32-unknown-unknown/release/importer.wasm");
    let (code, report) = merezha(
        &[
            "componentize",
            core.to_str().unwrap(),
            root.path().join("importer.wasm").to_str().unwrap(),
        ],
        &target,
        &[],
    );
    assert_eq!(code, Some(1), "{report}");
    assert_eq!(report["error"]["code"], "component_rejected");
    assert!(!root.path().join("importer.wasm").exists());
}

#[test]
fn refuses_unknown_packages_and_libraries_without_cdylib_before_building() {
    if !wasm_target_installed() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let workspace = workspace(&root.path().join("workspace"));
    let target = root.path().join("target");
    for (id, code) in [
        ("ghost", "unknown_package"),
        ("plain", "no_cdylib"),
        ("example-only", "no_cdylib"),
    ] {
        let dir = root.path().join(id);
        let (exit, report) = build(&manifest(&dir, &["domain", id]), &workspace, &target);
        assert_eq!(exit, Some(2), "{report}");
        assert_eq!(report["error"]["code"], code, "{report}");
        assert_eq!(report["error"]["service"], id, "{report}");
        assert!(!dir.join("components").exists());
    }
    assert!(
        !root.path().join("workspace/Cargo.lock").exists(),
        "nothing was built"
    );

    // A Cargo failure: the workspace manifest does not exist.
    let (exit, report) = build(
        &manifest(&root.path().join("domain-only"), &["domain"]),
        &root.path().join("absent/Cargo.toml"),
        &target,
    );
    assert_eq!(exit, Some(1), "{report}");
    assert_eq!(report["error"]["code"], "cargo_failed");
}

#[test]
fn refuses_invalid_manifests_and_unsupported_component_paths() {
    let root = tempfile::tempdir().unwrap();
    let workspace = workspace(&root.path().join("workspace"));
    let target = root.path().join("target");
    let dir = root.path().join("renamed-path");
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("runtime.toml");
    fs::write(
        &path,
        fs::read_to_string(manifest(&dir, &["domain"]))
            .unwrap()
            .replace("components/domain.wasm", "components/other.wasm"),
    )
    .unwrap();
    let (exit, report) = build(&path, &workspace, &target);
    assert_eq!(exit, Some(2), "{report}");
    assert_eq!(report["error"]["code"], "unsupported_component_path");
    assert_eq!(report["error"]["service"], "domain");

    fs::write(&path, "version = 1\n").unwrap();
    let (exit, report) = build(&path, &workspace, &target);
    assert_eq!(exit, Some(2), "{report}");
    assert_eq!(report["error"]["code"], "invalid_manifest");
    let (exit, report) = build(&dir.join("absent.toml"), &workspace, &target);
    assert_eq!(exit, Some(2), "{report}");
    assert_eq!(report["error"]["code"], "invalid_manifest");
}

#[cfg(unix)]
#[test]
fn a_missing_wasm_target_fails_before_cargo_runs() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let workspace = workspace(&root.path().join("workspace"));
    let marker = root.path().join("cargo-ran");
    let script = |name: &str, body: String| {
        let path = root.path().join(name);
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    };
    // A toolchain whose wasm32 library directory does not exist.
    let rustc = script(
        "rustc",
        format!("echo {}", root.path().join("no-such-libdir").display()),
    );
    let cargo = script("cargo", format!("touch {}", marker.display()));
    let target = root.path().join("target");
    let manifest = manifest(&root.path().join("project"), &["domain"]);
    let (exit, report) = merezha(
        &[
            "build",
            "--manifest",
            manifest.to_str().unwrap(),
            "--workspace",
            workspace.to_str().unwrap(),
        ],
        &target,
        &[("RUSTC", &rustc), ("CARGO", &cargo)],
    );
    assert_eq!(exit, Some(2), "{report}");
    assert_eq!(report["error"]["code"], "missing_wasm_target");
    assert!(!marker.exists(), "cargo ran");
}

#[test]
fn rejects_invalid_arguments() {
    let target = tempfile::tempdir().unwrap();
    for args in [
        &["build", "--manifest"][..],
        &["build", "--bogus", "x"],
        &["build", "--workspace", "a", "--workspace", "b"],
        &["componentize", "only-one.wasm"],
        &["componentize", "a", "b", "c"],
    ] {
        let (exit, report) = merezha(args, target.path(), &[]);
        assert_eq!(exit, Some(2), "{args:?}: {report}");
        assert_eq!(report["status"], "invalid_arguments", "{args:?}");
    }
    let (exit, report) = merezha(
        &[
            "componentize",
            "/nonexistent/core.wasm",
            "/nonexistent/out.wasm",
        ],
        target.path(),
        &[],
    );
    assert_eq!(exit, Some(1), "{report}");
    assert_eq!(report["error"]["code"], "component_source_missing");
}
