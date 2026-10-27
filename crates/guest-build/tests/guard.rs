//! Negative and positive cases for the service contract guard, built from
//! WIT worlds with dummy core modules, as `wasm-tools component embed
//! --dummy` does. Regression coverage for verifier finding V1-F2, moved from
//! `services/componentize` with the guard (scaffold SC-S1) and run on
//! `wit-component` 0.258.3's three-argument `targets`.

use merezha_guest_build::{ErrorKind, MAX_COMPONENT_BYTES, componentize, componentize_file};
use wit_component::{DecodedWasm, StringEncoding, dummy_module, embed_component_metadata};
use wit_parser::{LiftLowerAbi, ManglingAndAbi, Resolve};

const CANONICAL: &str = include_str!("../../../wit/service.wit");

/// A core module whose embedded component type is world `service` of `wit`.
fn core_module(wit: &str) -> Vec<u8> {
    let mut resolve = Resolve::default();
    let package = resolve
        .push_source("fixture.wit", wit)
        .expect("fixture WIT parses");
    let world = resolve
        .select_world(&[package], Some("service"))
        .expect("fixture has world `service`");
    let mut module = dummy_module(&resolve, world, ManglingAndAbi::Legacy(LiftLowerAbi::Sync));
    embed_component_metadata(&mut module, &resolve, world, StringEncoding::UTF8)
        .expect("metadata embeds");
    module
}

fn rejection(wit: &str) -> String {
    let error = componentize(&core_module(wit)).expect_err("the guard must reject this world");
    format!("{error:#}")
}

#[test]
fn accepts_the_canonical_world() {
    let component = componentize(&core_module(CANONICAL)).expect("canonical world is accepted");
    let DecodedWasm::Component(resolve, world) = wit_component::decode(&component).unwrap() else {
        panic!("expected a component");
    };
    let world = &resolve.worlds[world];
    assert!(world.imports.is_empty());
    let exports: Vec<String> = world
        .exports
        .keys()
        .map(|key| resolve.name_world_key(key))
        .collect();
    assert_eq!(exports, ["merezha:service/handler@0.1.0"]);
}

#[test]
fn rejects_any_import() {
    let message = rejection(
        "package merezha:service@0.1.0;
         interface handler { handle: func(input: string) -> result<string, string>; }
         interface clock { now: func() -> u64; }
         world service { import clock; export handler; }",
    );
    assert!(
        message.contains("it imports merezha:service/clock@0.1.0"),
        "{message}"
    );
}

#[test]
fn rejects_an_extra_export() {
    let message = rejection(
        "package merezha:service@0.1.0;
         interface handler { handle: func(input: string) -> result<string, string>; }
         interface admin { reset: func(); }
         world service { export handler; export admin; }",
    );
    assert!(
        message.contains("services must export exactly"),
        "{message}"
    );
}

#[test]
fn rejects_a_missing_handler() {
    let message = rejection(
        "package merezha:service@0.1.0;
         interface admin { reset: func(); }
         world service { export admin; }",
    );
    assert!(
        message.contains("it exports [merezha:service/admin@0.1.0]"),
        "{message}"
    );
}

#[test]
fn rejects_another_contract_version() {
    let message = rejection(
        "package merezha:service@0.2.0;
         interface handler { handle: func(input: string) -> result<string, string>; }
         world service { export handler; }",
    );
    assert!(
        message.contains("it exports [merezha:service/handler@0.2.0]"),
        "{message}"
    );
}

#[test]
fn rejects_the_canonical_name_with_a_different_signature() {
    for signature in [
        "handle: func(input: string) -> u32;",
        "handle: func(input: list<u8>) -> result<string, string>;",
        "handle: func(input: string) -> result<string>;",
        "handle: func(input: string, extra: u8) -> result<string, string>;",
        "process: func(input: string) -> result<string, string>;",
    ] {
        let wit = format!(
            "package merezha:service@0.1.0;
             interface handler {{ {signature} }}
             world service {{ export handler; }}"
        );
        let message = rejection(&wit);
        assert!(
            message.contains("does not have the canonical types"),
            "{signature}: {message}"
        );
    }
}

#[test]
fn rejects_an_oversized_module_before_encoding() {
    let message = format!(
        "{:#}",
        componentize(&vec![0; MAX_COMPONENT_BYTES + 1]).unwrap_err()
    );
    assert!(message.contains("at most 16777216"), "{message}");
}

#[test]
fn rejects_a_wasi_import() {
    let message = rejection(
        "package merezha:service@0.1.0;
         interface handler { handle: func(input: string) -> result<string, string>; }
         world service {
             import wasi:clocks/wall-clock@0.2.0;
             export handler;
         }
         package wasi:clocks@0.2.0 {
             interface wall-clock { now: func() -> u64; }
         }",
    );
    assert!(
        message.contains("it imports wasi:clocks/wall-clock@0.2.0"),
        "{message}"
    );
}

#[test]
fn componentize_file_writes_the_component_and_no_partial() {
    let dir = tempfile::tempdir().unwrap();
    let (input, output) = (dir.path().join("core.wasm"), dir.path().join("out.wasm"));
    std::fs::write(&input, core_module(CANONICAL)).unwrap();
    let bytes = componentize_file(&input, &output).expect("canonical module componentizes");
    assert_eq!(std::fs::read(&output).unwrap().len(), bytes);
    assert!(!dir.path().join("out.wasm.partial").exists());
}

#[test]
fn componentize_file_classifies_its_failures_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.wasm");

    let missing = componentize_file(&dir.path().join("absent.wasm"), &output).unwrap_err();
    assert_eq!(missing.kind, ErrorKind::ComponentSourceMissing);
    assert_eq!(missing.code(), "component_source_missing");

    let importer = dir.path().join("importer.wasm");
    std::fs::write(
        &importer,
        core_module(
            "package merezha:service@0.1.0;
             interface handler { handle: func(input: string) -> result<string, string>; }
             interface clock { now: func() -> u64; }
             world service { import clock; export handler; }",
        ),
    )
    .unwrap();
    let rejected = componentize_file(&importer, &output).unwrap_err();
    assert_eq!(rejected.code(), "component_rejected");
    assert!(
        rejected.reason.contains("services must import nothing"),
        "{rejected}"
    );

    let oversized = dir.path().join("oversized.wasm");
    std::fs::write(&oversized, vec![0; MAX_COMPONENT_BYTES + 1]).unwrap();
    let rejected = componentize_file(&oversized, &output).unwrap_err();
    assert_eq!(rejected.kind, ErrorKind::ComponentRejected);
    assert!(rejected.reason.contains("at most 16777216"), "{rejected}");

    let names: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert!(
        !names
            .iter()
            .any(|name| name.to_string_lossy().starts_with("out.wasm")),
        "{names:?}"
    );
}

#[test]
fn rejects_extra_functions_or_resources_inside_the_handler_interface() {
    // `targets` alone accepts these: they are subtypes of the canonical
    // interface.
    for (extra, what) in [
        ("reset: func();", "functions [handle, reset]"),
        ("resource session { close: func(); }", "types [session]"),
    ] {
        let message = rejection(&format!(
            "package merezha:service@0.1.0;
             interface handler {{
                 handle: func(input: string) -> result<string, string>;
                 {extra}
             }}
             world service {{ export handler; }}"
        ));
        assert!(message.contains(what), "{extra}: {message}");
        assert!(
            message.contains("services export exactly [handle]"),
            "{message}"
        );
    }
}

/// Appends a custom section so `module` is exactly `total` bytes.
fn pad(mut module: Vec<u8>, total: usize) -> Vec<u8> {
    fn leb(mut value: usize, out: &mut Vec<u8>) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }
    // id, 4-byte size (the sizes here are between 2^21 and 2^28), then
    // the name `pad` and the payload.
    let size = total - module.len() - 1 - 4;
    module.push(0);
    let mut encoded = Vec::new();
    leb(size, &mut encoded);
    assert_eq!(encoded.len(), 4);
    module.extend(encoded);
    module.extend([3, b'p', b'a', b'd']);
    module.resize(total, 0);
    module
}

#[test]
fn rejects_a_component_over_the_limit_from_a_module_under_it() {
    let module = pad(core_module(CANONICAL), 16_777_115);
    assert!(module.len() <= MAX_COMPONENT_BYTES);
    let message = format!("{:#}", componentize(&module).unwrap_err());
    assert!(
        message.contains("the component is")
            && message.contains("the host accepts at most 16777216"),
        "{message}"
    );
    // A smaller pad still fits.
    componentize(&pad(core_module(CANONICAL), 1 << 22)).expect("a 4 MiB module fits");
}

#[cfg(unix)]
#[test]
fn componentize_file_never_writes_through_a_planted_partial() {
    let dir = tempfile::tempdir().unwrap();
    let (input, output) = (dir.path().join("core.wasm"), dir.path().join("out.wasm"));
    std::fs::write(&input, core_module(CANONICAL)).unwrap();
    let victim = dir.path().join("victim");
    std::fs::write(&victim, "keep me").unwrap();
    std::os::unix::fs::symlink(&victim, dir.path().join("out.wasm.partial")).unwrap();
    componentize_file(&input, &output).expect("componentizes");
    assert_eq!(std::fs::read_to_string(&victim).unwrap(), "keep me");
    assert!(std::fs::read(&output).unwrap().starts_with(b"\0asm"));
    assert!(!dir.path().join("out.wasm.partial").exists());
}
