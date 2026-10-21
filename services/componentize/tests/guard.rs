//! Negative and positive cases for the service contract guard, built from
//! WIT worlds with dummy core modules, as `wasm-tools component embed
//! --dummy` does. Regression coverage for verifier finding V1-F2.

use merezha_componentize::{MAX_COMPONENT_BYTES, componentize};
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
    embed_component_metadata(&mut module, &resolve, world, StringEncoding::UTF8, false)
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
