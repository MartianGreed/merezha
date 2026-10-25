//! Guest protocol version 2 on real no-import components (AP §C.1, §C.2,
//! slice C1): the `describe` check at startup and after a reload, the
//! version 1 downgrade on the wire, the principal reaching version 2 guests
//! through the trusted entry points, and what stored events keep of it.
use merezha_application::{
    AllowAll, Application,
    config::Config,
    postgres::{MIGRATIONS, MODULE, PostgresOptions, PostgresStore},
    store::{EventStore, MemoryEventStore},
};
use merezha_domain::{Command, Failure, Identity, Principal, PrincipalKind, Query};
use merezha_postgres::{Database, MigrationMode, ModuleMigrations, TestDatabase, run_migrations};
use merezha_runtime::Runtime;
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

/// What a fixture guest checks about the principal.
#[derive(Clone, Copy)]
enum Rule {
    /// Any request containing `principal` answers `principal_seen`: the
    /// bytes a version 1 guest receives never name it.
    Forbid,
    /// `decide` and `query` without `principal` answer
    /// `principal_missing`.
    Require,
}

/// A component answering the application protocol in `version` only:
/// `validate`, `decide` (one `Added` event), `project` (state 1), `query`
/// (value 1) and, for version 2, `describe`. Any request of another
/// version, or another operation, answers a version 1 `protocol_version`
/// error, as a guest built on the version 1 SDK does.
fn component(version: u8, rule: Rule) -> Vec<u8> {
    let mut data = String::new();
    let mut offset = 128;
    let mut place = |bytes: &[u8]| {
        let escaped: String = bytes.iter().map(|b| format!("\\{b:02x}")).collect();
        data.push_str(&format!("(data (i32.const {offset}) \"{escaped}\")"));
        let placed = (offset, bytes.len());
        offset += bytes.len() + 1;
        placed
    };
    let response = |version: u8, outcome: Value| {
        json!({"version": version, "outcome": outcome})
            .to_string()
            .into_bytes()
    };
    let error = |version: u8, code: &str| {
        response(
            version,
            json!({"status":"err","error":{"code":code,"classification":"permanent"}}),
        )
    };
    let prefix = format!(r#"{{"version":{version},"body":{{"op":""#);
    let (prefix_at, prefix_len) = place(prefix.as_bytes());
    let (needle_at, needle_len) = place(b"principal");
    let (default_at, default_len) = place(&error(1, "protocol_version"));
    let (seen_at, seen_len) = place(&error(version, "principal_seen"));
    let (missing_at, missing_len) = place(&error(version, "principal_missing"));
    let mut replies = vec![
        (
            "validate",
            json!({"kind":"validated","payload":{"amount":1}}),
        ),
        (
            "decide",
            json!({"kind":"decided","events":[{"tag":"Added","schema_version":1,"payload":{"amount":1}}]}),
        ),
        ("project", json!({"kind":"projected","state":1})),
        ("query", json!({"kind":"queried","value":1})),
    ];
    if version == 2 {
        replies.push(("describe", json!({"kind":"described","protocol":2})));
    }
    let contains = format!(
        "(call $contains (local.get $p) (local.get $n) (i32.const {needle_at}) (i32.const {needle_len}))"
    );
    let mut branches = String::new();
    for (op, reply) in replies {
        let (at, len) = place(&response(version, json!({"status":"ok","reply":reply})));
        let set = format!("(call $set (i32.const {at}) (i32.const {len}))");
        let body = match (rule, op) {
            (Rule::Require, "decide" | "query") => format!(
                "(if {contains} (then {set}) (else (call $set (i32.const {missing_at}) (i32.const {missing_len}))))"
            ),
            _ => set,
        };
        // The third letter tells the operations apart.
        branches.push_str(&format!(
            "(if (i32.eq (i32.load8_u (i32.add (local.get $p) (i32.const {}))) (i32.const {})) (then {body}))",
            prefix_len + 2,
            op.as_bytes()[2]
        ));
    }
    let forbid = match rule {
        Rule::Forbid => format!(
            "(if {contains} (then (call $set (i32.const {seen_at}) (i32.const {seen_len}))))"
        ),
        Rule::Require => String::new(),
    };
    assert!(offset < 8192, "the fixture data overlaps the input");
    let wat = format!(
        r#"(component (core module $m
      (memory (export "memory") 2) {data}
      (func (export "realloc") (param i32 i32 i32 i32) (result i32) i32.const 8192)
      (func $set (param i32 i32) i32.const 4 local.get 0 i32.store i32.const 8 local.get 1 i32.store)
      (func $eq (param $a i32) (param $b i32) (param $m i32) (result i32) (local $j i32)
        (block $no (loop $l
          (if (i32.ge_u (local.get $j) (local.get $m)) (then (return (i32.const 1))))
          (br_if $no (i32.ne (i32.load8_u (i32.add (local.get $a) (local.get $j)))
                             (i32.load8_u (i32.add (local.get $b) (local.get $j)))))
          (local.set $j (i32.add (local.get $j) (i32.const 1)))
          (br $l)))
        (i32.const 0))
      (func $contains (param $p i32) (param $n i32) (param $q i32) (param $m i32) (result i32) (local $i i32)
        (block $no (loop $l
          (br_if $no (i32.gt_u (i32.add (local.get $i) (local.get $m)) (local.get $n)))
          (if (call $eq (i32.add (local.get $p) (local.get $i)) (local.get $q) (local.get $m))
            (then (return (i32.const 1))))
          (local.set $i (i32.add (local.get $i) (i32.const 1)))
          (br $l)))
        (i32.const 0))
      (func (export "handle") (param $p i32) (param $n i32) (result i32)
        i32.const 0 i32.const 0 i32.store
        (call $set (i32.const {default_at}) (i32.const {default_len}))
        (if (i32.ge_u (local.get $n) (i32.const {prefix_len}))
          (then (if (call $eq (local.get $p) (i32.const {prefix_at}) (i32.const {prefix_len}))
            (then {branches}))))
        {forbid}
        i32.const 0))
      (core instance $i (instantiate $m))
      (type $h (func (param "input" string) (result (result string (error string)))))
      (func $handle (type $h) (canon lift (core func $i "handle") (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
      (instance $handler (export "handle" (func $handle)))
      (export "merezha:service/handler@0.1.0" (instance $handler)))"#
    );
    wat::parse_str(wat).unwrap()
}

/// The components of these tests: `owner.wasm` speaks version 2 and needs
/// the principal; `legacy.wasm` speaks only version 1 and refuses any
/// request naming a principal.
fn components(dir: &Path) {
    std::fs::write(dir.join("owner.wasm"), component(2, Rule::Require)).unwrap();
    std::fs::write(dir.join("legacy.wasm"), component(1, Rule::Forbid)).unwrap();
}
/// A manifest with services `owner` and `legacy`: `(path, protocol)` each.
fn manifest(dir: &Path, owner: (&str, u16), legacy: (&str, u16)) -> std::path::PathBuf {
    let path = dir.join("runtime.toml");
    let service = |id: &str, (file, protocol): (&str, u16)| {
        format!("[[services]]\nid=\"{id}\"\npath=\"{file}\"\napplication_protocol={protocol}\n")
    };
    std::fs::write(
        &path,
        format!(
            "version=1\n[limits]\nmemory_bytes=16777216\nfuel=10000000\nmax_input_bytes=65536\n\
             max_output_bytes=65536\ntimeout_ms=2000\nmax_concurrent_invocations=8\n{}{}",
            service("owner", owner),
            service("legacy", legacy)
        ),
    )
    .unwrap();
    path
}
const ROUTES: &str = r#"
[[commands]]
tag="Add"
service="owner"
aggregate="Counter"
[[commands]]
tag="Legacy"
service="legacy"
aggregate="Counter"
[[queries]]
tag="Total"
service="legacy"
projection="total"
[[queries]]
tag="Who"
service="owner"
projection="total"
[[projections]]
name="total"
service="legacy"
initial=0
"#;
fn config() -> Config {
    Config::parse(&format!("version=1\n{ROUTES}")).unwrap()
}
fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn principal() -> Principal {
    Principal::new(
        id("usr_ada"),
        PrincipalKind::User,
        Some(id("org_1")),
        vec![id("member"), id("admin")],
    )
    .unwrap()
}
fn command(tag: &str, actor: Option<&str>) -> Command {
    serde_json::from_value(json!({
        "tag": tag, "stream": "one", "payload": {"amount": 1},
        "metadata": {"message_id": "m1", "correlation_id": "c1", "actor": actor}
    }))
    .unwrap()
}
fn query(tag: &str) -> Query {
    serde_json::from_value(
        json!({"tag": tag, "payload": {}, "metadata": {"message_id": "m2", "correlation_id": "c1"}}),
    )
    .unwrap()
}
fn code<T>(result: Result<T, Failure>) -> String {
    result.err().expect("expected a failure").code
}

/// C1 tests 2 and 3: the version 2 guest sees the principal in decide and
/// query; the version 1 guest's bytes never name it, in its own decide and
/// query metadata or in history and projection events written by version 2
/// commands. Stored events keep `{subject, kind, tenant}` without roles.
#[test]
fn version_1_guests_never_see_the_principal_version_2_guests_do() {
    let dir = tempfile::tempdir().unwrap();
    components(dir.path());
    let runtime =
        Runtime::load(manifest(dir.path(), ("owner.wasm", 2), ("legacy.wasm", 1))).unwrap();
    let store = Arc::new(MemoryEventStore::new(config().limits.clone()).unwrap());
    let app = Application::new(
        config(),
        Arc::new(runtime),
        Arc::new(AllowAll),
        store.clone(),
    )
    .unwrap();

    // The version 2 guest refuses a call without a principal.
    assert_eq!(code(app.command(command("Add", None))), "principal_missing");
    let receipt = app.command_as(principal(), command("Add", None)).unwrap();
    assert_eq!(receipt.version, 1);
    // The version 1 guest decides over that history, as the same principal.
    assert_eq!(
        app.command_as(principal(), command("Legacy", None))
            .unwrap()
            .version,
        2
    );
    assert_eq!(app.catch_up("total").unwrap().checkpoint, 2);
    assert_eq!(
        app.query_as(principal(), query("Total")).unwrap().value,
        json!(1)
    );
    assert_eq!(
        app.query_as(principal(), query("Who")).unwrap().value,
        json!(1)
    );
    assert_eq!(code(app.query(query("Who"))), "principal_missing");
    assert_eq!(app.query(query("Total")).unwrap().value, json!(1));

    let events = store.read_all(0, 16).unwrap();
    for event in &events {
        assert_eq!(event.metadata.actor, Some(id("usr_ada")));
        assert_eq!(
            serde_json::to_value(&event.metadata).unwrap()["principal"],
            json!({"subject":"usr_ada","kind":"user","tenant":"org_1"}),
            "stored metadata drops the roles"
        );
    }
    // A rebuild replays the principal-bearing history into version 1 again.
    assert_eq!(app.rebuild("total").unwrap().checkpoint, 2);
}

/// The trusted entry points: a principal in the caller's body is never
/// trusted, and a caller-supplied actor must be the principal's subject.
#[test]
fn principals_come_only_from_the_host() {
    let dir = tempfile::tempdir().unwrap();
    components(dir.path());
    let runtime =
        Runtime::load(manifest(dir.path(), ("owner.wasm", 2), ("legacy.wasm", 1))).unwrap();
    let app = Application::in_memory(config(), Arc::new(runtime), Arc::new(AllowAll)).unwrap();
    let injected: Command = serde_json::from_value(json!({
        "tag": "Add", "stream": "one", "payload": {"amount": 1},
        "metadata": {"message_id": "m1", "correlation_id": "c1",
                     "principal": {"subject": "usr_root", "kind": "user", "roles": ["admin"]}}
    }))
    .unwrap();
    assert_eq!(code(app.command(injected.clone())), "principal_not_trusted");
    assert_eq!(
        code(app.command_as(principal(), injected)),
        "principal_not_trusted"
    );
    let mut injected_query = query("Who");
    injected_query.metadata.principal = Some(principal());
    assert_eq!(code(app.query(injected_query)), "principal_not_trusted");
    assert_eq!(
        code(app.command_as(principal(), command("Add", Some("usr_bob")))),
        "actor_mismatch"
    );
    assert_eq!(
        app.command_as(principal(), command("Add", Some("usr_ada")))
            .unwrap()
            .version,
        1
    );
}

/// C1 test 4: a service declared as version 2 whose guest only speaks
/// version 1 fails startup with `service_protocol_mismatch`; so does a
/// guest answering `describe` with another protocol.
#[test]
fn a_mismatched_declaration_fails_startup() {
    let dir = tempfile::tempdir().unwrap();
    components(dir.path());
    let runtime =
        Runtime::load(manifest(dir.path(), ("legacy.wasm", 2), ("legacy.wasm", 1))).unwrap();
    assert_eq!(
        code(Application::in_memory(
            config(),
            Arc::new(runtime),
            Arc::new(AllowAll)
        )),
        "service_protocol_mismatch"
    );
    // A version 2 guest declared as version 1 is simply spoken to in
    // version 1 (and so never receives a principal).
    let runtime =
        Runtime::load(manifest(dir.path(), ("owner.wasm", 1), ("legacy.wasm", 1))).unwrap();
    let app = Application::in_memory(config(), Arc::new(runtime), Arc::new(AllowAll)).unwrap();
    assert_eq!(
        code(app.command_as(principal(), command("Add", None))),
        "protocol_version",
        "the version 2 fixture answers only version 2 requests"
    );
    // An undeclared version is an invalid manifest.
    let path = manifest(dir.path(), ("owner.wasm", 3), ("legacy.wasm", 1));
    assert!(Runtime::load(path).is_err());
}

/// C1 test 4: after a reload to a guest that does not speak its declared
/// version 2, dispatch to that service fails `service_protocol_mismatch`
/// (permanent) and the history is untouched; reloading back recovers.
#[test]
fn a_reload_to_a_mismatched_guest_fails_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    components(dir.path());
    let path = manifest(dir.path(), ("owner.wasm", 2), ("legacy.wasm", 1));
    let runtime = Runtime::load(&path).unwrap();
    let store = Arc::new(MemoryEventStore::new(config().limits.clone()).unwrap());
    let app = Application::new(
        config(),
        Arc::new(runtime.clone()),
        Arc::new(AllowAll),
        store.clone(),
    )
    .unwrap();
    app.command_as(principal(), command("Add", None)).unwrap();
    manifest(dir.path(), ("legacy.wasm", 2), ("legacy.wasm", 1));
    assert_eq!(runtime.reload().unwrap(), 2);
    let failure = app
        .command_as(principal(), command("Add", None))
        .unwrap_err();
    assert_eq!(failure, Failure::permanent("service_protocol_mismatch"));
    assert_eq!(
        code(app.query_as(principal(), query("Who"))),
        "service_protocol_mismatch"
    );
    // Version 1 services of the same generation keep working.
    assert_eq!(app.query(query("Total")).unwrap().value, json!(1));
    assert_eq!(store.head().unwrap(), 1, "history untouched");
    manifest(dir.path(), ("owner.wasm", 2), ("legacy.wasm", 1));
    assert_eq!(runtime.reload().unwrap(), 3);
    assert_eq!(
        app.command_as(principal(), command("Add", None))
            .unwrap()
            .version,
        2
    );
}

/// `describe` is a guest call of its own, `guest {service} describe`, made
/// once per generation and service.
#[test]
fn describe_runs_once_per_generation_in_its_own_span() {
    use merezha_telemetry::testing::Harness;
    let harness = Harness::new();
    let dir = tempfile::tempdir().unwrap();
    components(dir.path());
    let path = manifest(dir.path(), ("owner.wasm", 2), ("legacy.wasm", 1));
    let runtime = Runtime::load(&path).unwrap();
    let app = Application::in_memory(config(), Arc::new(runtime.clone()), Arc::new(AllowAll))
        .unwrap()
        .with_meters(&harness.telemetry().meters());
    let describes = || {
        harness
            .spans()
            .iter()
            .filter(|s| s.name == "guest owner describe")
            .count()
    };
    assert_eq!(describes(), 1, "the startup check");
    app.command_as(principal(), command("Add", None)).unwrap();
    app.query_as(principal(), query("Who")).unwrap();
    assert_eq!(describes(), 1, "cached for the generation");
    runtime.reload().unwrap();
    app.command_as(principal(), command("Add", None)).unwrap();
    app.command_as(principal(), command("Add", None)).unwrap();
    assert_eq!(describes(), 2, "once more after the reload");
    harness.assert_catalogued();
}

/// The principal round-trips through PostgreSQL's stored metadata: a
/// version 1 guest still receives none of it from loaded history, and the
/// stored JSON keeps `{subject, kind, tenant}` only.
#[test]
fn postgres_history_keeps_the_principal_without_roles_and_downgrades_it() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let deadline = || Instant::now() + Duration::from_secs(10);
    let tokio = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let admin = Database::connect(&test.config()).unwrap();
    tokio.block_on(async {
        let modules = [ModuleMigrations {
            module: MODULE,
            migrations: MIGRATIONS,
        }];
        run_migrations(&admin, &modules, MigrationMode::Apply, deadline())
            .await
            .unwrap();
    });
    let config = Config::parse(&format!(
        "version=1\n[storage]\nbackend=\"postgres\"\napplication=\"protocol-test\"\n{ROUTES}"
    ))
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    components(dir.path());
    let runtime =
        Runtime::load(manifest(dir.path(), ("owner.wasm", 2), ("legacy.wasm", 1))).unwrap();
    let mut pool = test.config();
    pool.pool_size = 4;
    let db = Database::connect(&pool).unwrap();
    let store = tokio
        .block_on(PostgresStore::connect(
            &db,
            &config,
            PostgresOptions::default(),
            deadline(),
        ))
        .unwrap();
    let app = Application::with_postgres(
        config,
        Arc::new(runtime),
        Arc::new(AllowAll),
        Arc::new(store),
    )
    .unwrap();
    app.command_as(principal(), command("Add", None)).unwrap();
    assert_eq!(
        app.command_as(principal(), command("Legacy", None))
            .unwrap()
            .position,
        2
    );
    assert_eq!(app.catch_up("total").unwrap().checkpoint, 2);
    let stored: Vec<String> = tokio.block_on(async {
        let client = admin.acquire(deadline()).await.unwrap();
        client
            .query(
                "SELECT metadata::text FROM merezha_app.events ORDER BY position",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect()
    });
    assert_eq!(stored.len(), 2);
    for metadata in stored {
        assert_eq!(
            metadata,
            r#"{"message_id":"m1","correlation_id":"c1","causation_id":null,"actor":"usr_ada","principal":{"subject":"usr_ada","kind":"user","tenant":"org_1"}}"#
        );
    }
}
