//! Route permissions and tenant streams (AP §C.4, slice C2), in memory and
//! on SQLite: every declared route against every kind of caller, checked
//! before any guest call, tenant streams composed from the principal, the
//! `actor_mismatch` rule and the anonymous-actor idempotency seam AP-C1
//! recorded.
use merezha_application::{
    AllowAll, Anonymous, Application, Caller, Guest, GuestSession, Scope, config::Config,
};
use merezha_domain::{
    Command, Event, Failure, Identity, Metadata, Operation, Principal, PrincipalKind, Query,
    Receipt, Reply,
};
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}

/// Counts every guest call and records the history length each `decide`
/// receives.
#[derive(Clone, Default)]
struct Counting {
    calls: Arc<AtomicUsize>,
    histories: Arc<Mutex<Vec<(String, usize)>>>,
}
impl Guest for Counting {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["domain".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
impl GuestSession for Counting {
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(match operation {
            Operation::Validate { payload, .. } => Reply::Validated { payload },
            Operation::Decide {
                stream, history, ..
            } => {
                self.histories
                    .lock()
                    .unwrap()
                    .push((stream.as_str().to_owned(), history.len()));
                Reply::Decided {
                    events: vec![Event {
                        tag: id("Done"),
                        schema_version: 1,
                        payload: json!({}),
                    }],
                }
            }
            Operation::Project { state, events, .. } => Reply::Projected {
                state: json!(state.as_u64().unwrap_or(0) + events.len() as u64),
            },
            Operation::Query { state, .. } => Reply::Queried { value: state },
            Operation::Describe {} => unreachable!("a version 1 fake is never described"),
        })
    }
}

const CONFIG: &str = r#"version = 2
[[permissions]]
name = "tasks.create"
[[permissions]]
name = "tasks.read"
[[permissions]]
name = "tasks.rename"

[[commands]]
tag = "Open"
service = "domain"
aggregate = "Task"
access = "public"
[[commands]]
tag = "Login"
service = "domain"
aggregate = "Task"
access = "authenticated"
[[commands]]
tag = "Create"
service = "domain"
aggregate = "Task"
permission = "tasks.create"
[[commands]]
tag = "TenantCreate"
service = "domain"
aggregate = "Task"
permission = "tasks.create"
scope = "tenant"
[[commands]]
tag = "Rename"
service = "domain"
aggregate = "Task"
permission = "tasks.rename"
scope = "tenant"
resource = "task"
[[commands]]
tag = "TenantOpen"
service = "domain"
aggregate = "Task"
access = "public"
scope = "tenant"
[[commands]]
tag = "Legacy"
service = "domain"
aggregate = "Task"
[[queries]]
tag = "Everything"
service = "domain"
projection = "all"
permission = "tasks.read"
[[queries]]
tag = "Anyone"
service = "domain"
projection = "all"
access = "public"
# Every tenant's events: an explicit choice beside tenant routes.
cross_tenant = true
[[projections]]
name = "all"
service = "domain"
initial = 0
"#;

/// A host policy in the platform's shape: a principal (subject, tenant)
/// with grants at scopes; an application grant covers every scope, a
/// tenant grant its tenant and the resources in it, a resource grant that
/// resource only; break-glass allows everything.
#[derive(Clone, Default)]
struct Policy {
    subject: Option<&'static str>,
    tenant: Option<&'static str>,
    grants: Vec<(&'static str, Scope)>,
    break_glass: bool,
    allow_all: bool,
}
impl Policy {
    fn user(tenant: Option<&'static str>) -> Self {
        Self {
            subject: Some("usr_ada"),
            tenant,
            ..Self::default()
        }
    }
    fn grant(mut self, permission: &'static str, scope: Scope) -> Self {
        self.grants.push((permission, scope));
        self
    }
}
impl Caller for Policy {
    fn principal(&self, _: &Scope) -> Option<Principal> {
        self.subject.map(|subject| {
            Principal::new(
                id(subject),
                PrincipalKind::User,
                self.tenant.map(id),
                vec![],
            )
            .unwrap()
        })
    }
    fn tenant(&self) -> Option<Identity> {
        self.tenant.map(id)
    }
    fn authenticated(&self) -> bool {
        self.subject.is_some()
    }
    fn check(&self, permission: &Identity, scope: &Scope) -> Result<(), Failure> {
        if self.allow_all {
            return Ok(());
        }
        if self.subject.is_none() {
            return Err(Failure::permanent("unauthenticated"));
        }
        let tenant_of = |scope: &Scope| match scope {
            Scope::Tenant(t) => Some(t.clone()),
            Scope::Resource { tenant, .. } => tenant.clone(),
            Scope::Application => None,
        };
        let held = self.grants.iter().any(|(name, granted)| {
            *name == permission.as_str()
                && (granted == scope
                    || *granted == Scope::Application
                    || matches!(granted, Scope::Tenant(t) if tenant_of(scope).as_ref() == Some(t)))
        });
        if self.break_glass || held {
            Ok(())
        } else {
            Err(Failure::permanent("unauthorized"))
        }
    }
    fn allows_all(&self) -> bool {
        self.allow_all
    }
    fn undeclared(&self) -> Result<(), Failure> {
        if self.allow_all {
            Ok(())
        } else {
            Err(Failure::permanent("unauthorized"))
        }
    }
}

fn metadata(actor: Option<&str>) -> Metadata {
    Metadata {
        message_id: id("m"),
        correlation_id: id("c"),
        causation_id: None,
        actor: actor.map(id),
        principal: None,
    }
}
fn command(tag: &str, stream: &str, key: Option<&str>, actor: Option<&str>) -> Command {
    Command {
        tag: id(tag),
        stream: id(stream),
        payload: json!({}),
        metadata: metadata(actor),
        idempotency_key: key.map(id),
        expected_version: None,
    }
}
fn query(tag: &str) -> Query {
    Query {
        tag: id(tag),
        payload: json!({}),
        metadata: metadata(None),
        min_position: None,
    }
}
fn memory(guest: &Counting) -> Application {
    Application::in_memory(
        Config::parse(CONFIG).unwrap(),
        Arc::new(guest.clone()),
        Arc::new(AllowAll),
    )
    .unwrap()
}

/// What a route answers a caller: `ok` or the failure code.
fn outcome(result: Result<Receipt, Failure>) -> String {
    result.map_or_else(|f| f.code, |_| "ok".into())
}

/// AP-C2 tests 1, 2 and 7, route-table driven: every declared route
/// against every kind of caller, each refusal before any guest call.
#[test]
fn every_route_admits_exactly_the_callers_its_declaration_names() {
    let org = || Scope::Tenant(id("org_1"));
    let callers: Vec<(&str, Policy)> = vec![
        ("anonymous", Policy::default()),
        (
            "local allow-all",
            Policy {
                allow_all: true,
                ..Policy::default()
            },
        ),
        ("member without grants", Policy::user(Some("org_1"))),
        (
            "application grant, no tenant",
            Policy::user(None)
                .grant("tasks.create", Scope::Application)
                .grant("tasks.read", Scope::Application)
                .grant("tasks.rename", Scope::Application),
        ),
        (
            "tenant grant",
            Policy::user(Some("org_1"))
                .grant("tasks.create", org())
                .grant("tasks.read", org())
                .grant("tasks.rename", org()),
        ),
        (
            "resource grant on t-1",
            Policy::user(Some("org_1")).grant(
                "tasks.rename",
                Scope::Resource {
                    tenant: Some(id("org_1")),
                    kind: id("task"),
                    id: id("t-1"),
                },
            ),
        ),
        (
            "platform_admin",
            Policy {
                break_glass: true,
                ..Policy::user(Some("org_1"))
            },
        ),
    ];
    // (route, stream, expected per caller in the order above).
    let table: [(&str, &str, [&str; 7]); 9] = [
        ("Open", "t-1", ["ok", "ok", "ok", "ok", "ok", "ok", "ok"]),
        (
            "Login",
            "t-1",
            ["unauthenticated", "ok", "ok", "ok", "ok", "ok", "ok"],
        ),
        (
            "Create",
            "t-1",
            [
                "unauthenticated",
                "ok",
                "unauthorized",
                "ok",
                "unauthorized",
                "unauthorized",
                "ok",
            ],
        ),
        (
            "TenantCreate",
            "t-1",
            [
                "unauthenticated",
                "tenant_required",
                "unauthorized",
                "tenant_required",
                "ok",
                "unauthorized",
                "ok",
            ],
        ),
        (
            "Rename",
            "t-1",
            [
                "unauthenticated",
                "tenant_required",
                "unauthorized",
                "tenant_required",
                "ok",
                "ok",
                "ok",
            ],
        ),
        (
            "Rename",
            "t-2",
            [
                "unauthenticated",
                "tenant_required",
                "unauthorized",
                "tenant_required",
                "ok",
                "unauthorized",
                "ok",
            ],
        ),
        (
            "TenantOpen",
            "t-1",
            [
                "tenant_required",
                "tenant_required",
                "ok",
                "tenant_required",
                "ok",
                "ok",
                "ok",
            ],
        ),
        (
            "Legacy",
            "t-1",
            [
                "unauthorized",
                "ok",
                "unauthorized",
                "unauthorized",
                "unauthorized",
                "unauthorized",
                "unauthorized",
            ],
        ),
        ("Missing", "t-1", ["handler_not_found"; 7]),
    ];
    let config = Config::parse(CONFIG).unwrap();
    let declared: BTreeSet<&str> = config.commands.iter().map(|r| r.tag.as_str()).collect();
    let tested: BTreeSet<&str> = table
        .iter()
        .map(|(tag, _, _)| *tag)
        .filter(|tag| *tag != "Missing")
        .collect();
    assert_eq!(declared, tested, "the table covers every declared command");
    for (tag, stream, expected) in table {
        for ((name, caller), expected) in callers.iter().zip(expected) {
            let guest = Counting::default();
            let app = memory(&guest);
            let got = outcome(app.command_as(caller, command(tag, stream, Some("k"), None)));
            assert_eq!(got, expected, "{tag} {stream} as {name}");
            let calls = guest.calls.load(Ordering::SeqCst);
            if expected == "ok" {
                assert_eq!(calls, 2, "{tag} as {name}: validate and decide");
            } else {
                assert_eq!(calls, 0, "{tag} as {name}: refused before any guest call");
            }
        }
    }
    // Queries: the permission route and the public one.
    for (tag, expected) in [
        (
            "Everything",
            [
                "unauthenticated",
                "ok",
                "unauthorized",
                "ok",
                "unauthorized",
                "unauthorized",
                "ok",
            ],
        ),
        ("Anyone", ["ok"; 7]),
    ] {
        for ((name, caller), expected) in callers.iter().zip(expected) {
            let guest = Counting::default();
            let app = memory(&guest);
            let got = app
                .query_as(caller, query(tag))
                .map_or_else(|f| f.code, |_| "ok".into());
            assert_eq!(got, expected, "{tag} as {name}");
            let calls = guest.calls.load(Ordering::SeqCst);
            assert_eq!(
                calls,
                if expected == "ok" { 2 } else { 0 },
                "{tag} as {name}"
            );
        }
    }
    // The bare entry points are anonymous: public and undeclared routes.
    let guest = Counting::default();
    let app = memory(&guest);
    assert!(app.command(command("Open", "t-1", None, None)).is_ok());
    assert!(app.command(command("Legacy", "t-1", None, None)).is_ok());
    assert_eq!(
        app.command(command("Create", "t-1", None, None))
            .unwrap_err()
            .code,
        "unauthenticated"
    );
    assert!(
        app.command_as(Anonymous, command("Open", "t-2", None, None))
            .is_ok()
    );
}

/// AP-C2 test 2: a principal who lost the grant is refused before its own
/// stored receipt is replayed, and before validation.
#[test]
fn a_refused_caller_never_reaches_a_stored_receipt() {
    let guest = Counting::default();
    let app = memory(&guest);
    let granted = Policy::user(None).grant("tasks.create", Scope::Application);
    let first = app
        .command_as(&granted, command("Create", "t-1", Some("k"), None))
        .unwrap();
    assert_eq!(
        app.command_as(&granted, command("Create", "t-1", Some("k"), None))
            .unwrap(),
        first,
        "the holder replays"
    );
    let calls = guest.calls.load(Ordering::SeqCst);
    let revoked = Policy::user(None);
    assert_eq!(
        app.command_as(&revoked, command("Create", "t-1", Some("k"), None))
            .unwrap_err()
            .code,
        "unauthorized"
    );
    assert_eq!(guest.calls.load(Ordering::SeqCst), calls, "no guest call");
}

/// AP-C2 test 7 and R6: a principal acting for another actor gets
/// `actor_mismatch`, before any guest call; its own subject is accepted.
#[test]
fn a_principal_naming_another_actor_is_refused() {
    let guest = Counting::default();
    let app = memory(&guest);
    let ada = Policy::user(None);
    assert_eq!(
        app.command_as(&ada, command("Open", "t-1", None, Some("usr_bob")))
            .unwrap_err()
            .code,
        "actor_mismatch"
    );
    assert_eq!(guest.calls.load(Ordering::SeqCst), 0);
    assert!(
        app.command_as(&ada, command("Open", "t-1", None, Some("usr_ada")))
            .is_ok()
    );
}

/// AP-C2 test 3 on memory: the same local stream id in two tenants is two
/// streams, composed from the principal's tenant, and a tenant cannot
/// address another tenant's stream by naming it.
#[test]
fn tenant_streams_are_composed_from_the_principal() {
    let guest = Counting::default();
    let app = memory(&guest);
    let a = Policy::user(Some("org_a"));
    let b = Policy::user(Some("org_b"));
    let first = app
        .command_as(&a, command("TenantOpen", "t-1", Some("k"), None))
        .unwrap();
    assert_eq!(first.stream.as_str(), "org_a:Task/t-1");
    assert_eq!(first.version, 1);
    // Same stream id and key in tenant B: its own stream, its own receipt.
    let other = app
        .command_as(&b, command("TenantOpen", "t-1", Some("k"), None))
        .unwrap();
    assert_eq!(other.stream.as_str(), "org_b:Task/t-1");
    assert_eq!(other.version, 1);
    assert_ne!(other.position, first.position);
    // Naming tenant A's stream from B nests it under B.
    let named = app
        .command_as(&b, command("TenantOpen", "org_a:Task/t-1", None, None))
        .unwrap();
    assert_eq!(named.stream.as_str(), "org_b:Task/org_a:Task/t-1");
    let again = app
        .command_as(&a, command("TenantOpen", "t-1", None, None))
        .unwrap();
    assert_eq!(again.version, 2, "B never wrote A's stream");
    // Every decision saw its own tenant's history only.
    assert_eq!(
        *guest.histories.lock().unwrap(),
        [
            ("org_a:Task/t-1".to_owned(), 0),
            ("org_b:Task/t-1".to_owned(), 0),
            ("org_b:Task/org_a:Task/t-1".to_owned(), 0),
            ("org_a:Task/t-1".to_owned(), 1),
        ]
    );
    // An application stream of the same id is a third stream.
    let application = app
        .command_as(&a, command("Open", "t-1", None, None))
        .unwrap();
    assert_eq!(application.stream.as_str(), "Task/t-1");
    // A composed stream past 128 bytes fails per request.
    assert_eq!(
        app.command_as(&a, command("TenantOpen", &"x".repeat(118), None, None))
            .unwrap_err()
            .code,
        "validation_failed"
    );
}

/// The seam AP-C1 recorded: an anonymous caller naming a principal's
/// subject as `actor` never shares that principal's idempotency scope, on
/// memory and on SQLite across a reopen.
#[test]
fn an_anonymous_actor_never_shares_a_principals_receipts() {
    let check = |app: &Application| {
        let ada = Policy::user(None);
        let signed = app
            .command_as(&ada, command("Open", "t-1", Some("k"), None))
            .unwrap();
        let forged = app
            .command(command("Open", "t-1", Some("k"), Some("usr_ada")))
            .unwrap();
        assert_ne!(forged, signed, "a new command, not the principal's replay");
        assert_eq!(forged.version, signed.version + 1);
        // Each replays its own.
        assert_eq!(
            app.command(command("Open", "t-1", Some("k"), Some("usr_ada")))
                .unwrap(),
            forged
        );
        assert_eq!(
            app.command_as(&ada, command("Open", "t-1", Some("k"), None))
                .unwrap(),
            signed
        );
        (signed, forged)
    };
    check(&memory(&Counting::default()));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let sqlite = |initialize: bool| {
        let mut config = Config::parse(CONFIG).unwrap();
        config.storage = merezha_application::config::StorageConfig::Sqlite {
            path: path.clone(),
            application: id("access-test"),
            initialize,
        };
        Application::open(config, Arc::new(Counting::default()), Arc::new(AllowAll)).unwrap()
    };
    let (signed, forged) = check(&sqlite(true));
    let reopened = sqlite(false);
    assert_eq!(
        reopened
            .command_as(Policy::user(None), command("Open", "t-1", Some("k"), None))
            .unwrap(),
        signed
    );
    assert_eq!(
        reopened
            .command(command("Open", "t-1", Some("k"), Some("usr_ada")))
            .unwrap(),
        forged
    );
}

/// Tenant streams work on SQLite: its tenant-prefix backstop accepts the
/// composed stream, and a tenant receipt replays after a reopen.
#[test]
fn sqlite_keeps_tenant_streams_and_receipts_apart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let sqlite = |initialize: bool| {
        let mut config = Config::parse(CONFIG).unwrap();
        config.storage = merezha_application::config::StorageConfig::Sqlite {
            path: path.clone(),
            application: id("tenant-test"),
            initialize,
        };
        Application::open(config, Arc::new(Counting::default()), Arc::new(AllowAll)).unwrap()
    };
    let app = sqlite(true);
    let a = app
        .command_as(
            Policy::user(Some("org_a")),
            command("TenantOpen", "t-1", Some("k"), None),
        )
        .unwrap();
    let b = app
        .command_as(
            Policy::user(Some("org_b")),
            command("TenantOpen", "t-1", Some("k"), None),
        )
        .unwrap();
    assert_eq!((a.stream.as_str(), a.version), ("org_a:Task/t-1", 1));
    assert_eq!((b.stream.as_str(), b.version), ("org_b:Task/t-1", 1));
    drop(app);
    let reopened = sqlite(false);
    assert_eq!(
        reopened
            .command_as(
                Policy::user(Some("org_b")),
                command("TenantOpen", "t-1", Some("k"), None)
            )
            .unwrap(),
        b
    );
}

/// Memory and SQLite refuse partitioned projections at construction.
#[test]
fn partitions_require_postgres() {
    let text = format!(
        "{CONFIG}[[projections]]\nname = \"mine\"\nservice = \"domain\"\ninitial = 0\npartition = \"tenant\"\n\
         [[queries]]\ntag = \"Mine\"\nservice = \"domain\"\nprojection = \"mine\"\naccess = \"authenticated\"\nscope = \"tenant\"\n"
    );
    assert_eq!(
        Config::parse(&text).unwrap_err().code,
        "partitions_require_postgres"
    );
}
