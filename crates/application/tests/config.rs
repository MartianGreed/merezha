//! Application configuration versions 1 and 2 (AP §A.6, §A.9, §C.4, §C.5).
use merezha_application::config::{Access, Config, Limits, Partition, RouteScope};

const ROUTES: &str = r#"
[[commands]]
tag = "CreateTask"
service = "task-domain"
aggregate = "Task"
[[queries]]
tag = "ListTasks"
service = "task-view"
projection = "tasks"
[[projections]]
name = "tasks"
service = "task-view"
initial = { tasks = {} }
"#;

fn code(text: &str) -> String {
    Config::parse(text).unwrap_err().code
}

#[test]
fn version_2_sets_max_stream_events_and_limits_record_which_keys_were_written() {
    let config = Config::parse(&format!(
        "version = 2\n[limits]\nmax_stream_events = 8\nbatch_size = 4\n{ROUTES}"
    ))
    .unwrap();
    assert_eq!(config.limits.max_stream_events, 8);
    assert_eq!(config.limits.batch_size, 4);
    assert_eq!(
        config.limits.present.iter().collect::<Vec<_>>(),
        ["batch_size", "max_stream_events"]
    );
    assert!(!config.limits.present.contains("max_events"));
    assert_eq!(config.limits.capacity_keys().count(), 0);
    assert!(config.limits.reject_capacity_keys().is_ok());
    // Without a [limits] table nothing is present and defaults apply.
    let config = Config::parse(&format!("version = 2\n{ROUTES}")).unwrap();
    assert_eq!(config.limits.max_stream_events, 4096);
    assert_eq!(config.limits.present.iter().count(), 0);
    assert_eq!(Limits::default().max_stream_events, 4096);
}

#[test]
fn written_capacity_keys_are_known_so_postgresql_can_refuse_them() {
    for version in [1, 2] {
        let config = Config::parse(&format!(
            "version = {version}\n[limits]\nmax_events = 10\nmax_idempotency = 5\ntimeout_ms = 100\n{ROUTES}"
        ))
        .unwrap();
        assert_eq!(
            config.limits.capacity_keys().collect::<Vec<_>>(),
            ["max_events", "max_idempotency"]
        );
        assert_eq!(
            config.limits.reject_capacity_keys().unwrap_err().code,
            "invalid_limits"
        );
    }
    for key in ["max_streams", "max_store_bytes"] {
        let config = Config::parse(&format!("version = 1\n[limits]\n{key} = 1\n{ROUTES}")).unwrap();
        assert_eq!(config.limits.capacity_keys().collect::<Vec<_>>(), [key]);
    }
    // A value set in code is not a written key.
    let limits = Limits {
        max_events: 1,
        ..Limits::default()
    };
    assert!(limits.reject_capacity_keys().is_ok());
}

#[test]
fn max_stream_events_is_bounded() {
    for value in [0, 4097] {
        assert_eq!(
            code(&format!(
                "version = 2\n[limits]\nmax_stream_events = {value}\n{ROUTES}"
            )),
            "invalid_limits"
        );
    }
    assert!(
        Config::parse(&format!(
            "version = 2\n[limits]\nmax_stream_events = 4096\n{ROUTES}"
        ))
        .is_ok()
    );
}

/// Each version 2 document below sets one version 2 key, with what this
/// build answers: `ok`, the access issue, or `unsupported_in_this_build`
/// for reactors (AP-C3).
fn version_2_documents() -> Vec<(&'static str, String, &'static str)> {
    let command = |extra: &str| {
        format!(
            "[[commands]]\ntag = \"CreateTask\"\nservice = \"task-domain\"\naggregate = \"Task\"\n{extra}\n[[queries]]\ntag = \"ListTasks\"\nservice = \"task-view\"\nprojection = \"tasks\"\n[[projections]]\nname = \"tasks\"\nservice = \"task-view\"\ninitial = {{}}\n"
        )
    };
    let query = |extra: &str| {
        format!(
            "[[queries]]\ntag = \"ListTasks\"\nservice = \"task-view\"\nprojection = \"tasks\"\n{extra}\n[[projections]]\nname = \"tasks\"\nservice = \"task-view\"\ninitial = {{}}\n"
        )
    };
    let projection = |extra: &str| {
        format!(
            "[[projections]]\nname = \"tasks\"\nservice = \"task-view\"\ninitial = {{}}\n{extra}\n"
        )
    };
    vec![
        (
            "permissions",
            format!(
                "{ROUTES}[[permissions]]\nname = \"tasks.create\"\ndescription = \"Create tasks\"\n"
            ),
            "ok",
        ),
        (
            "command permission",
            command("permission = \"tasks.create\""),
            "ok",
        ),
        ("command access", command("access = \"public\""), "ok"),
        // Tenant routes beside the undeclared `ListTasks`, which would then
        // read every tenant's data.
        (
            "command scope",
            command("scope = \"tenant\""),
            "cross_tenant_query",
        ),
        (
            "command scope application",
            command("scope = \"application\""),
            "ok",
        ),
        (
            "command resource",
            command("resource = \"task\""),
            "resource_requires_tenant",
        ),
        (
            "command resource in a tenant",
            command("resource = \"task\"\nscope = \"tenant\""),
            "cross_tenant_query",
        ),
        (
            "query permission",
            query("permission = \"tasks.read\""),
            "ok",
        ),
        ("query access", query("access = \"authenticated\""), "ok"),
        (
            "query scope",
            query("scope = \"tenant\""),
            "tenant_query_unpartitioned",
        ),
        (
            "projection partition",
            projection("partition = \"tenant\""),
            "partitions_require_postgres",
        ),
        (
            "projection partition none",
            projection("partition = \"none\""),
            "ok",
        ),
        (
            "reactors",
            format!(
                "{ROUTES}[[reactors]]\nname = \"task-notifier\"\nservice = \"task-notifier\"\n"
            ),
            "unsupported_in_this_build",
        ),
        // Writing the key at all is version 2, even as an empty list.
        (
            "empty permissions",
            format!("permissions = []\n{ROUTES}"),
            "ok",
        ),
        (
            "empty reactors",
            format!("reactors = []\n{ROUTES}"),
            "unsupported_in_this_build",
        ),
    ]
}

#[test]
fn version_2_access_and_tenancy_keys_are_accepted_and_reactors_refused_as_unsupported() {
    for (name, body, expected) in version_2_documents() {
        let parsed = Config::parse(&format!("version = 2\n{body}"));
        match expected {
            "ok" => assert!(parsed.is_ok(), "{name}: {parsed:?}"),
            code => assert_eq!(parsed.unwrap_err().code, code, "{name}"),
        }
    }
}

#[test]
fn version_1_documents_cannot_use_version_2_keys() {
    let mut documents: Vec<_> = version_2_documents()
        .into_iter()
        .map(|(name, body, _)| (name, body))
        .collect();
    documents.push((
        "max_stream_events",
        format!("[limits]\nmax_stream_events = 8\n{ROUTES}"),
    ));
    for (name, body) in documents {
        assert_eq!(
            code(&format!("version = 1\n{body}")),
            "invalid_config",
            "{name}"
        );
    }
}

#[test]
fn the_designed_version_2_examples_parse_completely_before_the_refusal() {
    // AP §C.4 and §C.5, verbatim apart from the shared service names and
    // the PostgreSQL storage partitions need.
    let text = r#"version = 2

[storage]
backend = "postgres"
application = "tasks"

[[permissions]]
name = "tasks.create"
description = "Create tasks"

[[permissions]]
name = "tasks.read"
description = "Read tasks"

[[commands]]
tag = "CreateTask"
service = "task-domain"
aggregate = "Task"
permission = "tasks.create"
scope = "tenant"
resource = "task"

[[queries]]
tag = "ListTasks"
service = "task-view"
projection = "tasks"
permission = "tasks.read"
scope = "tenant"

[[projections]]
name = "tasks"
service = "task-view"
initial = { tasks = {} }
revision = 2
partition = "tenant"

[[reactors]]
name = "task-notifier"
service = "task-notifier"
events = ["TaskCompleted"]
effects = ["notify", "command"]
commands = ["ArchiveTask"]
permissions = ["tasks.archive"]
allow_addresses = false
start = "head"
on_error = "halt"
"#;
    assert_eq!(code(text), "unsupported_in_this_build");
    // Without the reactor (AP-C3) the §C.4 example is accepted as written.
    let routes = Config::parse(text.split("[[reactors]]").next().unwrap()).unwrap();
    assert!(routes.route_issues().is_empty());
    assert_eq!(
        routes.resource_kinds().into_iter().collect::<Vec<_>>(),
        ["task"]
    );
    // The syntax itself decodes into the typed fields later slices enforce.
    let config: Config = toml::from_str(text).unwrap();
    assert_eq!(config.permissions.as_ref().unwrap().len(), 2);
    assert_eq!(config.commands[0].scope, Some(RouteScope::Tenant));
    assert_eq!(
        config.commands[0].resource.as_ref().unwrap().as_str(),
        "task"
    );
    assert_eq!(config.queries[0].access, None);
    assert_eq!(config.projections[0].partition, Some(Partition::Tenant));
    assert_eq!(
        config.reactors.as_ref().unwrap()[0].commands[0].as_str(),
        "ArchiveTask"
    );
    let public: Config = toml::from_str(
        "version = 2\n[[commands]]\ntag = \"A\"\nservice = \"s\"\naggregate = \"X\"\naccess = \"public\"\n",
    )
    .unwrap();
    assert_eq!(public.commands[0].access, Some(Access::Public));
}

#[test]
fn malformed_version_2_keys_and_unknown_versions_are_invalid_config() {
    for body in [
        // `resource` is for commands only (R6).
        "[[queries]]\ntag = \"Q\"\nservice = \"s\"\nprojection = \"p\"\nresource = \"task\"\n[[projections]]\nname = \"p\"\nservice = \"s\"\ninitial = 0\n",
        "[[commands]]\ntag = \"C\"\nservice = \"s\"\naggregate = \"X\"\nscope = \"global\"\n",
        "[[commands]]\ntag = \"C\"\nservice = \"s\"\naggregate = \"X\"\naccess = \"anyone\"\n",
        "[[projections]]\nname = \"p\"\nservice = \"s\"\ninitial = 0\npartition = \"user\"\n",
        "[[reactors]]\nname = \"r\"\nservice = \"s\"\neffects = [\"email\"]\n",
        "[[reactors]]\nname = \"r\"\nservice = \"s\"\ncolour = \"blue\"\n",
        "[[permissions]]\nname = \"p\"\nlabel = \"x\"\n",
    ] {
        assert_eq!(
            code(&format!("version = 2\n{body}")),
            "invalid_config",
            "{body}"
        );
    }
    for version in [0, 3] {
        assert_eq!(
            code(&format!("version = {version}\n{ROUTES}")),
            "invalid_config"
        );
    }
}

#[test]
fn version_1_configuration_encodes_without_any_version_2_key() {
    let config = Config::parse(&format!("version = 1\n{ROUTES}")).unwrap();
    let encoded = serde_json::to_value(&config).unwrap();
    for key in ["permissions", "reactors"] {
        assert!(encoded.get(key).is_none(), "{key}");
    }
    assert!(encoded["limits"].get("max_stream_events").is_none());
    assert!(encoded["limits"].get("present").is_none());
    for route in ["permission", "access", "scope", "resource"] {
        assert!(encoded["commands"][0].get(route).is_none(), "{route}");
    }
    assert!(encoded["projections"][0].get("partition").is_none());
}

/// The design leaves these bounds open (PR #20, deviation 7): at most 256
/// permissions with descriptions of at most 256 bytes, and 32 reactors. At
/// the bound the document passes validation (reactors then reach their
/// refusal); one over fails `invalid_config`.
#[test]
fn permission_and_reactor_counts_and_description_length_are_bounded() {
    let permissions = |count: usize, description: usize| {
        let mut text = format!("version = 2\n{ROUTES}");
        for i in 0..count {
            text.push_str(&format!(
                "[[permissions]]\nname = \"tasks.p{i}\"\ndescription = \"{}\"\n",
                "d".repeat(description)
            ));
        }
        text
    };
    let reactors = |count: usize| {
        let mut text = format!("version = 2\n{ROUTES}");
        for i in 0..count {
            text.push_str(&format!(
                "[[reactors]]\nname = \"r{i}\"\nservice = \"task-view\"\n"
            ));
        }
        text
    };
    for (name, at, over) in [
        ("permission count", permissions(256, 1), permissions(257, 1)),
        (
            "description length",
            permissions(1, 256),
            permissions(1, 257),
        ),
    ] {
        assert!(Config::parse(&at).is_ok(), "{name} at the bound");
        assert_eq!(code(&over), "invalid_config", "{name} over the bound");
    }
    assert_eq!(code(&reactors(32)), "unsupported_in_this_build");
    assert_eq!(code(&reactors(33)), "invalid_config");
}

/// AP §C.4: every problem of the access declarations is listed at once,
/// in document order, keyed by entry, with what only the host knows (the
/// catalog, whether identity is on) added by `host_issues`.
#[test]
fn access_declarations_list_every_issue_at_once() {
    let text = r#"version = 2
[[permissions]]
name = "tasks.create"
[[permissions]]
name = "Tasks"
[[permissions]]
name = "content.entries.read"
[[permissions]]
name = "tasks.create"
[[commands]]
tag = "Both"
service = "s"
aggregate = "Task"
permission = "tasks.create"
access = "public"
[[commands]]
tag = "Resource"
service = "s"
aggregate = "Task"
permission = "tasks.create"
resource = "task"
[[commands]]
tag = "Colon"
service = "s"
aggregate = "a:b"
access = "public"
scope = "tenant"
[[commands]]
tag = "Unknown"
service = "s"
aggregate = "Task"
permission = "tasks.delete"
[[commands]]
tag = "BlockPermission"
service = "s"
aggregate = "Task"
permission = "content.entries.read"
[[commands]]
tag = "Bare"
service = "s"
aggregate = "Task"
[[queries]]
tag = "Mine"
service = "s"
projection = "all"
permission = "tasks.read"
scope = "tenant"
[[queries]]
tag = "All"
service = "s"
projection = "split"
access = "public"
[[projections]]
name = "all"
service = "s"
initial = 0
[[projections]]
name = "split"
service = "s"
initial = 0
partition = "tenant"
"#;
    let config: Config = toml::from_str(text).unwrap();
    let issues = |list: Vec<merezha_application::config::RouteIssue>| {
        list.into_iter()
            .map(|i| format!("{} {}", i.key, i.code))
            .collect::<Vec<_>>()
    };
    let own = [
        "permissions.Tasks invalid_permission",
        "permissions.content.entries.read block_prefix",
        "permissions.tasks.create duplicate_permission",
        "commands.Both route_access_conflict",
        "commands.Resource resource_requires_tenant",
        "commands.Colon tenant_aggregate",
        "queries.Mine tenant_query_unpartitioned",
        "queries.All partitioned_query_unscoped",
        "projections.split partitions_require_postgres",
    ];
    assert_eq!(issues(config.route_issues()), own);
    // `validate` (and so `parse`) fails with the first.
    assert_eq!(config.validate().unwrap_err().code, "invalid_permission");
    // The host adds catalog and identity issues: `tasks.read` and
    // `content.entries.read` are in this catalog, `tasks.delete` is not.
    let catalog = |name: &str| matches!(name, "content.entries.read" | "tasks.read");
    let mut with_identity = own.to_vec();
    with_identity.extend([
        "commands.Unknown unknown_permission",
        "commands.Bare route_access_undeclared",
    ]);
    assert_eq!(
        issues(config.host_issues(&catalog, &|_| false, true)),
        with_identity
    );
    let mut without = own.to_vec();
    without.push("commands.Unknown unknown_permission");
    assert_eq!(
        issues(config.host_issues(&catalog, &|_| false, false)),
        without
    );
    // A clean document has none, and declared permissions need no catalog.
    let clean = Config::parse(
        "version = 2\n[[permissions]]\nname = \"tasks.create\"\n\
         [[commands]]\ntag = \"C\"\nservice = \"s\"\naggregate = \"Task\"\npermission = \"tasks.create\"\n",
    )
    .unwrap();
    assert!(clean.host_issues(&|_| false, &|_| false, true).is_empty());
}

/// The PR #58 review's PoC: with tenant routes, an anonymous `List` query on
/// an unpartitioned projection folds every tenant's events (org B's
/// `ssn` included) and answers them to anyone. Such open application
/// queries fail startup (`cross_tenant_query`) unless they opt in with
/// `cross_tenant = true`; so does one checking a permission every user
/// holds (`builtin_grants.user`), which the host passes as `everyone`.
#[test]
fn open_application_queries_over_tenant_data_must_opt_in() {
    let document = |query: &str| {
        format!(
            r#"version = 2
[storage]
backend = "postgres"
application = "leak"
[[permissions]]
name = "people.read"
[[commands]]
tag = "AddPerson"
service = "s"
aggregate = "Person"
access = "authenticated"
scope = "tenant"
[[queries]]
tag = "List"
service = "s"
projection = "people"
{query}
[[projections]]
name = "people"
service = "s"
initial = {{}}
"#
        )
    };
    for open in ["access = \"public\"", "access = \"authenticated\"", ""] {
        assert_eq!(code(&document(open)), "cross_tenant_query", "{open:?}");
        let opted = Config::parse(&document(&format!("{open}\ncross_tenant = true"))).unwrap();
        assert!(opted.route_issues().is_empty(), "{open:?}");
        assert!(opted.queries[0].cross_tenant);
    }
    // A permission every user holds is as open as `authenticated`; one
    // only some roles hold is the operator's cross-tenant grant.
    let granted = Config::parse(&document("permission = \"people.read\"")).unwrap();
    assert!(granted.route_issues().is_empty());
    let everyone = |name: &str| name == "people.read";
    assert_eq!(
        granted
            .host_issues(&|_| true, &everyone, true)
            .iter()
            .map(|i| (i.key.as_str(), i.code))
            .collect::<Vec<_>>(),
        [("queries.List", "cross_tenant_query")]
    );
    assert!(granted.host_issues(&|_| true, &|_| false, true).is_empty());
    let opted = Config::parse(&document(
        "permission = \"people.read\"\ncross_tenant = true",
    ))
    .unwrap();
    assert!(opted.host_issues(&|_| true, &everyone, true).is_empty());
    // Without tenant routes there is no other tenant to leak.
    let single = document("access = \"public\"").replace("scope = \"tenant\"\n", "");
    assert!(Config::parse(&single).is_ok());
    // The opt-in means nothing on a tenant query.
    let scoped = document("access = \"public\"\nscope = \"tenant\"\ncross_tenant = true")
        .replace("initial = {}", "initial = {}\npartition = \"tenant\"");
    assert_eq!(code(&scoped), "cross_tenant_scoped");
    // Version 1 cannot use it.
    let v1 = String::from(
        "version = 1\n[[queries]]\ntag = \"Q\"\nservice = \"s\"\nprojection = \"p\"\ncross_tenant = true\n\
         [[projections]]\nname = \"p\"\nservice = \"s\"\ninitial = 0\n",
    );
    assert_eq!(code(&v1), "invalid_config");
}

/// A route's `resource` cannot name a block's resource kind, as
/// `block_prefix` keeps block permission names out of `[[permissions]]`.
#[test]
fn route_resources_cannot_name_a_block_kind() {
    let config = Config::parse(
        "version = 2\n[[commands]]\ntag = \"Edit\"\nservice = \"s\"\naggregate = \"Page\"\n\
         access = \"authenticated\"\nscope = \"tenant\"\nresource = \"content_type\"\n\
         [[commands]]\ntag = \"Rename\"\nservice = \"s\"\naggregate = \"Task\"\n\
         access = \"authenticated\"\nscope = \"tenant\"\nresource = \"task\"\n",
    )
    .unwrap();
    let issues: Vec<_> = config
        .block_kind_issues(&["content_type"])
        .into_iter()
        .map(|i| (i.key, i.code))
        .collect();
    assert_eq!(
        issues,
        [("commands.Edit".to_owned(), "block_resource_kind")]
    );
    assert!(config.block_kind_issues(&[]).is_empty());
}
