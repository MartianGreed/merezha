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

/// Each version 2 document below sets one key this build parses but does
/// not enforce yet.
fn unsupported_documents() -> Vec<(&'static str, String)> {
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
        ),
        (
            "command permission",
            command("permission = \"tasks.create\""),
        ),
        ("command access", command("access = \"public\"")),
        ("command scope", command("scope = \"tenant\"")),
        (
            "command scope application",
            command("scope = \"application\""),
        ),
        ("command resource", command("resource = \"task\"")),
        ("query permission", query("permission = \"tasks.read\"")),
        ("query access", query("access = \"authenticated\"")),
        ("query scope", query("scope = \"tenant\"")),
        ("projection partition", projection("partition = \"tenant\"")),
        (
            "projection partition none",
            projection("partition = \"none\""),
        ),
        (
            "reactors",
            format!(
                "{ROUTES}[[reactors]]\nname = \"task-notifier\"\nservice = \"task-notifier\"\n"
            ),
        ),
    ]
}

#[test]
fn version_2_access_tenancy_and_reactor_keys_parse_and_are_refused_as_unsupported() {
    for (name, body) in unsupported_documents() {
        assert_eq!(
            code(&format!("version = 2\n{body}")),
            "unsupported_in_this_build",
            "{name}"
        );
    }
}

#[test]
fn version_1_documents_cannot_use_version_2_keys() {
    let mut documents = unsupported_documents();
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
    // AP §C.4 and §C.5, verbatim apart from the shared service names.
    let text = r#"version = 2

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
    // The syntax itself decodes into the typed fields later slices enforce.
    let config: Config = toml::from_str(text).unwrap();
    assert_eq!(config.permissions.len(), 2);
    assert_eq!(config.commands[0].scope, Some(RouteScope::Tenant));
    assert_eq!(
        config.commands[0].resource.as_ref().unwrap().as_str(),
        "task"
    );
    assert_eq!(config.queries[0].access, None);
    assert_eq!(config.projections[0].partition, Some(Partition::Tenant));
    assert_eq!(config.reactors[0].commands[0].as_str(), "ArchiveTask");
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
