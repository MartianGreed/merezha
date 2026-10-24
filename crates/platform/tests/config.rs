//! `merezha.toml` layering, secret resolution and error aggregation.
use merezha_platform::{
    ConfigInput, Module, ModuleSection,
    config::{ConfigIssue, LogFormat, Origin},
    host::configure,
};
use merezha_postgres::TlsMode;
use std::{path::PathBuf, time::Duration};

const MINIMAL: &str = r#"
[app]
id = "tasks"
[database]
url = { env = "APP_DATABASE_URL" }
[runtime]
manifest = "runtime.toml"
"#;

struct Project {
    dir: tempfile::TempDir,
}
impl Project {
    fn new(text: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("merezha.toml"), text).unwrap();
        Self { dir }
    }
    fn input(&self, env: &[(&str, &str)], flags: &[(&str, &str)]) -> ConfigInput {
        ConfigInput {
            path: self.dir.path().join("merezha.toml"),
            env: env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            overrides: flags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
    fn base(&self) -> PathBuf {
        self.dir.path().canonicalize().unwrap()
    }
}
const URL: (&str, &str) = ("APP_DATABASE_URL", "postgres://app:hunter2@db/app");

fn codes(errors: &merezha_platform::ConfigErrors) -> Vec<(String, &'static str)> {
    errors.0.iter().map(|i| (i.key.clone(), i.code)).collect()
}

#[test]
fn defaults_apply_and_paths_resolve_against_the_config_directory() {
    let project = Project::new(MINIMAL);
    let config = merezha_platform::ProjectConfig::load(&project.input(&[URL], &[])).unwrap();
    assert_eq!(config.app.name, "tasks");
    assert_eq!(config.http.listen, "127.0.0.1:8080".parse().unwrap());
    assert!(!config.http.public);
    assert_eq!(config.http.body_limit, 1_048_576);
    assert!(config.http.cors.allowed_origins.is_empty());
    assert_eq!(config.database.url.expose(), URL.1);
    assert_eq!(config.database.pool_size, 16);
    assert_eq!(config.database.statement_timeout, Duration::from_secs(5));
    assert!(!config.database.migrate);
    assert_eq!(config.database.tls, TlsMode::Disable);
    assert_eq!(config.runtime.manifest, project.base().join("runtime.toml"));
    assert!(config.application.is_none());
    assert_eq!(config.telemetry.format, LogFormat::Json);
    let printed = format!("{config:?}");
    assert!(
        !printed.contains("hunter2"),
        "Debug leaked the URL: {printed}"
    );
}

#[test]
fn precedence_is_defaults_then_file_then_env_then_flags() {
    let project = Project::new(&format!(
        "{MINIMAL}\n[http]\nlisten = \"127.0.0.1:9000\"\nbody_limit = 2048\n[http.cors]\nallowed_origins = [\"https://a.example\"]\n[telemetry]\nlog_level = \"warn\"\n"
    ));
    let env = [
        URL,
        ("MEREZHA_HTTP__LISTEN", "127.0.0.1:9001"),
        ("MEREZHA_DATABASE__POOL_SIZE", "12"),
        ("MEREZHA_DATABASE__MIGRATE", "true"),
        (
            "MEREZHA_HTTP__CORS__ALLOWED_ORIGINS",
            "https://b.example, https://c.example",
        ),
        ("MEREZHA_TELEMETRY__FORMAT", "text"),
        // Not configuration: no double underscore.
        ("MEREZHA_TEST_DATABASE_URL", "ignored"),
    ];
    let config = merezha_platform::ProjectConfig::load(&project.input(&env, &[])).unwrap();
    assert_eq!(config.http.listen, "127.0.0.1:9001".parse().unwrap());
    assert_eq!(config.http.body_limit, 2048, "file beats default");
    assert_eq!(config.database.pool_size, 12, "env beats default");
    assert!(config.database.migrate);
    assert_eq!(
        config.http.cors.allowed_origins,
        vec!["https://b.example", "https://c.example"]
    );
    assert_eq!(config.telemetry.format, LogFormat::Text);
    let flags = [
        ("http.listen", "127.0.0.1:9002"),
        ("database.pool_size", "3"),
    ];
    let config = merezha_platform::ProjectConfig::load(&project.input(&env, &flags)).unwrap();
    assert_eq!(config.http.listen, "127.0.0.1:9002".parse().unwrap());
    assert_eq!(config.database.pool_size, 3, "flag beats env");
}

#[test]
fn secrets_come_from_env_or_file_and_never_from_literals_or_flags() {
    let project = Project::new(
        r#"
[app]
id = "tasks"
[database]
url = { file = "secrets/db-url" }
[runtime]
manifest = "runtime.toml"
"#,
    );
    std::fs::create_dir(project.dir.path().join("secrets")).unwrap();
    std::fs::write(
        project.dir.path().join("secrets/db-url"),
        "postgres://file@db/app\n",
    )
    .unwrap();
    let config = merezha_platform::ProjectConfig::load(&project.input(&[], &[])).unwrap();
    assert_eq!(config.database.url.expose(), "postgres://file@db/app");
    // The environment layer may carry the secret value itself.
    let env = [("MEREZHA_DATABASE__URL", "postgres://env@db/app")];
    let config = merezha_platform::ProjectConfig::load(&project.input(&env, &[])).unwrap();
    assert_eq!(config.database.url.expose(), "postgres://env@db/app");
    let errors = merezha_platform::ProjectConfig::load(
        &project.input(&[], &[("database.url", "postgres://flag@db/app")]),
    )
    .unwrap_err();
    assert_eq!(
        errors.0,
        vec![ConfigIssue::new(
            "database.url",
            "secret_on_command_line",
            Origin::Flag
        )]
    );
    for (reference, code) in [
        (r#""postgres://literal:pw@db/app""#, "secret_literal"),
        (r#"{ env = "MISSING_VARIABLE" }"#, "secret_unavailable"),
        (r#"{ file = "secrets/absent" }"#, "secret_unavailable"),
        (r#"{ env = "A", file = "b" }"#, "invalid_secret_reference"),
        (r#"{ vault = "x" }"#, "invalid_secret_reference"),
    ] {
        let project = Project::new(&format!(
            "[app]\nid = \"tasks\"\n[database]\nurl = {reference}\n[runtime]\nmanifest = \"r.toml\"\n"
        ));
        let errors = merezha_platform::ProjectConfig::load(&project.input(&[], &[])).unwrap_err();
        assert_eq!(
            codes(&errors),
            vec![("database.url".into(), code)],
            "{reference}"
        );
        assert!(!errors.to_string().contains("pw@"), "{errors}");
    }
}

#[test]
fn every_problem_is_reported_at_once() {
    let project = Project::new(
        r#"
[app]
id = "Tasks!"
colour = "blue"
[http]
listen = "0.0.0.0:8080"
trusted_proxies = ["10.0.0.0/8", "not-a-cidr"]
body_limit = 10
[http.cors]
allowed_origins = ["*"]
allow_credentials = true
methods = ["GET"]
[database]
pool_size = "many"
tls = "maybe"
[unknown]
x = 1
"#,
    );
    let errors = merezha_platform::ProjectConfig::load(&project.input(
        &[("MEREZHA_HTTP__NOPE", "1"), ("MEREZHA_NOPE__X", "1")],
        &[],
    ))
    .unwrap_err();
    let found = codes(&errors);
    for expected in [
        ("unknown", "unknown_section"),
        ("http.nope", "unknown_key"),
        ("nope.x", "unknown_key"),
        ("app.id", "invalid_app_id"),
        ("app.colour", "unknown_key"),
        ("http.listen", "public_listen_not_enabled"),
        ("http.trusted_proxies", "invalid_cidr"),
        ("http.body_limit", "out_of_range"),
        ("http.cors.allowed_origins", "invalid_wildcard_origin"),
        ("http.cors.methods", "unknown_key"),
        ("database.url", "missing_required"),
        ("database.pool_size", "expected_integer"),
        ("database.tls", "invalid_choice"),
        ("runtime.manifest", "missing_required"),
    ] {
        assert!(
            found.contains(&(expected.0.to_string(), expected.1)),
            "missing {expected:?} in {found:?}"
        );
    }
    let env_issue = errors.0.iter().find(|i| i.key == "http.nope").unwrap();
    assert_eq!(env_issue.origin, Origin::Env("MEREZHA_HTTP__NOPE".into()));
}

#[test]
fn public_listen_requires_the_explicit_opt_in() {
    let project = Project::new(&format!(
        "{MINIMAL}\n[http]\nlisten = \"0.0.0.0:8080\"\npublic = true\n"
    ));
    let config = merezha_platform::ProjectConfig::load(&project.input(&[URL], &[])).unwrap();
    assert!(config.http.public);
    let errors =
        merezha_platform::ProjectConfig::load(&project.input(&[URL], &[("http.public", "false")]))
            .unwrap_err();
    assert_eq!(
        codes(&errors),
        vec![("http.listen".into(), "public_listen_not_enabled")]
    );
}

#[test]
fn invalid_toml_reports_a_line_and_never_the_text() {
    let project =
        Project::new("[app]\nid = \"tasks\"\n[database]\nurl = \"postgres://x:secretpw@db\n");
    let errors = merezha_platform::ProjectConfig::load(&project.input(&[], &[])).unwrap_err();
    assert_eq!(errors.0.len(), 1);
    assert_eq!(errors.0[0].code, "invalid_toml");
    assert!(errors.0[0].key.starts_with("line "));
    assert!(!errors.to_string().contains("secretpw"));
}

#[derive(Default)]
struct Greeter {
    greeting: Option<String>,
}
impl Module for Greeter {
    fn name(&self) -> &'static str {
        "greeter"
    }
    fn configure(&mut self, section: &ModuleSection) -> Result<(), Vec<ConfigIssue>> {
        let mut issues = section.unknown_keys(&["greeting", "token"]);
        match section.table.get("greeting").and_then(|v| v.as_str()) {
            Some(greeting) => self.greeting = Some(greeting.to_owned()),
            None => issues.push(ConfigIssue::new(
                section.key("greeting"),
                "missing_required",
                Origin::Default,
            )),
        }
        if let Err(issue) = section.secret("token") {
            issues.push(issue);
        }
        if issues.is_empty() {
            Ok(())
        } else {
            Err(issues)
        }
    }
}

#[test]
fn modules_validate_their_sections_and_unknown_modules_fail_with_kernel_errors() {
    let project = Project::new(&format!(
        "{MINIMAL}\n[modules.greeter]\ngreeting = \"hello\"\ntoken = {{ env = \"GREETER_TOKEN\" }}\n"
    ));
    let env = [URL, ("GREETER_TOKEN", "t0ken")];
    let (config, modules) = configure(
        &project.input(&env, &[]),
        vec![Box::new(Greeter::default())],
    )
    .unwrap();
    assert_eq!(modules.len(), 1);
    assert_eq!(
        config.modules["greeter"]
            .secret("token")
            .unwrap()
            .unwrap()
            .expose(),
        "t0ken"
    );
    // Environment overrides reach module sections as TOML literals.
    let env = [
        URL,
        ("GREETER_TOKEN", "t0ken"),
        ("MEREZHA_MODULES__GREETER__GREETING", "\"hi\""),
    ];
    let (config, _) = configure(
        &project.input(&env, &[]),
        vec![Box::new(Greeter::default())],
    )
    .unwrap();
    assert_eq!(
        config.modules["greeter"].table["greeting"].as_str(),
        Some("hi")
    );
    // A registered module without a section is disabled.
    let project = Project::new(MINIMAL);
    let (_, modules) = configure(
        &project.input(&[URL], &[]),
        vec![Box::new(Greeter::default())],
    )
    .unwrap();
    assert!(modules.is_empty());
    // Kernel, module and unknown-module problems arrive together.
    let project = Project::new(
        "[app]\nid = \"tasks\"\n[runtime]\nmanifest = \"r.toml\"\n[modules.greeter]\ntoken = \"literal\"\nextra = 1\n[modules.billing]\n",
    );
    let errors = configure(&project.input(&[], &[]), vec![Box::new(Greeter::default())])
        .err()
        .unwrap();
    let found = codes(&errors);
    for expected in [
        ("database.url", "missing_required"),
        ("modules.greeter.greeting", "missing_required"),
        ("modules.greeter.token", "secret_literal"),
        ("modules.greeter.extra", "unknown_key"),
        ("modules.billing", "unknown_module"),
    ] {
        assert!(
            found.contains(&(expected.0.to_string(), expected.1)),
            "missing {expected:?} in {found:?}"
        );
    }
}

#[test]
fn database_issues_name_the_layer_that_supplied_the_setting() {
    let project = Project::new(MINIMAL);
    let errors = merezha_platform::ProjectConfig::load(&project.input(
        &[URL, ("MEREZHA_DATABASE__URL", "postgres://db:notaport/app")],
        &[],
    ))
    .unwrap_err();
    assert_eq!(
        errors.0,
        vec![ConfigIssue::new(
            "database.url",
            "invalid_database_url",
            Origin::Env("MEREZHA_DATABASE__URL".into())
        )]
    );
    // A URL demanding TLS while `tls` stays disabled would be downgraded.
    let errors = merezha_platform::ProjectConfig::load(&project.input(
        &[(
            "APP_DATABASE_URL",
            "postgres://app:hunter2@db/app?sslmode=require",
        )],
        &[],
    ))
    .unwrap_err();
    assert_eq!(
        errors.0,
        vec![ConfigIssue::new(
            "database.url",
            "sslmode_requires_tls",
            Origin::File
        )]
    );
    assert!(!errors.to_string().contains("hunter2"));
    let config = merezha_platform::ProjectConfig::load(&project.input(
        &[(
            "APP_DATABASE_URL",
            "postgres://app:hunter2@db/app?sslmode=require",
        )],
        &[("database.tls", "require")],
    ))
    .unwrap();
    assert_eq!(config.database.tls, TlsMode::Require);
}

fn load(
    text: &str,
    env: &[(&str, &str)],
    flags: &[(&str, &str)],
) -> Result<merezha_platform::ProjectConfig, merezha_platform::ConfigErrors> {
    let project = Project::new(&format!("{MINIMAL}\n{text}"));
    let mut env = env.to_vec();
    env.push(URL);
    merezha_platform::ProjectConfig::load(&project.input(&env, flags))
}

#[test]
fn durations_accept_units_and_integer_milliseconds_for_ms_keys() {
    // `[database]` is in MINIMAL already: its keys come from the other layers.
    let config = load(
        "[jobs]\npoll_interval_ms = \"2s\"\nreaper_interval_ms = 750\n[jobs.retention]\ndead = \"14d\"\noutbox = \"36h\"\n",
        &[("MEREZHA_DATABASE__CONNECT_TIMEOUT_MS", "2500"), ("MEREZHA_DATABASE__ACQUIRE_TIMEOUT_MS", "1s")],
        &[("database.statement_timeout_ms", "1500ms")],
    )
    .unwrap();
    assert_eq!(config.jobs.poll_interval, Duration::from_secs(2));
    assert_eq!(config.jobs.reaper_interval, Duration::from_millis(750));
    assert_eq!(config.jobs.retention.dead, Duration::from_secs(14 * 86_400));
    assert_eq!(
        config.jobs.retention.outbox,
        Duration::from_secs(36 * 3_600)
    );
    assert_eq!(config.database.connect_timeout, Duration::from_millis(2500));
    assert_eq!(config.database.acquire_timeout, Duration::from_secs(1));
    assert_eq!(
        config.database.statement_timeout,
        Duration::from_millis(1500)
    );
    let errors = load(
        "[jobs]\npoll_interval_ms = \"2\"\nreaper_interval_ms = \"fast\"\ndrain_timeout_ms = -1\n[jobs.retention]\ndead = 30\nsucceeded = \"1.5d\"\noutbox = \"1s\"\naudit = \"5w\"\n",
        &[("MEREZHA_JOBS__RELAY__INTERVAL_MS", "10x")],
        &[],
    )
    .unwrap_err();
    assert_eq!(
        codes(&errors),
        vec![
            ("jobs.poll_interval_ms".into(), "expected_duration"),
            ("jobs.reaper_interval_ms".into(), "expected_duration"),
            ("jobs.drain_timeout_ms".into(), "expected_duration"),
            ("jobs.relay.interval_ms".into(), "expected_duration"),
            ("jobs.retention.succeeded".into(), "expected_duration"),
            ("jobs.retention.dead".into(), "expected_duration"),
            ("jobs.retention.outbox".into(), "out_of_range"),
            ("jobs.retention.audit".into(), "expected_duration"),
        ]
    );
    let relay = errors
        .0
        .iter()
        .find(|i| i.key == "jobs.relay.interval_ms")
        .unwrap();
    assert_eq!(
        relay.origin,
        Origin::Env("MEREZHA_JOBS__RELAY__INTERVAL_MS".into())
    );
}

#[test]
fn jobs_take_defaults_queues_and_schedules() {
    let config = load("", &[], &[]).unwrap();
    let jobs = &config.jobs;
    assert!(jobs.workers);
    assert_eq!(jobs.poll_interval, Duration::from_secs(1));
    assert_eq!(jobs.drain_timeout, Duration::from_secs(20));
    assert_eq!(
        (jobs.relay.interval, jobs.relay.batch),
        (Duration::from_secs(1), 100)
    );
    assert_eq!(jobs.scheduler_interval, Duration::from_secs(10));
    assert_eq!(jobs.defaults.max_attempts, 10);
    assert_eq!(jobs.defaults.backoff_max, Duration::from_secs(3_600));
    assert!(jobs.queues.is_empty() && jobs.schedules.is_empty());
    assert_eq!(jobs.retention.audit, Duration::from_secs(365 * 86_400));
    assert_eq!(jobs.readiness_max_lag, None);

    let config = load(
        r#"
[jobs]
workers = false
[jobs.defaults]
max_attempts = 5
[jobs.queues.default]
concurrency = 8
global_concurrency = 0
[jobs.queues."notify.webhook"]
concurrency = 16
backoff_base_ms = 1000
backoff_max_ms = 86400000
[jobs.readiness]
max_lag_ms = "2m"
[[jobs.schedules]]
name = "tasks.reminders"
cron = "0 9 * * mon-fri"
timezone = "Europe/Paris"
queue = "default"
kind = "tasks.send_reminders"
payload = { only = ["late"], limit = 50 }
"#,
        &[("MEREZHA_JOBS__QUEUES__DEFAULT__CONCURRENCY", "4")],
        &[
            ("jobs.relay.batch", "250"),
            (r#"jobs.queues."notify.webhook".concurrency"#, "12"),
        ],
    )
    .unwrap();
    let jobs = &config.jobs;
    assert!(!jobs.workers);
    assert_eq!(jobs.relay.batch, 250);
    let default = &jobs.queues["default"];
    assert_eq!(default.concurrency, 4, "env beats the file");
    assert_eq!(default.settings.max_attempts, 5, "inherits [jobs.defaults]");
    let webhook = &jobs.queues["notify.webhook"];
    assert_eq!(
        webhook.concurrency, 12,
        "a quoted flag segment names a dotted queue"
    );
    assert_eq!(webhook.settings.backoff_max, Duration::from_secs(86_400));
    assert_eq!(webhook.settings.max_attempts, 5);
    assert_eq!(jobs.readiness_max_lag, Some(Duration::from_secs(120)));
    assert_eq!(jobs.schedules.len(), 1);
    assert_eq!(
        jobs.schedules[0].payload,
        serde_json::json!({"only": ["late"], "limit": 50})
    );
    assert_eq!(jobs.schedules[0].timezone, "Europe/Paris");
}

#[test]
fn flags_address_dotted_queue_names_with_a_quoted_segment() {
    let config = load(
        "",
        &[],
        &[(r#"jobs.queues."notify.email".concurrency"#, "5")],
    )
    .unwrap();
    assert_eq!(config.jobs.queues["notify.email"].concurrency, 5);
    let errors = load(
        "",
        &[],
        &[(r#"jobs.queues."notify.email.concurrency"#, "5")],
    )
    .unwrap_err();
    assert_eq!(
        codes(&errors),
        vec![(
            r#"jobs.queues."notify.email.concurrency"#.into(),
            "unknown_key"
        )]
    );
}

#[test]
fn jobs_problems_are_reported_together() {
    let errors = load(
        r#"
[jobs]
enabled = true
[jobs.nope]
x = 1
[jobs.relay]
batch = 0
[jobs.defaults]
backoff_base_ms = 60000
backoff_max_ms = 1000
[jobs.queues.a]
concurrency = 200
[jobs.queues.b]
concurrency = 100
colour = "red"
[jobs.queues.Bad]
concurrency = 1
[jobs.queues]
c = 3
[[jobs.schedules]]
name = "one"
cron = "every day"
queue = "default"
payload = { at = 1979-05-27T07:32:00Z }
extra = 1
[[jobs.schedules]]
name = "one"
cron = "* * * * *"
queue = "default"
kind = "k"
"#,
        &[],
        &[],
    )
    .unwrap_err();
    let found = codes(&errors);
    for expected in [
        ("jobs.enabled", "unknown_key"),
        ("jobs.nope", "unknown_key"),
        ("jobs.relay.batch", "out_of_range"),
        ("jobs.defaults.backoff_base_ms", "backoff_base_exceeds_max"),
        ("jobs.queues.a.backoff_base_ms", "backoff_base_exceeds_max"),
        ("jobs.queues.b.colour", "unknown_key"),
        ("jobs.queues.Bad", "invalid_queue_name"),
        ("jobs.queues.c", "expected_table"),
        ("jobs.queues", "concurrency_total_exceeded"),
        ("jobs.schedules[0].cron", "invalid_cron"),
        ("jobs.schedules[0].kind", "missing_required"),
        ("jobs.schedules[0].payload", "invalid_payload"),
        ("jobs.schedules[0].extra", "unknown_key"),
        ("jobs.schedules[1].name", "duplicate_name"),
    ] {
        assert!(
            found.contains(&(expected.0.to_string(), expected.1)),
            "missing {expected:?} in {found:?}"
        );
    }
}

#[test]
fn application_extensions_validate_with_the_section() {
    let config = load("[application]\npath = \"application.toml\"\n", &[], &[]).unwrap();
    let application = config.application.unwrap();
    assert_eq!(
        application.projections.catch_up,
        merezha_platform::config::CatchUp::Auto
    );
    assert_eq!(application.projections.interval, Duration::from_secs(5));
    assert!(application.projections.warm_readiness);
    assert_eq!(application.reactors.retry_max, Duration::from_secs(300));
    assert_eq!(application.background.max_concurrent, 2);
    assert_eq!(application.append.lock_timeout_max, Duration::from_secs(5));
    let config = load(
        "[application]\npath = \"a.toml\"\n[application.projections]\ncatch_up = \"manual\"\nrun_timeout_ms = \"1m\"\n[application.background]\nmax_concurrent = 16\n",
        &[
            ("MEREZHA_APPLICATION__APPEND__LOCK_TIMEOUT_MAX_MS", "250"),
            // 16 background runs need 17 detached sessions.
            ("MEREZHA_DATABASE__MAX_DETACHED", "17"),
        ],
        &[],
    )
    .unwrap();
    let application = config.application.unwrap();
    assert_eq!(
        application.projections.catch_up,
        merezha_platform::config::CatchUp::Manual
    );
    assert_eq!(application.projections.run_timeout, Duration::from_secs(60));
    assert_eq!(application.background.max_concurrent, 16);
    assert_eq!(
        application.append.lock_timeout_max,
        Duration::from_millis(250)
    );
    let errors = load(
        "[application.projections]\ncatch_up = \"sometimes\"\ninterval_ms = 50\n[application.reactors]\nretry_base_ms = \"10m\"\nretry_max_ms = \"5s\"\nhalt = true\n[application.background]\nmax_concurrent = 17\n[application.append]\nlock_timeout_max_ms = \"31s\"\n",
        &[],
        &[],
    )
    .unwrap_err();
    assert_eq!(
        codes(&errors),
        vec![
            ("application.path".into(), "missing_required"),
            ("application.projections.catch_up".into(), "invalid_choice"),
            ("application.projections.interval_ms".into(), "out_of_range"),
            (
                "application.reactors.retry_base_ms".into(),
                "retry_base_exceeds_max"
            ),
            ("application.reactors.halt".into(), "unknown_key"),
            (
                "application.background.max_concurrent".into(),
                "out_of_range"
            ),
            (
                "application.append.lock_timeout_max_ms".into(),
                "out_of_range"
            ),
        ]
    );
}

#[test]
fn background_runs_leave_one_detached_session_free() {
    // Defaults: 2 background runs, 8 detached sessions.
    assert!(load("[application]\npath = \"a.toml\"\n", &[], &[]).is_ok());
    let fits = "[application]\npath = \"a.toml\"\n[application.background]\nmax_concurrent = 7\n";
    assert!(load(fits, &[], &[]).is_ok());
    let errors = load(
        "[application]\npath = \"a.toml\"\n[application.background]\nmax_concurrent = 8\n",
        &[],
        &[],
    )
    .unwrap_err();
    assert_eq!(
        codes(&errors),
        vec![
            (
                "application.background.max_concurrent".into(),
                "detached_sessions_exhausted"
            ),
            (
                "database.max_detached".into(),
                "detached_sessions_exhausted"
            ),
        ]
    );
    assert_eq!(errors.0[0].origin, Origin::File);
    assert_eq!(errors.0[1].origin, Origin::Default);
    let errors = load(fits, &[("MEREZHA_DATABASE__MAX_DETACHED", "4")], &[]).unwrap_err();
    assert_eq!(
        errors.0[1].origin,
        Origin::Env("MEREZHA_DATABASE__MAX_DETACHED".into())
    );
    assert!(load(fits, &[], &[("database.max_detached", "16")]).is_ok());
    // Without an application there are no background runs to fit.
    assert!(load("", &[("MEREZHA_DATABASE__MAX_DETACHED", "1")], &[]).is_ok());
}

#[test]
fn app_instance_id_defaults_to_host_and_pid_and_version_is_optional() {
    let config = load("", &[("HOSTNAME", "web-1.internal")], &[]).unwrap();
    assert_eq!(
        config.app.instance_id,
        format!("web-1.internal-{}", std::process::id())
    );
    assert_eq!(config.app.version, None);
    let project = Project::new(&MINIMAL.replace(
        "id = \"tasks\"",
        "id = \"tasks\"\ninstance_id = \"tasks-a:1\"\nversion = \"1.4.0\"",
    ));
    let config = merezha_platform::ProjectConfig::load(&project.input(&[URL], &[])).unwrap();
    assert_eq!(config.app.instance_id, "tasks-a:1");
    assert_eq!(config.app.version.as_deref(), Some("1.4.0"));
    let project = Project::new(&MINIMAL.replace(
        "id = \"tasks\"",
        "id = \"tasks\"\ninstance_id = \"has space\"\nversion = \"\"",
    ));
    let errors = merezha_platform::ProjectConfig::load(&project.input(&[URL], &[])).unwrap_err();
    assert_eq!(
        codes(&errors),
        vec![
            ("app.instance_id".into(), "invalid_instance_id"),
            ("app.version".into(), "invalid_version"),
        ]
    );
}

#[test]
fn telemetry_extensions_validate_and_keep_header_secrets_out_of_debug() {
    use merezha_platform::config::{Protocol, Sampler, Signal};
    let config = load("", &[], &[]).unwrap();
    let telemetry = &config.telemetry;
    assert_eq!(telemetry.endpoint, "http://127.0.0.1:4318");
    assert_eq!(telemetry.protocol, Protocol::HttpProtobuf);
    assert_eq!(
        telemetry.signals,
        vec![Signal::Traces, Signal::Metrics, Signal::Logs]
    );
    assert_eq!(telemetry.traces.ratio, 1.0);
    assert_eq!(telemetry.traces.sampler, Sampler::ParentBasedRatio);
    assert_eq!(
        (telemetry.traces.max_queue, telemetry.traces.max_batch),
        (2048, 512)
    );
    assert!(telemetry.privacy.record_tenant_id && !telemetry.privacy.record_subject_id);
    assert!(telemetry.headers.is_empty());

    let config = load(
        r#"
[telemetry]
endpoint = "https://otlp.example.com"
protocol = "grpc"
timeout_ms = "15s"
headers = { authorization = { env = "OTLP_AUTHORIZATION" } }
signals = []
[telemetry.resource]
service_namespace = "shop"
attributes = { "cloud.region" = "eu-west-3" }
[telemetry.traces]
ratio = 0.25
[telemetry.propagation]
outbound_hosts = ["api.partner.example"]
"#,
        &[
            ("OTLP_AUTHORIZATION", "Bearer s3cr3t-t0ken"),
            ("MEREZHA_TELEMETRY__TRACES__MAX_BATCH", "64"),
        ],
        &[],
    )
    .unwrap();
    let telemetry = &config.telemetry;
    assert_eq!(telemetry.protocol, Protocol::Grpc);
    assert_eq!(telemetry.timeout, Duration::from_secs(15));
    assert!(telemetry.signals.is_empty(), "signals = [] turns OTLP off");
    assert_eq!(
        telemetry.headers["authorization"].expose(),
        "Bearer s3cr3t-t0ken"
    );
    assert_eq!(telemetry.traces.ratio, 0.25);
    assert_eq!(telemetry.traces.max_batch, 64);
    assert_eq!(
        telemetry.resource.service_namespace.as_deref(),
        Some("shop")
    );
    assert_eq!(telemetry.resource.attributes["cloud.region"], "eu-west-3");
    assert_eq!(
        telemetry.propagation,
        vec!["api.partner.example".to_owned()]
    );
    let printed = format!("{config:?}");
    assert!(!printed.contains("s3cr3t-t0ken"), "{printed}");

    // A header from the environment carries the secret itself.
    let config = load(
        "[telemetry]\nendpoint = \"http://localhost:4318\"\n",
        &[("MEREZHA_TELEMETRY__HEADERS__X_API_KEY", "k3y")],
        &[],
    );
    let config = config.unwrap();
    assert_eq!(config.telemetry.headers["x-api-key"].expose(), "k3y");

    let errors = load(
        r#"
[telemetry]
endpoint = "http://collector.internal:4318"
compression = "brotli"
headers = { "content-type" = { env = "X" }, authorization = "Bearer literal" }
signals = ["traces", "profiles", "traces"]
ca_file = ""
[telemetry.resource]
attributes = { "user.name" = "ada" }
[telemetry.traces]
ratio = 1.5
max_queue = 100
max_batch = 200
endpoint = "https://user:pw@collector/v1/traces"
[telemetry.metrics]
interval_ms = 500
[telemetry.logs]
level = "loud"
[telemetry.privacy]
record_everything = true
[telemetry.propagation]
outbound_hosts = ["Bad Host"]
"#,
        &[("X", "v")],
        &[],
    )
    .unwrap_err();
    let found = codes(&errors);
    for expected in [
        ("telemetry.compression", "invalid_choice"),
        ("telemetry.signals", "invalid_signal"),
        ("telemetry.signals", "duplicate_signal"),
        ("telemetry.ca_file", "invalid_path"),
        ("telemetry.headers.content-type", "invalid_header_name"),
        ("telemetry.headers.authorization", "secret_literal"),
        (
            "telemetry.resource.attributes.user.name",
            "invalid_resource_attribute",
        ),
        ("telemetry.traces.ratio", "out_of_range"),
        ("telemetry.traces.max_batch", "batch_exceeds_queue"),
        ("telemetry.traces.endpoint", "invalid_endpoint"),
        ("telemetry.metrics.interval_ms", "out_of_range"),
        ("telemetry.logs.level", "invalid_choice"),
        ("telemetry.privacy.record_everything", "unknown_key"),
        ("telemetry.propagation.outbound_hosts", "invalid_host"),
    ] {
        assert!(
            found.contains(&(expected.0.to_string(), expected.1)),
            "missing {expected:?} in {found:?}"
        );
    }
    assert!(!errors.to_string().contains("Bearer literal"));

    // Secret headers never go to a remote plaintext collector by accident.
    let remote = "[telemetry]\nendpoint = \"http://collector.internal:4318\"\nheaders = { authorization = { env = \"A\" } }\n";
    let errors = load(remote, &[("A", "x")], &[]).unwrap_err();
    assert_eq!(
        codes(&errors),
        vec![("telemetry.headers".into(), "plaintext_headers_refused")]
    );
    assert!(
        load(
            remote,
            &[("A", "x")],
            &[("telemetry.allow_plaintext_headers", "true")]
        )
        .is_ok()
    );
    let local = remote.replace("collector.internal", "127.0.0.1");
    assert!(load(&local, &[("A", "x")], &[]).is_ok());
}
