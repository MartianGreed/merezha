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
