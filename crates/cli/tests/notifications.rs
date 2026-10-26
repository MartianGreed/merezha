//! The notifications block through the `merezha` binary: `merezha
//! notifications check` and the block mounted by `merezha run`.
mod support;
use merezha_postgres::TestDatabase;
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};
use support::{Host, request};

/// base64 of 32 bytes.
const KEY: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";

fn project(dir: &Path, notifications: &str) -> PathBuf {
    fs::write(
        dir.join("echo.wasm"),
        wat::parse_str(include_str!("../../runtime/tests/fixtures/echo.wat")).unwrap(),
    )
    .unwrap();
    fs::write(
        dir.join("runtime.toml"),
        "version=1\n[limits]\nmemory_bytes=16777216\nfuel=1000000\nmax_input_bytes=65536\n\
         max_output_bytes=65536\ntimeout_ms=1000\nmax_concurrent_invocations=4\n\
         [[services]]\nid=\"echo\"\npath=\"echo.wasm\"\n",
    )
    .unwrap();
    let path = dir.join("merezha.toml");
    fs::write(
        &path,
        format!(
            "[app]\nid = \"notify-check\"\n[http]\nlisten = \"127.0.0.1:0\"\n\
             [database]\nurl = {{ env = \"NOTIFY_TEST_DATABASE_URL\" }}\nmigrate = true\n\
             [runtime]\nmanifest = \"runtime.toml\"\n{notifications}"
        ),
    )
    .unwrap();
    path
}

fn check(config: &Path, env: &[(&str, &str)]) -> (Option<i32>, Value) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_merezha"));
    command
        .args(["notifications", "check", "--config"])
        .arg(config);
    command.env("NOTIFY_TEST_DATABASE_URL", "postgres://app@127.0.0.1:1/app");
    for (key, value) in env {
        command.env(key, value);
    }
    let output = command.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    (
        output.status.code(),
        serde_json::from_str(stdout.trim()).unwrap_or(Value::String(stdout)),
    )
}

fn codes(report: &Value) -> Vec<(String, String)> {
    report["issues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| {
            (
                i["key"].as_str().unwrap().to_owned(),
                i["code"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

#[test]
fn check_accepts_a_valid_section_and_summarises_it() {
    let dir = tempfile::tempdir().unwrap();
    let config = project(
        dir.path(),
        "[modules.notifications]\nsecret_key = { env = \"NOTIFY_KEY\" }\n\
         [modules.notifications.templates]\nlocales = [\"en\", \"fr\"]\n",
    );
    let (code, report) = check(&config, &[("NOTIFY_KEY", KEY)]);
    assert_eq!(code, Some(0), "{report}");
    assert_eq!(report["status"], "ok");
    assert_eq!(report["topics"], 10);
    assert_eq!(report["templates"], 10);
    assert_eq!(report["locales"], serde_json::json!(["en", "fr"]));
    assert_eq!(report["email"], "capture");
    assert_eq!(report["warnings"], serde_json::json!([]));
}

/// NT-N1 review: with the email driver disabled, built-in security mail is
/// recorded as skipped (`channel_disabled`) and never sent, so the check
/// warns while still answering ok.
#[test]
fn check_warns_when_security_mail_is_disabled() {
    let dir = tempfile::tempdir().unwrap();
    let config = project(
        dir.path(),
        "[modules.notifications]\nsecret_key = { env = \"NOTIFY_KEY\" }\n\
         [modules.notifications.email]\ndriver = \"disabled\"\n",
    );
    let (code, report) = check(&config, &[("NOTIFY_KEY", KEY)]);
    assert_eq!(code, Some(0), "{report}");
    assert_eq!(report["status"], "ok");
    assert_eq!(
        report["warnings"],
        serde_json::json!([{
            "key": "modules.notifications.email.driver",
            "code": "security_mail_disabled"
        }])
    );
}

#[test]
fn check_reports_every_problem_without_values() {
    let dir = tempfile::tempdir().unwrap();
    let templates = dir.path().join("notifications/templates/tasks.assigned/en");
    fs::create_dir_all(&templates).unwrap();
    fs::write(templates.join("subject.txt"), "{{ title }").unwrap();
    fs::write(templates.join("body.txt"), "x").unwrap();
    let config = project(
        dir.path(),
        "[modules.notifications]\nsecret_key = { env = \"NOTIFY_KEY\" }\n\
         [modules.notifications.email]\nfrom = \"broken\"\n\
         [modules.notifications.topics.\"tasks.assigned\"]\ncategory = \"transactional\"\nchannels = [\"push\"]\n",
    );
    let (code, report) = check(&config, &[("NOTIFY_KEY", "c2hvcnQtYnV0LXNlY3JldA==")]);
    assert_eq!(code, Some(2));
    assert_eq!(report["status"], "invalid");
    let issues = codes(&report);
    for (key, code) in [
        ("modules.notifications.secret_key", "invalid_secret_key"),
        ("modules.notifications.email.from", "invalid_sender"),
        (
            "modules.notifications.topics.\"tasks.assigned\".channels",
            "channel_unavailable",
        ),
        (
            "modules.notifications.templates.\"tasks.assigned/en/subject.txt\"",
            "template_syntax",
        ),
    ] {
        assert!(
            issues.iter().any(|(k, c)| k == key && c == code),
            "{key} {code} not in {issues:?}"
        );
    }
    assert!(!report.to_string().contains("c2hvcnQ"), "no secret value");

    let (code, report) = check(&project(dir.path(), ""), &[]);
    assert_eq!(code, Some(2));
    assert_eq!(
        codes(&report),
        [(
            "modules.notifications".to_owned(),
            "missing_required".to_owned()
        )]
    );

    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["notifications", "verify"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stdout).contains("invalid_arguments"));
}

#[test]
fn run_mounts_the_block_and_applies_its_migration() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(
        dir.path(),
        "[modules.notifications]\nsecret_key = { env = \"NOTIFY_KEY\" }\n",
    );
    let mut host = Host::spawn(
        &config,
        &[
            ("NOTIFY_TEST_DATABASE_URL", test.url()),
            ("NOTIFY_KEY", KEY),
        ],
        "run",
    );
    let address = host.ready();
    let templates = request(address, "GET", "/v1/notifications/templates", "");
    assert_eq!(templates.status, 200, "{}", templates.body);
    assert!(templates.body.to_string().contains("identity.magic_link"));
    let captures = request(address, "GET", "/v1/notifications/dev/captures", "");
    assert_eq!(captures.status, 200);
    assert_eq!(request(address, "GET", "/health/ready", "").status, 200);
    let text = fs::read_to_string(&host.log).unwrap();
    assert!(text.contains("\"module\":\"notifications\""), "{text}");
    assert!(!text.contains(KEY), "the secret key is never logged");
}

/// `merezha notifications brevo register-webhook` (NT-N3): one `POST
/// /v3/webhooks` creating the transactional webhook with the callback's
/// events and the bearer token, to `public_base_url` plus the callback
/// path; the key and the token never reach stdout or stderr.
#[test]
fn brevo_register_webhook_creates_the_bearer_webhook() {
    use merezha_notifications::testkit::FakeBrevo;
    let key = "xkeysib-cli-0123456789abcdef";
    let token = "cli-webhook-token-0123456789abcdef-0123";
    // The fake serves from this runtime's workers while the binary runs.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let fake = runtime.block_on(FakeBrevo::start(key)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = project(
        dir.path(),
        &format!(
            "[modules.notifications]\nsecret_key = {{ env = \"NOTIFY_KEY\" }}\n\
             public_base_url = \"https://api.acme.test\"\n\
             [modules.notifications.email]\ndriver = \"brevo\"\nfrom = \"Acme <no-reply@acme.test>\"\n\
             [modules.notifications.brevo]\napi_key = {{ env = \"CLI_BREVO_KEY\" }}\n\
             webhook_token = {{ env = \"CLI_BREVO_TOKEN\" }}\nbase_url = \"{}\"\n",
            fake.base_url()
        ),
    );
    let run = |api_key: &str, extra: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_merezha"))
            .args(["notifications", "brevo", "register-webhook", "--config"])
            .arg(&config)
            .args(extra)
            .env("NOTIFY_TEST_DATABASE_URL", "postgres://app@127.0.0.1:1/app")
            .env("NOTIFY_KEY", KEY)
            .env("CLI_BREVO_KEY", api_key)
            .env("CLI_BREVO_TOKEN", token)
            .output()
            .unwrap()
    };
    let output = run(key, &[]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(output.status.code(), Some(0), "{stdout} {stderr}");
    let report: Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(report["status"], "registered");
    assert_eq!(report["id"], 1);
    assert_eq!(report["warnings"], serde_json::json!([]));
    assert_eq!(
        report["url"],
        "https://api.acme.test/v1/notifications/providers/brevo/email"
    );
    for secret in [key, token] {
        assert!(!stdout.contains(secret) && !stderr.contains(secret));
    }
    let created = fake.requests_to("/v3/webhooks");
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].headers["api-key"], key);
    let body = &created[0].body;
    assert_eq!(body["type"], "transactional");
    assert_eq!(
        body["auth"],
        serde_json::json!({"type": "bearer", "token": token})
    );
    assert_eq!(
        body["events"],
        serde_json::json!([
            "delivered",
            "hardBounce",
            "softBounce",
            "blocked",
            "spam",
            "invalid",
            "deferred",
            "unsubscribed"
        ])
    );
    // An explicit URL must be https (or loopback http).
    let output = run(key, &["--url", "http://example.com/hook"]);
    assert_eq!(output.status.code(), Some(1));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["code"], "invalid_url");
    // Another path, or another host than `public_base_url`'s, is refused;
    // `--allow-host` lets the host through with a warning.
    for (url, code) in [
        (
            "https://api.acme.test/v1/notifications/oops",
            "callback_path_mismatch",
        ),
        (
            "https://hooks.other.test/v1/notifications/providers/brevo/email",
            "callback_host_mismatch",
        ),
    ] {
        let output = run(key, &["--url", url]);
        assert_eq!(output.status.code(), Some(1), "{url}");
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["code"], code, "{url}");
    }
    let other = "https://hooks.other.test/v1/notifications/providers/brevo/email";
    let output = run(key, &["--url", other, "--allow-host"]);
    assert_eq!(output.status.code(), Some(0));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        (report["url"].clone(), report["warnings"].clone()),
        (
            serde_json::json!(other),
            serde_json::json!(["host_override"])
        )
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("host_override"));
    assert_eq!(fake.requests_to("/v3/webhooks").len(), 2);
    // A wrong key is an account error, reported by code only.
    let output = run("xkeysib-wrong", &[]);
    assert_eq!(output.status.code(), Some(1));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["code"], "provider_account");
    assert_eq!(fake.requests_to("/v3/webhooks").len(), 3);
    drop(fake);
}
