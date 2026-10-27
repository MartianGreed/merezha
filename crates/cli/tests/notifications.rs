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
    assert_eq!(report["topics"], 11);
    assert_eq!(report["templates"], 11);
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

/// NT-N6: `vapid generate` prints a key pair that `check` accepts as
/// `push.vapid_private_key`, and `merezha run` serves its public half at
/// `GET /v1/notifications/push/vapid-public-key` without logging the key.
#[test]
fn vapid_generate_prints_a_pair_the_push_section_accepts() {
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["notifications", "vapid", "generate"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    let keys: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(keys["status"], "generated");
    let private = keys["private_key"].as_str().unwrap().to_owned();
    let public = keys["public_key"].as_str().unwrap().to_owned();
    assert_eq!(private.len(), 43, "base64url of 32 bytes");
    assert_eq!(public.len(), 87, "base64url of 65 bytes");
    assert_eq!(keys["key_id"].as_str().unwrap().len(), 16);
    assert!(
        keys["private_key_pem"]
            .as_str()
            .unwrap()
            .starts_with("-----BEGIN PRIVATE KEY-----")
    );
    let extra = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["notifications", "vapid", "generate", "--now"])
        .output()
        .unwrap();
    assert_eq!(extra.status.code(), Some(2));

    let dir = tempfile::tempdir().unwrap();
    let section = "[modules.notifications]\nsecret_key = { env = \"NOTIFY_KEY\" }\n\
         [modules.notifications.push]\nvapid_private_key = { env = \"VAPID_KEY\" }\n\
         vapid_subject = \"mailto:ops@example.com\"\n";
    let config = project(dir.path(), section);
    let (code, report) = check(&config, &[("NOTIFY_KEY", KEY), ("VAPID_KEY", &private)]);
    assert_eq!(
        (code, report["push"].clone()),
        (Some(0), Value::Bool(true)),
        "{report}"
    );
    // A key that is not a P-256 scalar fails startup, without its value.
    let (code, report) = check(
        &config,
        &[
            ("NOTIFY_KEY", KEY),
            ("VAPID_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        ],
    );
    assert_eq!(code, Some(2));
    assert_eq!(
        codes(&report),
        [(
            "modules.notifications.push.vapid_private_key".to_owned(),
            "invalid_vapid_key".to_owned()
        )]
    );

    let Some(test) = TestDatabase::create() else {
        return;
    };
    let mut host = Host::spawn(
        &config,
        &[
            ("NOTIFY_TEST_DATABASE_URL", test.url()),
            ("NOTIFY_KEY", KEY),
            ("VAPID_KEY", &private),
        ],
        "run",
    );
    let address = host.ready();
    let reply = request(
        address,
        "GET",
        "/v1/notifications/push/vapid-public-key",
        "",
    );
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.body["public_key"], Value::String(public));
    assert_eq!(reply.body["key_id"], keys["key_id"]);
    let text = fs::read_to_string(&host.log).unwrap();
    assert!(!text.contains(&private), "the VAPID key is never logged");
}

/// `merezha notifications purge-organization` with `args` on `config`:
/// exit code, stdout and stderr.
fn purge(config: &Path, url: &str, args: &[&str]) -> (Option<i32>, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["notifications", "purge-organization"])
        .args(args)
        .arg("--config")
        .arg(config)
        .env("NOTIFY_TEST_DATABASE_URL", url)
        .env("NOTIFY_KEY", KEY)
        .output()
        .unwrap();
    (
        output.status.code(),
        String::from_utf8(output.stdout).unwrap(),
        String::from_utf8(output.stderr).unwrap(),
    )
}

/// Security review of #82: `merezha notifications purge-organization`
/// marks and purges an organization deleted before the block consumed
/// `identity.organization_deleted`. Without identity's tables it cannot
/// confirm the deletion and refuses (exit 1) unless `--force`, which warns
/// first; the purge logs its events with source `operator`; a second run
/// finds it done; bad arguments exit 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_operator_purges_an_organization_deleted_before() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(
        dir.path(),
        "[modules.notifications]\nsecret_key = { env = \"NOTIFY_KEY\" }\n",
    );
    // `merezha run` applies the block's migrations.
    {
        let mut host = Host::spawn(
            &config,
            &[
                ("NOTIFY_TEST_DATABASE_URL", test.url()),
                ("NOTIFY_KEY", KEY),
            ],
            "run",
        );
        host.ready();
        host.stop();
    }
    let db = merezha_postgres::Database::connect(&test.config()).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let client = db.acquire(deadline).await.unwrap();
    client
        .batch_execute(
            "INSERT INTO merezha_notify.notifications (id, source, dedupe_key, topic, template, \
               tenant_id, category, priority, data, channels, status, target_count, requested_at, \
               expanded_at, settled_at) VALUES \
               ('00000000-0000-0000-0000-0000000000a1', 'api', 'k:x', 'identity.password_changed', \
                'identity.password_changed', 'org_x', 'security', 'normal', '{}', '{inbox}', \
                'expanded', 1, now(), now(), now()), \
               ('00000000-0000-0000-0000-0000000000a2', 'api', 'k:y', 'identity.password_changed', \
                'identity.password_changed', 'org_y', 'security', 'normal', '{}', '{inbox}', \
                'expanded', 1, now(), now(), now());
             INSERT INTO merezha_notify.inbox_items (id, user_id, tenant_id, notification_id, \
               topic, title, body, seq, change_seq, created_at, expires_at) VALUES \
               (gen_random_uuid(), 'usr_ada', 'org_x', '00000000-0000-0000-0000-0000000000a1', \
                'identity.password_changed', 't', 'b', 1, 1, now(), now() + interval '1 day'), \
               (gen_random_uuid(), 'usr_ada', 'org_y', '00000000-0000-0000-0000-0000000000a2', \
                'identity.password_changed', 't', 'b', 2, 2, now(), now() + interval '1 day');
             INSERT INTO merezha_notify.inbox_counters (user_id, last_seq) VALUES ('usr_ada', 2);",
        )
        .await
        .unwrap();
    let count = |sql: &'static str| {
        let db = db.clone();
        async move {
            let client = db.acquire(deadline).await.unwrap();
            client.query_one(sql, &[]).await.unwrap().get::<_, i64>(0)
        }
    };
    let (code, stdout, _) = purge(&config, test.url(), &[]);
    assert_eq!(code, Some(2), "{stdout}");
    let (code, stdout, _) = purge(&config, test.url(), &["org_x"]);
    assert_eq!(code, Some(1), "{stdout}");
    assert!(
        stdout.starts_with("refused: identity cannot confirm"),
        "{stdout}"
    );
    assert_eq!(
        count("SELECT count(*) FROM merezha_notify.tenants_deleted").await,
        0
    );
    let (code, stdout, stderr) = purge(&config, test.url(), &["org_x", "--force"]);
    assert_eq!(code, Some(0), "{stdout}{stderr}");
    assert!(stdout.starts_with("warning: --force:"), "{stdout}");
    assert!(
        stdout.contains("organization org_x marked deleted"),
        "{stdout}"
    );
    assert!(stdout.contains("inbox_items 1 notifications 1"), "{stdout}");
    assert!(stdout.contains("status purged\n"), "{stdout}");
    for event in [
        "notification.organization_deleted",
        "notification.organization_purged",
    ] {
        assert!(stderr.contains(event), "{event}: {stderr}");
    }
    assert!(stderr.contains("\"source\":\"operator\""), "{stderr}");
    assert_eq!(
        count(
            "SELECT count(*) FROM merezha_notify.tenants_deleted \
             WHERE tenant_id = 'org_x' AND source = 'operator' AND purged_at IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        count("SELECT count(*) FROM merezha_notify.notifications").await,
        1,
        "org_y's stays"
    );
    assert_eq!(
        count("SELECT count(*) FROM merezha_notify.inbox_items").await,
        1
    );
    let (code, stdout, _) = purge(&config, test.url(), &["org_x", "--force"]);
    assert_eq!(code, Some(0), "{stdout}");
    assert!(stdout.contains("already marked deleted"), "{stdout}");
    assert!(stdout.contains("status purged (already)"), "{stdout}");
    db.close();
}
