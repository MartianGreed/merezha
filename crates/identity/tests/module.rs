//! The identity `Module`: `[modules.identity]` validation, migration 1, and
//! the start-time refusal of email flows without a delivery path
//! (acceptance criteria 1 and 12).
mod support;

use merezha_identity::{HostFacts, MIGRATIONS};
use merezha_jobs::{PgJobs, RelayMode};
use merezha_platform::{
    AppContext, Database, JobsRuntime, Module,
    ports::{CaptureNotifications, LoopbackAllowAll},
};
use merezha_postgres::{MigrationMode, ModuleMigrations, TestDatabase, checksum, run_migrations};
use std::sync::Arc;
use support::*;

fn issues(section: &str, facts: Option<HostFacts>) -> Vec<(String, String)> {
    match configured(section, facts) {
        Ok(_) => Vec::new(),
        Err(issues) => issues,
    }
}
fn public() -> HostFacts {
    HostFacts {
        listen: "0.0.0.0:8080".parse().unwrap(),
        public: true,
        behind_proxy: false,
        app_name: None,
    }
}

#[test]
fn the_defaults_follow_the_owner_answers() {
    let (_, _, _) = configured(&base(), Some(HostFacts::loopback())).unwrap();
    let section = "[modules.identity]\napp_origin = \"https://app.example\"\n\
                   secret_key = { env = \"IDENTITY_SECRET\" }\n[modules.identity.password]\n";
    let mut module = merezha_identity::IdentityModule::new(Some(HostFacts::loopback()));
    let dir = tempfile::tempdir().unwrap();
    let input = project(dir.path(), section);
    let config = merezha_platform::ProjectConfig::load(&input).unwrap();
    use merezha_platform::Module;
    module.configure(&config.modules["identity"]).unwrap();
    let settings = module.settings().unwrap();
    let day = 86_400;
    assert_eq!(settings.sessions.idle_timeout.as_secs(), 7 * day);
    assert_eq!(settings.sessions.absolute_timeout.as_secs(), 30 * day);
    assert_eq!(settings.sessions.reauthentication_window.as_secs(), 600);
    assert_eq!(settings.sessions.touch_interval.as_secs(), 60);
    assert_eq!(settings.sessions.max_per_user, 50);
    let password = settings.password.as_ref().unwrap();
    assert_eq!(password.min_length, 12);
    assert_eq!(password.max_length, 256);
    assert_eq!(
        (
            password.memory_kib,
            password.iterations,
            password.parallelism
        ),
        (65_536, 2, 1)
    );
    assert_eq!(
        settings.registration,
        merezha_identity::config::Registration::Open
    );
    assert!(settings.require_verified_email);
    assert_eq!(settings.unverified_account_ttl.as_secs(), 7 * day);
    assert!(
        settings.magic_links.is_none(),
        "magic links need their table"
    );
    assert_eq!(settings.cleanup_cron, "*/10 * * * *");
    assert_eq!(settings.cookie_name(), "__Host-merezha_session");
    assert_eq!(
        settings.links.magic_link,
        "https://app.example/auth/magic-link"
    );
    assert_eq!(
        settings.allowed_origins,
        vec!["https://app.example".to_owned()]
    );
}

#[test]
fn every_problem_is_reported_at_once() {
    let section = r#"
[modules.identity]
app_origin = "http://app.example"
secret_key = { env = "SHORT_SECRET" }
registration = "sometimes"
require_verified_email = "yes"
unverified_account_ttl = "1s"
dev_print_links = true
enabled = true

[modules.identity.password]
min_length = 7
argon2_memory_kib = 1024
argon2_iterations = 1

[modules.identity.sessions]
idle_timeout = "40d"
absolute_timeout = "30d"

[modules.identity.rate_limits.sign_in_ip]
limit = 0
burst = 3

[modules.identity.rate_limits.passkey_ip]
limit = 0

[modules.identity.links]
oauth_error = "https://evil.example/error"

[modules.identity.oauth]
flow_ttl = "2h"

[modules.identity.oauth.providers.Bad]
client_id = "x"

[modules.identity.oauth.providers.acme]
kind = "oidc"
issuer = "http://idp.example"
client_id = "merezha"
client_secret = { env = "SHORT_SECRET" }
scopes = ["email"]

[modules.identity.oauth.providers.github]
kind = "oidc"

[modules.identity.oauth.providers.google]
client_id = "123"
allowed_hosted_domains = ["Example.COM"]

[modules.identity.passkeys]
rp_id = "other.example"

[modules.identity.organizations]
max_per_user = 0

[modules.identity.rbac.builtin_grants]
user = ["tasks.read"]
owner = []

[modules.identity.rbac.application_roles.owner]
name = "Owner again"

[modules.identity.rbac.application_roles.support]
name = "Support"
permissions = ["identity.*"]
"#;
    let dir = tempfile::tempdir().unwrap();
    let mut input = project(dir.path(), section);
    input.env.push(("SHORT_SECRET".into(), "too short".into()));
    let module = merezha_identity::IdentityModule::new(Some(public()));
    let Err(errors) = merezha_platform::host::configure(&input, vec![Box::new(module)]) else {
        panic!("the section is invalid");
    };
    let mut found: Vec<(String, String)> = errors
        .0
        .iter()
        .filter(|i| i.key.starts_with("modules.identity"))
        .map(|i| (i.key.clone(), i.code.to_owned()))
        .collect();
    found.sort();
    let mut expected: Vec<(String, String)> = [
        ("app_origin", "insecure_origin"),
        ("secret_key", "secret_too_short"),
        ("registration", "invalid_choice"),
        ("require_verified_email", "expected_boolean"),
        ("unverified_account_ttl", "out_of_range"),
        ("dev_print_links", "refused_when_public"),
        ("enabled", "unknown_key"),
        ("organizations.max_per_user", "out_of_range"),
        ("rbac.builtin_grants.user", "unknown_permission"),
        ("rbac.builtin_grants.owner", "unknown_key"),
        ("rbac.application_roles.owner", "reserved_role_key"),
        (
            "rbac.application_roles.support.permissions",
            "unknown_permission",
        ),
        ("password.min_length", "out_of_range"),
        ("password.argon2_memory_kib", "out_of_range"),
        ("password.argon2_iterations", "out_of_range"),
        ("sessions.idle_timeout", "idle_exceeds_absolute"),
        ("rate_limits.sign_in_ip.limit", "out_of_range"),
        ("rate_limits.sign_in_ip.burst", "unknown_key"),
        ("rate_limits.passkey_ip.limit", "out_of_range"),
        ("links.oauth_error", "invalid_url"),
        ("oauth.flow_ttl", "out_of_range"),
        ("oauth.providers.Bad", "invalid_provider_name"),
        ("oauth.providers.acme.issuer", "insecure_origin"),
        ("oauth.providers.acme.scopes", "missing_openid_scope"),
        ("oauth.providers.github.kind", "invalid_choice"),
        ("oauth.providers.google.client_secret", "missing_required"),
        (
            "oauth.providers.google.allowed_hosted_domains",
            "invalid_domain",
        ),
        ("passkeys.rp_id", "rp_id_mismatch"),
    ]
    .iter()
    .map(|(k, c)| (format!("modules.identity.{k}"), (*c).to_owned()))
    .collect();
    if !cfg!(feature = "passkeys") {
        expected.push((
            "modules.identity.passkeys".to_owned(),
            "passkeys_unavailable".to_owned(),
        ));
    }
    expected.sort();
    assert_eq!(found, expected);
}

/// `passkeys.android_origins` (section 6): at most 16 distinct native app
/// origins, each `android:apk-key-hash:` and the unpadded base64url of a
/// 32-byte certificate digest, never a web origin.
#[test]
fn android_origins_are_exact_apk_key_hashes() {
    use base64::{
        Engine,
        engine::general_purpose::{STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD},
    };
    let android = |byte: u8| {
        format!(
            "android:apk-key-hash:{}",
            URL_SAFE_NO_PAD.encode([byte; 32])
        )
    };
    let check = |value: &str| -> Vec<(String, String)> {
        let table = format!("[modules.identity.passkeys]\nandroid_origins = {value}\n");
        issues(&section("", &[], &table), Some(HostFacts::loopback()))
            .into_iter()
            .filter(|(_, code)| code != "passkeys_unavailable")
            .collect()
    };
    let list = |origins: &[String]| format!("{origins:?}");
    let issue = |code: &str| {
        vec![(
            "modules.identity.passkeys.android_origins".to_owned(),
            code.to_owned(),
        )]
    };

    assert_eq!(check("[]"), Vec::new());
    assert_eq!(check(&list(&[android(1), android(2)])), Vec::new());
    let sixteen: Vec<String> = (0..16).map(android).collect();
    assert_eq!(check(&list(&sixteen)), Vec::new());

    let hash = URL_SAFE_NO_PAD.encode([1; 32]);
    for bad in [
        format!("android:apk-key-hash:{}", URL_SAFE.encode([1; 32])),
        format!(
            "android:apk-key-hash:{}",
            STANDARD_NO_PAD.encode([0xfb; 32])
        ),
        format!("android:apk-key-hash:{}", URL_SAFE_NO_PAD.encode([1; 31])),
        format!("android:apk-key-hash:{}", URL_SAFE_NO_PAD.encode([1; 33])),
        format!("android:apk-key-hash:{}", &hash[..42]),
        format!("ANDROID:apk-key-hash:{hash}"),
        format!("android:apk-key-hash: {hash}"),
        format!("{}\t", android(1)),
        format!("android:apk-hash:{hash}"),
        format!("ios:apk-key-hash:{hash}"),
        "android:apk-key-hash:*".to_owned(),
        "android:*".to_owned(),
        "android:apk-key-hash:".to_owned(),
        "https://app.example".to_owned(),
        "http://localhost:5173".to_owned(),
    ] {
        assert_eq!(
            check(&list(std::slice::from_ref(&bad))),
            issue("invalid_android_origin"),
            "{bad}"
        );
    }
    assert_eq!(check("[7]"), issue("invalid_android_origin"));
    assert_eq!(
        check(&format!("{:?}", android(1))),
        issue("expected_string_list")
    );
    let seventeen: Vec<String> = (0..17).map(android).collect();
    assert_eq!(check(&list(&seventeen)), issue("out_of_range"));
    assert_eq!(
        check(&list(&[android(1), android(2), android(1)])),
        issue("duplicate_android_origin")
    );

    // A native origin is no web origin: `origins` and `allowed_origins`
    // keep refusing it.
    let table = format!(
        "[modules.identity.passkeys]\norigins = [{:?}]\n",
        android(1)
    );
    let found: Vec<(String, String)> =
        issues(&section("", &[], &table), Some(HostFacts::loopback()))
            .into_iter()
            .filter(|(_, code)| code != "passkeys_unavailable")
            .collect();
    assert!(
        found.contains(&(
            "modules.identity.passkeys.origins".to_owned(),
            "invalid_origin".to_owned()
        )),
        "{found:?}"
    );
    let top = format!("allowed_origins = [{:?}]\n", android(1));
    let found = issues(&section(&top, &[], ""), Some(HostFacts::loopback()));
    assert!(
        found.contains(&(
            "modules.identity.allowed_origins".to_owned(),
            "invalid_origin".to_owned()
        )),
        "{found:?}"
    );
}

/// The implicit `user` role holds in every organization, so it takes no
/// wildcard inside a block namespace; `admin` and `member` may.
#[test]
fn builtin_user_grants_take_no_block_wildcard() {
    let section = section(
        "",
        &[],
        "[modules.identity.rbac.builtin_grants]\nuser = [\"identity.*\", \"content.*\"]\n\
         admin = [\"content.*\"]\nmember = [\"content.entries.*\"]\n",
    );
    assert_eq!(
        issues(&section, Some(HostFacts::loopback())),
        vec![
            (
                "modules.identity.rbac.builtin_grants.user".to_owned(),
                "unknown_permission".to_owned()
            );
            2
        ]
    );
    let fine = section_with_user("[\"tasks.*\", \"content.entries.read\"]");
    assert!(issues(&fine, Some(HostFacts::loopback())).is_empty());
}
fn section_with_user(user: &str) -> String {
    section(
        "",
        &[],
        &format!("[modules.identity.rbac.builtin_grants]\nuser = {user}\n"),
    )
}

#[test]
fn origins_secrets_and_methods_are_required() {
    let found = issues("[modules.identity]\n", Some(HostFacts::loopback()));
    assert_eq!(
        found,
        vec![
            (
                "modules.identity.app_origin".to_owned(),
                "missing_required".to_owned()
            ),
            (
                "modules.identity.secret_key".to_owned(),
                "missing_required".to_owned()
            ),
            (
                "modules.identity.password".to_owned(),
                "no_sign_in_method".to_owned()
            ),
        ]
    );
    let literal = "[modules.identity]\napp_origin = \"https://app.example\"\n\
                   secret_key = \"literal-secrets-are-refused-everywhere\"\n[modules.identity.magic_links]\n";
    assert_eq!(
        issues(literal, Some(HostFacts::loopback())),
        vec![(
            "modules.identity.secret_key".to_owned(),
            "secret_literal".to_owned()
        )]
    );
    let bad_link = section(
        "",
        &[],
        "[modules.identity.links]\nmagic_link = \"https://evil.example/x\"\n",
    );
    assert_eq!(
        issues(&bad_link, Some(HostFacts::loopback())),
        vec![(
            "modules.identity.links.magic_link".to_owned(),
            "invalid_url".to_owned()
        )]
    );
}

#[test]
fn the_loopback_cookie_and_printed_links_need_a_private_loopback_listener() {
    let dev = section("dev_print_links = true", &[], "").replace(APP, "http://localhost:5173");
    let (_, _, _) = configured(&dev, Some(HostFacts::loopback())).unwrap();
    let mut module = merezha_identity::IdentityModule::new(Some(HostFacts::loopback()));
    let dir = tempfile::tempdir().unwrap();
    let input = project(dir.path(), &dev);
    let config = merezha_platform::ProjectConfig::load(&input).unwrap();
    use merezha_platform::Module;
    module.configure(&config.modules["identity"]).unwrap();
    assert_eq!(module.settings().unwrap().cookie_name(), "merezha_session");
    let mut found = issues(&dev, Some(public()));
    found.sort();
    assert_eq!(
        found,
        vec![
            (
                "modules.identity.app_origin".to_owned(),
                "insecure_origin".to_owned()
            ),
            (
                "modules.identity.dev_print_links".to_owned(),
                "refused_when_public".to_owned()
            ),
        ]
    );
    let proxied = HostFacts {
        behind_proxy: true,
        ..HostFacts::loopback()
    };
    assert!(!issues(&dev, Some(proxied)).is_empty());
}

async fn plain_database(test: &TestDatabase) -> Database {
    Database::connect(&test.config()).unwrap()
}

/// Acceptance 1 (every PR): migrations 1 to 3 apply on an empty
/// database, each recorded with its checksum, and a second start applies
/// nothing. A changed checksum stops verification.
#[tokio::test]
async fn migration_one_applies_once_with_its_checksum() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = plain_database(&test).await;
    let declared = [ModuleMigrations {
        module: "identity",
        migrations: MIGRATIONS,
    }];
    let first = run_migrations(&db, &declared, MigrationMode::Apply, soon())
        .await
        .unwrap();
    assert_eq!(first.applied.len(), 3);
    let client = db.acquire(soon()).await.unwrap();
    let rows = client
        .query(
            "SELECT version, name, checksum FROM merezha.migrations WHERE module = 'identity'
              ORDER BY version",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    for (row, (version, name)) in rows.iter().zip([
        (1, "identity_core"),
        (2, "organizations_rbac_api_keys"),
        (3, "oauth_passkeys"),
    ]) {
        assert_eq!(row.get::<_, i32>(0), version);
        assert_eq!(row.get::<_, String>(1), name);
        assert_eq!(
            row.get::<_, String>(2),
            checksum(MIGRATIONS[version as usize - 1].sql)
        );
    }
    let tables: i64 = client
        .query_one(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'merezha_identity'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(tables, 19);
    let second = run_migrations(&db, &declared, MigrationMode::Apply, soon())
        .await
        .unwrap();
    assert!(second.applied.is_empty() && second.is_current());
    // The audit table refuses updates.
    let update = client
        .batch_execute(
            "INSERT INTO merezha_identity.audit_events (id, occurred_at, action, outcome)
             VALUES (gen_random_uuid(), now(), 'x', 'succeeded');
             UPDATE merezha_identity.audit_events SET action = 'y';",
        )
        .await;
    assert!(update.is_err());
    client
        .batch_execute(
            "UPDATE merezha.migrations SET checksum = 'tampered' WHERE module = 'identity'",
        )
        .await
        .unwrap();
    let refused = run_migrations(&db, &declared, MigrationMode::Verify, soon())
        .await
        .unwrap_err();
    assert_eq!(refused.failure.code, "migration_checksum_mismatch");
}

/// Acceptance 12: with the capture fallback (no delivery path) identity
/// refuses to start, unless `dev_print_links` on a loopback listener.
#[tokio::test]
async fn email_flows_need_a_delivery_path_except_printed_links_on_loopback() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test, 4).await;
    for (section, expected) in [
        (base(), Some("notifications_required")),
        (section("dev_print_links = true", &[], ""), None),
    ] {
        let (config, module, _) = configured(&section, Some(HostFacts::loopback())).unwrap();
        let jobs = PgJobs::with_mode(&config.jobs, RelayMode::Off);
        let ports = jobs.ports(&db);
        let capture = CaptureNotifications::default();
        let ctx = AppContext::builder(
            "tasks",
            db.clone(),
            Arc::new(LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap()),
        )
        .outbox(ports.outbox)
        .jobs(ports.jobs)
        .notifications(Arc::new(capture))
        .build()
        .unwrap();
        assert!(!ctx.notifications_deliver());
        let result = module.start(ctx).await;
        assert_eq!(
            result.err().map(|f| f.code),
            expected.map(str::to_owned),
            "{section}"
        );
    }
}

/// Starting declares the cleanup handler and its cron schedule.
#[tokio::test]
async fn start_registers_the_cleanup_and_link_request_jobs_and_the_schedule() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test, 4).await;
    let world_section = base();
    let (config, module, _) = configured(&world_section, Some(HostFacts::loopback())).unwrap();
    let jobs = PgJobs::with_mode(&config.jobs, RelayMode::Off);
    let ports = jobs.ports(&db);
    let mail = Arc::new(Mail::default());
    let ctx = AppContext::builder(
        "tasks",
        db.clone(),
        Arc::new(LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap()),
    )
    .outbox(ports.outbox)
    .jobs(ports.jobs)
    .notifications(Arc::new(MailFactory(mail)))
    .build()
    .unwrap();
    module.start(ctx.clone()).await.unwrap();
    let mut kinds: Vec<String> = jobs
        .registrations()
        .iter()
        .map(|r| format!("{}@{}", r.kind.as_str(), r.queue.as_str()))
        .collect();
    kinds.sort();
    assert_eq!(
        kinds,
        vec![
            "identity.cleanup@identity".to_owned(),
            "identity.link_request@identity".to_owned()
        ]
    );
    jobs.start(ctx).await.unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while jobs.ready().await.is_err() {
        assert!(std::time::Instant::now() < deadline, "jobs never ready");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let schedules = jobs.list_schedules(None, 10).await.unwrap();
    let text = format!("{schedules:?}");
    assert!(
        text.contains("identity.cleanup") && text.contains("*/10 * * * *"),
        "{text}"
    );
    jobs.shutdown(std::time::Instant::now() + std::time::Duration::from_secs(5))
        .await;
}
