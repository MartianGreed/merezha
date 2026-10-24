//! Migration 1 and the capture drivers against real PostgreSQL, and the
//! block inside a started host. Each test creates its own database through
//! `MEREZHA_TEST_DATABASE_URL` and skips with a message when it is unset.
mod support;
use axum::{
    body::{Body, to_bytes},
    extract::ConnectInfo,
    http::{Request, StatusCode},
};
use merezha_notifications::{
    HostFacts, MIGRATIONS, Notifications,
    drivers::{
        CaptureDriver, EmailDriver, EmailMessage, Provider, SendOutcome, SmsDriver, SmsMessage,
    },
    templates::{Encoding, RenderedEmail, RenderedSms},
};
use merezha_platform::{
    Host, HostParts, Module, Timeouts, host,
    ports::{Channel, EmailAddress, LoopbackAllowAll, PhoneNumber},
};
use merezha_postgres::{
    Database, MigrationMode, ModuleMigrations, TestDatabase, checksum, run_migrations,
};
use serde_json::Value;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use support::*;
use tower::ServiceExt;
use uuid::Uuid;

/// SHA-256 of migration 1 as applied. A changed migration is refused by
/// every database it already ran on (`migration_checksum_mismatch`), so the
/// text is frozen: later slices add migrations instead.
const CORE_CHECKSUM: &str = "40f9a70cd7cacfcc33d316f559a3cf499925152663fe5a1ad271add029cfdb70";

/// A distinct id per call.
fn fresh() -> Uuid {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    Uuid::from_u64_pair(nanos, n)
}

fn soon() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

#[test]
fn migration_checksum_is_pinned() {
    assert_eq!(MIGRATIONS.len(), 1);
    assert_eq!((MIGRATIONS[0].version, MIGRATIONS[0].name), (1, "core"));
    assert_eq!(checksum(MIGRATIONS[0].sql), CORE_CHECKSUM);
}

async fn migrated() -> Option<(TestDatabase, Database)> {
    let test = TestDatabase::create()?;
    let db = Database::connect(&test.config()).unwrap();
    let declared = [ModuleMigrations {
        module: "notifications",
        migrations: &MIGRATIONS,
    }];
    let report = run_migrations(&db, &declared, MigrationMode::Apply, soon())
        .await
        .unwrap();
    assert_eq!(report.applied.len(), 1);
    Some((test, db))
}

#[tokio::test]
async fn migration_one_creates_the_core_tables() {
    let Some((_test, db)) = migrated().await else {
        return;
    };
    let declared = [ModuleMigrations {
        module: "notifications",
        migrations: &MIGRATIONS,
    }];
    let verified = run_migrations(&db, &declared, MigrationMode::Verify, soon())
        .await
        .unwrap();
    assert!(verified.is_current());
    let client = db.acquire(soon()).await.unwrap();
    let rows = client
        .query(
            "SELECT table_name::text FROM information_schema.tables \
             WHERE table_schema = 'merezha_notify' ORDER BY 1",
            &[],
        )
        .await
        .unwrap();
    let tables: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(
        tables,
        [
            "captures",
            "deliveries",
            "delivery_attempts",
            "notifications",
            "preferences",
            "provider_events",
            "suppressions",
            "user_settings"
        ]
    );
    // The schema enforces the closed sets of NT §2.4.
    let id = fresh().to_string();
    let insert = |status: &'static str, source: &'static str| {
        format!(
            "INSERT INTO merezha_notify.notifications (id, source, dedupe_key, topic, template, \
             category, priority, channels, status, target_count, requested_at, expanded_at) \
             VALUES ('{id}', '{source}', 'k:1', 't.x', 't.x', 'security', 'high', '{{email}}', \
             '{status}', 0, now(), now())"
        )
    };
    assert!(
        client
            .batch_execute(&insert("pending", "identity"))
            .await
            .is_err()
    );
    assert!(
        client
            .batch_execute(&insert("expanded", "Identity"))
            .await
            .is_err()
    );
    client
        .batch_execute(&insert("expanded", "identity"))
        .await
        .unwrap();
    let duplicate = client.batch_execute(&insert("expanded", "identity")).await;
    assert!(duplicate.is_err(), "one notification per id");
    let delivery = format!(
        "INSERT INTO merezha_notify.deliveries (id, notification_id, channel, target_key, locale, \
         status, run_at, created_at, updated_at) VALUES ('{}', '{id}', 'fax', 'x', 'en', 'queued', \
         now(), now(), now())",
        fresh()
    );
    assert!(client.batch_execute(&delivery).await.is_err());
}

fn email(delivery: Uuid, key: Uuid, subject: &str) -> EmailMessage {
    EmailMessage {
        delivery_id: delivery,
        idempotency_key: key,
        topic: "identity.magic_link".into(),
        to: EmailAddress::new("ada@example.com").unwrap(),
        to_name: Some("Ada".into()),
        content: RenderedEmail {
            subject: subject.into(),
            text: "https://app.example.com/m/secret-token".into(),
            html: None,
        },
        unsubscribe: None,
    }
}

#[tokio::test]
async fn capture_drivers_keep_the_newest_rows_and_honour_idempotency() {
    let Some((_test, db)) = migrated().await else {
        return;
    };
    let driver = CaptureDriver::new(db.clone(), 3);
    assert_eq!(EmailDriver::provider(&driver), Provider::Capture);
    let first = fresh();
    let key = fresh();
    let outcome = EmailDriver::send(&driver, &email(first, key, "first"), soon()).await;
    assert!(matches!(
        outcome,
        SendOutcome::Accepted {
            provider: Provider::Capture,
            message_id: Some(_)
        }
    ));
    // The same delivery and idempotency key: the provider-like duplicate.
    assert_eq!(
        EmailDriver::send(&driver, &email(first, key, "first again"), soon()).await,
        SendOutcome::Duplicate {
            provider: Provider::Capture
        }
    );
    // A new generation (new key) captures again.
    let again = EmailDriver::send(
        &driver,
        &email(first, fresh(), "first, generation 1"),
        soon(),
    )
    .await;
    assert!(matches!(again, SendOutcome::Accepted { .. }));
    for n in 0..3 {
        let sms = SmsMessage {
            delivery_id: fresh(),
            topic: "tasks.assigned".into(),
            to: PhoneNumber::new("+33612345678").unwrap(),
            content: RenderedSms {
                text: format!("sms {n}"),
                encoding: Encoding::Gsm7,
                segments: 1,
            },
            marketing: false,
        };
        assert!(matches!(
            SmsDriver::send(&driver, &sms, soon()).await,
            SendOutcome::Accepted { .. }
        ));
    }
    let all = driver.list(None, 100, soon()).await.unwrap();
    assert_eq!(all.len(), 3, "only the newest retention.captures rows stay");
    assert_eq!(
        all.iter()
            .map(|c| c.rendered["text"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>(),
        ["sms 2", "sms 1", "sms 0"]
    );
    assert_eq!(all[0].target, "+33612345678");
    assert!(
        driver
            .list(Some(Channel::Email), 10, soon())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !format!("{:?}", all[0]).contains("+336"),
        "Debug hides the target"
    );
    assert_eq!(
        driver
            .list(Some(Channel::Sms), 2, soon())
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn the_host_starts_the_block_with_its_migration_source_and_routes() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let project = Project::new(&format!("[database]\nmigrate = true\n{MINIMAL}"));
    // Replace the kernel database section with the test database.
    let text = std::fs::read_to_string(project.path().join("merezha.toml"))
        .unwrap()
        .replacen("[database]\nurl = { env = \"APP_DATABASE_URL\" }\n", "", 1)
        .replacen(
            "[database]\nmigrate = true\n",
            "[database]\nurl = { env = \"APP_DATABASE_URL\" }\nmigrate = true\n",
            1,
        );
    std::fs::write(project.path().join("merezha.toml"), text).unwrap();
    let project = project.env("APP_DATABASE_URL", test.url());
    let input = project.input();
    let modules: Vec<Box<dyn Module>> = vec![Box::new(Notifications::new(HostFacts::load(&input)))];
    let (config, modules) = host::configure(&input, modules).unwrap();
    assert_eq!(modules.len(), 1);
    assert_eq!(modules[0].extra_sources(), ["api"]);
    let host = Host::start(
        config,
        modules,
        HostParts {
            routes: axum::Router::new(),
            authorizer: Arc::new(LoopbackAllowAll::new("127.0.0.1:8080".parse().unwrap()).unwrap()),
            timeouts: Timeouts::default(),
            jobs: None,
        },
    )
    .await
    .unwrap();
    assert!(host.migrations().is_current());
    assert_eq!(
        host.errors().status_of("render_failed").map(|s| s.as_u16()),
        Some(422)
    );

    // A message captured through the driver is listed on loopback only.
    let db = Database::connect(&test.config()).unwrap();
    let driver = CaptureDriver::new(db, 1000);
    EmailDriver::send(
        &driver,
        &email(fresh(), fresh(), "Your sign-in link"),
        soon(),
    )
    .await;
    let get_from = |path: &str, peer: &str, host_header: &str| {
        let mut request = Request::builder()
            .uri(path)
            .header("host", host_header)
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(ConnectInfo(peer.parse::<SocketAddr>().unwrap()));
        host.router().oneshot(request)
    };
    let get = |path: &str, peer: &str| get_from(path, peer, "127.0.0.1:8080");
    let response = get(
        "/v1/notifications/dev/captures?channel=email",
        "127.0.0.1:5000",
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let listed: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(listed[0]["rendered"]["subject"], "Your sign-in link");
    assert_eq!(listed[0]["target"], "ada@example.com");
    assert_eq!(listed[0]["channel"], "email");
    let response = get("/v1/notifications/dev/captures", "203.0.113.9:5000")
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    // DNS rebinding: a loopback peer with a foreign Host is refused.
    for foreign in [
        "evil.example",
        "evil.example:8080",
        "127.0.0.1.evil.example",
    ] {
        let response = get_from("/v1/notifications/dev/captures", "127.0.0.1:5000", foreign)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{foreign}");
    }
    for local in ["localhost", "LOCALHOST:3000", "[::1]:8080"] {
        let response = get_from("/v1/notifications/dev/captures", "127.0.0.1:5000", local)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{local}");
    }
    let response = get("/v1/notifications/dev/captures?limit=0", "127.0.0.1:5000")
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    // Previews are mounted under /v1/notifications behind the edge.
    let response = get("/v1/notifications/templates", "127.0.0.1:5000")
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
