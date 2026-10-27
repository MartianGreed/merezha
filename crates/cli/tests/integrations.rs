//! NT-N8 through the `merezha` binary with the identity, content and
//! notifications blocks: identity's security mail goes through the
//! notifications block (one reset email whose link comes from
//! `secret_data`, no second email for the same token, the
//! `password_changed` notice in the inbox as well), and publishing an
//! organization's entry delivers `content.entry_published` to that
//! organization's webhook endpoint through the block's AP subscription,
//! without content calling the `Notifier`. A real process against real
//! PostgreSQL; skips with a message when `MEREZHA_TEST_DATABASE_URL` is
//! unset.
mod support;
use merezha_notifications::testkit::WebhookReceiver;
use merezha_postgres::{Database, TestDatabase};
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use support::Host;

const SECRET: &str = "a-process-test-identity-secret-of-32-bytes";
const NOTIFY_KEY: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
const DATABASE_ENV: &str = "INTEGRATIONS_DATABASE_URL";
const ORIGIN: &str = "http://localhost:5173";
/// The bound of every wait on the host: polls return as soon as their
/// condition holds. Registration and the reset hash with 19 MiB of Argon2
/// in a debug build.
const WAIT: Duration = Duration::from_secs(30);

const MODEL: &str = r#"
version = 1
locales = [{ code = "en", default = true }]

[types.handbook]
scope = "organization"
access = "authenticated"
fields = [{ name = "title", type = "string", required = true }]
"#;

fn project(dir: &Path) -> PathBuf {
    fs::write(dir.join("protocol.wasm"), support::component()).unwrap();
    fs::write(
        dir.join("runtime.toml"),
        "version=1\n[limits]\nmemory_bytes=16777216\nfuel=1000000\nmax_input_bytes=65536\n\
         max_output_bytes=65536\ntimeout_ms=1000\nmax_concurrent_invocations=4\n\
         [[services]]\nid=\"protocol\"\npath=\"protocol.wasm\"\n",
    )
    .unwrap();
    fs::write(dir.join("content.toml"), MODEL).unwrap();
    let path = dir.join("merezha.toml");
    fs::write(
        &path,
        format!(
            "[app]\nid = \"integrations\"\n[http]\nlisten = \"127.0.0.1:0\"\n\
             [database]\nurl = {{ env = \"{DATABASE_ENV}\" }}\nmigrate = true\n\
             [runtime]\nmanifest = \"runtime.toml\"\n\
             [jobs]\npoll_interval_ms = 100\n[jobs.relay]\ninterval_ms = 100\n\
             [jobs.queues.\"notify.webhook\"]\nbackoff_max_ms = 86400000\n\
             [modules.identity]\napp_origin = \"{ORIGIN}\"\n\
             secret_key = {{ env = \"IDENTITY_SECRET\" }}\nrequire_verified_email = false\n\
             [modules.identity.password]\nargon2_memory_kib = 19456\n\
             [modules.content]\n\
             [modules.content.media]\nlocal = {{ root = {:?} }}\nspool_dir = {:?}\n\
             [modules.notifications]\nsecret_key = {{ env = \"NOTIFY_KEY\" }}\n\
             egress = {{ allow_private_networks = true }}\n\
             [modules.notifications.templates]\nallow_http_links = true\n\
             [modules.notifications.webhooks]\nallow_http = true\n",
            dir.join("media"),
            dir.join("spool"),
        ),
    )
    .unwrap();
    path
}

struct Reply {
    status: u16,
    body: Value,
}
fn call(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &Value,
) -> Reply {
    let body = match body {
        Value::Null => String::new(),
        other => other.to_string(),
    };
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    stream
        .write_all(format!("{head}\r\n{body}").as_bytes())
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    Reply {
        status: headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        body: serde_json::from_str(body).unwrap_or(Value::Null),
    }
}

/// The captured messages of `topic`, oldest first.
fn captured(address: SocketAddr, topic: &str) -> Vec<Value> {
    let listed = call(
        address,
        "GET",
        "/v1/notifications/dev/captures?limit=100",
        &[],
        &Value::Null,
    );
    assert_eq!(listed.status, 200, "{}", listed.body);
    let mut found: Vec<Value> = listed
        .body
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["rendered"]["topic"] == topic)
        .cloned()
        .collect();
    found.reverse();
    found
}
/// Wait until `topic` has `n` captures; then they are returned.
fn captures(address: SocketAddr, topic: &str, n: usize) -> Vec<Value> {
    let deadline = Instant::now() + WAIT;
    loop {
        let found = captured(address, topic);
        if found.len() >= n {
            return found;
        }
        assert!(Instant::now() < deadline, "{topic}: {} of {n}", found.len());
        std::thread::sleep(Duration::from_millis(100));
    }
}
/// The token of the link in a captured email's text.
fn token(capture: &Value) -> String {
    capture["rendered"]["text"]
        .as_str()
        .unwrap()
        .split_once("#token=")
        .unwrap_or_else(|| panic!("no link in {capture}"))
        .1
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned()
}

/// One query against the host's database, on `runtime`.
fn count(runtime: &tokio::runtime::Runtime, db: &Database, sql: &str) -> i64 {
    runtime.block_on(async {
        let client = db
            .acquire(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        client.query_one(sql, &[]).await.unwrap().get(0)
    })
}

/// The NT-N8 acceptance criteria for identity's topics and content's
/// subscription, through one host.
#[test]
fn identity_mail_and_content_webhooks_go_through_the_notifications_block() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    // The receiver and the database client run on their own runtime; the
    // test drives the host over blocking HTTP.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let receiver = runtime.block_on(WebhookReceiver::start());
    let db = Database::connect(&test.config()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path());
    let mut host = Host::spawn(
        &config,
        &[
            (DATABASE_ENV, test.url()),
            ("IDENTITY_SECRET", SECRET),
            ("NOTIFY_KEY", NOTIFY_KEY),
        ],
        "integrations",
    );
    let address = host.ready();
    let origin = [("Origin", ORIGIN)];
    let password = "correct horse battery staple";
    let registered = call(
        address,
        "POST",
        "/v1/identity/registrations",
        &origin,
        &json!({"email": "ada@example.com", "password": password}),
    );
    assert_eq!(registered.status, 202, "{}", registered.body);

    // A password reset: one email, its link from `secret_data`.
    let requested = call(
        address,
        "POST",
        "/v1/identity/password-resets",
        &origin,
        &json!({"email": "ada@example.com"}),
    );
    assert_eq!(requested.status, 202, "{}", requested.body);
    let resets = captures(address, "identity.password_reset", 1);
    assert_eq!(resets.len(), 1);
    assert_eq!(resets[0]["target"], "ada@example.com");
    let link = resets[0]["rendered"]["text"].as_str().unwrap();
    assert!(
        link.contains("http://localhost:5173/auth/reset-password#token="),
        "{link}"
    );
    let token = token(&resets[0]);
    // The link lived only in flagged payloads: never in the notification
    // row, and nulled at relay and at the jobs' terminal state.
    assert_eq!(
        count(
            &runtime,
            &db,
            &format!(
                "SELECT count(*) FROM merezha_notify.notifications \
                 WHERE topic = 'identity.password_reset' \
                 AND (data::text LIKE '%{token}%' OR NOT has_secret)"
            )
        ),
        0
    );
    let new_password = "another horse battery staple";
    let completed = call(
        address,
        "POST",
        "/v1/identity/password-resets/complete",
        &origin,
        &json!({"token": token, "new_password": new_password, "transport": "bearer"}),
    );
    assert!(
        (200..300).contains(&completed.status),
        "{}: {}",
        completed.status,
        completed.body
    );
    let bearer = format!(
        "Bearer {}",
        completed.body["session"]["token"].as_str().unwrap()
    );
    // The same token again: refused, and no second email of either kind.
    let again = call(
        address,
        "POST",
        "/v1/identity/password-resets/complete",
        &origin,
        &json!({"token": token, "new_password": "a third horse battery staple",
                "transport": "bearer"}),
    );
    assert!(
        (400..500).contains(&again.status),
        "{}: {}",
        again.status,
        again.body
    );
    let changed = captures(address, "identity.password_changed", 1);
    assert_eq!(changed.len(), 1);
    // The notice reaches the inbox too, the topic's default channels.
    let deadline = Instant::now() + WAIT;
    let auth = [("Authorization", bearer.as_str())];
    loop {
        let inbox = call(
            address,
            "GET",
            "/v1/notifications/inbox",
            &auth,
            &Value::Null,
        );
        assert_eq!(inbox.status, 200, "{}", inbox.body);
        let topics: Vec<&str> = inbox.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|i| i["topic"].as_str())
            .collect();
        if topics.contains(&"identity.password_changed") {
            assert_eq!(topics, ["identity.password_changed"]);
            break;
        }
        assert!(Instant::now() < deadline, "no inbox item: {}", inbox.body);
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(
        count(
            &runtime,
            &db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind LIKE 'notify.%' \
             AND state IN ('available', 'running')"
        ),
        0,
        "nothing left to send"
    );
    assert_eq!(captured(address, "identity.password_reset").len(), 1);
    assert_eq!(captured(address, "identity.password_changed").len(), 1);

    // An organization, its webhook endpoint and an API key for content.
    let created = call(
        address,
        "POST",
        "/v1/identity/organizations",
        &auth,
        &json!({"name": "acme", "slug": "acme"}),
    );
    assert_eq!(created.status, 201, "{}", created.body);
    let org = created.body["organization"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    // The owner acts in the organization it made active.
    let endpoint = call(
        address,
        "POST",
        &format!("/v1/notifications/tenants/{org}/webhooks"),
        &[auth[0], ("Merezha-Organization", org.as_str())],
        &json!({"url": receiver.url("acme"), "event_types": ["content.*"]}),
    );
    assert_eq!(endpoint.status, 201, "{}", endpoint.body);
    receiver.trust(endpoint.body["secret"].as_str().unwrap());
    let expires = jiff::Timestamp::now() + jiff::SignedDuration::from_hours(24 * 30);
    let key = call(
        address,
        "POST",
        &format!("/v1/identity/organizations/{org}/api-keys"),
        &auth,
        &json!({"name": "content", "expires_at": expires.to_string(),
                "permissions": ["content.entries.read", "content.entries.write",
                                "content.entries.publish"]}),
    );
    assert_eq!(key.status, 201, "{}", key.body);
    let key = format!("Bearer {}", key.body["key"].as_str().unwrap());
    let key = [("Authorization", key.as_str())];

    // Publishing the entry delivers `content.entry_published`.
    let entry = call(
        address,
        "POST",
        "/v1/content/manage/entries/handbook",
        &key,
        &json!({"fields": {"title": "Welcome"}}),
    );
    assert_eq!(entry.status, 201, "{}", entry.body);
    let id = entry.body["sys"]["id"].as_str().unwrap().to_owned();
    let published = call(
        address,
        "POST",
        "/v1/content/manage/batch/publish",
        &key,
        &json!({"items": [{"type": "handbook", "id": id, "locale": "en", "revision": 1}]}),
    );
    assert_eq!(published.status, 200, "{}", published.body);
    let deadline = Instant::now() + WAIT;
    let received = loop {
        let received = receiver.received();
        if !received.is_empty() {
            break received;
        }
        assert!(Instant::now() < deadline, "no webhook");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].name, "acme");
    assert_eq!(
        received[0].verified_by,
        [0],
        "signed with the endpoint's secret"
    );
    let body = received[0].json();
    assert_eq!(body["type"], "content.entry_published");
    assert_eq!(body["data"]["id"], id.as_str());
    assert_eq!(body["data"]["type"], "handbook");
    // Through the subscription: a webhook event of source `content` from
    // a `notify.module_event` job, and no `emit_webhook` record.
    assert_eq!(
        count(
            &runtime,
            &db,
            "SELECT count(*) FROM merezha_notify.webhook_events \
             WHERE source = 'content' AND event_type = 'content.entry_published'"
        ),
        1
    );
    assert_eq!(
        count(
            &runtime,
            &db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.webhook_event'"
        ),
        0
    );
    assert_eq!(
        count(
            &runtime,
            &db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.module_event' \
             AND state = 'succeeded'"
        ),
        1
    );
    let log = fs::read_to_string(&host.log).unwrap();
    assert!(!log.contains(&token), "the reset link is never logged");
    host.stop();
}
