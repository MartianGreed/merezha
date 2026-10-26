//! CT-C9 through the `merezha` binary with the identity block: `merezha
//! run` accepts organization content types beside `[modules.identity]`,
//! and real organization API keys minted by identity act in their own
//! organization only. Two users each create an organization and an API
//! key; the key of one never reads or writes the other's entries, and the
//! other's public delivery shows only its own. A real process against
//! real PostgreSQL; skips with a message when `MEREZHA_TEST_DATABASE_URL`
//! is unset.
mod support;
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    time::Duration,
};
use support::Host;

const SECRET: &str = "a-process-test-identity-secret-of-32-bytes";
const DATABASE_ENV: &str = "CONTENT_ORGS_DATABASE_URL";
const ORIGIN: &str = "http://localhost:5173";

const MODEL: &str = r#"
version = 1
locales = [{ code = "en", default = true }]

[types.page]
access = "public"
fields = [{ name = "title", type = "string", required = true }]

[types.handbook]
scope = "organization"
access = "authenticated"
fields = [
  { name = "title", type = "string", required = true },
  { name = "page", type = "reference", to = ["page"] },
]

[types.notice]
scope = "organization"
access = "public"
fields = [{ name = "title", type = "string", required = true }]
"#;

fn project(dir: &Path, identity: bool) -> PathBuf {
    project_with(dir, identity, "")
}

/// [`project`] with `top`, more top-level sections such as `[jobs]`.
fn project_with(dir: &Path, identity: bool, top: &str) -> PathBuf {
    fs::write(dir.join("protocol.wasm"), support::component()).unwrap();
    fs::write(
        dir.join("runtime.toml"),
        "version=1\n[limits]\nmemory_bytes=16777216\nfuel=1000000\nmax_input_bytes=65536\n\
         max_output_bytes=65536\ntimeout_ms=1000\nmax_concurrent_invocations=4\n\
         [[services]]\nid=\"protocol\"\npath=\"protocol.wasm\"\n",
    )
    .unwrap();
    fs::write(dir.join("content.toml"), MODEL).unwrap();
    let identity = if identity {
        format!(
            "[modules.identity]\napp_origin = \"{ORIGIN}\"\n\
             secret_key = {{ env = \"IDENTITY_SECRET\" }}\nrequire_verified_email = false\n\
             dev_print_links = true\n[modules.identity.password]\nargon2_memory_kib = 19456\n"
        )
    } else {
        String::new()
    };
    let path = dir.join("merezha.toml");
    fs::write(
        &path,
        format!(
            "[app]\nid = \"orgs\"\n[http]\nlisten = \"127.0.0.1:0\"\n\
             [database]\nurl = {{ env = \"{DATABASE_ENV}\" }}\nmigrate = true\n\
             [runtime]\nmanifest = \"runtime.toml\"\n{top}\n{identity}[modules.content]\n\
             [modules.content.media]\nlocal = {{ root = {:?} }}\nspool_dir = {:?}\n",
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
        // A raw body: sent as is, its type among `headers`.
        Value::String(raw) => raw.clone(),
        other => other.to_string(),
    };
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Length: {}\r\n",
        body.len()
    );
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-type"))
    {
        head.push_str("Content-Type: application/json\r\n");
    }
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

/// Register `email`, sign in with a bearer session, create an
/// organization and mint its API key with the content permissions: the
/// organization id and the key.
fn organization(address: SocketAddr, email: &str, slug: &str) -> (String, String) {
    let (org, key, _) = owner_of(address, email, slug);
    (org, key)
}

/// [`organization`] with the owner's bearer session as well.
fn owner_of(address: SocketAddr, email: &str, slug: &str) -> (String, String, String) {
    let origin = [("Origin", ORIGIN)];
    let password = "correct horse battery staple";
    let registered = call(
        address,
        "POST",
        "/v1/identity/registrations",
        &origin,
        &json!({"email": email, "password": password}),
    );
    assert_eq!(registered.status, 202, "{}", registered.body);
    let session = call(
        address,
        "POST",
        "/v1/identity/sessions",
        &origin,
        &json!({"email": email, "password": password, "transport": "bearer"}),
    );
    assert_eq!(session.status, 201, "{}", session.body);
    let bearer = format!(
        "Bearer {}",
        session.body["session"]["token"].as_str().unwrap()
    );
    let auth = [("Authorization", bearer.as_str())];
    let created = call(
        address,
        "POST",
        "/v1/identity/organizations",
        &auth,
        &json!({"name": slug, "slug": slug}),
    );
    assert_eq!(created.status, 201, "{}", created.body);
    let org = created.body["organization"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let expires = jiff::Timestamp::now() + jiff::SignedDuration::from_hours(24 * 30);
    let key = call(
        address,
        "POST",
        &format!("/v1/identity/organizations/{org}/api-keys"),
        &auth,
        &json!({"name": "content", "expires_at": expires.to_string(),
                "permissions": ["content.entries.read", "content.entries.write",
                                "content.entries.publish", "content.model.read",
                                "content.assets.read", "content.assets.write"]}),
    );
    assert_eq!(key.status, 201, "{}", key.body);
    (org, key.body["key"].as_str().unwrap().to_owned(), bearer)
}

/// Create and publish a `content_type` entry with `key`; its id.
fn published(address: SocketAddr, key: &str, content_type: &str, title: &str) -> String {
    let bearer = format!("Bearer {key}");
    let auth = [("Authorization", bearer.as_str())];
    let created = call(
        address,
        "POST",
        &format!("/v1/content/manage/entries/{content_type}"),
        &auth,
        &json!({"fields": {"title": title}}),
    );
    assert_eq!(created.status, 201, "{}", created.body);
    let id = created.body["sys"]["id"].as_str().unwrap().to_owned();
    let published = call(
        address,
        "POST",
        "/v1/content/manage/batch/publish",
        &auth,
        &json!({"items": [{"type": content_type, "id": id, "locale": "en", "revision": 1}]}),
    );
    assert_eq!(published.status, 200, "{}", published.body);
    id
}

#[test]
fn organization_keys_act_in_their_own_organization_only() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    // Without identity there is no organization: the model is refused.
    let config = project(dir.path(), false);
    let mut host = Host::spawn(&config, &[(DATABASE_ENV, test.url())], "alone");
    let failed = host.failed();
    assert!(
        failed.to_string().contains("content_model_invalid"),
        "{failed}"
    );
    let config = project(dir.path(), true);
    let mut host = Host::spawn(
        &config,
        &[(DATABASE_ENV, test.url()), ("IDENTITY_SECRET", SECRET)],
        "orgs",
    );
    let address = host.ready();
    let (acme, acme_key) = organization(address, "ada@example.com", "acme");
    let (globex, globex_key) = organization(address, "bob@example.com", "globex");
    let mine = published(address, &acme_key, "handbook", "Acme only");
    let theirs = published(address, &globex_key, "handbook", "Globex only");
    let notice = published(address, &globex_key, "notice", "Globex news");
    published(address, &acme_key, "notice", "Acme news");
    let acme_auth = format!("Bearer {acme_key}");
    let acme_auth = [("Authorization", acme_auth.as_str())];
    let globex_auth = format!("Bearer {globex_key}");
    let globex_auth = [("Authorization", globex_auth.as_str())];
    // Management: the other organization's id is absent, in reads and
    // writes alike.
    let read = call(
        address,
        "GET",
        &format!("/v1/content/manage/entries/handbook/{theirs}/en"),
        &acme_auth,
        &Value::Null,
    );
    assert_eq!(
        (read.status, read.body["error"]["code"].as_str()),
        (404, Some("entry_not_found"))
    );
    let write = call(
        address,
        "PUT",
        &format!("/v1/content/manage/entries/handbook/{theirs}/en"),
        &[acme_auth[0], ("If-Match", "\"r1\"")],
        &json!({"fields": {"title": "Taken"}}),
    );
    assert_eq!(
        (write.status, write.body["error"]["code"].as_str()),
        (404, Some("entry_not_found"))
    );
    let list = call(
        address,
        "GET",
        "/v1/content/manage/entries/handbook",
        &acme_auth,
        &Value::Null,
    );
    assert_eq!(list.status, 200, "{}", list.body);
    let ids: Vec<&str> = list.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["sys"]["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![mine.as_str()]);
    // An organization key never acts on application types.
    let page = call(
        address,
        "GET",
        "/v1/content/manage/entries/page",
        &acme_auth,
        &Value::Null,
    );
    assert_eq!(page.status, 403, "{}", page.body);
    // Delivery: members-only entries for their own organization's key.
    let path = format!("/v1/content/o/{globex}/handbook/{theirs}");
    let refused = call(address, "GET", &path, &acme_auth, &Value::Null);
    assert_eq!(
        (refused.status, refused.body["error"]["code"].as_str()),
        (403, Some("unauthorized"))
    );
    assert_eq!(call(address, "GET", &path, &[], &Value::Null).status, 401);
    let own = call(address, "GET", &path, &globex_auth, &Value::Null);
    assert_eq!(own.status, 200, "{}", own.body);
    assert_eq!(own.body["fields"]["title"], "Globex only");
    // Public delivery on an organization's path shows only it.
    let news = call(
        address,
        "GET",
        &format!("/v1/content/o/{globex}/notice"),
        &[],
        &Value::Null,
    );
    assert_eq!(news.status, 200, "{}", news.body);
    let ids: Vec<&str> = news.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["sys"]["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![notice.as_str()]);
    // Assets: the path names the scope. A key uploads on its own
    // organization's path; the application's and another organization's
    // refuse it.
    let text = [acme_auth[0], ("Content-Type", "text/plain")];
    let body = json!("notes");
    let uploaded = call(
        address,
        "POST",
        &format!("/v1/content/o/{acme}/manage/assets?filename=notes.txt"),
        &text,
        &body,
    );
    assert_eq!(uploaded.status, 201, "{}", uploaded.body);
    assert!(
        uploaded.body["url"]
            .as_str()
            .unwrap()
            .starts_with(&format!("/v1/content/o/{acme}/assets/")),
        "{}",
        uploaded.body
    );
    for path in [
        "/v1/content/manage/assets?filename=notes.txt".to_owned(),
        format!("/v1/content/o/{globex}/manage/assets?filename=notes.txt"),
    ] {
        let refused = call(address, "POST", &path, &text, &body);
        assert_eq!(
            (refused.status, refused.body["error"]["code"].as_str()),
            (403, Some("unauthorized")),
            "{path}"
        );
    }
    let theirs = call(
        address,
        "GET",
        &format!(
            "/v1/content/o/{globex}/manage/assets/{}",
            uploaded.body["id"].as_str().unwrap()
        ),
        &globex_auth,
        &Value::Null,
    );
    assert_eq!(theirs.status, 404, "{}", theirs.body);
    assert_ne!(acme, globex);
    host.stop();
}

// ---------------------------------------------------------------------------
// Issue #56: deleting an organization stops and purges its content.
// ---------------------------------------------------------------------------

/// A pool on the test database, for seeding and inspecting rows.
async fn pool(test: &TestDatabase) -> merezha_postgres::Database {
    merezha_postgres::Database::connect(&test.config()).unwrap()
}

fn soon() -> std::time::Instant {
    std::time::Instant::now() + Duration::from_secs(30)
}

async fn texts(db: &merezha_postgres::Database, sql: &str) -> Vec<String> {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{e}: {sql}"))
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect()
}

/// Wait up to `bound` for `sql`'s single text value to be `expected`.
async fn wait_for(db: &merezha_postgres::Database, sql: &str, expected: &str, bound: Duration) {
    let deadline = std::time::Instant::now() + bound;
    loop {
        let found = texts(db, sql).await;
        if found == [expected] {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{sql}: {found:?}, not {expected}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn state_of(org: &str) -> String {
    format!(
        "SELECT coalesce((SELECT source || ' ' || state FROM merezha_content.scopes_deleted \
          WHERE scope = 'org:{org}'), 'none')"
    )
}

fn count_of(table: &str, org: &str) -> String {
    format!("SELECT count(*)::text FROM merezha_content.{table} WHERE scope = 'org:{org}'")
}

/// `merezha content purge-organization` with `args` on `config`: exit
/// code and stdout.
fn purge_command(config: &Path, url: &str, args: &[&str]) -> (Option<i32>, String, String) {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["content", "purge-organization"])
        .args(args)
        .arg("--config")
        .arg(config)
        .env(DATABASE_ENV, url)
        .env("IDENTITY_SECRET", SECRET)
        .output()
        .unwrap();
    (
        output.status.code(),
        String::from_utf8(output.stdout).unwrap(),
        String::from_utf8(output.stderr).unwrap(),
    )
}

/// The identity id of `org` (`org_<hex>`) as its UUID.
fn uuid_of(org: &str) -> String {
    merezha_identity::ids::parse(merezha_identity::ids::ORGANIZATION, org)
        .unwrap()
        .hyphenated()
        .to_string()
}

#[test]
fn content_consumes_the_topic_identity_publishes() {
    assert_eq!(
        merezha_content::ORGANIZATION_DELETED_TOPIC,
        merezha_identity::event::ORGANIZATION_DELETED
    );
}

/// Issue #56, the operator purge: an organization identity deleted while
/// content did not listen (here, its row deleted with the host stopped)
/// still serves its public content; `merezha content purge-organization`
/// marks and purges it and hands its blob to the cleanup, which the next
/// host runs. A second run finds it done. A live organization is refused
/// without `--force` and purged, with a warning, with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_operator_purges_an_organization_identity_deleted_before() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), true);
    let env = [(DATABASE_ENV, test.url()), ("IDENTITY_SECRET", SECRET)];
    let mut host = Host::spawn(&config, &env, "first");
    let address = host.ready();
    let (gone, gone_key) = organization(address, "ada@example.com", "acme");
    let (live, live_key) = organization(address, "bob@example.com", "globex");
    let notice = published(address, &gone_key, "notice", "Acme news");
    published(address, &live_key, "notice", "Globex news");
    let bearer = format!("Bearer {gone_key}");
    let uploaded = call(
        address,
        "POST",
        &format!("/v1/content/o/{gone}/manage/assets?filename=notes.txt"),
        &[
            ("Authorization", bearer.as_str()),
            ("Content-Type", "text/plain"),
        ],
        &json!("notes"),
    );
    assert_eq!(uploaded.status, 201, "{}", uploaded.body);
    host.stop();
    // Deleted in identity while content did not listen.
    let db = pool(&test).await;
    db.acquire(soon())
        .await
        .unwrap()
        .batch_execute(&format!(
            "DELETE FROM merezha_identity.organizations WHERE id = '{}'",
            uuid_of(&gone)
        ))
        .await
        .unwrap();
    let mut host = Host::spawn(&config, &env, "stale");
    let address = host.ready();
    let path = format!("/v1/content/o/{gone}/notice/{notice}");
    assert_eq!(call(address, "GET", &path, &[], &Value::Null).status, 200);
    host.stop();

    let (code, out, err) = purge_command(&config, test.url(), &[&gone]);
    assert_eq!(code, Some(0), "{out}{err}");
    let logged = |err: &str, event: &str| -> Vec<Value> {
        err.lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|e| e["event"] == event)
            .collect()
    };
    let marked = logged(&err, "content.organization_deleted");
    assert_eq!(marked.len(), 1, "{err}");
    assert_eq!(
        (
            &marked[0]["tenant"],
            &marked[0]["source"],
            &marked[0]["forced"]
        ),
        (&json!(gone), &json!("operator"), &json!(false)),
        "{err}"
    );
    let purged = logged(&err, "content.organization_purged");
    assert_eq!(purged.len(), 1, "{err}");
    assert_eq!(
        (
            &purged[0]["entries"],
            &purged[0]["assets"],
            &purged[0]["forced"]
        ),
        (&json!(1), &json!(1), &json!(false)),
        "{err}"
    );
    assert_eq!(
        out,
        format!(
            "organization {gone} marked deleted\npurged entries 1 assets 1 blobs 1\n\
             status purged\nblobs released to content.blob_cleanup\n"
        )
    );
    assert_eq!(texts(&db, &state_of(&gone)).await, ["operator purged"]);
    for table in [
        "entries",
        "variants",
        "published",
        "refs",
        "assets",
        "generations",
    ] {
        assert_eq!(texts(&db, &count_of(table, &gone)).await, ["0"], "{table}");
    }
    assert_eq!(
        texts(
            &db,
            &format!(
                "SELECT state || ' ' || (orphaned_at = '-infinity') FROM merezha_content.blobs \
                  WHERE scope = 'org:{gone}'"
            )
        )
        .await,
        ["orphaned true"]
    );
    // Again: done already.
    let (code, out, _) = purge_command(&config, test.url(), &[&gone]);
    assert_eq!(code, Some(0));
    assert_eq!(
        out,
        format!(
            "organization {gone} already marked deleted\npurged entries 0 assets 0 blobs 0\n\
             status purged (already)\n"
        )
    );
    // A live organization: refused, then forced.
    let (code, out, _) = purge_command(&config, test.url(), &[&live]);
    assert_eq!(code, Some(1), "{out}");
    assert!(
        out.starts_with(&format!("refused: identity still has organization {live}")),
        "{out}"
    );
    assert_eq!(texts(&db, &state_of(&live)).await, ["none"]);
    assert_eq!(texts(&db, &count_of("entries", &live)).await, ["1"]);
    let (code, out, err) = purge_command(&config, test.url(), &["--force", &live]);
    assert_eq!(code, Some(0), "{out}");
    for event in [
        "content.organization_deleted",
        "content.organization_purged",
    ] {
        let found: Vec<Value> = err
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|e| e["event"] == event)
            .collect();
        assert_eq!(found.len(), 1, "{event}: {err}");
        assert_eq!(found[0]["forced"], true, "{event}: {err}");
        assert_eq!(found[0]["tenant"], json!(live), "{event}: {err}");
    }
    assert!(
        out.starts_with(&format!(
            "warning: --force: identity still has organization {live}"
        )),
        "{out}"
    );
    assert_eq!(texts(&db, &count_of("entries", &live)).await, ["0"]);
    // An id identity never issues needs --force too; bad arguments fail.
    let (code, out, _) = purge_command(&config, test.url(), &["acme"]);
    assert_eq!(code, Some(1), "{out}");
    assert!(out.contains("identity cannot confirm"), "{out}");
    let (code, _, err) = purge_command(&config, test.url(), &[]);
    assert_eq!(code, Some(2));
    assert!(err.contains("\"code\":\"invalid_arguments\""), "{err}");
    // The next host refuses the organization and deletes its object.
    let mut host = Host::spawn(&config, &env, "after");
    let address = host.ready();
    let refused = call(address, "GET", &path, &[], &Value::Null);
    assert_eq!(
        (refused.status, refused.body["error"]["code"].as_str()),
        (404, Some("organization_not_found"))
    );
    wait_for(&db, &count_of("blobs", &gone), "0", Duration::from_secs(20)).await;
    host.stop();
}

/// Tests that kill a host and wait for a lease to expire: they race
/// deadlines, so CI runs them alone (`timing::`).
mod timing {
    use super::*;

    /// Issue #56 through the binary: identity's `DELETE
    /// /organizations/{org}` makes the organization's public delivery
    /// answer 404 as soon as content's subscription commits; the purge is
    /// killed (SIGKILL) after its first batch, and another host finishes
    /// it once the lease expires. The other organization keeps its content.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_purge_killed_mid_way_is_finished_by_another_host() {
        let Some(test) = TestDatabase::create() else {
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        let config = project_with(
            dir.path(),
            true,
            "[jobs]\npoll_interval_ms = 100\nreaper_interval_ms = 200\n\
             [jobs.queues.content]\nlease_ms = 1000\n",
        );
        let env = [(DATABASE_ENV, test.url()), ("IDENTITY_SECRET", SECRET)];
        let mut first = Host::spawn(&config, &env, "first");
        let address = first.ready();
        let (acme, acme_key, owner) = owner_of(address, "ada@example.com", "acme");
        let (globex, globex_key) = organization(address, "bob@example.com", "globex");
        let notice = published(address, &acme_key, "notice", "Acme news");
        let kept = published(address, &globex_key, "notice", "Globex news");
        let db = pool(&test).await;
        // 250 more entries, so the purge takes three batches.
        db.acquire(soon())
            .await
            .unwrap()
            .batch_execute(&format!(
                "INSERT INTO merezha_content.entries (id, scope, content_type, created_at, created_by) \
                 SELECT gen_random_uuid(), 'org:{acme}', 'notice', now(), 'sys_test' \
                   FROM generate_series(1, 250)"
            ))
            .await
            .unwrap();
        // Hold the 151st entry: the first batch commits, the second waits.
        let locker = db.acquire(soon()).await.unwrap();
        let locker_pid: i32 = locker
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        locker
            .batch_execute(&format!(
                "BEGIN; SELECT 1 FROM merezha_content.entries WHERE id = ( \
                   SELECT id FROM merezha_content.entries WHERE scope = 'org:{acme}' \
                    ORDER BY id OFFSET 150 LIMIT 1) FOR UPDATE"
            ))
            .await
            .unwrap();
        let deleted = call(
            address,
            "DELETE",
            &format!("/v1/identity/organizations/{acme}"),
            &[("Authorization", owner.as_str())],
            &json!({"confirm_slug": "acme"}),
        );
        assert_eq!(deleted.status, 204, "{}", deleted.body);
        wait_for(
            &db,
            &state_of(&acme),
            "event purging",
            Duration::from_secs(10),
        )
        .await;
        let path = format!("/v1/content/o/{acme}/notice/{notice}");
        let refused = call(address, "GET", &path, &[], &Value::Null);
        assert_eq!(
            (refused.status, refused.body["error"]["code"].as_str()),
            (404, Some("organization_not_found"))
        );
        wait_for(
            &db,
            &count_of("entries", &acme),
            "151",
            Duration::from_secs(10),
        )
        .await;
        // Kill the host mid-purge, and the backend it left waiting.
        first.child.kill().unwrap();
        first.child.wait().unwrap();
        db.acquire(soon())
            .await
            .unwrap()
            .execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                  WHERE $1 = ANY(pg_blocking_pids(pid))",
                &[&locker_pid],
            )
            .await
            .unwrap();
        locker.batch_execute("ROLLBACK").await.unwrap();
        drop(locker);
        let mut second = Host::spawn(&config, &env, "second");
        let address = second.ready();
        wait_for(
            &db,
            &state_of(&acme),
            "event purged",
            Duration::from_secs(20),
        )
        .await;
        assert_eq!(texts(&db, &count_of("entries", &acme)).await, ["0"]);
        // Two leases of one purge job: the killed host's expired, the
        // second host's succeeded.
        let leases = texts(
            &db,
            "SELECT coalesce(a.outcome, 'none') FROM merezha_jobs.attempts a \
               JOIN merezha_jobs.jobs j ON j.id = a.job_id \
              WHERE j.kind = 'content.organization_purge' ORDER BY a.started_at",
        )
        .await;
        assert_eq!(
            leases.first().map(String::as_str),
            Some("lease_expired"),
            "{leases:?}"
        );
        assert_eq!(
            leases.last().map(String::as_str),
            Some("succeeded"),
            "{leases:?}"
        );
        let owners = texts(
            &db,
            "SELECT count(DISTINCT a.lease_owner)::text FROM merezha_jobs.attempts a \
               JOIN merezha_jobs.jobs j ON j.id = a.job_id \
              WHERE j.kind = 'content.organization_purge'",
        )
        .await;
        assert_eq!(owners, ["2"]);
        let refused = call(address, "GET", &path, &[], &Value::Null);
        assert_eq!(refused.status, 404);
        // The other organization is untouched.
        let other = call(
            address,
            "GET",
            &format!("/v1/content/o/{globex}/notice/{kept}"),
            &[],
            &Value::Null,
        );
        assert_eq!(other.status, 200, "{}", other.body);
        second.stop();
    }
}
