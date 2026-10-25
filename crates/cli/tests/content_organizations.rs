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
             [runtime]\nmanifest = \"runtime.toml\"\n{identity}[modules.content]\n\
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
    (org, key.body["key"].as_str().unwrap().to_owned())
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
