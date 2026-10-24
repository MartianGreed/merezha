//! `merezha run` mounts the content management routes (CT-C3, and C4's
//! publishing, deletion and trash routes): a private
//! listener allows them through the loopback policy, a public one without
//! identity denies them. A real process against real PostgreSQL; skips
//! with a message when `MEREZHA_TEST_DATABASE_URL` is unset.
mod support;
use merezha_postgres::TestDatabase;
use serde_json::json;
use std::{fs, path::Path, path::PathBuf};
use support::Host;

const DATABASE_ENV: &str = "CONTENT_RUN_DATABASE_URL";

fn project(dir: &Path, http: &str) -> PathBuf {
    fs::write(dir.join("protocol.wasm"), support::component()).unwrap();
    fs::write(
        dir.join("runtime.toml"),
        "version=1\n[limits]\nmemory_bytes=16777216\nfuel=1000000\nmax_input_bytes=65536\n\
         max_output_bytes=65536\ntimeout_ms=1000\nmax_concurrent_invocations=4\n\
         [[services]]\nid=\"protocol\"\npath=\"protocol.wasm\"\n",
    )
    .unwrap();
    fs::write(
        dir.join("content.toml"),
        "version = 1\nlocales = [{ code = \"en\", default = true }]\n\
         [types.page]\naccess = \"public\"\ntitle_field = \"title\"\n\
         fields = [{ name = \"title\", type = \"string\", required = true }]\n",
    )
    .unwrap();
    let path = dir.join("merezha.toml");
    fs::write(
        &path,
        format!(
            "[app]\nid = \"site\"\n[http]\n{http}\n[database]\nurl = {{ env = \"{DATABASE_ENV}\" }}\n\
             migrate = true\n[runtime]\nmanifest = \"runtime.toml\"\n[modules.content]\n"
        ),
    )
    .unwrap();
    path
}

#[test]
fn the_host_serves_content_management_routes_behind_its_policy() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "listen = \"127.0.0.1:0\"");
    let mut host = Host::spawn(&config, &[(DATABASE_ENV, test.url())], "private");
    let address = host.ready();
    let created = support::request(
        address,
        "POST",
        "/v1/content/manage/entries/page",
        &json!({"fields": {"title": "Home"}}).to_string(),
    );
    assert_eq!(created.status, 201, "{:?}", created.body);
    assert!(
        created.headers.contains("etag: \"r1\""),
        "{}",
        created.headers
    );
    assert_eq!(created.body["sys"]["created_by"], "anonymous");
    let list = support::request(address, "GET", "/v1/content/manage/entries/page", "");
    assert_eq!(list.status, 200);
    assert_eq!(list.body["items"][0]["fields"]["title"], "Home");
    // C4: batch publish, delete and the trash through the binary.
    let id = created.body["sys"]["id"].as_str().unwrap().to_owned();
    let published = support::request(
        address,
        "POST",
        "/v1/content/manage/batch/publish",
        &json!({"items": [{"type": "page", "id": id, "locale": "en", "revision": 1}]}).to_string(),
    );
    assert_eq!(published.status, 200, "{:?}", published.body);
    assert_eq!(published.body["items"][0]["sys"]["status"], "published");
    let deleted = support::request(
        address,
        "DELETE",
        &format!("/v1/content/manage/entries/page/{id}"),
        "",
    );
    assert_eq!(deleted.status, 204, "{:?}", deleted.body);
    let trash = support::request(address, "GET", "/v1/content/manage/trash", "");
    assert_eq!(trash.status, 200, "{:?}", trash.body);
    assert_eq!(trash.body["items"][0]["id"], id.as_str());
    host.stop();

    let config = project(dir.path(), "listen = \"127.0.0.1:0\"\npublic = true");
    let mut host = Host::spawn(&config, &[(DATABASE_ENV, test.url())], "public");
    let address = host.ready();
    let denied = support::request(address, "GET", "/v1/content/manage/entries/page", "");
    assert_eq!(denied.status, 401, "{:?}", denied.body);
    assert_eq!(denied.body["error"]["code"], "unauthenticated");
    host.stop();
}
