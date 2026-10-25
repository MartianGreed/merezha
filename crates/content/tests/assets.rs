//! CT-C7 assets over the axum router, a real database and both blob
//! drivers (a temp directory; the in-process S3 server of `merezha-blob`'s
//! tests; and MinIO or another S3 service when `MEREZHA_TEST_S3_*` is
//! set): uploads with sniffing, dimensions and dedupe, admission and spool
//! hygiene, the asset routes, proxy and redirect delivery, deletes with
//! `force` and `purge`, and the blob cleanup.
mod http;
#[path = "../../blob/tests/support/mod.rs"]
mod s3;

use axum::{
    body::Body,
    http::{Method, StatusCode},
};
use http::*;
use merezha_content::media::{Assets, Media};
use merezha_platform::ports::{BlobKey, BlobStore};
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tower::ServiceExt;

const MODEL: &str = r#"
version = 1
locales = [{ code = "en", default = true }, { code = "fr", fallback = "en" }]

[types.article]
access = "public"
title_field = "title"
fields = [
  { name = "title", type = "string", required = true },
  { name = "cover", type = "asset" },
  { name = "gallery", type = "asset", cardinality = "many", accept = ["image/*"] },
  { name = "body", type = "rich_text", embeds = ["asset"] },
]

[types.memo]
access = { permission = "content.delivery.read" }
fields = [{ name = "title", type = "string" }, { name = "file", type = "asset" }]

[types.note]
access = "authenticated"
fields = [{ name = "title", type = "string" }, { name = "file", type = "asset" }]
"#;

const ASSETS: &str = "/v1/content/manage/assets";

/// Where a test's media lives: the blob root (local driver) and the spool
/// directory, both created 0700 by the host.
struct Dirs {
    dir: tempfile::TempDir,
}
impl Dirs {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }
    fn root(&self) -> PathBuf {
        self.dir.path().join("media")
    }
    fn spool(&self) -> PathBuf {
        self.dir.path().join("spool")
    }
    /// `media = { ... }` for the local driver plus `extra` keys.
    fn local(&self, extra: &str) -> String {
        format!(
            "media = {{ driver = \"local\", local = {{ root = {:?} }}, spool_dir = {:?}{extra} }}",
            self.root(),
            self.spool()
        )
    }
    /// `media = { ... }` for the in-process S3 server, credentials through
    /// nested `{ file = .. }` secrets.
    fn s3(&self, mock: &s3::MockS3, extra: &str) -> String {
        let id = self.dir.path().join("s3-id");
        let secret = self.dir.path().join("s3-secret");
        std::fs::write(&id, s3::ACCESS_KEY).unwrap();
        std::fs::write(&secret, s3::SECRET_KEY).unwrap();
        format!(
            "media = {{ driver = \"s3\", spool_dir = {:?}, s3 = {{ bucket = \"{}\", region = \"{}\", \
             endpoint = \"{}\", allow_http = true, timeout = \"5s\", \
             access_key_id = {{ file = {id:?} }}, secret_access_key = {{ file = {secret:?} }} }}{extra} }}",
            self.spool(),
            s3::BUCKET,
            s3::REGION,
            mock.endpoint(),
        )
    }
    /// `media = { ... }` for the S3 service named by `MEREZHA_TEST_S3_*`,
    /// with a per-run key prefix; `None` (and a message) when unset.
    fn external(&self, extra: &str) -> Option<String> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        let Some(endpoint) = var("MEREZHA_TEST_S3_ENDPOINT") else {
            eprintln!(
                "skipping the external S3 run: set MEREZHA_TEST_S3_ENDPOINT, MEREZHA_TEST_S3_BUCKET, \
                 MEREZHA_TEST_S3_ACCESS_KEY_ID and MEREZHA_TEST_S3_SECRET_ACCESS_KEY \
                 (and optionally MEREZHA_TEST_S3_REGION) to run against MinIO"
            );
            return None;
        };
        let id = self.dir.path().join("s3-id");
        let secret = self.dir.path().join("s3-secret");
        std::fs::write(&id, var("MEREZHA_TEST_S3_ACCESS_KEY_ID").unwrap()).unwrap();
        std::fs::write(&secret, var("MEREZHA_TEST_S3_SECRET_ACCESS_KEY").unwrap()).unwrap();
        Some(format!(
            "media = {{ driver = \"s3\", spool_dir = {:?}, s3 = {{ bucket = \"{}\", region = \"{}\", \
             endpoint = \"{endpoint}\", allow_http = {}, key_prefix = \"merezha-c7-{}-{}\", \
             access_key_id = {{ file = {id:?} }}, secret_access_key = {{ file = {secret:?} }} }}{extra} }}",
            self.spool(),
            var("MEREZHA_TEST_S3_BUCKET").unwrap(),
            var("MEREZHA_TEST_S3_REGION").unwrap_or_else(|| "us-east-1".into()),
            endpoint.starts_with("http://"),
            std::process::id(),
            jiff::Timestamp::now().as_millisecond(),
        ))
    }
    /// Spool files left behind.
    fn spooled(&self) -> Vec<String> {
        match std::fs::read_dir(self.spool()) {
            Ok(entries) => entries
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect(),
            Err(_) => Vec::new(),
        }
    }
    /// Objects the local driver stored (files under the root).
    fn objects(&self) -> usize {
        fn walk(path: &Path) -> usize {
            match std::fs::read_dir(path) {
                Ok(entries) => entries
                    .flatten()
                    .map(|e| {
                        if e.path().is_dir() {
                            walk(&e.path())
                        } else {
                            1
                        }
                    })
                    .sum(),
                Err(_) => 0,
            }
        }
        walk(&self.root())
    }
}

async fn delete_asset(fx: &Fixture, path: &str) -> Reply {
    fx.call(Method::DELETE, &format!("{ASSETS}/{path}"))
        .by(Some("media"))
        .send()
        .await
}

fn media(fx: &Fixture) -> Arc<Media> {
    fx.handle.state().unwrap().media().unwrap().clone()
}
fn assets(fx: &Fixture) -> Assets {
    media(fx).assets().unwrap().clone()
}

/// A PNG of `width` by `height`: signature, IHDR and `pad` bytes after it.
fn png(width: u32, height: u32, pad: usize) -> Vec<u8> {
    let mut out = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    out.extend_from_slice(&width.to_be_bytes());
    out.extend_from_slice(&height.to_be_bytes());
    out.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
    out.extend((0..pad).map(|i| (i % 251) as u8));
    out
}

/// The first 64 bytes of an ELF executable.
fn elf() -> Vec<u8> {
    let mut out = b"\x7fELF\x02\x01\x01\0".to_vec();
    out.extend_from_slice(&[0; 8]);
    out.extend_from_slice(&[2, 0, 0x3e, 0, 1, 0, 0, 0]);
    out.resize(64, 0);
    out
}

/// `POST /assets?filename=..` with `bytes` declared as `content_type`.
async fn upload(fx: &Fixture, filename: &str, content_type: &str, bytes: &[u8]) -> Reply {
    fx.post(&format!("{ASSETS}?filename={filename}"))
        .by(Some("media"))
        .header("content-type", content_type)
        .raw(bytes)
        .send()
        .await
}
async fn uploaded(fx: &Fixture, filename: &str, content_type: &str, bytes: &[u8]) -> Value {
    let reply = upload(fx, filename, content_type, bytes).await;
    assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
    reply.body
}

/// A raw response: status, headers and body bytes.
struct Raw {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}
impl Raw {
    fn header(&self, name: &str) -> &str {
        self.headers
            .get(name)
            .map(|v| v.to_str().unwrap())
            .unwrap_or("")
    }
}
async fn raw(fx: &Fixture, uri: &str, principal: Option<&str>, headers: &[(&str, &str)]) -> Raw {
    let mut builder = axum::http::Request::builder().method(Method::GET).uri(uri);
    if let Some(principal) = principal {
        builder = builder.header("x-test-principal", principal);
    }
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let response = fx
        .client
        .router
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), 64 << 20)
        .await
        .unwrap()
        .to_vec();
    Raw {
        status,
        headers,
        body,
    }
}

/// An article in `en` referencing `asset` as its cover; its id.
async fn article_with(fx: &Fixture, asset: &str) -> String {
    let reply = fx
        .create(
            "article",
            json!({"fields": {"title": "With a cover", "cover": {"id": asset, "alt": "Cover"}}}),
        )
        .await;
    id_of(&reply)
}

fn delivery(asset: &Value) -> String {
    asset["url"].as_str().unwrap().to_owned()
}

async fn blob_rows(fx: &Fixture) -> Vec<String> {
    strings(
        &fx.db,
        "SELECT state || ':' || sha256 FROM merezha_content.blobs ORDER BY sha256",
    )
    .await
}

async fn object_exists(fx: &Fixture, key: &str) -> bool {
    let store = media(fx).store().unwrap();
    store.head(&BlobKey::new(key).unwrap()).await.is_ok()
}

/// The storage keys of every asset's blob, with whether the blob is
/// `stored` and its object exists.
async fn assets_keep_their_blobs(fx: &Fixture) -> bool {
    let rows = strings(
        &fx.db,
        "SELECT coalesce(b.state, 'missing') || ' ' || coalesce(b.storage_key, '') \
           FROM merezha_content.assets a \
           LEFT JOIN merezha_content.blobs b ON b.scope = a.scope AND b.sha256 = a.sha256",
    )
    .await;
    for row in rows {
        let (state, key) = row.split_once(' ').unwrap();
        if state != "stored" || !object_exists(fx, key).await {
            return false;
        }
    }
    true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn png_bytes_declared_as_jpeg_are_refused_with_415_and_leave_nothing() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let reply = upload(&fx, "team.jpg", "image/jpeg", &png(16, 9, 64)).await;
    assert_eq!(reply.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(reply.code(), "media_type_rejected");
    // A type outside `allowed_types`, a missing or broken type, an
    // executable declared as text, and an SVG without its root.
    for (content_type, bytes) in [
        ("application/zip", &b"PK\x03\x04rest"[..]),
        ("", b"hello"),
        ("text", b"hello"),
        ("text/plain", &elf()[..]),
        ("text/plain", b"not \xff utf-8"),
        ("image/svg+xml", b"<html><svg/></html>"),
        ("image/png", b"plain text pretending"),
    ] {
        let reply = upload(&fx, "x", content_type, bytes).await;
        assert_eq!(
            (reply.status, reply.code()),
            (StatusCode::UNSUPPORTED_MEDIA_TYPE, "media_type_rejected"),
            "{content_type}"
        );
    }
    assert!(dirs.spooled().is_empty(), "{:?}", dirs.spooled());
    assert!(blob_rows(&fx).await.is_empty());
    assert_eq!(dirs.objects(), 0);
    assert_eq!(
        scalar(&fx.db, "SELECT count(*) FROM merezha_content.assets").await,
        0
    );
    // The same PNG declared as PNG is stored, with its dimensions.
    let asset = uploaded(&fx, "team.jpg", "image/png", &png(16, 9, 64)).await;
    assert_eq!(asset["content_type"], "image/png");
    assert_eq!(asset["filename"], "team.png");
    assert_eq!(
        (asset["width"].clone(), asset["height"].clone()),
        (json!(16), json!(9))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn html_declared_as_text_is_served_as_an_attachment_with_nosniff_and_the_sandbox_csp() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let html = b"<!DOCTYPE html><html><body><script>alert(document.cookie)</script></body></html>";
    let asset = uploaded(&fx, "page.html", "text/plain", html).await;
    assert_eq!(asset["content_type"], "text/plain");
    assert_eq!(asset["filename"], "page.txt");
    let article = article_with(&fx, asset["id"].as_str().unwrap()).await;
    fx.publish("article", &article, "en", 1).await;
    for (uri, principal) in [
        (delivery(&asset), None),
        (
            format!("{ASSETS}/{}/file", asset["id"].as_str().unwrap()),
            Some("media"),
        ),
    ] {
        let reply = raw(&fx, &uri, principal, &[]).await;
        assert_eq!(reply.status, StatusCode::OK, "{uri}");
        assert_eq!(reply.body, html);
        assert_eq!(reply.header("content-type"), "text/plain; charset=utf-8");
        assert_eq!(
            reply.header("content-disposition"),
            "attachment; filename=\"page.txt\""
        );
        assert_eq!(reply.header("x-content-type-options"), "nosniff");
        assert_eq!(
            reply.header("content-security-policy"),
            "default-src 'none'; sandbox"
        );
        assert_eq!(
            reply.header("etag"),
            format!("\"{}\"", asset["sha256"].as_str().unwrap())
        );
    }
    // An SVG is never inline either; an inline type is, on delivery only.
    let svg = uploaded(
        &fx,
        "logo.svg",
        "image/svg+xml",
        b"<svg xmlns=\"http://www.w3.org/2000/svg\"><script>alert(1)</script></svg>",
    )
    .await;
    let image = uploaded(&fx, "cover.png", "image/png", &png(4, 4, 16)).await;
    for asset in [&svg, &image] {
        let article = article_with(&fx, asset["id"].as_str().unwrap()).await;
        fx.publish("article", &article, "en", 1).await;
    }
    let reply = raw(&fx, &delivery(&svg), None, &[]).await;
    assert_eq!(reply.header("content-type"), "image/svg+xml");
    assert_eq!(
        reply.header("content-disposition"),
        "attachment; filename=\"logo.svg\""
    );
    assert_eq!(
        reply.header("content-security-policy"),
        "default-src 'none'; sandbox"
    );
    let reply = raw(&fx, &delivery(&image), None, &[]).await;
    assert_eq!(reply.header("content-disposition"), "inline");
    assert_eq!(reply.header("x-content-type-options"), "nosniff");
    assert_eq!(reply.header("cache-control"), "public, max-age=3600");
    assert_eq!(reply.header("vary"), "Authorization, Cookie");
    let file = format!("{ASSETS}/{}/file", image["id"].as_str().unwrap());
    let reply = raw(&fx, &file, Some("media"), &[]).await;
    assert_eq!(
        reply.header("content-disposition"),
        "attachment; filename=\"cover.png\""
    );
    assert_eq!(reply.header("cache-control"), "private, no-store");
    // A matching `If-None-Match` is 304 without a body.
    let tag = reply.header("etag").to_owned();
    let reply = raw(&fx, &delivery(&image), None, &[("if-none-match", &tag)]).await;
    assert_eq!(reply.status, StatusCode::NOT_MODIFIED);
    assert!(reply.body.is_empty());
    assert_hardened(&reply);
}

/// The headers every asset response carries, whatever its status.
fn assert_hardened(reply: &Raw) {
    assert_eq!(reply.header("x-content-type-options"), "nosniff");
    assert_eq!(
        reply.header("content-security-policy"),
        "default-src 'none'; sandbox"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_uploads_give_413_and_leave_no_spool_file_or_blob_row() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    // Over the edge's `http.body_limit` (1 MiB): the route bounds its own.
    let fx = start(&test, &dirs.local(", max_upload_bytes = 2097152"), MODEL).await;
    let big = vec![b'a'; 2_097_153];
    // Declared: refused before a byte is read (the few bytes sent would
    // fit).
    let reply = fx
        .post(&format!("{ASSETS}?filename=big.txt"))
        .by(Some("media"))
        .header("content-type", "text/plain")
        .header("content-length", "2097153")
        .raw(b"a few bytes")
        .send()
        .await;
    assert_eq!(
        (reply.status, reply.code()),
        (StatusCode::PAYLOAD_TOO_LARGE, "asset_too_large")
    );
    // Undeclared: stopped while it streams.
    let reply = upload(&fx, "big.txt", "text/plain", &big).await;
    assert_eq!(
        (reply.status, reply.code()),
        (StatusCode::PAYLOAD_TOO_LARGE, "asset_too_large")
    );
    assert!(dirs.spooled().is_empty(), "{:?}", dirs.spooled());
    assert!(blob_rows(&fx).await.is_empty());
    assert_eq!(dirs.objects(), 0);
    // At the limit it is stored, though larger than `http.body_limit`.
    let asset = uploaded(&fx, "big.txt", "text/plain", &big[1..]).await;
    assert_eq!(asset["size"], 2_097_152);
    assert!(dirs.spooled().is_empty());
}

/// Two uploads of the same bytes on `settings`' driver: two assets, one
/// blob row and one object.
async fn dedupe_on(test: &TestDatabase, dirs: &Dirs, settings: &str, objects: impl Fn() -> usize) {
    let fx = start(test, settings, MODEL).await;
    let bytes = png(32, 32, 4096);
    let a = uploaded(&fx, "a.png", "image/png", &bytes).await;
    let b = uploaded(&fx, "b.png", "image/png", &bytes).await;
    assert_ne!(a["id"], b["id"]);
    assert_eq!(a["sha256"], b["sha256"]);
    assert_eq!(
        blob_rows(&fx).await,
        [format!("stored:{}", a["sha256"].as_str().unwrap())]
    );
    assert_eq!(objects(), 1);
    // Each serves the same bytes under its own name.
    for asset in [&a, &b] {
        let file = format!("{ASSETS}/{}/file", asset["id"].as_str().unwrap());
        let reply = raw(&fx, &file, Some("media"), &[]).await;
        assert_eq!(reply.body, bytes);
        assert!(
            reply
                .header("content-disposition")
                .contains(asset["filename"].as_str().unwrap())
        );
    }
    assert!(dirs.spooled().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_bytes_give_two_assets_and_one_object_on_the_local_driver() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    dedupe_on(&test, &dirs, &dirs.local(""), || dirs.objects()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_bytes_give_two_assets_and_one_object_on_s3() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let mock = s3::MockS3::start().await;
    let dirs = Dirs::new();
    dedupe_on(&test, &dirs, &dirs.s3(&mock, ""), || mock.object_count()).await;
    // The object carries the type and a filename-less disposition for
    // redirect mode.
    let object = mock.object(&mock_key(&mock)).unwrap();
    assert_eq!(object.content_type.as_deref(), Some("image/png"));
    assert_eq!(object.content_disposition.as_deref(), Some("inline"));
}

/// The one object key the mock holds.
fn mock_key(mock: &s3::MockS3) -> String {
    let seen = mock.seen();
    let put = seen
        .iter()
        .find(|r| r.method == Method::PUT && r.path.contains("/content/app/sha256/"))
        .unwrap();
    put.path
        .strip_prefix(&format!("/{}/", s3::BUCKET))
        .unwrap()
        .to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_bytes_give_two_assets_and_one_object_on_an_external_s3_service() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let Some(settings) = dirs.external("") else {
        return;
    };
    let fx = start(&test, &settings, MODEL).await;
    let bytes = png(8, 8, 128);
    let a = uploaded(&fx, "a.png", "image/png", &bytes).await;
    let b = uploaded(&fx, "b.png", "image/png", &bytes).await;
    assert_eq!(a["sha256"], b["sha256"]);
    assert_eq!(blob_rows(&fx).await.len(), 1);
    let file = format!("{ASSETS}/{}/file", a["id"].as_str().unwrap());
    let reply = raw(&fx, &file, Some("media"), &[("range", "bytes=0-7")]).await;
    assert_eq!(reply.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(reply.body, &bytes[..8]);
    for asset in [&a, &b] {
        let uri = format!("{ASSETS}/{}?purge=true", asset["id"].as_str().unwrap());
        let reply = fx.call(Method::DELETE, &uri).by(Some("media")).send().await;
        assert_eq!(reply.status, StatusCode::NO_CONTENT);
    }
    execute(
        &fx.db,
        "UPDATE merezha_content.blobs SET orphaned_at = now() - interval '2 days'",
    )
    .await;
    assert_eq!(assets(&fx).cleanup(1000).await.unwrap().deleted, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivery_refuses_a_draft_only_asset_and_serves_it_after_publish() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let asset = uploaded(&fx, "team.txt", "text/plain", b"team photo, as text").await;
    let url = delivery(&asset);
    // Unreferenced, then referenced by a draft only: not found, even with
    // the right id and name.
    assert_eq!(
        raw(&fx, &url, None, &[]).await.status,
        StatusCode::NOT_FOUND
    );
    let article = article_with(&fx, asset["id"].as_str().unwrap()).await;
    let reply = raw(&fx, &url, Some("media"), &[]).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    assert_eq!(
        serde_json::from_slice::<Value>(&reply.body).unwrap()["error"]["code"],
        "asset_not_found"
    );
    // Published: served, anonymously too.
    fx.publish("article", &article, "en", 1).await;
    let reply = raw(&fx, &url, None, &[]).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body, b"team photo, as text");
    // The filename segment must match.
    let wrong = url.replace("team.txt", "other.txt");
    assert_eq!(
        raw(&fx, &wrong, None, &[]).await.status,
        StatusCode::NOT_FOUND
    );
    // Unpublished again: refused again.
    fx.unpublish("article", &article, "en", 1).await;
    assert_eq!(
        raw(&fx, &url, None, &[]).await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivery_follows_the_access_of_the_referencing_types() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let memo_file = uploaded(&fx, "memo.txt", "text/plain", b"for readers only").await;
    let note_file = uploaded(&fx, "note.txt", "text/plain", b"for principals").await;
    for (content_type, asset) in [("memo", &memo_file), ("note", &note_file)] {
        let id = id_of(
            &fx.post(&format!("/v1/content/manage/entries/{content_type}"))
                .by(Some("admin"))
                .json(&json!({"fields": {"title": "x", "file": {"id": asset["id"]}}}))
                .send()
                .await,
        );
        let reply = fx.publish(content_type, &id, "en", 1).await;
        assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    }
    // A permission type: only a principal granted it.
    let memo = delivery(&memo_file);
    assert_eq!(
        raw(&fx, &memo, None, &[]).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        raw(&fx, &memo, Some("editor"), &[]).await.status,
        StatusCode::NOT_FOUND
    );
    let reply = raw(&fx, &memo, Some("delivery_reader"), &[]).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.header("cache-control"), "private, no-cache");
    // An authenticated type: any principal.
    let note = delivery(&note_file);
    assert_eq!(
        raw(&fx, &note, None, &[]).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        raw(&fx, &note, Some("editor"), &[]).await.status,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_range_gives_a_correct_206() {
    let mock = s3::MockS3::start().await;
    for driver in ["local", "s3"] {
        // A database per driver: blobs deduplicate across hosts of one.
        let Some(test) = TestDatabase::create() else {
            return;
        };
        let dirs = Dirs::new();
        let settings = match driver {
            "local" => dirs.local(""),
            _ => dirs.s3(&mock, ""),
        };
        let fx = start(&test, &settings, MODEL).await;
        let text = b"0123456789abcdefghij";
        let asset = uploaded(&fx, "digits.txt", "text/plain", text).await;
        let article = article_with(&fx, asset["id"].as_str().unwrap()).await;
        fx.publish("article", &article, "en", 1).await;
        let url = delivery(&asset);
        for (range, start, end) in [
            ("bytes=2-5", 2usize, 6usize),
            ("bytes=15-", 15, 20),
            ("bytes=-4", 16, 20),
            ("bytes=18-400", 18, 20),
        ] {
            let reply = raw(&fx, &url, None, &[("range", range)]).await;
            assert_eq!(
                reply.status,
                StatusCode::PARTIAL_CONTENT,
                "{driver} {range}"
            );
            assert_eq!(reply.body, &text[start..end], "{driver} {range}");
            assert_eq!(
                reply.header("content-range"),
                format!("bytes {start}-{}/20", end - 1)
            );
            assert_eq!(reply.header("content-length"), (end - start).to_string());
            assert_eq!(reply.header("accept-ranges"), "bytes");
            assert_hardened(&reply);
        }
        // Past the end: 416 with the size.
        let reply = raw(&fx, &url, None, &[("range", "bytes=20-")]).await;
        assert_eq!(reply.status, StatusCode::RANGE_NOT_SATISFIABLE, "{driver}");
        assert_eq!(reply.header("content-range"), "bytes */20");
        assert_hardened(&reply);
        // Several ranges, or an `If-Range` for other bytes: the full body.
        for headers in [
            &[("range", "bytes=0-1,4-5")][..],
            &[("range", "bytes=0-1"), ("if-range", "\"other\"")],
        ] {
            let reply = raw(&fx, &url, None, headers).await;
            assert_eq!(reply.status, StatusCode::OK, "{driver} {headers:?}");
            assert_eq!(reply.body, text);
        }
        // Management reads take ranges too.
        let file = format!("{ASSETS}/{}/file", asset["id"].as_str().unwrap());
        let reply = raw(&fx, &file, Some("media"), &[("range", "bytes=0-0")]).await;
        assert_eq!(reply.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(reply.body, b"0");
        drop(fx);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_referenced_asset_deletes_only_with_force() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let asset = uploaded(&fx, "cover.png", "image/png", &png(2, 2, 8)).await;
    let id = asset["id"].as_str().unwrap().to_owned();
    let article = article_with(&fx, &id).await;
    fx.publish("article", &article, "en", 1).await;
    let reply = delete_asset(&fx, &id).await;
    assert_eq!(
        (reply.status, reply.code()),
        (StatusCode::CONFLICT, "asset_in_use")
    );
    let reply = delete_asset(&fx, &format!("{id}?purge=true")).await;
    assert_eq!(reply.code(), "asset_in_use");
    // Without the permission: refused before anything.
    let reply = fx
        .call(Method::DELETE, &format!("{ASSETS}/{id}?force=true"))
        .by(Some("editor"))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    let generation_before = generation(&fx.db, "app").await;
    let reply = delete_asset(&fx, &format!("{id}?force=true")).await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    assert!(generation(&fx.db, "app").await > generation_before);
    let deleted = events(&fx.db, "content.asset_deleted").await;
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0]["payload"]["forced"], true);
    assert_eq!(deleted[0]["payload"]["id"], id.as_str());
    // The references stay; delivery and saves no longer see the asset.
    assert_eq!(
        scalar(
            &fx.db,
            &format!("SELECT count(*) FROM merezha_content.refs WHERE target_id = '{id}'")
        )
        .await,
        2
    );
    assert_eq!(
        raw(&fx, &delivery(&asset), None, &[]).await.status,
        StatusCode::NOT_FOUND
    );
    let refused = fx
        .post("/v1/content/manage/entries/article")
        .json(&json!({"fields": {"title": "Again", "cover": {"id": id}}}))
        .send()
        .await;
    assert_eq!(
        refused.details(),
        [("/fields/cover".to_owned(), "asset_not_found".to_owned())]
    );
    let saved = fx
        .put(&variant("article", &article, "en"))
        .if_match(1)
        .json(&json!({"fields": {"title": "Edit", "cover": {"id": id}}}))
        .send()
        .await;
    assert_eq!(
        saved.details(),
        [("/fields/cover".to_owned(), "asset_not_found".to_owned())]
    );
    // An unreferenced asset deletes without `force`.
    let other = uploaded(&fx, "free.png", "image/png", &png(3, 3, 8)).await;
    let reply = fx
        .call(
            Method::DELETE,
            &format!("{ASSETS}/{}", other["id"].as_str().unwrap()),
        )
        .by(Some("media"))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_put_is_cleaned_after_the_lease() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    // Hold the assets table so the final transaction blocks after the put.
    let mut holder = fx.db.acquire(soon()).await.unwrap();
    let lock = holder.transaction().await.unwrap();
    lock.batch_execute("LOCK TABLE merezha_content.assets IN EXCLUSIVE MODE")
        .await
        .unwrap();
    let pid: i32 = lock
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let client = fx.client.clone();
    let task = tokio::spawn(async move {
        client
            .post(&format!("{ASSETS}?filename=crash.txt"))
            .by(Some("media"))
            .header("content-type", "text/plain")
            .raw(b"bytes put before a crash")
            .send()
            .await
    });
    let blocked = wait_blocked_by(&fx.db, pid).await;
    // The process "dies" here: the upload stops and its session goes.
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    execute(&fx.db, &format!("SELECT pg_terminate_backend({blocked})")).await;
    lock.rollback().await.unwrap();
    drop(holder);
    // The put happened and the claim stays `pending`; the spool file went
    // with the upload.
    let rows = blob_rows(&fx).await;
    assert_eq!(rows.len(), 1);
    assert!(rows[0].starts_with("pending:"), "{rows:?}");
    assert_eq!(dirs.objects(), 1);
    assert!(dirs.spooled().is_empty());
    // Not before the lease expires.
    let cleanup = assets(&fx);
    assert_eq!(cleanup.cleanup(1000).await.unwrap().deleted, 0);
    assert_eq!(blob_rows(&fx).await.len(), 1);
    execute(
        &fx.db,
        "UPDATE merezha_content.blobs SET pending_until = now() - interval '1 second'",
    )
    .await;
    assert_eq!(cleanup.cleanup(1000).await.unwrap().deleted, 1);
    assert!(blob_rows(&fx).await.is_empty());
    assert_eq!(dirs.objects(), 0);
    assert_eq!(
        scalar(&fx.db, "SELECT count(*) FROM merezha_content.assets").await,
        0
    );
}

/// `rounds` races of an upload against the cleanup of an orphaned blob
/// of the same bytes, past its grace; after each, every asset row's blob
/// is `stored` with its object. Returns how many rounds the cleanup
/// deleted the blob first.
async fn race(fx: &Fixture, rounds: usize) -> usize {
    let cleanup = assets(fx);
    let mut cleanup_first = 0;
    for round in 0..rounds {
        let bytes = format!("round {round} of the cleanup race").into_bytes();
        // An orphaned blob past its grace: uploaded, purged, aged.
        let first = uploaded(fx, "race.txt", "text/plain", &bytes).await;
        let sha256 = first["sha256"].as_str().unwrap().to_owned();
        let uri = format!("{ASSETS}/{}?purge=true", first["id"].as_str().unwrap());
        let reply = fx.call(Method::DELETE, &uri).by(Some("media")).send().await;
        assert_eq!(reply.status, StatusCode::NO_CONTENT);
        execute(
            &fx.db,
            &format!(
                "UPDATE merezha_content.blobs SET orphaned_at = now() - interval '2 days' \
                  WHERE sha256 = '{sha256}'"
            ),
        )
        .await;
        let client = fx.client.clone();
        let body = bytes.clone();
        let upload = tokio::spawn(async move {
            client
                .post(&format!("{ASSETS}?filename=race.txt"))
                .by(Some("media"))
                .header("content-type", "text/plain")
                .raw(&body)
                .send()
                .await
        });
        // Vary the interleaving: the sweep starts up to 2 ms later.
        let delay = Duration::from_micros((round % 8) as u64 * 250);
        let sweep = {
            let cleanup = cleanup.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                cleanup.cleanup(1000).await.unwrap()
            })
        };
        let reply = upload.await.unwrap();
        let report = sweep.await.unwrap();
        assert_eq!(
            reply.status,
            StatusCode::CREATED,
            "round {round}: {:?}",
            reply.body
        );
        if report.deleted > 0 {
            cleanup_first += 1;
        }
        assert!(assets_keep_their_blobs(fx).await, "round {round}");
        // Clear the round's asset (its blob stays orphaned, within grace).
        let uri = format!("{ASSETS}/{}?purge=true", reply.body["id"].as_str().unwrap());
        let reply = fx.call(Method::DELETE, &uri).by(Some("media")).send().await;
        assert_eq!(reply.status, StatusCode::NO_CONTENT);
    }
    cleanup_first
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cleanup_and_an_upload_of_the_same_bytes_race_200_times_and_every_asset_keeps_its_blob() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let first = race(&fx, 200).await;
    assert!(dirs.spooled().is_empty());
    eprintln!("local: the cleanup deleted first in {first} of 200 rounds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cleanup_and_an_upload_race_200_times_on_s3_with_slow_deletes() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let mock = s3::MockS3::start().await;
    // Object deletes take a while, so uploads meet blobs being deleted.
    mock.delay_deletes(Duration::from_millis(15));
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.s3(&mock, ""), MODEL).await;
    let first = race(&fx, 200).await;
    assert!(dirs.spooled().is_empty());
    eprintln!("s3: the cleanup deleted first in {first} of 200 rounds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_put_leaves_no_spool_file_blob_row_or_object() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let mock = s3::MockS3::start().await;
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.s3(&mock, ""), MODEL).await;
    // Every attempt of the put fails: blob_unavailable after the retries.
    mock.fail_puts(&[500; merezha_blob::MAX_RETRIES + 1]);
    let reply = upload(&fx, "lost.txt", "text/plain", b"never stored").await;
    assert_eq!(
        (reply.status, reply.code()),
        (StatusCode::SERVICE_UNAVAILABLE, "blob_unavailable")
    );
    assert_eq!(reply.headers["retry-after"], "1");
    assert!(dirs.spooled().is_empty());
    assert!(blob_rows(&fx).await.is_empty());
    assert_eq!(mock.object_count(), 0);
    assert_eq!(
        scalar(&fx.db, "SELECT count(*) FROM merezha_content.assets").await,
        0
    );
    // The next upload of the same bytes is stored.
    let asset = uploaded(&fx, "lost.txt", "text/plain", b"never stored").await;
    assert_eq!(asset["size"], 12);
    assert_eq!(mock.object_count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_admission_refuses_past_max_concurrent_uploads() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(", max_concurrent_uploads = 1"), MODEL).await;
    // An upload whose body is still arriving holds the one slot.
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(4);
    let body = Body::from_stream(tokio_stream(receiver));
    let request = axum::http::Request::builder()
        .method(Method::POST)
        .uri(format!("{ASSETS}?filename=slow.txt"))
        .header("x-test-principal", "media")
        .header("content-type", "text/plain")
        .body(body)
        .unwrap();
    let router = fx.client.router.clone();
    let slow = tokio::spawn(async move { router.oneshot(request).await.unwrap() });
    sender.send(Ok("first part, ".into())).await.unwrap();
    // It holds the slot once it spools.
    let deadline = Instant::now() + Duration::from_secs(10);
    while dirs.spooled().is_empty() {
        assert!(!slow.is_finished(), "the slow upload ended early");
        assert!(Instant::now() < deadline, "the slow upload never spooled");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let busy = upload(&fx, "fast.txt", "text/plain", b"second").await;
    assert_eq!(busy.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(busy.code(), "overloaded");
    assert_eq!(busy.headers["retry-after"], "1");
    sender.send(Ok("second part".into())).await.unwrap();
    drop(sender);
    let response = slow.await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    // The slot is free again.
    uploaded(&fx, "fast.txt", "text/plain", b"second").await;
}

/// A stream of the receiver's items.
fn tokio_stream<T: Send + 'static>(
    mut receiver: tokio::sync::mpsc::Receiver<T>,
) -> impl futures_util::Stream<Item = T> + Send {
    futures_util::stream::poll_fn(move |cx| receiver.poll_recv(cx))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uploads_validate_their_request_before_reading_the_body() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let reply = fx
        .post(ASSETS)
        .by(Some("media"))
        .header("content-type", "text/plain")
        .raw(b"x")
        .send()
        .await;
    assert_eq!(reply.code(), "invalid_query");
    assert_eq!(
        reply.details(),
        [("/filename".to_owned(), "required".to_owned())]
    );
    let reply = fx
        .post(&format!("{ASSETS}?filename=a.txt&path=../x"))
        .by(Some("media"))
        .header("content-type", "text/plain")
        .raw(b"x")
        .send()
        .await;
    assert_eq!(reply.code(), "invalid_query");
    let reply = fx
        .post(&format!("{ASSETS}?filename=a.txt"))
        .by(Some("editor"))
        .header("content-type", "text/plain")
        .raw(b"x")
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    let reply = fx
        .post(&format!("{ASSETS}?filename=a.txt"))
        .by(None)
        .header("content-type", "text/plain")
        .raw(b"x")
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    // Paths in the filename never reach a key or the stored name.
    let asset = uploaded(&fx, "..%2F..%2Fsecret%00.txt", "text/plain", b"x").await;
    assert_eq!(asset["filename"], "secret-.txt");
    let key = strings(&fx.db, "SELECT storage_key FROM merezha_content.blobs").await;
    assert_eq!(
        key,
        [format!(
            "content/app/sha256/{}/{}/{}",
            &asset["sha256"].as_str().unwrap()[..2],
            &asset["sha256"].as_str().unwrap()[2..4],
            asset["sha256"].as_str().unwrap()
        )]
    );
    assert!(dirs.spooled().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raster_dimensions_are_read_and_oversized_sides_refused() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let asset = uploaded(&fx, "wide.png", "image/png", &png(1600, 900, 32)).await;
    assert_eq!(
        (asset["width"].clone(), asset["height"].clone()),
        (json!(1600), json!(900))
    );
    for (width, height) in [(50_001, 10), (10, 60_000), (0, 10)] {
        let reply = upload(&fx, "huge.png", "image/png", &png(width, height, 32)).await;
        assert_eq!(reply.code(), "media_type_rejected", "{width}x{height}");
    }
    // A truncated header is malformed too.
    let reply = upload(&fx, "cut.png", "image/png", b"\x89PNG\r\n\x1a\n\0\0").await;
    assert_eq!(reply.code(), "media_type_rejected");
    // Text has no dimensions.
    let text = uploaded(&fx, "t.txt", "text/plain", b"plain").await;
    assert_eq!(text["width"], Value::Null);
    assert!(dirs.spooled().is_empty());
    assert_eq!(blob_rows(&fx).await.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asset_routes_list_read_patch_delete_undelete_and_purge() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let mut ids = Vec::new();
    for n in 0..5 {
        let asset = uploaded(
            &fx,
            &format!("photo-{n}.png"),
            "image/png",
            &png(n + 1, 1, 8),
        )
        .await;
        ids.push(asset["id"].as_str().unwrap().to_owned());
    }
    let text = uploaded(&fx, "readme.txt", "text/plain", b"read me").await;
    // The upload event: ids and metadata, never the filename.
    let uploaded_events = events(&fx.db, "content.asset_uploaded").await;
    assert_eq!(uploaded_events.len(), 6);
    let payload = &uploaded_events[5]["payload"];
    assert_eq!(payload["id"], text["id"]);
    assert_eq!(payload["content_type"], "text/plain");
    assert_eq!(payload["size"], 7);
    assert_eq!(payload["sha256"], text["sha256"]);
    assert_eq!(payload["scope"], "app");
    assert_eq!(payload["actor"]["subject"], "usr_media");
    assert!(!uploaded_events[5].to_string().contains("readme"));
    // The list: newest first, filtered, paged by cursor.
    let list = fx
        .get(&format!("{ASSETS}?content_type=image/*&limit=2"))
        .by(Some("asset_reader"))
        .send()
        .await;
    assert_eq!(list.status, StatusCode::OK, "{:?}", list.body);
    let mut seen: Vec<String> = Vec::new();
    let mut page = list.body;
    loop {
        for item in page["items"].as_array().unwrap() {
            assert_eq!(item["content_type"], "image/png");
            seen.push(item["id"].as_str().unwrap().to_owned());
        }
        let Some(cursor) = page["page"]["next_cursor"].as_str() else {
            break;
        };
        page = fx
            .get(&format!(
                "{ASSETS}?content_type=image/*&limit=2&cursor={cursor}"
            ))
            .by(Some("asset_reader"))
            .send()
            .await
            .body;
    }
    let mut newest_first = ids.clone();
    newest_first.reverse();
    assert_eq!(seen, newest_first);
    let reply = fx
        .get(&format!("{ASSETS}?filename_prefix=readme"))
        .by(Some("asset_reader"))
        .send()
        .await;
    assert_eq!(reply.body["items"].as_array().unwrap().len(), 1);
    let reply = fx
        .get(&format!("{ASSETS}?content_type=text/plain"))
        .by(Some("asset_reader"))
        .send()
        .await;
    assert_eq!(reply.body["items"][0]["id"], text["id"]);
    // A cursor of another listing is refused.
    let cursor = fx
        .get(&format!("{ASSETS}?limit=1"))
        .by(Some("asset_reader"))
        .send()
        .await
        .body["page"]["next_cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    let reply = fx
        .get(&format!("{ASSETS}?content_type=image/*&cursor={cursor}"))
        .by(Some("asset_reader"))
        .send()
        .await;
    assert_eq!(reply.code(), "invalid_cursor");
    // Read with usage: one draft ref, then a published one.
    let id = &ids[0];
    let article = article_with(&fx, id).await;
    let reply = fx
        .get(&format!("{ASSETS}/{id}"))
        .by(Some("asset_reader"))
        .send()
        .await;
    assert_eq!(reply.body["usage"], json!({"draft": 1, "published": 0}));
    assert_eq!(reply.headers["cache-control"], "private, no-store");
    fx.publish("article", &article, "en", 1).await;
    let reply = fx
        .get(&format!("{ASSETS}/{id}"))
        .by(Some("asset_reader"))
        .send()
        .await;
    assert_eq!(reply.body["usage"], json!({"draft": 1, "published": 1}));
    // Patch: a merge over title and focal point, an event, a new generation.
    let before = generation(&fx.db, "app").await;
    let reply = fx
        .patch(&format!("{ASSETS}/{id}"))
        .by(Some("media"))
        .json(&json!({"title": "Team", "focal_point": {"x": 0.5, "y": 0.35}}))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert_eq!(reply.body["title"], "Team");
    assert_eq!(reply.body["focal_point"], json!({"x": 0.5, "y": 0.35}));
    let reply = fx
        .patch(&format!("{ASSETS}/{id}"))
        .by(Some("media"))
        .json(&json!({"focal_point": null}))
        .send()
        .await;
    assert_eq!(reply.body["title"], "Team");
    assert_eq!(reply.body["focal_point"], Value::Null);
    assert_eq!(generation(&fx.db, "app").await, before + 2);
    assert_eq!(events(&fx.db, "content.asset_updated").await.len(), 2);
    let reply = fx
        .patch(&format!("{ASSETS}/{id}"))
        .by(Some("media"))
        .json(&json!({"focal_point": {"x": 2, "y": 0}, "title": 3}))
        .send()
        .await;
    assert_eq!(reply.code(), "validation_failed");
    assert_eq!(reply.details().len(), 2);
    let reply = fx
        .patch(&format!("{ASSETS}/{id}"))
        .by(Some("media"))
        .json(&json!({"filename": "renamed.png"}))
        .send()
        .await;
    assert_eq!(reply.code(), "invalid_json");
    // Soft delete, readable through management, undelete.
    let free = &ids[1];
    let reply = fx
        .call(Method::DELETE, &format!("{ASSETS}/{free}"))
        .by(Some("media"))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    let reply = fx
        .get(&format!("{ASSETS}/{free}"))
        .by(Some("asset_reader"))
        .send()
        .await;
    assert!(reply.body["deleted_at"].is_string());
    let file = raw(
        &fx,
        &format!("{ASSETS}/{free}/file"),
        Some("asset_reader"),
        &[],
    )
    .await;
    assert_eq!(file.status, StatusCode::OK);
    let listed = fx.get(ASSETS).by(Some("asset_reader")).send().await;
    assert!(!listed.text.contains(free.as_str()));
    let reply = fx
        .call(Method::DELETE, &format!("{ASSETS}/{free}"))
        .by(Some("media"))
        .send()
        .await;
    assert_eq!(reply.code(), "asset_not_found");
    let reply = fx
        .post(&format!("{ASSETS}/{free}/undelete"))
        .by(Some("media"))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body["deleted_at"], Value::Null);
    // Purge: the row goes, the blob is orphaned when no other asset uses it.
    let reply = fx
        .call(Method::DELETE, &format!("{ASSETS}/{free}?purge=true"))
        .by(Some("media"))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    assert_eq!(
        fx.get(&format!("{ASSETS}/{free}"))
            .by(Some("asset_reader"))
            .send()
            .await
            .code(),
        "asset_not_found"
    );
    assert_eq!(
        scalar(
            &fx.db,
            "SELECT count(*) FROM merezha_content.blobs WHERE state = 'orphaned'"
        )
        .await,
        1
    );
    // Ids of no asset, and not ids at all, are not found.
    for uri in [
        format!("{ASSETS}/0192f7a1-0b44-7aa2-8c55-1d3e9f20a7b4"),
        format!("{ASSETS}/not-an-id"),
        "/v1/content/assets/not-an-id/x.png".to_owned(),
    ] {
        assert_eq!(
            fx.get(&uri).by(Some("asset_reader")).send().await.status,
            StatusCode::NOT_FOUND,
            "{uri}"
        );
    }
    // Reads need `content.assets.read`.
    assert_eq!(
        fx.get(ASSETS).by(Some("editor")).send().await.status,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_asset_trash_is_purged_after_retention_and_cleanup_deletes_the_orphans() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(
        &test,
        &format!("trash = {{ retention = \"1d\" }}\n{}", dirs.local("")),
        MODEL,
    )
    .await;
    let old = uploaded(&fx, "old.txt", "text/plain", b"old bytes").await;
    let shared_a = uploaded(&fx, "a.txt", "text/plain", b"shared bytes").await;
    let shared_b = uploaded(&fx, "b.txt", "text/plain", b"shared bytes").await;
    let recent = uploaded(&fx, "recent.txt", "text/plain", b"recent bytes").await;
    for asset in [&old, &shared_a, &recent] {
        let uri = format!("{ASSETS}/{}", asset["id"].as_str().unwrap());
        fx.call(Method::DELETE, &uri).by(Some("media")).send().await;
    }
    execute(
        &fx.db,
        &format!(
            "UPDATE merezha_content.assets SET deleted_at = now() - interval '2 days' \
              WHERE id IN ('{}', '{}')",
            old["id"].as_str().unwrap(),
            shared_a["id"].as_str().unwrap()
        ),
    )
    .await;
    // The cleanup job purges the expired trash and frees `old`'s blob; the
    // shared blob stays for `b`; nothing is deleted before the grace.
    run_cleanup_job(&fx, "c1").await;
    assert_eq!(
        scalar(&fx.db, "SELECT count(*) FROM merezha_content.assets").await,
        2
    );
    let states = strings(
        &fx.db,
        "SELECT state FROM merezha_content.blobs ORDER BY state",
    )
    .await;
    assert_eq!(states, ["orphaned", "stored", "stored"]);
    assert_eq!(dirs.objects(), 3);
    execute(
        &fx.db,
        "UPDATE merezha_content.blobs SET orphaned_at = now() - interval '2 days' WHERE state = 'orphaned'",
    )
    .await;
    run_cleanup_job(&fx, "c2").await;
    assert_eq!(blob_rows(&fx).await.len(), 2);
    assert_eq!(dirs.objects(), 2);
    assert!(assets_keep_their_blobs(&fx).await);
    let _ = shared_b;
}

/// Run one `content.blob_cleanup` job on the fixture's jobs runtime and
/// wait for it to succeed.
async fn run_cleanup_job(fx: &Fixture, key: &str) {
    let id: i64 = fx
        .db
        .acquire(soon())
        .await
        .unwrap()
        .query_one(
            "INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload, payload_digest, \
                 max_attempts, correlation_id) \
             VALUES ('content', 'content.blob_cleanup', $1, '{}'::json, \
                     sha256('{}'::bytea), 3, 'c1') RETURNING id",
            &[&key],
        )
        .await
        .unwrap()
        .get(0);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let state = strings(
            &fx.db,
            &format!("SELECT state FROM merezha_jobs.jobs WHERE id = {id}"),
        )
        .await;
        if state == ["succeeded"] {
            return;
        }
        assert!(Instant::now() < deadline, "the cleanup job ended {state:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_blob_cleanup_is_declared_on_its_cron_schedule() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(", cleanup_cron = \"5 2 * * *\""), MODEL).await;
    let sql = "SELECT source || '|' || cron || '|' || timezone || '|' || queue || '|' || kind \
                 FROM merezha_jobs.schedules WHERE name = 'content.blob_cleanup'";
    let deadline = Instant::now() + Duration::from_secs(10);
    while strings(&fx.db, sql).await.is_empty() {
        assert!(Instant::now() < deadline, "the schedule was never stored");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        strings(&fx.db, sql).await,
        ["content|5 2 * * *|UTC|content|content.blob_cleanup"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redirect_mode_answers_a_presigned_url_bounded_by_the_ttl() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let mock = s3::MockS3::start().await;
    let dirs = Dirs::new();
    let fx = start(
        &test,
        &dirs.s3(&mock, ", serve = \"redirect\", signed_url_ttl = \"2m\""),
        MODEL,
    )
    .await;
    let asset = uploaded(&fx, "photo.png", "image/png", &png(5, 5, 64)).await;
    let url = delivery(&asset);
    // The same access rule: a draft-only asset is not redirected.
    let article = article_with(&fx, asset["id"].as_str().unwrap()).await;
    assert_eq!(
        raw(&fx, &url, None, &[]).await.status,
        StatusCode::NOT_FOUND
    );
    fx.publish("article", &article, "en", 1).await;
    let reply = raw(&fx, &url, None, &[]).await;
    assert_eq!(reply.status, StatusCode::FOUND);
    assert_eq!(reply.header("cache-control"), "private, max-age=90");
    let location = reply.header("location");
    assert!(location.starts_with(&mock.endpoint()), "{location}");
    assert!(location.contains("X-Amz-Expires=120"), "{location}");
    assert!(location.contains("/content/app/sha256/"), "{location}");
    // Management always proxies, as an attachment.
    let file = format!("{ASSETS}/{}/file", asset["id"].as_str().unwrap());
    let reply = raw(&fx, &file, Some("media"), &[]).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body, png(5, 5, 64));
    assert!(
        reply
            .header("content-disposition")
            .starts_with("attachment")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_clears_old_spool_files_and_readiness_includes_the_blob_store() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    std::fs::create_dir_all(dirs.spool()).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dirs.spool(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let old = dirs.spool().join("merezha-upload-old");
    let fresh = dirs.spool().join("merezha-upload-fresh");
    let foreign = dirs.spool().join("keep-me");
    for path in [&old, &fresh, &foreign] {
        std::fs::write(path, b"x").unwrap();
    }
    let two_hours_ago = std::time::SystemTime::now() - Duration::from_secs(7200);
    for path in [&old, &foreign] {
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(two_hours_ago)
            .unwrap();
    }
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let mut left = dirs.spooled();
    left.sort();
    assert_eq!(left, ["keep-me", "merezha-upload-fresh"]);
    assert!(eventually_ready(&fx.host).await);
    // The local root gone: not ready (past the 250 ms readiness cache).
    std::fs::remove_dir_all(dirs.root()).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!fx.host.readiness().check().await);
    // The store is the host's: every context's `blobs()` port.
    assert!(media(&fx).store().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_superseded_host_refuses_asset_writes() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let asset = uploaded(&fx, "a.txt", "text/plain", b"before").await;
    let id = asset["id"].as_str().unwrap().to_owned();
    execute(
        &fx.db,
        "UPDATE merezha_content.models SET state = 'superseded'; \
         INSERT INTO merezha_content.models (version, fingerprint, definition, state, activated_at) \
         VALUES (2, 'newer', '{}', 'active', now())",
    )
    .await;
    let before = snapshot(&fx.db).await;
    let blobs = blob_rows(&fx).await;
    for reply in [
        upload(&fx, "b.txt", "text/plain", b"after").await,
        fx.patch(&format!("{ASSETS}/{id}"))
            .by(Some("media"))
            .json(&json!({"title": "x"}))
            .send()
            .await,
        fx.call(Method::DELETE, &format!("{ASSETS}/{id}"))
            .by(Some("media"))
            .send()
            .await,
        fx.post(&format!("{ASSETS}/{id}/undelete"))
            .by(Some("media"))
            .send()
            .await,
    ] {
        assert_eq!(
            (reply.status, reply.code()),
            (StatusCode::SERVICE_UNAVAILABLE, "content_model_superseded")
        );
        assert_eq!(reply.headers["retry-after"], "1");
    }
    assert_eq!(snapshot(&fx.db).await, before);
    assert_eq!(blob_rows(&fx).await, blobs);
    assert!(dirs.spooled().is_empty());
    // Reads keep working on the loaded model.
    assert_eq!(
        fx.get(&format!("{ASSETS}/{id}"))
            .by(Some("asset_reader"))
            .send()
            .await
            .status,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_identical_uploads_never_conflict() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    for pair in 0..120 {
        let bytes = format!("pair {pair} uploaded twice at once").into_bytes();
        let send = |bytes: Vec<u8>| {
            let client = fx.client.clone();
            tokio::spawn(async move {
                client
                    .post(&format!("{ASSETS}?filename=pair.txt"))
                    .by(Some("media"))
                    .header("content-type", "text/plain")
                    .raw(&bytes)
                    .send()
                    .await
            })
        };
        let (a, b) = (send(bytes.clone()), send(bytes));
        let (a, b) = (a.await.unwrap(), b.await.unwrap());
        assert_eq!(a.status, StatusCode::CREATED, "pair {pair}: {:?}", a.body);
        assert_eq!(b.status, StatusCode::CREATED, "pair {pair}: {:?}", b.body);
        assert_eq!(a.body["sha256"], b.body["sha256"]);
    }
    assert_eq!(blob_rows(&fx).await.len(), 120);
    assert_eq!(dirs.objects(), 120);
    assert!(assets_keep_their_blobs(&fx).await);
}

/// An orphaned blob past its grace, as a purge leaves it; its sha256.
async fn aged_orphan(fx: &Fixture, bytes: &[u8]) -> String {
    let asset = uploaded(fx, "orphan.txt", "text/plain", bytes).await;
    let reply = delete_asset(fx, &format!("{}?purge=true", asset["id"].as_str().unwrap())).await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    let sha256 = asset["sha256"].as_str().unwrap().to_owned();
    execute(
        &fx.db,
        &format!(
            "UPDATE merezha_content.blobs SET orphaned_at = now() - interval '2 days' \
              WHERE sha256 = '{sha256}'"
        ),
    )
    .await;
    sha256
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cleanup_that_lost_its_lease_never_deletes_a_reuploaded_object() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let bytes = b"deleted, then uploaded again";
    aged_orphan(&fx, bytes).await;
    let assets = assets(&fx);
    // A first worker claims the blob, then stalls past its lease.
    let stale = assets.claim_due(10).await.unwrap();
    assert_eq!(stale.len(), 1);
    execute(
        &fx.db,
        "UPDATE merezha_content.blobs SET pending_until = now() - interval '1 second'",
    )
    .await;
    // A second worker takes it over and deletes it; the bytes come back.
    let report = assets.cleanup(10).await.unwrap();
    assert_eq!(report.deleted, 1);
    let again = uploaded(&fx, "again.txt", "text/plain", bytes).await;
    assert_eq!(dirs.objects(), 1);
    // The stale worker resumes: its claim is lost, nothing is touched.
    assert_eq!(
        assets.sweep(&stale[0]).await.unwrap(),
        merezha_content::media::Swept::Lost
    );
    assert_eq!(dirs.objects(), 1);
    assert!(assets_keep_their_blobs(&fx).await);
    let reply = raw(
        &fx,
        &format!("{ASSETS}/{}/file", again["id"].as_str().unwrap()),
        Some("media"),
        &[],
    )
    .await;
    assert_eq!(reply.body, bytes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cleanup_retakes_a_deletion_a_crash_abandoned() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    aged_orphan(&fx, b"a deletion that died").await;
    // A worker set `deleting` and died: within its lease nothing happens.
    execute(
        &fx.db,
        "UPDATE merezha_content.blobs SET state = 'deleting', pending_until = now() + interval '5 minutes'",
    )
    .await;
    let assets = assets(&fx);
    assert_eq!(assets.cleanup(10).await.unwrap().deleted, 0);
    assert_eq!(dirs.objects(), 1);
    // Past it, the next run finishes the job.
    execute(
        &fx.db,
        "UPDATE merezha_content.blobs SET pending_until = now() - interval '1 second'",
    )
    .await;
    assert_eq!(assets.cleanup(10).await.unwrap().deleted, 1);
    assert!(blob_rows(&fx).await.is_empty());
    assert_eq!(dirs.objects(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upload_takes_over_an_expired_deletion_and_waits_for_a_live_one() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let bytes = b"bytes whose deletion stalled";
    aged_orphan(&fx, bytes).await;
    // A live deletion: the upload waits, then gives up.
    execute(
        &fx.db,
        "UPDATE merezha_content.blobs SET state = 'deleting', pending_until = now() + interval '5 minutes'",
    )
    .await;
    let started = Instant::now();
    let reply = upload(&fx, "x.txt", "text/plain", bytes).await;
    assert_eq!(reply.code(), "blob_unavailable");
    assert!(started.elapsed() >= Duration::from_secs(2));
    // Its worker died (the lease is over): the upload takes the row over.
    execute(
        &fx.db,
        "UPDATE merezha_content.blobs SET pending_until = now() - interval '1 second'",
    )
    .await;
    let asset = uploaded(&fx, "x.txt", "text/plain", bytes).await;
    assert_eq!(asset["size"], bytes.len());
    assert_eq!(
        blob_rows(&fx).await,
        [format!("stored:{}", asset["sha256"].as_str().unwrap())]
    );
    assert!(assets_keep_their_blobs(&fx).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_give_up_leaves_the_blob_for_the_next_upload() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let mock = s3::MockS3::start().await;
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.s3(&mock, ""), MODEL).await;
    // The put fails, and so does the give-up's object delete.
    mock.fail_puts(&[500; merezha_blob::MAX_RETRIES + 1]);
    mock.fail_deletes(&[500; merezha_blob::MAX_RETRIES + 1]);
    let bytes = b"put and delete both failed";
    let reply = upload(&fx, "f.txt", "text/plain", bytes).await;
    assert_eq!(reply.code(), "blob_unavailable");
    // The row is back to `pending` with its lease over, not `deleting`.
    assert_eq!(
        strings(
            &fx.db,
            "SELECT state || ':' || (pending_until <= now())::text FROM merezha_content.blobs"
        )
        .await,
        ["pending:true"]
    );
    assert!(dirs.spooled().is_empty());
    // The next upload joins it and puts again, at once.
    let started = Instant::now();
    let asset = uploaded(&fx, "f.txt", "text/plain", bytes).await;
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(mock.object_count(), 1);
    assert!(assets_keep_their_blobs(&fx).await);
    assert_eq!(asset["size"], bytes.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_final_transaction_gives_up_the_blob_it_put() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    execute(
        &fx.db,
        "CREATE FUNCTION public.refuse_assets() RETURNS trigger LANGUAGE plpgsql AS \
           $$ BEGIN RAISE EXCEPTION 'refused'; END $$; \
         CREATE TRIGGER refuse BEFORE INSERT ON merezha_content.assets \
           FOR EACH ROW EXECUTE FUNCTION public.refuse_assets()",
    )
    .await;
    let reply = upload(&fx, "t.txt", "text/plain", b"put, then the insert fails").await;
    assert!(!reply.status.is_success(), "{:?}", reply.body);
    // The upload had put its object; giving up removed it and its row.
    assert!(blob_rows(&fx).await.is_empty());
    assert_eq!(dirs.objects(), 0);
    assert!(dirs.spooled().is_empty());
    execute(&fx.db, "DROP TRIGGER refuse ON merezha_content.assets").await;
    uploaded(&fx, "t.txt", "text/plain", b"put, then the insert fails").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_give_up_never_releases_a_claim_another_upload_extended() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let mock = s3::MockS3::start().await;
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.s3(&mock, ""), MODEL).await;
    mock.delay_puts(Duration::from_millis(500));
    mock.fail_puts(&[500; merezha_blob::MAX_RETRIES + 1]);
    let client = fx.client.clone();
    let failing = tokio::spawn(async move {
        client
            .post(&format!("{ASSETS}?filename=s.txt"))
            .by(Some("media"))
            .header("content-type", "text/plain")
            .raw(b"shared claim")
            .send()
            .await
    });
    // Once its claim is in, a second upload of the same bytes joins it
    // and extends the lease (as `BLOB_EXTEND` does).
    wait_for(
        &fx.db,
        "SELECT count(*)::text FROM merezha_content.blobs WHERE state = 'pending'",
        "1",
    )
    .await;
    execute(
        &fx.db,
        "UPDATE merezha_content.blobs SET pending_until = pending_until + interval '1 minute'",
    )
    .await;
    let reply = failing.await.unwrap();
    assert_eq!(reply.code(), "blob_unavailable");
    // The claim is no longer only the failed upload's: it stays.
    assert_eq!(
        strings(&fx.db, "SELECT state FROM merezha_content.blobs").await,
        ["pending"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joined_upload_extends_the_lease_so_cleanup_waits_for_it() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let mock = s3::MockS3::start().await;
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.s3(&mock, ""), MODEL).await;
    let bytes = b"a crashed claim, joined";
    // What a crashed upload leaves: `pending`, its lease over.
    let asset = uploaded(&fx, "c.txt", "text/plain", bytes).await;
    delete_asset(
        &fx,
        &format!("{}?purge=true", asset["id"].as_str().unwrap()),
    )
    .await;
    execute(
        &fx.db,
        "UPDATE merezha_content.blobs SET state = 'pending', orphaned_at = NULL, \
                pending_until = now() - interval '1 second'",
    )
    .await;
    mock.delay_puts(Duration::from_millis(1500));
    let client = fx.client.clone();
    let joined = tokio::spawn(async move {
        client
            .post(&format!("{ASSETS}?filename=c.txt"))
            .by(Some("media"))
            .header("content-type", "text/plain")
            .raw(bytes)
            .send()
            .await
    });
    // While its put is in flight the lease is the joiner's, not expired.
    wait_for(
        &fx.db,
        "SELECT (pending_until > now())::text FROM merezha_content.blobs",
        "true",
    )
    .await;
    assert_eq!(assets(&fx).cleanup(10).await.unwrap().deleted, 0);
    let reply = joined.await.unwrap();
    assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
    assert!(assets_keep_their_blobs(&fx).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_purge_racing_an_upload_of_the_same_bytes_never_orphans_its_blob() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs.local(""), MODEL).await;
    let bytes = b"purged while uploaded again";
    let old = uploaded(&fx, "old.txt", "text/plain", bytes).await;
    // Trashed first, so its purge appends no event.
    delete_asset(&fx, old["id"].as_str().unwrap()).await;
    // Hold the outbox: the upload's final transaction stops at its event,
    // with its asset inserted and the blob row locked.
    let mut holder = fx.db.acquire(soon()).await.unwrap();
    let lock = holder.transaction().await.unwrap();
    lock.batch_execute("LOCK TABLE merezha_jobs.outbox IN EXCLUSIVE MODE")
        .await
        .unwrap();
    let pid: i32 = lock
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let client = fx.client.clone();
    let upload = tokio::spawn(async move {
        client
            .post(&format!("{ASSETS}?filename=new.txt"))
            .by(Some("media"))
            .header("content-type", "text/plain")
            .raw(bytes)
            .send()
            .await
    });
    let uploader = wait_blocked_by(&fx.db, pid).await;
    // The purge of the old asset meets the locked blob and waits.
    let client = fx.client.clone();
    let old_id = old["id"].as_str().unwrap().to_owned();
    let purge = tokio::spawn(async move {
        client
            .call(Method::DELETE, &format!("{ASSETS}/{old_id}?purge=true"))
            .by(Some("media"))
            .send()
            .await
    });
    wait_blocked_by(&fx.db, uploader).await;
    lock.commit().await.unwrap();
    drop(holder);
    assert_eq!(upload.await.unwrap().status, StatusCode::CREATED);
    assert_eq!(purge.await.unwrap().status, StatusCode::NO_CONTENT);
    // The purge saw the new asset after the lock: the blob stays stored.
    assert_eq!(
        strings(&fx.db, "SELECT state FROM merezha_content.blobs").await,
        ["stored"]
    );
    assert!(assets_keep_their_blobs(&fx).await);
}
