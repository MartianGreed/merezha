//! Issue #46 over the axum router and a real database: delivery expands
//! every asset reference into its metadata in one batched statement,
//! `"asset": null` after a forced delete, never across organization scope
//! or stage or past the referencing types' access, never on a deleted
//! organization's path; ETags follow the asset metadata; and
//! `delivery.public_base_url` makes asset and `Location` URLs absolute.
mod http;

use axum::http::{Method, StatusCode};
use http::*;
use merezha_postgres::TestDatabase;
use merezha_telemetry::testing::Harness;
use serde_json::{Value, json};

const MODEL: &str = r#"
version = 1
locales = [{ code = "en", default = true }, { code = "fr", fallback = "en" }]

[components.figure]
fields = [{ name = "image", type = "asset" }, { name = "caption", type = "string" }]

[types.page]
access = "public"
title_field = "title"
fields = [
  { name = "title", type = "string", required = true },
  { name = "cover", type = "asset" },
  { name = "gallery", type = "asset", cardinality = "many" },
  { name = "body", type = "rich_text", embeds = ["asset", "figure"] },
  { name = "figures", type = "blocks", components = ["figure"] },
  { name = "author", type = "reference", to = ["author"] },
  { name = "brief", type = "reference", to = ["secret"] },
]

[types.author]
access = "public"
fields = [{ name = "name", type = "string" }, { name = "photo", type = "asset" }]

[types.secret]
access = { permission = "content.delivery.read" }
fields = [{ name = "title", type = "string" }, { name = "file", type = "asset" }]

# An organization type that references entries: the application
# generation is in its tags anyway.
[types.help]
scope = "organization"
access = "public"
fields = [
  { name = "title", type = "string", required = true },
  { name = "image", type = "asset" },
  { name = "page", type = "reference", to = ["page"] },
]

# An organization type with assets and no entry reference: the
# application generation enters its tags for the application assets.
[types.badge]
scope = "organization"
access = "public"
fields = [{ name = "title", type = "string", required = true }, { name = "image", type = "asset" }]
"#;

const C: &str = "/v1/content";

fn media(dir: &tempfile::TempDir) -> String {
    format!(
        "media = {{ driver = \"local\", local = {{ root = {:?} }}, spool_dir = {:?} }}\n",
        dir.path().join("media"),
        dir.path().join("spool")
    )
}

async fn start(test: &TestDatabase, dir: &tempfile::TempDir, extra: &str) -> (Fixture, Harness) {
    let harness = Harness::new();
    let settings = format!("{}{extra}", media(dir));
    let fx = start_metered_organizations(test, &settings, MODEL, &harness).await;
    (fx, harness)
}

/// A PNG of `width` by `height` with `pad` bytes after its header.
fn png(width: u32, height: u32, pad: usize) -> Vec<u8> {
    let mut out = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    out.extend_from_slice(&width.to_be_bytes());
    out.extend_from_slice(&height.to_be_bytes());
    out.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
    out.extend((0..pad).map(|i| (i % 251) as u8));
    out
}

/// The asset management path of `who`'s scope.
fn assets_of(who: &str) -> String {
    match who {
        "key_a" => format!("{C}/o/{ORG_A}/manage/assets"),
        "key_b" => format!("{C}/o/{ORG_B}/manage/assets"),
        _ => format!("{C}/manage/assets"),
    }
}

/// Upload a `width` by `height` PNG in `who`'s scope; the asset JSON.
async fn upload(fx: &Fixture, who: &str, name: &str, width: u32, height: u32) -> Value {
    let reply = fx
        .post(&format!("{}?filename={name}", assets_of(who)))
        .by(Some(who))
        .header("content-type", "image/png")
        .raw(&png(width, height, name.len() * 7))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
    reply.body
}

async fn create(fx: &Fixture, who: &str, content_type: &str, fields: Value) -> String {
    let reply = fx
        .post(&format!("{C}/manage/entries/{content_type}"))
        .by(Some(who))
        .json(&json!({ "fields": fields }))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
    id_of(&reply)
}

async fn publish(fx: &Fixture, who: &str, content_type: &str, id: &str, revision: i64) {
    let reply = fx
        .post(&format!("{}/publish", variant(content_type, id, "en")))
        .by(Some(who))
        .if_match(revision)
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
}

async fn published(fx: &Fixture, who: &str, content_type: &str, fields: Value) -> String {
    let id = create(fx, who, content_type, fields).await;
    publish(fx, who, content_type, &id, 1).await;
    id
}

/// A delivery read as `who` (`None` anonymous).
async fn read(fx: &Fixture, uri: &str, who: Option<&str>) -> Reply {
    let reply = fx.get(uri).by(who).send().await;
    assert_eq!(reply.status, StatusCode::OK, "{uri}: {}", reply.text);
    reply
}

/// The `asset` member delivery gives `asset` at `url`.
fn metadata(asset: &Value, url: &str) -> Value {
    json!({
        "id": asset["id"],
        "filename": asset["filename"],
        "content_type": asset["content_type"],
        "size": asset["size"],
        "width": asset["width"],
        "height": asset["height"],
        "url": url,
    })
}

/// The relative delivery path of an application asset.
fn app_url(asset: &Value) -> String {
    format!(
        "{C}/assets/{}/{}",
        str_of(&asset["id"]),
        str_of(&asset["filename"])
    )
}
/// The delivery path of an asset on organization `org`'s path.
fn org_url(org: &str, asset: &Value) -> String {
    format!(
        "{C}/o/{org}/assets/{}/{}",
        str_of(&asset["id"]),
        str_of(&asset["filename"])
    )
}
fn str_of(value: &Value) -> &str {
    value.as_str().unwrap()
}

/// The registered content statements run on `collection` so far.
fn statements_on(harness: &Harness, collection: &str) -> f64 {
    harness
        .metric("db.client.operation.duration")
        .points()
        .iter()
        .filter(|p| {
            p.attributes.get("db.collection.name").map(String::as_str) == Some(collection)
                && p.attributes.get("merezha.module").map(String::as_str) == Some("content")
        })
        .map(|p| p.value)
        .sum()
}
const ASSETS_TABLE: &str = "merezha_content.assets";

/// `GET uri` as `who`, with the number of asset statements it ran.
async fn counted(fx: &Fixture, harness: &Harness, uri: &str, who: Option<&str>) -> (Reply, f64) {
    let before = statements_on(harness, ASSETS_TABLE);
    let reply = read(fx, uri, who).await;
    (reply, statements_on(harness, ASSETS_TABLE) - before)
}

/// Plant `asset` as the `published` value of `field` of entry `id`, with
/// a `published` ref recorded in `scope`, behind validation.
async fn plant(fx: &Fixture, scope: &str, id: &str, field: &str, asset: &str, with_ref: bool) {
    execute(
        &fx.db,
        &format!(
            "UPDATE merezha_content.published \
                SET data = jsonb_set(data, '{{{field}}}', jsonb_build_object('id', '{asset}')) \
              WHERE entry_id = '{id}'"
        ),
    )
    .await;
    if with_ref {
        execute(
            &fx.db,
            &format!(
                "INSERT INTO merezha_content.refs \
                   (scope, from_entry, from_locale, stage, path, target_kind, target_id) \
                 VALUES ('{scope}', '{id}', 'en', 'published', '/fields/{field}', 'asset', '{asset}')"
            ),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivery_expands_every_asset_reference_in_one_batched_statement() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let (fx, harness) = start(&test, &dir, "").await;
    let mut assets = Vec::new();
    for n in 0..26u32 {
        assets.push(upload(&fx, "app_key", &format!("a{n}.png"), 16 + n, 9 + n).await);
    }
    let (a, b, c) = (&assets[0], &assets[1], &assets[2]);
    let author = published(
        &fx,
        "app_key",
        "author",
        json!({"name": "Ada", "photo": {"id": c["id"]}}),
    )
    .await;
    let gallery: Vec<Value> = assets.iter().map(|x| json!({"id": x["id"]})).collect();
    let mut pages = Vec::new();
    for n in 0..40 {
        pages.push(
            published(
                &fx,
                "app_key",
                "page",
                json!({
                    "title": format!("Page {n}"),
                    "cover": {"id": a["id"], "alt": "Cover"},
                    "gallery": gallery,
                    "body": [{"_type": "asset", "_key": "e1", "id": b["id"], "alt": "Inline"}],
                    "figures": [{"_type": "figure", "_key": "f1", "image": {"id": c["id"]}, "caption": "Fig"}],
                    "author": {"id": author, "type": "author"},
                }),
            )
            .await,
        );
    }

    // One entry: every asset reference, wherever it sits, gains its
    // metadata, and a populated entry's too; one statement for them all.
    let (reply, statements) = counted(
        &fx,
        &harness,
        &format!("{C}/page/{}?populate=author", pages[0]),
        None,
    )
    .await;
    assert_eq!(statements, 1.0);
    let fields = &reply.body["fields"];
    assert_eq!(
        fields["cover"],
        json!({"id": a["id"], "alt": "Cover", "asset": metadata(a, &app_url(a))})
    );
    assert_eq!(fields["gallery"].as_array().unwrap().len(), 26);
    for (item, asset) in fields["gallery"].as_array().unwrap().iter().zip(&assets) {
        assert_eq!(item["asset"], metadata(asset, &app_url(asset)));
    }
    assert_eq!(fields["body"][0]["asset"], metadata(b, &app_url(b)));
    assert_eq!(fields["body"][0]["alt"], "Inline");
    assert_eq!(
        fields["figures"][0]["image"]["asset"],
        metadata(c, &app_url(c))
    );
    assert_eq!(fields["figures"][0]["caption"], "Fig");
    assert_eq!(
        fields["author"]["entry"]["fields"]["photo"]["asset"],
        metadata(c, &app_url(c))
    );
    assert_eq!(a["width"], 16);
    assert_eq!(fields["cover"]["asset"]["height"], 9);
    // The URL is the asset route's, which serves it.
    let bytes = fx.get(&app_url(a)).by(None).send().await;
    assert_eq!(bytes.status, StatusCode::OK);

    // A page of 38: 38 x 26 = 988 (entry, asset) pairs, one statement.
    let (reply, statements) = counted(&fx, &harness, &format!("{C}/page?limit=38"), None).await;
    assert_eq!(statements, 1.0);
    let items = reply.body["items"].as_array().unwrap();
    assert_eq!(items.len(), 38);
    for item in items {
        assert_eq!(item["fields"]["cover"]["asset"], metadata(a, &app_url(a)));
        assert_eq!(
            item["fields"]["gallery"][25]["asset"],
            metadata(&assets[25], &app_url(&assets[25]))
        );
    }
    // 40 x 26 = 1040 pairs: two statements of at most 1000 pairs, never
    // one per entry or per asset.
    let (reply, statements) = counted(&fx, &harness, &format!("{C}/page?limit=40"), None).await;
    assert_eq!(statements, 2.0);
    assert!(
        reply.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|i| i["fields"]["figures"][0]["image"]["asset"] == metadata(c, &app_url(c)))
    );
    // An entry without asset references reads none.
    let bare = published(&fx, "app_key", "page", json!({"title": "Bare"})).await;
    let (_, statements) = counted(&fx, &harness, &format!("{C}/page/{bare}"), None).await;
    assert_eq!(statements, 0.0);
    // Management reads show references as stored.
    let draft = fx
        .get(&variant("page", &pages[0], "en"))
        .by(Some("app_key"))
        .send()
        .await;
    assert_eq!(
        draft.body["fields"]["cover"],
        json!({"id": a["id"], "alt": "Cover"})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forced_delete_delivers_asset_null_and_asset_changes_change_the_etag() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let (fx, _harness) = start(&test, &dir, "").await;
    let x = upload(&fx, "app_key", "x.png", 40, 30).await;
    let x_id = str_of(&x["id"]).to_owned();
    let page = published(
        &fx,
        "app_key",
        "page",
        json!({"title": "Team", "cover": {"id": x_id, "alt": "Team"}, "gallery": [{"id": x_id}]}),
    )
    .await;
    let uri = format!("{C}/page/{page}");
    let first = read(&fx, &uri, None).await;
    assert_eq!(
        first.body["fields"]["cover"]["asset"],
        metadata(&x, &app_url(&x))
    );
    let tag = first.etag();

    // Forced: the refs stay, delivery shows `"asset": null`, and the
    // previous tag no longer matches.
    let deleted = fx
        .call(
            Method::DELETE,
            &format!("{C}/manage/assets/{x_id}?force=true"),
        )
        .by(Some("app_key"))
        .send()
        .await;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT, "{}", deleted.text);
    let after = fx
        .get(&uri)
        .by(None)
        .header("if-none-match", &tag)
        .send()
        .await;
    assert_eq!(after.status, StatusCode::OK, "{}", after.text);
    assert_eq!(
        after.body["fields"]["cover"],
        json!({"id": x_id, "alt": "Team", "asset": null})
    );
    assert_eq!(
        after.body["fields"]["gallery"],
        json!([{"id": x_id, "asset": null}])
    );
    assert_ne!(after.etag(), tag);
    let gone = after.etag();
    // The bytes are gone from delivery too.
    assert_eq!(
        fx.get(&app_url(&x)).by(None).send().await.status,
        StatusCode::NOT_FOUND
    );

    // Undeleted: back, under a new tag.
    let back = fx
        .post(&format!("{C}/manage/assets/{x_id}/undelete"))
        .by(Some("app_key"))
        .send()
        .await;
    assert_eq!(back.status, StatusCode::OK);
    let restored = read(&fx, &uri, None).await;
    assert_eq!(
        restored.body["fields"]["cover"]["asset"],
        metadata(&x, &app_url(&x))
    );
    assert_ne!(restored.etag(), gone);
    // A metadata patch bumps the generation: a new tag.
    let patched = fx
        .patch(&format!("{C}/manage/assets/{x_id}"))
        .by(Some("app_key"))
        .json(&json!({"title": "Renamed"}))
        .send()
        .await;
    assert_eq!(patched.status, StatusCode::OK, "{}", patched.text);
    let renamed = read(&fx, &uri, None).await;
    assert_ne!(renamed.etag(), restored.etag());
    // Purged with force: still a reference, `null` for good.
    let purged = fx
        .call(
            Method::DELETE,
            &format!("{C}/manage/assets/{x_id}?force=true&purge=true"),
        )
        .by(Some("app_key"))
        .send()
        .await;
    assert_eq!(purged.status, StatusCode::NO_CONTENT);
    let last = read(&fx, &uri, None).await;
    assert_eq!(last.body["fields"]["cover"]["asset"], Value::Null);
    assert_ne!(last.etag(), renamed.etag());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asset_expansion_never_crosses_organizations_stages_access_or_a_deleted_organization() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let (fx, harness) = start(&test, &dir, "").await;
    let shared = upload(&fx, "app_key", "shared.png", 20, 10).await;
    let mine = upload(&fx, "key_a", "mine.png", 30, 10).await;
    let theirs = upload(&fx, "key_b", "theirs.png", 50, 10).await;
    let page = published(
        &fx,
        "app_key",
        "page",
        json!({"title": "Site", "cover": {"id": shared["id"]}}),
    )
    .await;
    let help = published(
        &fx,
        "key_a",
        "help",
        json!({"title": "Help", "image": {"id": mine["id"]}, "page": {"id": page, "type": "page"}}),
    )
    .await;
    let badge = published(
        &fx,
        "key_a",
        "badge",
        json!({"title": "Badge", "image": {"id": shared["id"]}}),
    )
    .await;

    // On A's path: A's asset and the application's under A's asset path
    // (A's refs decide there); a populated application entry's under the
    // application's (the application's refs decide there). Each URL serves.
    let reply = read(
        &fx,
        &format!("{C}/o/{ORG_A}/help/{help}?populate=page"),
        None,
    )
    .await;
    let fields = &reply.body["fields"];
    assert_eq!(
        fields["image"]["asset"],
        metadata(&mine, &org_url(ORG_A, &mine))
    );
    assert_eq!(
        fields["page"]["entry"]["fields"]["cover"]["asset"],
        metadata(&shared, &app_url(&shared))
    );
    let reply = read(&fx, &format!("{C}/o/{ORG_A}/badge/{badge}"), None).await;
    assert_eq!(
        reply.body["fields"]["image"]["asset"],
        metadata(&shared, &org_url(ORG_A, &shared))
    );
    for url in [
        org_url(ORG_A, &mine),
        app_url(&shared),
        org_url(ORG_A, &shared),
    ] {
        assert_eq!(
            fx.get(&url).by(None).send().await.status,
            StatusCode::OK,
            "{url}"
        );
    }

    // An application asset's change reaches an organization tag even for
    // a type without entry references.
    let badge_uri = format!("{C}/o/{ORG_A}/badge/{badge}");
    let tag = read(&fx, &badge_uri, None).await.etag();
    let patched = fx
        .patch(&format!("{C}/manage/assets/{}", str_of(&shared["id"])))
        .by(Some("app_key"))
        .json(&json!({"title": "Shared"}))
        .send()
        .await;
    assert_eq!(patched.status, StatusCode::OK, "{}", patched.text);
    let not_modified = fx
        .get(&badge_uri)
        .by(None)
        .header("if-none-match", &tag)
        .send()
        .await;
    assert_eq!(not_modified.status, StatusCode::OK);
    assert_ne!(not_modified.etag(), tag);

    // B's asset planted into A's published badge, its ref too: never
    // expanded on A's path, and never served there.
    let theirs_id = str_of(&theirs["id"]);
    plant(
        &fx,
        &format!("org:{ORG_A}"),
        &badge,
        "image",
        theirs_id,
        true,
    )
    .await;
    let reply = read(&fx, &badge_uri, None).await;
    assert_eq!(
        reply.body["fields"]["image"],
        json!({"id": theirs_id, "asset": null})
    );
    assert!(!reply.text.contains("theirs.png"), "{}", reply.text);
    assert_eq!(
        fx.get(&org_url(ORG_A, &theirs))
            .by(None)
            .send()
            .await
            .status,
        StatusCode::NOT_FOUND
    );
    // A's asset planted into an application page: never on the
    // application's path.
    let mine_id = str_of(&mine["id"]);
    let other = published(&fx, "app_key", "page", json!({"title": "Other"})).await;
    plant(&fx, "app", &other, "cover", mine_id, true).await;
    let reply = read(&fx, &format!("{C}/page/{other}"), None).await;
    assert_eq!(
        reply.body["fields"]["cover"],
        json!({"id": mine_id, "asset": null})
    );
    // Nor when A's own response populates that application page: an
    // application entry never shows an organization's asset.
    let via = published(
        &fx,
        "key_a",
        "help",
        json!({"title": "Via", "page": {"id": other, "type": "page"}}),
    )
    .await;
    let reply = read(
        &fx,
        &format!("{C}/o/{ORG_A}/help/{via}?populate=page"),
        None,
    )
    .await;
    assert_eq!(
        reply.body["fields"]["page"]["entry"]["fields"]["cover"],
        json!({"id": mine_id, "asset": null})
    );

    // Stage: an asset only a draft references is not expanded into the
    // published response (the asset route would not serve it), even
    // planted; preview, which reads the draft, shows it.
    let draft_only = upload(&fx, "app_key", "draft.png", 12, 12).await;
    let draft_id = str_of(&draft_only["id"]);
    let staged = published(&fx, "app_key", "page", json!({"title": "Staged"})).await;
    let saved = fx
        .put(&variant("page", &staged, "en"))
        .by(Some("app_key"))
        .if_match(1)
        .json(&json!({"fields": {"title": "Staged", "cover": {"id": draft_id}}}))
        .send()
        .await;
    assert_eq!(saved.status, StatusCode::OK, "{}", saved.text);
    plant(&fx, "app", &staged, "cover", draft_id, false).await;
    let reply = read(&fx, &format!("{C}/page/{staged}"), None).await;
    assert_eq!(
        reply.body["fields"]["cover"],
        json!({"id": draft_id, "asset": null})
    );
    assert_eq!(
        fx.get(&app_url(&draft_only)).by(None).send().await.status,
        StatusCode::NOT_FOUND
    );
    let preview = read(
        &fx,
        &format!("{C}/page/{staged}?stage=draft"),
        Some("app_key"),
    )
    .await;
    assert_eq!(
        preview.body["fields"]["cover"]["asset"],
        metadata(&draft_only, &app_url(&draft_only))
    );

    // Access: a referenced entry the caller may not read is `missing`,
    // and its assets are never expanded; a reader of it gets them.
    let file = upload(&fx, "app_key", "file.png", 8, 8).await;
    let secret = published(
        &fx,
        "app_key",
        "secret",
        json!({"title": "Secret", "file": {"id": file["id"]}}),
    )
    .await;
    let front = published(
        &fx,
        "app_key",
        "page",
        json!({"title": "Front", "brief": {"id": secret, "type": "secret"}}),
    )
    .await;
    let uri = format!("{C}/page/{front}?populate=brief");
    let anonymous = read(&fx, &uri, None).await;
    assert_eq!(anonymous.body["fields"]["brief"]["missing"], true);
    assert!(!anonymous.text.contains("file.png"), "{}", anonymous.text);
    let reader = read(&fx, &uri, Some("delivery_reader")).await;
    assert_eq!(
        reader.body["fields"]["brief"]["entry"]["fields"]["file"]["asset"],
        metadata(&file, &app_url(&file))
    );

    // A deleted organization (issue #56): its path answers
    // `organization_not_found` and no asset is read.
    execute(
        &fx.db,
        &format!(
            "INSERT INTO merezha_content.scopes_deleted (scope, source, state, deleted_at) \
             VALUES ('org:{ORG_A}', 'operator', 'purging', now())"
        ),
    )
    .await;
    let before = statements_on(&harness, ASSETS_TABLE);
    for uri in [
        format!("{C}/o/{ORG_A}/help/{help}?populate=page"),
        format!("{C}/o/{ORG_A}/badge"),
        badge_uri.clone(),
    ] {
        let reply = fx.get(&uri).by(None).send().await;
        assert_eq!(
            (reply.status.as_u16(), reply.code()),
            (404, "organization_not_found"),
            "{uri}"
        );
        assert!(!reply.text.contains("mine.png") && !reply.text.contains("shared.png"));
    }
    assert_eq!(statements_on(&harness, ASSETS_TABLE), before);
    // B is untouched.
    assert_eq!(
        read(&fx, &format!("{C}/o/{ORG_B}/badge"), None).await.body["items"],
        json!([])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_base_url_makes_asset_and_location_urls_absolute_and_relative_without_it() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let base = "https://cms.example.com/site";
    let (fx, _harness) = start(
        &test,
        &dir,
        &format!("delivery = {{ public_base_url = \"{base}/\" }}\n"),
    )
    .await;
    // Management: the asset's `url` and the upload's `Location`.
    let reply = fx
        .post(&format!("{C}/manage/assets?filename=logo.png"))
        .by(Some("app_key"))
        .header("content-type", "image/png")
        .raw(&png(64, 32, 10))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.text);
    let logo = reply.body.clone();
    let logo_id = str_of(&logo["id"]).to_owned();
    assert_eq!(logo["url"], format!("{base}{}", app_url(&logo)));
    assert_eq!(
        reply.headers["location"],
        format!("{base}{C}/manage/assets/{logo_id}").as_str()
    );
    let listed = fx
        .get(&format!("{C}/manage/assets"))
        .by(Some("app_key"))
        .send()
        .await;
    assert_eq!(listed.body["items"][0]["url"], logo["url"]);
    let org_asset = fx
        .post(&format!("{C}/o/{ORG_A}/manage/assets?filename=org.png"))
        .by(Some("key_a"))
        .header("content-type", "image/png")
        .raw(&png(10, 10, 3))
        .send()
        .await;
    assert_eq!(org_asset.status, StatusCode::CREATED, "{}", org_asset.text);
    let org_id = str_of(&org_asset.body["id"]).to_owned();
    assert_eq!(
        org_asset.headers["location"],
        format!("{base}{C}/o/{ORG_A}/manage/assets/{org_id}").as_str()
    );
    assert_eq!(
        org_asset.body["url"],
        format!("{base}{}", org_url(ORG_A, &org_asset.body))
    );
    // A created entry's `Location`.
    let created = fx
        .post(&format!("{C}/manage/entries/page"))
        .by(Some("app_key"))
        .json(&json!({"fields": {"title": "Home", "cover": {"id": logo_id}}}))
        .send()
        .await;
    assert_eq!(created.status, StatusCode::CREATED);
    let page = id_of(&created);
    assert_eq!(
        created.headers["location"],
        format!("{base}{C}/manage/entries/page/{page}/en").as_str()
    );
    publish(&fx, "app_key", "page", &page, 1).await;
    let help = published(
        &fx,
        "key_a",
        "help",
        json!({"title": "Help", "image": {"id": org_id}}),
    )
    .await;
    // Delivery: absolute asset URLs, on both paths.
    let uri = format!("{C}/page/{page}");
    let with_base = read(&fx, &uri, None).await;
    assert_eq!(
        with_base.body["fields"]["cover"]["asset"],
        metadata(&logo, &format!("{base}{}", app_url(&logo)))
    );
    let org = read(&fx, &format!("{C}/o/{ORG_A}/help/{help}"), None).await;
    assert_eq!(
        org.body["fields"]["image"]["asset"]["url"],
        format!("{base}{}", org_url(ORG_A, &org_asset.body))
    );

    // A host on the same database without the key: paths, and a tag of
    // its own for the same content.
    let plain_dir = tempfile::tempdir().unwrap();
    let (plain, _plain_harness) = start(&test, &plain_dir, "").await;
    let without = read(&plain, &uri, None).await;
    assert_eq!(
        without.body["fields"]["cover"]["asset"],
        metadata(&logo, &app_url(&logo))
    );
    assert_ne!(without.etag(), with_base.etag());
    let tag = |etag: &str| etag.rsplit_once('-').unwrap().0.to_owned();
    assert_eq!(
        tag(&without.etag()),
        tag(&with_base.etag()),
        "same generation"
    );
    let created = plain
        .post(&format!("{C}/manage/entries/page"))
        .by(Some("app_key"))
        .json(&json!({"fields": {"title": "Plain"}}))
        .send()
        .await;
    assert_eq!(
        created.headers["location"],
        format!("{C}/manage/entries/page/{}/en", id_of(&created)).as_str()
    );
    let read_back = plain
        .get(&format!("{C}/manage/assets/{logo_id}"))
        .by(Some("app_key"))
        .send()
        .await;
    assert_eq!(read_back.body["url"], app_url(&logo));
}
