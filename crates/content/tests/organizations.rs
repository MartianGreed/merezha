//! CT-C9 organization scope over the axum router and a real database:
//! organization content types, the management scope taken from the
//! principal's tenant, delivery under `/v1/content/o/{organization}`,
//! the cross-scope reference rules, per-organization blobs, and a
//! two-organization matrix generated from the route table: every route
//! the crate mounts is sent organization B's ids and paths with A's
//! credentials, and a route missing from the table fails the scan.
mod http;

use axum::http::{Method, StatusCode};
use http::*;
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

const MODEL: &str = include_str!("fixtures/organizations.toml");
const C: &str = "/v1/content";

/// Where a test's media lives: the blob root and the spool directory.
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
    fn settings(&self) -> String {
        format!(
            "media = {{ driver = \"local\", local = {{ root = {:?} }}, spool_dir = {:?} }}",
            self.root(),
            self.dir.path().join("spool")
        )
    }
    /// Every object the local driver stored, relative to the root.
    fn objects(&self) -> BTreeSet<String> {
        fn walk(root: &Path, path: &Path, out: &mut BTreeSet<String>) {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    if entry.path().is_dir() {
                        walk(root, &entry.path(), out);
                    } else if let Ok(relative) = entry.path().strip_prefix(root) {
                        out.insert(relative.to_string_lossy().into_owned());
                    }
                }
            }
        }
        let mut out = BTreeSet::new();
        walk(&self.root(), &self.root(), &mut out);
        out
    }
}

async fn start(test: &TestDatabase, dirs: &Dirs) -> Fixture {
    start_organizations(test, &dirs.settings(), MODEL).await
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

fn manage(path: &str) -> String {
    format!("{C}/manage{path}")
}

/// `POST /manage/entries/{type}` as `who`; the new id.
async fn create(fx: &Fixture, who: &str, content_type: &str, fields: Value) -> String {
    let reply = fx
        .post(&manage(&format!("/entries/{content_type}")))
        .by(Some(who))
        .json(&json!({ "fields": fields }))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
    id_of(&reply)
}

async fn publish(fx: &Fixture, who: &str, content_type: &str, id: &str, locale: &str, rev: i64) {
    let reply = fx
        .post(&format!("{}/publish", variant(content_type, id, locale)))
        .by(Some(who))
        .if_match(rev)
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
}

/// The asset management path of `who`'s scope: its organization's, or
/// the application's for `app_key`.
fn assets_of(who: &str) -> String {
    match who {
        "key_a" | "member_a" => format!("{C}/o/{ORG_A}/manage/assets"),
        "key_b" | "member_b" => format!("{C}/o/{ORG_B}/manage/assets"),
        _ => manage("/assets"),
    }
}

/// `POST .../manage/assets` in `who`'s scope; the asset JSON.
async fn upload(fx: &Fixture, who: &str, filename: &str, bytes: &[u8]) -> Value {
    let reply = fx
        .post(&format!("{}?filename={filename}", assets_of(who)))
        .by(Some(who))
        .header("content-type", "image/png")
        .raw(bytes)
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
    reply.body
}

/// What one organization holds after [`seed`].
#[derive(Clone, Debug)]
struct Org {
    /// A published handbook entry with an `fr` variant, a reference to
    /// the application page and an asset.
    handbook: String,
    slug: String,
    notice: String,
    brief: String,
    settings: String,
    asset: String,
    filename: String,
    trashed: String,
}
impl Org {
    /// Ids that must never appear in another organization's answers.
    fn ids(&self) -> [&str; 7] {
        [
            &self.handbook,
            &self.notice,
            &self.brief,
            &self.settings,
            &self.asset,
            &self.trashed,
            &self.slug,
        ]
    }
}

/// Seed one organization through its own key: every entry carries
/// `marker` in its title. Without `singleton` the organization has no
/// settings document, so a singleton route of its own addresses nothing.
async fn seed(
    fx: &Fixture,
    who: &str,
    marker: &str,
    page: &str,
    bytes: &[u8],
    singleton: bool,
) -> Org {
    let asset = upload(fx, who, &format!("{}.png", marker.to_lowercase()), bytes).await;
    let asset_id = asset["id"].as_str().unwrap().to_owned();
    let filename = asset["filename"].as_str().unwrap().to_owned();
    let handbook = create(
        fx,
        who,
        "handbook",
        json!({"title": format!("{marker} handbook"), "code": format!("{marker}-1"),
               "page": {"id": page}, "image": {"id": asset_id}}),
    )
    .await;
    let fr = fx
        .put(&variant("handbook", &handbook, "fr"))
        .by(Some(who))
        .header("if-none-match", "*")
        .json(&json!({"fields": {"title": format!("{marker} manuel")}}))
        .send()
        .await;
    assert_eq!(fr.status, StatusCode::CREATED, "{:?}", fr.body);
    publish(fx, who, "handbook", &handbook, "en", 1).await;
    let read = fx
        .get(&variant("handbook", &handbook, "en"))
        .by(Some(who))
        .send()
        .await;
    let slug = read.body["fields"]["slug"].as_str().unwrap().to_owned();
    let notice = create(
        fx,
        who,
        "notice",
        json!({"title": format!("{marker} notice"), "page": {"id": page}}),
    )
    .await;
    publish(fx, who, "notice", &notice, "en", 1).await;
    let brief = create(
        fx,
        who,
        "brief",
        json!({"title": format!("{marker} brief")}),
    )
    .await;
    publish(fx, who, "brief", &brief, "en", 1).await;
    let mut settings = String::new();
    if singleton {
        let created = fx
            .put(&manage("/singletons/org_settings/en"))
            .by(Some(who))
            .header("if-none-match", "*")
            .json(&json!({"fields": {"name": format!("{marker} settings")}}))
            .send()
            .await;
        assert_eq!(created.status, StatusCode::CREATED, "{:?}", created.body);
        settings = id_of(&created);
        let published = fx
            .post(&manage("/singletons/org_settings/en/publish"))
            .by(Some(who))
            .if_match(1)
            .send()
            .await;
        assert_eq!(published.status, StatusCode::OK, "{:?}", published.body);
    }
    let trashed = create(
        fx,
        who,
        "handbook",
        json!({"title": format!("{marker} trashed")}),
    )
    .await;
    let deleted = fx
        .call(
            Method::DELETE,
            &manage(&format!("/entries/handbook/{trashed}")),
        )
        .by(Some(who))
        .send()
        .await;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT, "{:?}", deleted.body);
    Org {
        handbook,
        slug,
        notice,
        brief,
        settings,
        asset: asset_id,
        filename,
        trashed,
    }
}

/// The application page every organization references, published.
async fn page(fx: &Fixture, title: &str) -> String {
    let id = create(fx, "app_key", "page", json!({ "title": title })).await;
    publish(fx, "app_key", "page", &id, "en", 1).await;
    id
}

/// A digest of every row outside organization A's scope, the outbox
/// records of every other tenant and the objects outside A's key tag:
/// what A must never change.
async fn outside(fx: &Fixture, dirs: &Dirs) -> String {
    let a = format!("org:{ORG_A}");
    let mut out = String::new();
    for table in [
        "entries",
        "variants",
        "published",
        "refs",
        "assets",
        "blobs",
        "generations",
    ] {
        out.push_str(
            &strings(
                &fx.db,
                &format!(
                    "SELECT coalesce(string_agg(t::text, '|' ORDER BY t::text), '') \
                       FROM merezha_content.{table} t WHERE scope <> '{a}'"
                ),
            )
            .await
            .join(""),
        );
        out.push('\n');
    }
    out.push_str(
        &strings(
            &fx.db,
            &format!(
                "SELECT coalesce(string_agg(r::text, '|' ORDER BY r::text), '') \
                   FROM merezha_content.revisions r JOIN merezha_content.variants v \
                     ON v.entry_id = r.entry_id AND v.locale = r.locale WHERE v.scope <> '{a}'"
            ),
        )
        .await
        .join(""),
    );
    out.push('\n');
    out.push_str(
        &strings(
            &fx.db,
            &format!(
                "SELECT count(*)::text FROM merezha_jobs.outbox \
                  WHERE payload->>'tenant' IS DISTINCT FROM '{ORG_A}'"
            ),
        )
        .await
        .join(""),
    );
    let tag = a_tag();
    for object in dirs.objects() {
        if !object.contains(&tag) {
            out.push('\n');
            out.push_str(&object);
        }
    }
    out
}

/// The blob key tag of organization A.
fn a_tag() -> String {
    let key = merezha_content::media::blob_key(&format!("org:{ORG_A}"), &"0".repeat(64)).unwrap();
    key.as_str().split('/').nth(1).unwrap().to_owned()
}

/// One request of the matrix: a route of the table, addressed with B's
/// ids or paths.
struct Row {
    method: Method,
    /// The route as the crate declares it, under `/v1/content`.
    route: String,
    path: String,
    headers: Vec<(&'static str, String)>,
    body: Option<Vec<u8>>,
    /// What A's credentials get: the status and error code (empty on
    /// success, which only reads or writes A's own scope).
    refused: (u16, &'static str),
    /// The status B's own credentials get for the same request, for the
    /// reads: the path really addresses B's data.
    owner: Option<u16>,
}

fn row(method: Method, route: &str, path: String, refused: (u16, &'static str)) -> Row {
    Row {
        method,
        route: route.to_owned(),
        path,
        headers: Vec::new(),
        body: None,
        refused,
        owner: None,
    }
}
impl Row {
    fn owner(mut self, status: u16) -> Self {
        self.owner = Some(status);
        self
    }
    fn json(mut self, body: Value) -> Self {
        self.body = Some(serde_json::to_vec(&body).unwrap());
        self.headers
            .push(("content-type", "application/json".to_owned()));
        self
    }
    fn merge(mut self, body: Value) -> Self {
        self.body = Some(serde_json::to_vec(&body).unwrap());
        self.headers
            .push(("content-type", "application/merge-patch+json".to_owned()));
        self
    }
    fn if_match(mut self, revision: i64) -> Self {
        self.headers.push(("if-match", format!("\"r{revision}\"")));
        self
    }
    fn header(mut self, name: &'static str, value: &str) -> Self {
        self.headers.push((name, value.to_owned()));
        self
    }
    async fn send(&self, fx: &Fixture, who: &str) -> Reply {
        let mut call = fx.call(self.method.clone(), &self.path).by(Some(who));
        for (name, value) in &self.headers {
            call = call.header(name, value);
        }
        if let Some(body) = &self.body {
            call = call.raw(body);
        }
        call.send().await
    }
}

/// The matrix: every route of the crate with B's ids and paths.
fn matrix(b: &Org, png: &[u8]) -> Vec<Row> {
    use Method as M;
    let not_found = (404, "entry_not_found");
    let no_asset = (404, "asset_not_found");
    let no_type = (404, "content_type_not_found");
    let refused = (403, "unauthorized");
    let ok = (200, "");
    let h = &b.handbook;
    let entry = |rest: &str| manage(&format!("/entries/handbook/{h}{rest}"));
    let single = |rest: &str| manage(&format!("/singletons/org_settings/en{rest}"));
    let org_b = format!("{C}/o/{ORG_B}");
    let org_a = format!("{C}/o/{ORG_A}");
    vec![
        // Management, entries: B's ids are absent in A's scope.
        row(
            M::GET,
            "/manage/entries/{type}",
            manage("/entries/handbook"),
            ok,
        )
        .owner(200),
        row(
            M::POST,
            "/manage/entries/{type}",
            manage("/entries/handbook"),
            (409, "entry_exists"),
        )
        .json(json!({"id": h, "fields": {"title": "Copy"}})),
        row(
            M::POST,
            "/manage/entries/{type}",
            manage("/entries/handbook"),
            (422, "validation_failed"),
        )
        .json(json!({"fields": {"title": "Link", "related": [{"id": h}]}})),
        row(M::GET, "/manage/entries/{type}/{id}", entry(""), not_found).owner(200),
        row(
            M::DELETE,
            "/manage/entries/{type}/{id}",
            entry(""),
            not_found,
        ),
        row(
            M::DELETE,
            "/manage/entries/{type}/{id}",
            entry("?purge=true"),
            not_found,
        ),
        row(
            M::GET,
            "/manage/entries/{type}/{id}/referrers",
            entry("/referrers"),
            not_found,
        )
        .owner(200),
        row(
            M::POST,
            "/manage/entries/{type}/{id}/undelete",
            manage(&format!("/entries/handbook/{}/undelete", b.trashed)),
            not_found,
        ),
        row(
            M::GET,
            "/manage/entries/{type}/{id}/{locale}",
            entry("/en"),
            not_found,
        )
        .owner(200),
        row(
            M::GET,
            "/manage/entries/{type}/{id}/{locale}",
            entry("/en?populate=page"),
            not_found,
        )
        .owner(200),
        row(
            M::PUT,
            "/manage/entries/{type}/{id}/{locale}",
            entry("/en"),
            not_found,
        )
        .if_match(1)
        .json(json!({"fields": {"title": "Taken"}})),
        row(
            M::PUT,
            "/manage/entries/{type}/{id}/{locale}",
            entry("/fr"),
            not_found,
        )
        .header("if-none-match", "*")
        .json(json!({"fields": {"title": "Pris"}})),
        row(
            M::PATCH,
            "/manage/entries/{type}/{id}/{locale}",
            entry("/en"),
            not_found,
        )
        .if_match(1)
        .merge(json!({"fields": {"title": "Taken"}})),
        row(
            M::DELETE,
            "/manage/entries/{type}/{id}/{locale}",
            entry("/fr"),
            not_found,
        )
        .if_match(1),
        row(
            M::POST,
            "/manage/entries/{type}/{id}/{locale}/publish",
            entry("/en/publish"),
            not_found,
        )
        .if_match(1),
        row(
            M::POST,
            "/manage/entries/{type}/{id}/{locale}/unpublish",
            entry("/en/unpublish"),
            not_found,
        )
        .if_match(1),
        row(
            M::GET,
            "/manage/entries/{type}/{id}/{locale}/revisions",
            entry("/en/revisions"),
            not_found,
        )
        .owner(200),
        row(
            M::GET,
            "/manage/entries/{type}/{id}/{locale}/revisions/{n}",
            entry("/en/revisions/1"),
            not_found,
        )
        .owner(200),
        row(
            M::POST,
            "/manage/entries/{type}/{id}/{locale}/revisions/{n}/restore",
            entry("/en/revisions/1/restore"),
            not_found,
        )
        .if_match(1),
        // Singletons resolve the caller's own scope: A has none, and
        // B's revision number reaches nothing.
        row(
            M::GET,
            "/manage/singletons/{type}/{locale}",
            single(""),
            not_found,
        )
        .owner(200),
        row(
            M::PUT,
            "/manage/singletons/{type}/{locale}",
            single(""),
            not_found,
        )
        .if_match(1)
        .json(json!({"fields": {"name": "Taken"}})),
        row(
            M::PATCH,
            "/manage/singletons/{type}/{locale}",
            single(""),
            not_found,
        )
        .if_match(1)
        .merge(json!({"fields": {"name": "Taken"}})),
        row(
            M::POST,
            "/manage/singletons/{type}/{locale}/publish",
            single("/publish"),
            not_found,
        )
        .if_match(1),
        row(
            M::POST,
            "/manage/singletons/{type}/{locale}/unpublish",
            single("/unpublish"),
            not_found,
        )
        .if_match(1),
        row(
            M::GET,
            "/manage/singletons/{type}/{locale}/revisions",
            single("/revisions"),
            not_found,
        )
        .owner(200),
        row(
            M::GET,
            "/manage/singletons/{type}/{locale}/revisions/{n}",
            single("/revisions/1"),
            not_found,
        )
        .owner(200),
        row(
            M::POST,
            "/manage/singletons/{type}/{locale}/revisions/{n}/restore",
            single("/revisions/1/restore"),
            not_found,
        )
        .if_match(1),
        // The trash lists A's scope only.
        row(M::GET, "/manage/trash", manage("/trash"), ok).owner(200),
        row(M::GET, "/manage/trash", manage("/trash?type=handbook"), ok).owner(200),
        row(
            M::POST,
            "/manage/batch/publish",
            manage("/batch/publish"),
            not_found,
        )
        .json(json!({"items": [{"type": "handbook", "id": h, "locale": "en", "revision": 1}]})),
        row(
            M::POST,
            "/manage/batch/unpublish",
            manage("/batch/unpublish"),
            not_found,
        )
        .json(json!({"items": [{"type": "handbook", "id": h, "locale": "en"}]})),
        row(M::GET, "/manage/model", manage("/model"), ok).owner(200),
        // Delivery: organization types never answer on the application
        // path, and B's path refuses A's credentials before reading.
        row(M::GET, "/model", format!("{C}/model"), ok).owner(200),
        row(M::GET, "/{type}", format!("{C}/handbook"), no_type),
        row(M::GET, "/{type}/{id}", format!("{C}/handbook/{h}"), no_type),
        row(M::GET, "/{type}/{id}", format!("{C}/page/{h}"), not_found),
        row(
            M::GET,
            "/{type}/by-slug/{slug}",
            format!("{C}/handbook/by-slug/{}", b.slug),
            no_type,
        ),
        row(
            M::GET,
            "/o/{organization}/model",
            format!("{org_b}/model"),
            ok,
        )
        .owner(200),
        row(
            M::GET,
            "/o/{organization}/{type}",
            format!("{org_b}/handbook"),
            refused,
        )
        .owner(200),
        row(
            M::GET,
            "/o/{organization}/{type}",
            format!("{org_b}/handbook?stage=draft"),
            refused,
        )
        .owner(200),
        row(
            M::GET,
            "/o/{organization}/{type}",
            format!("{org_b}/brief"),
            refused,
        )
        .owner(200),
        row(
            M::GET,
            "/o/{organization}/{type}",
            format!("{org_b}/org_settings"),
            refused,
        )
        .owner(200),
        row(
            M::GET,
            "/o/{organization}/{type}/{id}",
            format!("{org_b}/handbook/{h}"),
            refused,
        )
        .owner(200),
        row(
            M::GET,
            "/o/{organization}/{type}/{id}",
            format!("{org_b}/brief/{}", b.brief),
            refused,
        )
        .owner(200),
        row(
            M::GET,
            "/o/{organization}/{type}/{id}",
            format!("{org_a}/handbook/{h}"),
            not_found,
        ),
        row(
            M::GET,
            "/o/{organization}/{type}/{id}",
            format!("{org_a}/notice/{}", b.notice),
            not_found,
        ),
        row(
            M::GET,
            "/o/{organization}/{type}/by-slug/{slug}",
            format!("{org_b}/handbook/by-slug/{}", b.slug),
            refused,
        )
        .owner(200),
        row(
            M::GET,
            "/o/{organization}/{type}/by-slug/{slug}",
            format!("{org_a}/handbook/by-slug/{}", b.slug),
            not_found,
        ),
        // Assets: the path names the scope. The application's refuses an
        // organization key, B's refuses A, and in A's own scope B's ids
        // are absent; delivery answers like an unknown id.
        row(M::GET, "/manage/assets", manage("/assets"), refused),
        row(
            M::GET,
            "/o/{organization}/manage/assets",
            format!("{org_b}/manage/assets"),
            refused,
        )
        .owner(200),
        row(
            M::GET,
            "/o/{organization}/manage/assets",
            format!("{org_a}/manage/assets"),
            ok,
        ),
        Row {
            body: Some(png.to_vec()),
            headers: vec![("content-type", "image/png".to_owned())],
            ..row(
                M::POST,
                "/manage/assets",
                manage("/assets?filename=same.png"),
                refused,
            )
        },
        Row {
            body: Some(png.to_vec()),
            headers: vec![("content-type", "image/png".to_owned())],
            ..row(
                M::POST,
                "/o/{organization}/manage/assets",
                format!("{org_b}/manage/assets?filename=same.png"),
                refused,
            )
        },
        Row {
            body: Some(png.to_vec()),
            headers: vec![("content-type", "image/png".to_owned())],
            ..row(
                M::POST,
                "/o/{organization}/manage/assets",
                format!("{org_a}/manage/assets?filename=same.png"),
                (201, ""),
            )
        },
    ]
    .into_iter()
    .chain(
        [
            (format!("{C}/manage/assets"), "", refused, None),
            (
                format!("{org_b}/manage/assets"),
                "/o/{organization}",
                refused,
                Some(200),
            ),
            (
                format!("{org_a}/manage/assets"),
                "/o/{organization}",
                no_asset,
                None,
            ),
        ]
        .into_iter()
        .flat_map(|(base, prefix, refusal, owner)| {
            let at = |rest: &str| format!("{base}/{}{rest}", b.asset);
            let route = |rest: &str| format!("{prefix}/manage/assets/{{id}}{rest}");
            let mut rows = vec![
                row(M::GET, &route(""), at(""), refusal),
                row(M::PATCH, &route(""), at(""), refusal).merge(json!({"title": "Taken"})),
                row(M::DELETE, &route(""), at(""), refusal),
                row(M::DELETE, &route(""), at("?force=true&purge=true"), refusal),
                row(M::GET, &route("/file"), at("/file"), refusal),
                row(M::POST, &route("/undelete"), at("/undelete"), refusal),
            ];
            if let Some(status) = owner {
                for read in rows.iter_mut().filter(|r| r.method == M::GET) {
                    read.owner = Some(status);
                }
            }
            rows
        }),
    )
    .chain([
        row(
            M::GET,
            "/assets/{id}/{filename}",
            format!("{C}/assets/{}/{}", b.asset, b.filename),
            no_asset,
        ),
        row(
            M::GET,
            "/o/{organization}/assets/{id}/{filename}",
            format!("{org_b}/assets/{}/{}", b.asset, b.filename),
            no_asset,
        )
        .owner(200),
        row(
            M::GET,
            "/o/{organization}/assets/{id}/{filename}",
            format!("{org_a}/assets/{}/{}", b.asset, b.filename),
            no_asset,
        ),
    ])
    .collect()
}

/// Every `(method, route)` the crate's sources mount, so a new route
/// cannot skip the matrix. Each `.route(path, ..)` must name its methods
/// inline (`get(..)`, `.post(..)` and the rest); any routing form the scan
/// cannot read (`any`, `on`, a method router held in a variable,
/// `route_service`, `nest`, `nest_service`, `fallback`) fails it instead
/// of passing unseen.
fn mounted_routes() -> BTreeSet<(String, String)> {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    fn collect(path: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(path).unwrap().flatten() {
            if entry.path().is_dir() {
                collect(&entry.path(), out);
            } else if entry.path().extension().is_some_and(|e| e == "rs") {
                out.push(entry.path());
            }
        }
    }
    collect(&src, &mut files);
    let mut out = BTreeSet::new();
    for file in files {
        let text = std::fs::read_to_string(&file).unwrap();
        routes_in(&text, &file.display().to_string(), &mut out);
    }
    out
}

/// The `(method, route)` pairs one source file mounts, into `out`;
/// panics on a routing form [`mounted_routes`] cannot read.
fn routes_in(text: &str, name: &str, out: &mut BTreeSet<(String, String)>) {
    /// `token(` as a call, not the tail of a longer name.
    fn calls(text: &str, token: &str) -> bool {
        let token = format!("{token}(");
        text.match_indices(&token)
            .any(|(i, _)| i == 0 || !text[..i].ends_with(|c: char| c.is_alphanumeric() || c == '_'))
    }
    const METHODS: [&str; 8] = [
        "get", "post", "put", "patch", "delete", "head", "options", "trace",
    ];
    {
        for unread in [
            ".route_service(",
            ".nest_service(",
            ".nest(",
            ".fallback(",
            ".fallback_service(",
        ] {
            assert!(
                !text.contains(unread),
                "{name} mounts with `{unread}`, which the matrix scan cannot read"
            );
        }
        let mut rest = text;
        while let Some(at) = rest.find(".route(") {
            rest = &rest[at + ".route(".len()..];
            // The call's arguments, up to its closing parenthesis.
            let mut depth = 1;
            let end = rest
                .char_indices()
                .find(|(_, c)| {
                    match c {
                        '(' => depth += 1,
                        ')' => depth -= 1,
                        _ => {}
                    }
                    depth == 0
                })
                .map(|(i, _)| i)
                .unwrap();
            let call = &rest[..end];
            let (first, methods) = call.split_once(',').unwrap();
            let first = first.trim();
            let path = match first.strip_prefix('"') {
                Some(literal) => literal.trim_end_matches('"').to_owned(),
                // A constant of the same file, such as `UPLOAD_PATH`.
                None => {
                    let declaration = format!("const {first}: &str = \"");
                    let at = text
                        .find(&declaration)
                        .unwrap_or_else(|| panic!("{first} in {name}"));
                    let value = &text[at + declaration.len()..];
                    value[..value.find('"').unwrap()].to_owned()
                }
            };
            for unread in ["any", "on"] {
                assert!(
                    !calls(methods, unread),
                    "{name}: `{path}` routes through `{unread}`, which the matrix scan cannot read"
                );
            }
            let found: Vec<&str> = METHODS
                .into_iter()
                .filter(|method| calls(methods, method))
                .collect();
            assert!(
                !found.is_empty(),
                "{name}: `{path}` names no method the matrix scan reads: `{}`",
                methods.trim()
            );
            for method in found {
                out.insert((method.to_uppercase(), path.clone()));
            }
        }
    }
}

#[test]
fn the_route_scan_refuses_every_form_it_cannot_read() {
    let mut out = BTreeSet::new();
    routes_in(
        "const P: &str = \"/p\";\n.route(P, get(a).post(b)).route(\"/x/{id}\", axum::routing::delete(c))",
        "ok",
        &mut out,
    );
    assert_eq!(
        out.into_iter().collect::<Vec<_>>(),
        [
            ("DELETE".to_owned(), "/x/{id}".to_owned()),
            ("GET".to_owned(), "/p".to_owned()),
            ("POST".to_owned(), "/p".to_owned()),
        ]
    );
    for source in [
        ".route(\"/a\", handlers)",
        ".route(\"/a\", any(h))",
        ".route(\"/a\", on(MethodFilter::GET, h))",
        ".route_service(\"/a\", service)",
        ".nest(\"/a\", router)",
        ".nest_service(\"/a\", service)",
        ".fallback(h)",
        ".fallback_service(service)",
    ] {
        let refused = std::panic::catch_unwind(|| routes_in(source, "test", &mut BTreeSet::new()));
        assert!(refused.is_err(), "{source} passed the scan");
    }
}

#[test]
fn the_matrix_covers_every_mounted_route() {
    let org = Org {
        handbook: "h".into(),
        slug: "s".into(),
        notice: "n".into(),
        brief: "b".into(),
        settings: "t".into(),
        asset: "a".into(),
        filename: "f".into(),
        trashed: "x".into(),
    };
    let covered: BTreeSet<(String, String)> = matrix(&org, b"")
        .iter()
        .map(|r| (r.method.to_string(), r.route.clone()))
        .collect();
    let mounted = mounted_routes();
    assert!(mounted.len() > 40, "the scan found the routes: {mounted:?}");
    let missing: Vec<_> = mounted.difference(&covered).collect();
    assert!(missing.is_empty(), "routes the matrix skips: {missing:?}");
    let stale: Vec<_> = covered.difference(&mounted).collect();
    assert!(stale.is_empty(), "matrix routes nothing mounts: {stale:?}");
}

#[tokio::test]
async fn a_key_of_one_organization_reaches_nothing_of_another_through_any_route() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs).await;
    let page = page(&fx, "Shared").await;
    let bytes = png(4, 3, 64);
    let b = seed(&fx, "key_b", "B-secret", &page, &bytes, true).await;
    // A holds content of its own, so its answers are not empty by luck,
    // but no settings document: B's must stay out of A's reach.
    let a = seed(&fx, "key_a", "A-own", &page, &png(5, 5, 32), false).await;
    let rows = matrix(&b, &bytes);
    for who in ["key_a", "member_a"] {
        let before = outside(&fx, &dirs).await;
        for row in &rows {
            let reply = row.send(&fx, who).await;
            let what = format!("{who} {} {}", row.method, row.path);
            assert_eq!(
                (reply.status.as_u16(), reply.code()),
                row.refused,
                "{what}: {}",
                reply.text
            );
            for marker in b.ids().iter().copied().chain(["B-secret", "b-secret"]) {
                assert!(
                    !reply.text.contains(marker),
                    "{what} shows B's {marker}: {}",
                    reply.text
                );
            }
        }
        assert_eq!(outside(&fx, &dirs).await, before, "{who} changed B");
    }
    // The same requests address B's data for B's own key.
    for row in rows.iter().filter(|r| r.owner.is_some()) {
        let reply = row.send(&fx, "key_b").await;
        assert_eq!(
            Some(reply.status.as_u16()),
            row.owner,
            "key_b {} {}: {}",
            row.method,
            row.path,
            reply.text
        );
    }
    // A's own reads see A's content and only A's.
    let list = fx
        .get(&manage("/entries/handbook"))
        .by(Some("key_a"))
        .send()
        .await;
    let ids: Vec<&str> = list.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["sys"]["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![a.handbook.as_str()]);
    let trash = fx.get(&manage("/trash")).by(Some("key_b")).send().await;
    assert_eq!(trash.body["items"][0]["id"], b.trashed.as_str());
    assert_eq!(trash.body["items"].as_array().unwrap().len(), 1);
    // B's handbook still says what B wrote.
    let own = fx
        .get(&variant("handbook", &b.handbook, "en"))
        .by(Some("key_b"))
        .send()
        .await;
    assert_eq!(own.body["fields"]["title"], "B-secret handbook");
    assert_eq!(own.body["sys"]["draft_revision"], 1);
}

#[tokio::test]
async fn management_takes_the_scope_from_the_principals_tenant() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs).await;
    let page = page(&fx, "Shared").await;
    let a = create(
        &fx,
        "key_a",
        "handbook",
        json!({"title": "A one", "code": "same"}),
    )
    .await;
    // Unique values, slugs and singletons are per scope.
    let b = create(
        &fx,
        "key_b",
        "handbook",
        json!({"title": "A one", "code": "same"}),
    )
    .await;
    assert_eq!(
        strings(
            &fx.db,
            &format!(
                "SELECT scope FROM merezha_content.entries WHERE id IN ('{a}', '{b}') \
                  ORDER BY scope"
            )
        )
        .await,
        vec![format!("org:{ORG_A}"), format!("org:{ORG_B}")]
    );
    let again = fx
        .post(&manage("/entries/handbook"))
        .by(Some("member_a"))
        .json(&json!({"fields": {"title": "A two", "code": "same"}}))
        .send()
        .await;
    assert_eq!(
        again.details(),
        vec![("/fields/code".into(), "unique".into())]
    );
    // Events carry the organization as their tenant, and the scope.
    let saved = events(&fx.db, "content.entry_draft_saved").await;
    let mine: Vec<&Value> = saved
        .iter()
        .filter(|e| e["payload"]["id"] == a.as_str())
        .collect();
    assert_eq!(mine.len(), 1);
    assert_eq!(mine[0]["tenant"], ORG_A);
    assert_eq!(mine[0]["payload"]["scope"], format!("org:{ORG_A}"));
    let app: Vec<&Value> = saved
        .iter()
        .filter(|e| e["payload"]["id"] == page.as_str())
        .collect();
    assert_eq!(app[0]["tenant"], Value::Null);
    // Without a tenant: checked at `Application` first, then
    // `tenant_required`; a caller without that grant cannot tell an
    // organization type from an unknown one.
    let tenantless = fx
        .get(&manage("/entries/handbook"))
        .by(Some("app_key"))
        .send()
        .await;
    assert_eq!(
        (tenantless.status, tenantless.code()),
        (StatusCode::FORBIDDEN, "tenant_required")
    );
    for path in ["/entries/handbook", "/entries/nothing"] {
        let reply = fx.get(&manage(path)).by(Some("nobody")).send().await;
        assert_eq!(
            (reply.status, reply.code()),
            (StatusCode::FORBIDDEN, "unauthorized"),
            "{path}"
        );
    }
    // A tenant key never acts on application types.
    let app_type = fx
        .get(&manage("/entries/page"))
        .by(Some("key_a"))
        .send()
        .await;
    assert_eq!(app_type.code(), "unauthorized");
    // The trash without `type` keeps the organization types of the
    // principal's tenant and the application types it reads.
    let trash = fx.get(&manage("/trash")).by(Some("app_key")).send().await;
    assert_eq!(trash.status, StatusCode::OK, "{:?}", trash.body);
}

#[tokio::test]
async fn public_delivery_on_an_organizations_path_shows_only_that_organization() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs).await;
    let page = page(&fx, "Shared").await;
    let a = create(&fx, "key_a", "notice", json!({"title": "Launch"})).await;
    publish(&fx, "key_a", "notice", &a, "en", 1).await;
    // The same slug in both organizations: unique per scope.
    let b = create(
        &fx,
        "key_b",
        "notice",
        json!({"title": "Launch", "page": {"id": page}}),
    )
    .await;
    publish(&fx, "key_b", "notice", &b, "en", 1).await;
    let list = fx
        .get(&format!("{C}/o/{ORG_B}/notice"))
        .by(None)
        .send()
        .await;
    assert_eq!(list.status, StatusCode::OK, "{:?}", list.body);
    let ids: Vec<&str> = list.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["sys"]["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![b.as_str()]);
    assert_eq!(
        list.headers["cache-control"],
        "public, max-age=60, stale-while-revalidate=300"
    );
    let slug = fx
        .get(&format!("{C}/o/{ORG_B}/notice/by-slug/launch"))
        .by(None)
        .send()
        .await;
    assert_eq!(slug.body["sys"]["id"], b.as_str());
    let other = fx
        .get(&format!("{C}/o/{ORG_B}/notice/{a}"))
        .by(None)
        .send()
        .await;
    assert_eq!(other.code(), "entry_not_found");
    // Organization types never answer on the application path, nor
    // application types under an organization.
    let root = fx.get(&format!("{C}/notice")).by(None).send().await;
    assert_eq!(root.code(), "content_type_not_found");
    let nested = fx
        .get(&format!("{C}/o/{ORG_B}/page/{page}"))
        .by(None)
        .send()
        .await;
    assert_eq!(nested.code(), "content_type_not_found");
    // Population reaches the application page from B's notice.
    let populated = fx
        .get(&format!("{C}/o/{ORG_B}/notice/{b}?populate=page"))
        .by(None)
        .send()
        .await;
    assert_eq!(
        populated.body["fields"]["page"]["entry"]["fields"]["title"], "Shared",
        "{:?}",
        populated.body
    );
    // A reference into another organization, planted behind validation,
    // is only ever `missing`: population reads B's scope and the
    // application's, never A's.
    let planted = json!([{"id": a, "type": "notice"}]).to_string();
    execute(
        &fx.db,
        &format!(
            "UPDATE merezha_content.published SET data = jsonb_set(data, '{{related}}', '{planted}') \
              WHERE entry_id = '{b}';
             UPDATE merezha_content.variants SET draft_data = jsonb_set(draft_data, '{{related}}', '{planted}') \
              WHERE entry_id = '{b}'"
        ),
    )
    .await;
    let missing = fx
        .get(&format!("{C}/o/{ORG_B}/notice/{b}?populate=related"))
        .by(None)
        .send()
        .await;
    assert_eq!(
        missing.body["fields"]["related"],
        json!([{"id": a, "type": "notice", "missing": true}]),
        "{:?}",
        missing.body
    );
    let draft = fx
        .get(&format!("{}?populate=related", variant("notice", &b, "en")))
        .by(Some("key_b"))
        .send()
        .await;
    assert_eq!(
        draft.body["fields"]["related"],
        json!([{"id": a, "type": "notice", "missing": true}]),
        "{:?}",
        draft.body
    );
    let preview = fx
        .get(&format!(
            "{C}/o/{ORG_B}/notice/{b}?populate=related&stage=draft"
        ))
        .by(Some("key_b"))
        .send()
        .await;
    assert_eq!(preview.body["fields"]["related"][0]["missing"], true);
    // The restricted model of B's path: its public type and the
    // application's, nothing members-only.
    let model = fx
        .get(&format!("{C}/o/{ORG_B}/model"))
        .by(None)
        .send()
        .await;
    let types: BTreeSet<&str> = model.body["types"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(types, BTreeSet::from(["notice", "page"]));
    // Members-only types answer 401 anonymously and 403 to another
    // organization's member, 200 to their own.
    let handbook = create(&fx, "key_b", "handbook", json!({"title": "Members"})).await;
    publish(&fx, "key_b", "handbook", &handbook, "en", 1).await;
    let path = format!("{C}/o/{ORG_B}/handbook/{handbook}");
    for (who, status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some("member_a"), StatusCode::FORBIDDEN),
        (Some("app_key"), StatusCode::FORBIDDEN),
        (Some("member_b"), StatusCode::OK),
    ] {
        let reply = fx.get(&path).by(who).send().await;
        assert_eq!(reply.status, status, "{who:?}: {:?}", reply.body);
    }
}

#[tokio::test]
async fn organization_entries_reference_the_application_and_never_the_reverse() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs).await;
    let page = page(&fx, "Shared").await;
    let shared = upload(&fx, "app_key", "logo.png", &png(2, 2, 8)).await;
    let shared = shared["id"].as_str().unwrap().to_owned();
    let mine = upload(&fx, "key_a", "mine.png", &png(3, 3, 8)).await;
    let mine_name = mine["filename"].as_str().unwrap().to_owned();
    let mine = mine["id"].as_str().unwrap().to_owned();
    let theirs = upload(&fx, "key_b", "theirs.png", &png(4, 4, 8)).await;
    let theirs = theirs["id"].as_str().unwrap().to_owned();
    let b = create(&fx, "key_b", "handbook", json!({"title": "B"})).await;
    // Organization to application: entries and assets.
    let a = create(
        &fx,
        "key_a",
        "handbook",
        json!({"title": "A", "page": {"id": page}, "image": {"id": shared}}),
    )
    .await;
    publish(&fx, "key_a", "handbook", &a, "en", 1).await;
    let read = fx
        .get(&format!("{C}/o/{ORG_A}/handbook/{a}?populate=page"))
        .by(Some("key_a"))
        .send()
        .await;
    assert_eq!(
        read.body["fields"]["page"]["type"], "page",
        "{:?}",
        read.body
    );
    assert_eq!(
        read.body["fields"]["page"]["entry"]["fields"]["title"],
        "Shared"
    );
    // Across organizations, and from the application into one: refused,
    // with the answer an absent id gets.
    let nowhere = "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11";
    for (who, content_type, fields, path, code) in [
        (
            "key_a",
            "handbook",
            json!({"title": "X", "related": [{"id": b}]}),
            "/fields/related/0",
            "reference_not_found",
        ),
        (
            "key_a",
            "handbook",
            json!({"title": "X", "related": [{"id": nowhere}]}),
            "/fields/related/0",
            "reference_not_found",
        ),
        (
            "app_key",
            "page",
            json!({"title": "X", "see_also": [{"id": a}]}),
            "/fields/see_also/0",
            "reference_not_found",
        ),
        (
            "key_a",
            "handbook",
            json!({"title": "X", "image": {"id": theirs}}),
            "/fields/image",
            "asset_not_found",
        ),
        (
            "app_key",
            "page",
            json!({"title": "X", "image": {"id": mine}}),
            "/fields/image",
            "asset_not_found",
        ),
    ] {
        let reply = fx
            .post(&manage(&format!("/entries/{content_type}")))
            .by(Some(who))
            .json(&json!({ "fields": fields }))
            .send()
            .await;
        assert_eq!(
            (reply.status, reply.details()),
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                vec![(path.to_owned(), code.to_owned())]
            ),
            "{who} {fields}"
        );
    }
    // An application asset an organization's published entry references
    // is served on that organization's path, and on no other.
    let logo = fx
        .get(&manage(&format!("/assets/{shared}")))
        .by(Some("app_key"))
        .send()
        .await;
    let logo_name = logo.body["filename"].as_str().unwrap().to_owned();
    assert_eq!(logo.body["url"], format!("{C}/assets/{shared}/{logo_name}"));
    let served = fx
        .get(&format!("{C}/o/{ORG_A}/assets/{shared}/{logo_name}"))
        .by(Some("member_a"))
        .send()
        .await;
    assert_eq!(served.status, StatusCode::OK, "{:?}", served.body);
    for (path, who) in [
        (format!("{C}/assets/{shared}/{logo_name}"), "member_a"),
        (
            format!("{C}/o/{ORG_B}/assets/{shared}/{logo_name}"),
            "member_b",
        ),
        (
            format!("{C}/o/{ORG_A}/assets/{shared}/{logo_name}"),
            "member_b",
        ),
    ] {
        let reply = fx.get(&path).by(Some(who)).send().await;
        assert_eq!(reply.code(), "asset_not_found", "{who} {path}");
    }
    // An organization's own asset has its path under the organization.
    let own = fx
        .get(&format!("{}/{mine}", assets_of("key_a")))
        .by(Some("key_a"))
        .send()
        .await;
    assert_eq!(
        own.body["url"],
        format!("{C}/o/{ORG_A}/assets/{mine}/{mine_name}")
    );
    // Referrers list the target's scope only: the application page's
    // organization referrers stay with their organizations.
    let referrers = fx
        .get(&manage(&format!("/entries/page/{page}/referrers")))
        .by(Some("app_key"))
        .send()
        .await;
    assert_eq!(referrers.body["items"], json!([]), "{:?}", referrers.body);
}

#[tokio::test]
async fn identical_uploads_in_two_organizations_are_two_blobs_and_two_objects() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs).await;
    let bytes = png(8, 8, 512);
    let a = upload(&fx, "key_a", "same.png", &bytes).await;
    let b = upload(&fx, "key_b", "same.png", &bytes).await;
    let app = upload(&fx, "app_key", "same.png", &bytes).await;
    // Deduplicated within a scope only.
    let again = upload(&fx, "member_a", "again.png", &bytes).await;
    assert_eq!(a["sha256"], b["sha256"]);
    assert_eq!(dirs.objects().len(), 3, "{:?}", dirs.objects());
    let keys = strings(
        &fx.db,
        "SELECT scope || ' ' || storage_key FROM merezha_content.blobs ORDER BY scope",
    )
    .await;
    assert_eq!(keys.len(), 3, "{keys:?}");
    let sha = a["sha256"].as_str().unwrap();
    let expected: Vec<String> = [
        "app".to_owned(),
        format!("org:{ORG_A}"),
        format!("org:{ORG_B}"),
    ]
    .iter()
    .map(|scope| {
        let key = merezha_content::media::blob_key(scope, sha).unwrap();
        format!("{scope} {}", key.as_str())
    })
    .collect();
    assert_eq!(keys, expected);
    for key in &keys {
        let storage = key.split_once(' ').unwrap().1;
        assert!(
            !storage.contains(ORG_A) && !storage.contains(ORG_B),
            "{storage}"
        );
    }
    assert_eq!(
        scalar(&fx.db, "SELECT count(*) FROM merezha_content.assets").await,
        4
    );
    let uploaded = events(&fx.db, "content.asset_uploaded").await;
    let tenant_of = |asset: &Value| {
        uploaded
            .iter()
            .find(|e| e["payload"]["id"] == asset["id"])
            .map(|e| e["tenant"].clone())
            .unwrap()
    };
    assert_eq!(tenant_of(&a), ORG_A);
    assert_eq!(tenant_of(&b), ORG_B);
    assert_eq!(tenant_of(&again), ORG_A);
    assert_eq!(tenant_of(&app), Value::Null);
    // Each organization lists its own asset rows.
    let listed = fx.get(&assets_of("key_a")).by(Some("key_a")).send().await;
    let mut ids: Vec<&str> = listed.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["id"].as_str().unwrap())
        .collect();
    ids.sort();
    let mut expected = vec![a["id"].as_str().unwrap(), again["id"].as_str().unwrap()];
    expected.sort();
    assert_eq!(ids, expected);
    // Deleting B's asset leaves A's blob alone.
    let deleted = fx
        .call(
            Method::DELETE,
            &format!(
                "{}/{}?purge=true",
                assets_of("key_b"),
                b["id"].as_str().unwrap()
            ),
        )
        .by(Some("key_b"))
        .send()
        .await;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT, "{:?}", deleted.body);
    assert_eq!(
        strings(
            &fx.db,
            "SELECT scope || '=' || state FROM merezha_content.blobs ORDER BY scope"
        )
        .await,
        vec![
            "app=stored".to_owned(),
            format!("org:{ORG_A}=stored"),
            format!("org:{ORG_B}=orphaned")
        ]
    );
}

#[tokio::test]
async fn organization_etags_follow_their_own_and_the_application_generations_only() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs).await;
    page(&fx, "Shared").await;
    let handbook = create(&fx, "key_a", "handbook", json!({"title": "A"})).await;
    publish(&fx, "key_a", "handbook", &handbook, "en", 1).await;
    let brief = create(&fx, "key_a", "brief", json!({"title": "A"})).await;
    publish(&fx, "key_a", "brief", &brief, "en", 1).await;
    let tags = || async {
        let mut out = Vec::new();
        for path in [
            format!("{C}/o/{ORG_A}/handbook/{handbook}"),
            format!("{C}/o/{ORG_A}/brief/{brief}"),
        ] {
            let reply = fx.get(&path).by(Some("key_a")).send().await;
            assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
            out.push(reply.etag());
        }
        out
    };
    let first = tags().await;
    // Another organization's publish enters no tag.
    let b = create(&fx, "key_b", "handbook", json!({"title": "B"})).await;
    publish(&fx, "key_b", "handbook", &b, "en", 1).await;
    assert_eq!(tags().await, first);
    // An application publish changes the tag of a type that can
    // reference application entries, not the other.
    let other = create(&fx, "app_key", "page", json!({"title": "Other"})).await;
    publish(&fx, "app_key", "page", &other, "en", 1).await;
    let second = tags().await;
    assert_ne!(second[0], first[0]);
    assert_eq!(second[1], first[1]);
    // A matching `If-None-Match` is a 304 on the organization's path.
    let reply = fx
        .get(&format!("{C}/o/{ORG_A}/handbook/{handbook}"))
        .by(Some("key_a"))
        .header("if-none-match", &second[0])
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn the_path_chooses_the_asset_scope_whatever_the_active_organization() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = Dirs::new();
    let fx = start(&test, &dirs).await;
    // An application editor whose active organization is A uploads on
    // the application path: the asset is the application's, and an
    // application entry can use it.
    let logo = fx
        .post(&manage("/assets?filename=logo.png"))
        .by(Some("app_editor_a"))
        .header("content-type", "image/png")
        .raw(&png(2, 2, 8))
        .send()
        .await;
    assert_eq!(logo.status, StatusCode::CREATED, "{:?}", logo.body);
    let logo_id = logo.body["id"].as_str().unwrap().to_owned();
    assert_eq!(
        logo.headers["location"],
        format!("{C}/manage/assets/{logo_id}").as_str()
    );
    assert_eq!(
        strings(
            &fx.db,
            &format!("SELECT scope FROM merezha_content.assets WHERE id = '{logo_id}'")
        )
        .await,
        vec!["app".to_owned()]
    );
    let page = create(
        &fx,
        "app_editor_a",
        "page",
        json!({"title": "Home", "image": {"id": logo_id}}),
    )
    .await;
    publish(&fx, "app_editor_a", "page", &page, "en", 1).await;
    let name = logo.body["filename"].as_str().unwrap();
    let served = fx
        .get(&format!("{C}/assets/{logo_id}/{name}"))
        .by(None)
        .send()
        .await;
    assert_eq!(served.status, StatusCode::OK);
    // The same principal on its organization's path works in A ...
    let mine = fx
        .post(&format!("{C}/o/{ORG_A}/manage/assets?filename=mine.png"))
        .by(Some("app_editor_a"))
        .header("content-type", "image/png")
        .raw(&png(3, 3, 8))
        .send()
        .await;
    assert_eq!(mine.status, StatusCode::CREATED, "{:?}", mine.body);
    let mine_id = mine.body["id"].as_str().unwrap();
    // The organization's upload route bounds its own body too: past
    // `http.body_limit`, within `media.max_upload_bytes`.
    let large = fx
        .post(&format!("{C}/o/{ORG_A}/manage/assets?filename=large.png"))
        .by(Some("key_a"))
        .header("content-type", "image/png")
        .raw(&png(64, 64, 3 << 20))
        .send()
        .await;
    assert_eq!(large.status, StatusCode::CREATED, "{:?}", large.body);
    assert_eq!(
        mine.headers["location"],
        format!("{C}/o/{ORG_A}/manage/assets/{mine_id}").as_str()
    );
    // ... and never on another organization's, although its application
    // grant would satisfy a check there: the path's organization must be
    // its active one.
    for who in ["app_editor_a", "app_key", "key_a"] {
        let other = fx
            .get(&format!("{C}/o/{ORG_B}/manage/assets"))
            .by(Some(who))
            .send()
            .await;
        assert_eq!(
            (other.status, other.code()),
            (StatusCode::FORBIDDEN, "unauthorized"),
            "{who}"
        );
    }
    let anonymous = fx
        .get(&format!("{C}/o/{ORG_A}/manage/assets"))
        .by(None)
        .send()
        .await;
    assert_eq!(anonymous.code(), "unauthenticated");
    // A tenant key never reaches the application's assets.
    let refused = fx
        .get(&manage(&format!("/assets/{logo_id}")))
        .by(Some("key_a"))
        .send()
        .await;
    assert_eq!(refused.code(), "unauthorized");
    // `usage` counts the asset's own scope by stage and every other scope
    // together, the refs a plain delete is refused for.
    create(
        &fx,
        "key_a",
        "handbook",
        json!({"title": "Uses the logo", "image": {"id": logo_id}}),
    )
    .await;
    let read = fx
        .get(&manage(&format!("/assets/{logo_id}")))
        .by(Some("app_key"))
        .send()
        .await;
    assert_eq!(
        read.body["usage"],
        json!({"draft": 1, "published": 1, "elsewhere": 1}),
        "{:?}",
        read.body
    );
    let delete = fx
        .call(Method::DELETE, &manage(&format!("/assets/{logo_id}")))
        .by(Some("app_key"))
        .send()
        .await;
    assert_eq!(delete.code(), "asset_in_use");
}
