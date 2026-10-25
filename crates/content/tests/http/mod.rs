//! An HTTP harness over a started host: the content module on a real
//! database, its router with test principals, and request helpers.
#![allow(dead_code)]
use axum::{
    Router,
    body::Body,
    extract::Request,
    http::{HeaderMap, Method, StatusCode},
    middleware::Next,
};
use merezha_content::{ActivationOptions, ContentHandle, ContentModule};
use merezha_domain::{Failure, Identity, MessageKind, Metadata};
use merezha_jobs::PgJobs;
use merezha_platform::{
    ConfigInput, Database, Host, HostOptions, HostParts, Timeouts,
    edge::Authenticated,
    host::configure,
    ports::{
        AccessDenied, Allowed, Authorizer, DenyReason, GrantSnapshot, Grants, KeyLimit, Permission,
        PermissionSet, Principal, PrincipalKind, ResourceScope,
    },
};
use merezha_postgres::TestDatabase;
use serde_json::Value;
use std::{path::Path, sync::Arc, time::Duration};
use tower::ServiceExt;

/// The model most tests use: two locales, an article with every filter
/// and sort shape, an author, a singleton.
pub const MODEL: &str = r#"
version = 1
locales = [{ code = "en", default = true }, { code = "fr", fallback = "en" }]

[components.quote]
fields = [{ name = "text", type = "text", max_length = 100 }]

[types.author]
access = "public"
title_field = "name"
fields = [{ name = "name", type = "string", sortable = true }]

[types.article]
access = "public"
title_field = "title"
fields = [
  { name = "title", type = "string", required = true, max_length = 200, sortable = true },
  { name = "slug", type = "slug", from = "title" },
  { name = "category", type = "enum", values = ["news", "guide", "release"], filterable = true },
  { name = "reading_minutes", type = "integer", min = 1, max = 600, sortable = true },
  { name = "score", type = "decimal", precision = 10, scale = 2, sortable = true },
  { name = "weight", type = "float", sortable = true },
  { name = "featured", type = "boolean", filterable = true },
  { name = "published_on", type = "date", sortable = true },
  { name = "legacy_code", type = "string", unique = true },
  { name = "author", type = "reference", to = ["author"], localized = false, filterable = true },
  { name = "related", type = "reference", to = ["article", "author"], cardinality = "many", filterable = true },
  { name = "tags", type = "list", of = { type = "string", max_length = 40 }, max_items = 20, filterable = true },
  { name = "body", type = "rich_text", annotations = ["link", "entry_link"] },
  { name = "sections", type = "blocks", components = ["quote"] },
]

[types.site_settings]
kind = "singleton"
access = "public"
fields = [{ name = "site_name", type = "string", required = true }]
"#;

/// Three locales in a chain (`fr-BE` falls back to `fr`, `fr` to `en`):
/// a localized article with a shared, `require_published` author and a
/// unique slug, the author, a non-localized page and a singleton.
pub const CHAIN_MODEL: &str = r#"
version = 1
locales = [
  { code = "en", default = true },
  { code = "fr", fallback = "en" },
  { code = "fr-BE", fallback = "fr" },
]

[types.author]
access = "public"
title_field = "name"
fields = [{ name = "name", type = "string", required = true }]

[types.article]
access = "public"
title_field = "title"
fields = [
  { name = "title", type = "string", required = true, sortable = true },
  { name = "slug", type = "slug", from = "title" },
  { name = "code", type = "string", unique = true },
  { name = "author", type = "reference", to = ["author"], localized = false, require_published = true },
  { name = "related", type = "reference", to = ["article"], cardinality = "many" },
]

[types.page]
access = "public"
localized = false
title_field = "title"
fields = [{ name = "title", type = "string", required = true }]

[types.site_settings]
kind = "singleton"
access = "public"
fields = [{ name = "site_name", type = "string", required = true }]
"#;

/// The content permissions an editor holds.
pub const EDITOR: [&str; 2] = ["content.entries.read", "content.entries.write"];

/// A table-driven fixture policy over K's scopes until ID PR 2: no
/// principal is `unauthenticated`, a principal is decided by its own
/// grant snapshot.
pub struct FixturePolicy;
impl merezha_application::Authorizer for FixturePolicy {
    fn authorize(
        &self,
        _: MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &Metadata,
    ) -> Result<(), Failure> {
        Err(Failure::permanent("unauthorized"))
    }
}
impl Authorizer for FixturePolicy {
    fn check(
        &self,
        principal: Option<&Principal>,
        permission: &Permission,
        scope: &ResourceScope,
    ) -> Result<Allowed, AccessDenied> {
        match principal {
            None => Err(AccessDenied::new(DenyReason::Unauthenticated)),
            Some(principal) => principal.check(permission, scope),
        }
    }
}

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn set(names: &[&str]) -> PermissionSet {
    names.iter().map(|n| id(n)).collect()
}

/// Organization A of the C9 tests, an identity-shaped `org_` id.
pub const ORG_A: &str = "org_0192f5c4e1b07a3c9d2e4f6a8b0c1d2a";
/// Organization B of the C9 tests.
pub const ORG_B: &str = "org_0192f5c4e1b07a3c9d2e4f6a8b0c1d2b";
/// Every content permission, as an organization owner holds them.
pub const CONTENT_ALL: [&str; 11] = [
    "content.model.read",
    "content.entries.read",
    "content.entries.write",
    "content.entries.publish",
    "content.entries.delete",
    "content.entries.purge",
    "content.assets.read",
    "content.assets.write",
    "content.assets.delete",
    "content.delivery.read",
    "content.preview",
];

/// The organization principals of the C9 tests, as identity builds them:
/// `key_a` and `key_b` are API keys pinned to organization A or B, each
/// listing every content permission over an owner's tenant grants;
/// `member_a` is a session of a member of A with those grants; `app_key`
/// an application-scoped key with every content permission and no
/// tenant; `app_editor_a` a session holding every content permission at
/// application scope whose active organization is A.
fn organization_principal(name: &str) -> Option<Principal> {
    let tenant = match name {
        "key_a" | "member_a" => Some(id(ORG_A)),
        "key_b" | "member_b" => Some(id(ORG_B)),
        "app_key" => None,
        "app_editor_a" => Some(id(ORG_A)),
        _ => return None,
    };
    let all = set(&CONTENT_ALL);
    let grants = match &tenant {
        Some(_) if name != "app_editor_a" => {
            GrantSnapshot::empty().with_tenant(Grants::new(vec![id("owner")], all.clone()))
        }
        _ => GrantSnapshot::empty().with_application(Grants::new(vec![id("editor")], all.clone())),
    };
    let key = name.starts_with("key_") || name == "app_key";
    let grants = if key {
        grants.with_key_limit(KeyLimit::new(all, tenant.clone()))
    } else {
        grants
    };
    Some(Principal {
        subject: id(&format!("{}_{name}", if key { "key" } else { "usr" })),
        kind: if key {
            PrincipalKind::ApiKey
        } else {
            PrincipalKind::User
        },
        user: Some(id(&format!("usr_{name}"))),
        service_account: None,
        tenant,
        grants: Arc::new(grants),
        session: None,
    })
}

/// The principals tests send through the `x-test-principal` header.
pub fn principal(name: &str) -> Principal {
    if let Some(principal) = organization_principal(name) {
        return principal;
    }
    let grants = match name {
        "editor" => {
            GrantSnapshot::empty().with_application(Grants::new(vec![id("editor")], set(&EDITOR)))
        }
        "reader" => GrantSnapshot::empty().with_application(Grants::new(
            vec![id("reader")],
            set(&["content.entries.read"]),
        )),
        // A per-type grant: write `article` only.
        "article_writer" => GrantSnapshot::empty().with_application_resource(
            id("content_type"),
            id("article"),
            set(&EDITOR),
        ),
        "publisher" => GrantSnapshot::empty().with_application(Grants::new(
            vec![id("publisher")],
            set(&["content.entries.publish"]),
        )),
        "deleter" => GrantSnapshot::empty().with_application(Grants::new(
            vec![id("deleter")],
            set(&["content.entries.delete"]),
        )),
        "purger" => GrantSnapshot::empty().with_application(Grants::new(
            vec![id("purger")],
            set(&["content.entries.delete", "content.entries.purge"]),
        )),
        // Publish `article` only.
        "article_publisher" => GrantSnapshot::empty().with_application_resource(
            id("content_type"),
            id("article"),
            set(&["content.entries.publish", "content.entries.read"]),
        ),
        // Every content entry permission.
        "admin" => GrantSnapshot::empty().with_application(Grants::new(
            vec![id("admin")],
            set(&[
                "content.entries.read",
                "content.entries.write",
                "content.entries.publish",
                "content.entries.delete",
                "content.entries.purge",
            ]),
        )),
        // Read and write on another type only.
        "author_writer" => GrantSnapshot::empty().with_application_resource(
            id("content_type"),
            id("author"),
            set(&EDITOR),
        ),
        // Delivery (C5): drafts through `stage=draft`.
        "previewer" => GrantSnapshot::empty().with_application(Grants::new(
            vec![id("previewer")],
            set(&["content.preview"]),
        )),
        // C7: every asset permission with the entry ones.
        "media" => GrantSnapshot::empty().with_application(Grants::new(
            vec![id("media")],
            set(&[
                "content.entries.read",
                "content.entries.write",
                "content.entries.publish",
                "content.assets.read",
                "content.assets.write",
                "content.assets.delete",
            ]),
        )),
        // Reads assets only.
        "asset_reader" => GrantSnapshot::empty().with_application(Grants::new(
            vec![id("asset_reader")],
            set(&["content.assets.read"]),
        )),
        // The ready-made delivery permission at application scope.
        "delivery_reader" => GrantSnapshot::empty().with_application(Grants::new(
            vec![id("delivery_reader")],
            set(&["content.delivery.read"]),
        )),
        // `content.delivery.read` on the `brief` type only.
        "brief_reader" => GrantSnapshot::empty().with_application_resource(
            id("content_type"),
            id("brief"),
            set(&["content.delivery.read"]),
        ),
        // `content.delivery.read` on the `article` type only.
        "article_delivery" => GrantSnapshot::empty().with_application_resource(
            id("content_type"),
            id("article"),
            set(&["content.delivery.read"]),
        ),
        "model_reader" => GrantSnapshot::empty().with_application(Grants::new(
            vec![id("model_reader")],
            set(&["content.model.read"]),
        )),
        // Tenant grants never satisfy application-scoped types.
        "tenant_editor" => {
            GrantSnapshot::empty().with_tenant(Grants::new(vec![id("editor")], set(&EDITOR)))
        }
        _ => GrantSnapshot::empty(),
    };
    Principal {
        subject: id(&format!("usr_{name}")),
        kind: PrincipalKind::User,
        user: Some(id(&format!("usr_{name}"))),
        service_account: None,
        tenant: (name == "tenant_editor").then(|| id("acme")),
        grants: Arc::new(grants),
        session: None,
    }
}

/// Insert the principal named by `x-test-principal`, as an identity
/// authenticator would.
async fn authenticate(mut request: Request, next: Next) -> axum::response::Response {
    let principal = request
        .headers()
        .get("x-test-principal")
        .and_then(|v| v.to_str().ok())
        .map(|name| Arc::new(principal(name)));
    if principal.is_some() {
        request.extensions_mut().insert(Authenticated(principal));
    }
    next.run(request).await
}

/// A started host serving the content module.
pub struct Fixture {
    pub host: Host,
    pub client: Client,
    pub handle: ContentHandle,
    pub db: Database,
    pub dir: tempfile::TempDir,
    pub jobs: Arc<PgJobs>,
}

/// Write the project into `dir`: `merezha.toml` with `[modules.content]`
/// plus `settings` (keys of that section), and `content.toml`.
pub fn project(dir: &Path, migrate: bool, settings: &str, model: &str) {
    project_with(dir, migrate, "", settings, model);
}

/// [`project`] with `top`, other top-level sections such as `[jobs]`,
/// before `[modules.content]`.
pub fn project_with(dir: &Path, migrate: bool, top: &str, settings: &str, model: &str) {
    std::fs::write(dir.join("content.toml"), model).unwrap();
    std::fs::write(
        dir.join("merezha.toml"),
        format!(
            "[app]\nid = \"site\"\n[database]\nurl = {{ env = \"CONTENT_DATABASE_URL\" }}\n\
             migrate = {migrate}\npool_size = 16\nstatement_timeout_ms = 30000\n\
             [runtime]\nmanifest = \"runtime.toml\"\n{top}\n[modules.content]\n{settings}\n"
        ),
    )
    .unwrap();
}

/// A migrating host on `model` with the top-level sections `top`.
pub async fn start_with(test: &TestDatabase, top: &str, settings: &str, model: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    project_with(dir.path(), true, top, settings, model);
    start_in(dir, test, Arc::new(FixturePolicy)).await
}

/// `[jobs]` for a host that runs no job workers.
pub const NO_WORKERS: &str = "[jobs]\nworkers = false\n";

fn fast() -> ActivationOptions {
    ActivationOptions {
        timeout: Duration::from_secs(60),
        retry_backoff: Duration::from_millis(10),
    }
}

/// Start a host on `dir` with `policy`.
pub async fn start_in(
    dir: tempfile::TempDir,
    test: &TestDatabase,
    policy: Arc<dyn Authorizer>,
) -> Fixture {
    start_with_options(dir, test, policy, HostOptions::default(), false).await
}

/// A migrating host on `model` with `settings` whose content module
/// accepts organization types, as beside identity (C9).
pub async fn start_organizations(test: &TestDatabase, settings: &str, model: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), true, settings, model);
    start_with_options(
        dir,
        test,
        Arc::new(FixturePolicy),
        HostOptions::default(),
        true,
    )
    .await
}

/// A migrating host on `model` whose pool records its statements in
/// `harness`'s metrics (`db.client.operation.duration`).
pub async fn start_metered(
    test: &TestDatabase,
    settings: &str,
    model: &str,
    harness: &merezha_telemetry::testing::Harness,
) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), true, settings, model);
    let options = HostOptions {
        meters: harness.telemetry().meters(),
        ..HostOptions::default()
    };
    start_with_options(dir, test, Arc::new(FixturePolicy), options, false).await
}

async fn start_with_options(
    dir: tempfile::TempDir,
    test: &TestDatabase,
    policy: Arc<dyn Authorizer>,
    options: HostOptions,
    organizations: bool,
) -> Fixture {
    let module = ContentModule::with_options(fast()).with_organizations(organizations);
    let handle = module.handle();
    let input = ConfigInput {
        path: dir.path().join("merezha.toml"),
        env: vec![("CONTENT_DATABASE_URL".into(), test.url().into())],
        overrides: Vec::new(),
    };
    let (config, modules) = configure(&input, vec![Box::new(module)]).expect("configured");
    let parts = HostParts {
        routes: Router::new(),
        authorizer: policy,
        timeouts: Timeouts {
            drain: Duration::from_secs(5),
            ..Timeouts::default()
        },
        jobs: None,
    };
    let jobs = Arc::new(PgJobs::new(&config.jobs));
    let parts = HostParts {
        jobs: Some(jobs.clone()),
        ..parts
    };
    let db = Database::connect(&config.database).unwrap();
    let host = Host::start_with(config, modules, parts, options)
        .await
        .expect("host starts");
    let router = host.router().layer(axum::middleware::from_fn(authenticate));
    Fixture {
        host,
        client: Client { router },
        handle,
        db,
        dir,
        jobs,
    }
}

/// A migrating host on `model` with `settings` and the fixture policy.
pub async fn start(test: &TestDatabase, settings: &str, model: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), true, settings, model);
    start_in(dir, test, Arc::new(FixturePolicy)).await
}

/// One response: status, headers and the JSON body (`Null` when empty).
#[derive(Debug)]
pub struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Value,
    /// The body as sent, to check member order.
    pub text: String,
}
impl Reply {
    pub fn code(&self) -> &str {
        self.body["error"]["code"].as_str().unwrap_or("")
    }
    pub fn details(&self) -> Vec<(String, String)> {
        self.body["error"]["details"]
            .as_array()
            .map(|d| {
                d.iter()
                    .map(|d| {
                        (
                            d["path"].as_str().unwrap().to_owned(),
                            d["code"].as_str().unwrap().to_owned(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn etag(&self) -> String {
        self.headers["etag"].to_str().unwrap().to_owned()
    }
}

/// A request builder bound to a fixture's router.
pub struct Call<'a> {
    router: &'a Router,
    method: Method,
    uri: String,
    principal: Option<String>,
    headers: Vec<(String, String)>,
    body: Option<Vec<u8>>,
}
/// Requests against a router, cheap to clone into tasks.
#[derive(Clone)]
pub struct Client {
    pub router: Router,
}
impl std::ops::Deref for Fixture {
    type Target = Client;
    fn deref(&self) -> &Client {
        &self.client
    }
}
impl Client {
    pub fn call(&self, method: Method, uri: &str) -> Call<'_> {
        Call {
            router: &self.router,
            method,
            uri: uri.to_owned(),
            principal: Some("editor".into()),
            headers: Vec::new(),
            body: None,
        }
    }
    pub fn get(&self, uri: &str) -> Call<'_> {
        self.call(Method::GET, uri)
    }
    pub fn post(&self, uri: &str) -> Call<'_> {
        self.call(Method::POST, uri)
    }
    pub fn put(&self, uri: &str) -> Call<'_> {
        self.call(Method::PUT, uri)
    }
    pub fn patch(&self, uri: &str) -> Call<'_> {
        self.call(Method::PATCH, uri)
            .header("content-type", "application/merge-patch+json")
    }
    /// `POST /entries/{type}` with `body`; the created variant.
    pub async fn create(&self, content_type: &str, body: Value) -> Reply {
        let reply = self
            .post(&format!("/v1/content/manage/entries/{content_type}"))
            .json(&body)
            .send()
            .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
        reply
    }
}
impl Call<'_> {
    pub fn by(mut self, name: Option<&str>) -> Self {
        self.principal = name.map(str::to_owned);
        self
    }
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }
    pub fn if_match(self, revision: i64) -> Self {
        self.header("if-match", &format!("\"r{revision}\""))
    }
    pub fn json(mut self, body: &Value) -> Self {
        self.body = Some(serde_json::to_vec(body).unwrap());
        self
    }
    pub fn raw(mut self, body: &[u8]) -> Self {
        self.body = Some(body.to_vec());
        self
    }
    pub async fn send(self) -> Reply {
        let mut builder = axum::http::Request::builder()
            .method(self.method)
            .uri(&self.uri);
        if let Some(principal) = &self.principal {
            builder = builder.header("x-test-principal", principal);
        }
        for (name, value) in &self.headers {
            builder = builder.header(name, value);
        }
        if self.body.is_some()
            && !self
                .headers
                .iter()
                .any(|(n, _)| n.eq_ignore_ascii_case("content-type"))
        {
            builder = builder.header("content-type", "application/json");
        }
        let request = builder
            .body(Body::from(self.body.unwrap_or_default()))
            .unwrap();
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), 64 << 20)
            .await
            .unwrap();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()))
        };
        Reply {
            status,
            headers,
            body,
            text: String::from_utf8_lossy(&bytes).into_owned(),
        }
    }
}

/// `/v1/content/manage/entries/{type}/{id}/{locale}`.
pub fn variant(content_type: &str, id: &str, locale: &str) -> String {
    format!("/v1/content/manage/entries/{content_type}/{id}/{locale}")
}

/// A digest of every content row and outbox record, to prove a request
/// changed nothing.
pub async fn snapshot(db: &Database) -> String {
    let client = db.acquire(soon()).await.unwrap();
    let mut out = String::new();
    for sql in [
        "SELECT coalesce(string_agg(t::text, '|' ORDER BY t::text), '') FROM merezha_content.entries t",
        "SELECT coalesce(string_agg(t::text, '|' ORDER BY t::text), '') FROM merezha_content.variants t",
        "SELECT coalesce(string_agg(t::text, '|' ORDER BY t::text), '') FROM merezha_content.revisions t",
        "SELECT coalesce(string_agg(t::text, '|' ORDER BY t::text), '') FROM merezha_content.refs t",
        "SELECT coalesce(string_agg(t::text, '|' ORDER BY t::text), '') FROM merezha_content.published t",
        "SELECT coalesce(string_agg(t::text, '|' ORDER BY t::text), '') FROM merezha_content.generations t",
        "SELECT count(*)::text FROM merezha_jobs.outbox",
    ] {
        let row = client.query_one(sql, &[]).await.unwrap();
        out.push_str(&row.get::<_, String>(0));
        out.push('\n');
    }
    out
}

pub fn soon() -> std::time::Instant {
    std::time::Instant::now() + Duration::from_secs(30)
}

pub async fn execute(db: &Database, sql: &str) {
    let client = db.acquire(soon()).await.unwrap();
    client
        .batch_execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{e}: {sql}"));
}

pub async fn scalar(db: &Database, sql: &str) -> i64 {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query_one(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{e}: {sql}"))
        .get(0)
}

/// Seed one entry of `content_type` in `scope` with an `en` variant at
/// revision 1, straight through SQL; returns its id.
pub async fn seed_row(db: &Database, scope: &str, content_type: &str, data: &str) -> String {
    let client = db.acquire(soon()).await.unwrap();
    let id: String = client
        .query_one(
            "WITH e AS (
               INSERT INTO merezha_content.entries (id, scope, content_type, created_at, created_by)
               VALUES (gen_random_uuid(), $1, $2, now(), 'sys_test') RETURNING id
             ), v AS (
               INSERT INTO merezha_content.variants
                   (entry_id, locale, scope, content_type, draft_revision, draft_data,
                    draft_updated_at, draft_updated_by)
               SELECT id, 'en', $1, $2, 1, $3::text::jsonb, now(), 'sys_test' FROM e
               RETURNING entry_id
             )
             INSERT INTO merezha_content.revisions
                 (entry_id, locale, revision, model_version, kind, data, created_at, created_by)
             SELECT entry_id, 'en', 1, 1, 'create', $3::text::jsonb, now(), 'sys_test' FROM v
             RETURNING entry_id::text",
            &[&scope, &content_type, &data],
        )
        .await
        .unwrap()
        .get(0);
    id
}

/// `"<locale>:<source_locale>:<revision>"` of every `published` row of
/// entry `id`, by locale.
pub async fn rows(db: &Database, id: &str) -> Vec<String> {
    strings(
        db,
        &format!(
            "SELECT locale || ':' || source_locale || ':' || revision \
               FROM merezha_content.published WHERE entry_id = '{id}' ORDER BY locale"
        ),
    )
    .await
}

/// One text column of every row of `sql`.
pub async fn strings(db: &Database, sql: &str) -> Vec<String> {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{e}: {sql}"))
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect()
}

/// The envelopes of the content events on `topic`, oldest first.
pub async fn events(db: &Database, topic: &str) -> Vec<Value> {
    strings(
        db,
        &format!(
            "SELECT payload::text FROM merezha_jobs.outbox \
              WHERE source = 'content' AND topic = '{topic}' ORDER BY id"
        ),
    )
    .await
    .iter()
    .map(|text| serde_json::from_str(text).unwrap())
    .collect()
}

/// Content event records per topic, as `"<topic>=<count>"`.
pub async fn event_counts(db: &Database) -> Vec<String> {
    strings(
        db,
        "SELECT topic || '=' || count(*) FROM merezha_jobs.outbox \
          WHERE source = 'content' AND topic <> 'content.model_activated' \
          GROUP BY topic ORDER BY topic",
    )
    .await
}

/// The generation of `scope`, 0 without a row.
pub async fn generation(db: &Database, scope: &str) -> i64 {
    scalar(
        db,
        &format!(
            "SELECT coalesce((SELECT generation FROM merezha_content.generations \
              WHERE scope = '{scope}'), 0)"
        ),
    )
    .await
}

impl Client {
    /// `POST .../{locale}/publish` with `If-Match: "r<revision>"`, as
    /// `admin`.
    pub async fn publish(
        &self,
        content_type: &str,
        id: &str,
        locale: &str,
        revision: i64,
    ) -> Reply {
        self.post(&format!("{}/publish", variant(content_type, id, locale)))
            .by(Some("admin"))
            .if_match(revision)
            .send()
            .await
    }
    /// `POST .../{locale}/unpublish` with `If-Match: "r<revision>"`, as
    /// `admin`.
    pub async fn unpublish(
        &self,
        content_type: &str,
        id: &str,
        locale: &str,
        revision: i64,
    ) -> Reply {
        self.post(&format!("{}/unpublish", variant(content_type, id, locale)))
            .by(Some("admin"))
            .if_match(revision)
            .send()
            .await
    }
    /// Create the `locale` variant of `id` with `fields`.
    pub async fn add_variant(
        &self,
        content_type: &str,
        id: &str,
        locale: &str,
        fields: Value,
    ) -> Reply {
        let reply = self
            .put(&variant(content_type, id, locale))
            .header("if-none-match", "*")
            .json(&serde_json::json!({ "fields": fields }))
            .send()
            .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
        reply
    }
}

/// The id of a variant document.
pub fn id_of(reply: &Reply) -> String {
    reply.body["sys"]["id"].as_str().unwrap().to_owned()
}

/// Whether `host` reports ready within a few seconds (the jobs runtime
/// turns ready after its first subscription and schedule sync).
pub async fn eventually_ready(host: &Host) -> bool {
    for _ in 0..50 {
        if host.readiness().check().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Wait up to ten seconds for `sql`'s single text value to be `expected`.
pub async fn wait_for(db: &Database, sql: &str, expected: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let found = strings(db, sql).await;
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

/// Backends of `db`'s database blocked by backend `pid`.
pub async fn blocked_by(db: &Database, pid: i32) -> Vec<i32> {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query(
            "SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
            &[&pid],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect()
}

/// Wait up to ten seconds for a backend blocked by `pid`; returns it.
pub async fn wait_blocked_by(db: &Database, pid: i32) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(found) = blocked_by(db, pid).await.first() {
            return *found;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "nothing blocked by backend {pid}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
