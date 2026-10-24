//! CT-C5 delivery over the axum router and a real database: access modes,
//! preview, `fields`, `populate`, by-slug lookups, ETags and 304s from the
//! scope generations, cache headers and the model endpoints.
mod http;

use axum::http::StatusCode;
use http::*;
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};
use std::time::Duration;

/// Three locales in a chain, a public article referencing every access
/// mode, an authenticated note, a permission-guarded brief, a
/// non-localized page and a singleton.
const DELIVERY_MODEL: &str = r#"
version = 1
locales = [
  { code = "en", default = true },
  { code = "fr", fallback = "en" },
  { code = "fr-BE", fallback = "fr" },
]

[types.author]
access = "public"
title_field = "name"
fields = [
  { name = "name", type = "string", sortable = true },
  { name = "friend", type = "reference", to = ["author"] },
]

[types.article]
access = "public"
title_field = "title"
fields = [
  { name = "title", type = "string", sortable = true },
  { name = "slug", type = "slug", from = "title" },
  { name = "category", type = "enum", values = ["news", "guide"], filterable = true },
  { name = "reading_minutes", type = "integer", sortable = true },
  { name = "author", type = "reference", to = ["author"], localized = false },
  { name = "related", type = "reference", to = ["article", "note", "brief"], cardinality = "many" },
  { name = "body", type = "rich_text", annotations = ["link", "entry_link"] },
]

[types.note]
access = "authenticated"
title_field = "title"
fields = [{ name = "title", type = "string" }]

[types.brief]
access = { permission = "content.delivery.read" }
title_field = "title"
fields = [
  { name = "title", type = "string" },
  { name = "author", type = "reference", to = ["author"] },
]

[types.page]
access = "public"
localized = false
fields = [{ name = "title", type = "string" }]

[types.settings]
kind = "singleton"
access = "public"
fields = [{ name = "site_name", type = "string" }]
"#;

const PUBLIC_CACHE: &str = "public, max-age=60, stale-while-revalidate=300";
const PRIVATE_CACHE: &str = "private, no-cache";

/// Create an entry of `content_type` with `fields` (as an editor) and
/// publish its `en` variant; its id.
async fn published(fx: &Fixture, content_type: &str, fields: Value) -> String {
    let id = draft(fx, content_type, fields).await;
    let reply = fx.publish(content_type, &id, "en", 1).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    id
}
/// Create an entry and leave it a draft; its id.
async fn draft(fx: &Fixture, content_type: &str, fields: Value) -> String {
    id_of(&fx.create(content_type, json!({ "fields": fields })).await)
}
/// Save a new draft revision of `id`'s `locale` variant.
async fn save(
    fx: &Fixture,
    content_type: &str,
    id: &str,
    locale: &str,
    revision: i64,
    fields: Value,
) {
    let reply = fx
        .put(&variant(content_type, id, locale))
        .if_match(revision)
        .json(&json!({ "fields": fields }))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
}
fn author_ref(id: &str) -> Value {
    json!({"id": id, "type": "author"})
}
/// `GET` as `who` (`None` is anonymous).
async fn read(fx: &Fixture, uri: &str, who: Option<&str>) -> Reply {
    fx.get(uri).by(who).send().await
}

#[tokio::test]
async fn anonymous_reads_work_on_public_types_and_get_401_on_the_others() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", DELIVERY_MODEL).await;
    let article = published(&fx, "article", json!({"title": "Hello"})).await;
    let note = published(&fx, "note", json!({"title": "Members"})).await;
    let brief = published(&fx, "brief", json!({"title": "Partners"})).await;

    let reply = read(&fx, &format!("/v1/content/article/{article}"), None).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert_eq!(reply.body["sys"]["id"], article.as_str());
    assert_eq!(reply.body["sys"]["type"], "article");
    assert_eq!(reply.body["sys"]["locale"], "en");
    assert_eq!(reply.body["sys"]["source_locale"], "en");
    assert_eq!(reply.body["sys"]["revision"], 1);
    assert!(reply.body["sys"]["published_at"].is_string());
    assert_eq!(reply.body["fields"]["title"], "Hello");
    assert_eq!(reply.body["fields"]["slug"], "hello");
    let list = read(&fx, "/v1/content/article", None).await;
    assert_eq!(list.status, StatusCode::OK);
    assert_eq!(list.body["items"].as_array().unwrap().len(), 1);

    // Every route of a non-public type needs a principal, before anything
    // about the entry is read: an unknown id answers the same.
    let missing = "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11";
    for uri in [
        format!("/v1/content/note/{note}"),
        format!("/v1/content/note/{missing}"),
        "/v1/content/note".to_owned(),
        "/v1/content/note/by-slug/members".to_owned(),
        format!("/v1/content/brief/{brief}"),
        format!("/v1/content/brief/{missing}"),
        "/v1/content/brief?stage=draft".to_owned(),
    ] {
        let reply = read(&fx, &uri, None).await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{uri}");
        assert_eq!(reply.code(), "unauthenticated", "{uri}");
        assert!(reply.headers.get("etag").is_none(), "{uri}");
        assert_eq!(reply.headers["cache-control"], "private, no-store");
    }
}

#[tokio::test]
async fn authenticated_types_serve_any_principal_and_organization_paths_only_their_types() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", DELIVERY_MODEL).await;
    let note = published(&fx, "note", json!({"title": "Members"})).await;
    let article = published(&fx, "article", json!({"title": "Hello"})).await;
    // A principal without a single grant reads an authenticated type.
    for who in ["nobody", "editor", "tenant_editor"] {
        let reply = read(&fx, &format!("/v1/content/note/{note}"), Some(who)).await;
        assert_eq!(reply.status, StatusCode::OK, "{who}");
        assert_eq!(reply.body["fields"]["title"], "Members");
    }
    let list = read(&fx, "/v1/content/note", Some("nobody")).await;
    assert_eq!(list.body["items"].as_array().unwrap().len(), 1);
    // An application type is not served under an organization's path, and
    // an organization name that is no identity is no path at all.
    for uri in [
        format!("/v1/content/o/acme/article/{article}"),
        "/v1/content/o/acme/article".to_owned(),
        format!("/v1/content/o/acme/note/{note}"),
        format!("/v1/content/o/bad%20org/article/{article}"),
    ] {
        let reply = read(&fx, &uri, Some("tenant_editor")).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(reply.code(), "content_type_not_found", "{uri}");
    }
    // Organization types and their tenant rule are unit-tested in
    // `delivery::tests` (the model refuses them until C9).
}

#[tokio::test]
async fn a_permission_type_needs_the_permission_in_its_type_resource_scope() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", DELIVERY_MODEL).await;
    let brief = published(&fx, "brief", json!({"title": "Partners"})).await;
    let uri = format!("/v1/content/brief/{brief}");
    for (who, status) in [
        ("nobody", StatusCode::FORBIDDEN),
        // Granted on another type only.
        ("article_delivery", StatusCode::FORBIDDEN),
        // Management grants are not delivery grants.
        ("admin", StatusCode::FORBIDDEN),
        ("brief_reader", StatusCode::OK),
        ("delivery_reader", StatusCode::OK),
    ] {
        let reply = read(&fx, &uri, Some(who)).await;
        assert_eq!(reply.status, status, "{who}: {:?}", reply.body);
        if status == StatusCode::FORBIDDEN {
            assert_eq!(reply.code(), "unauthorized");
        } else {
            assert_eq!(reply.body["fields"]["title"], "Partners");
        }
    }
    let list = read(&fx, "/v1/content/brief", Some("brief_reader")).await;
    assert_eq!(list.status, StatusCode::OK);
    assert_eq!(
        read(&fx, "/v1/content/brief", Some("article_delivery"))
            .await
            .status,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn unpublished_and_missing_entries_both_give_404() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", DELIVERY_MODEL).await;
    let draft_only = draft(&fx, "article", json!({"title": "Draft"})).await;
    let author = published(&fx, "author", json!({"name": "Ada"})).await;
    let gone = published(&fx, "article", json!({"title": "Gone"})).await;
    assert_eq!(
        fx.unpublish("article", &gone, "en", 1).await.status,
        StatusCode::OK
    );
    let mut bodies = Vec::new();
    for uri in [
        format!("/v1/content/article/{draft_only}"),
        format!("/v1/content/article/{gone}"),
        "/v1/content/article/0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11".to_owned(),
        "/v1/content/article/not-a-uuid".to_owned(),
        // An entry of another type.
        format!("/v1/content/article/{author}"),
        "/v1/content/article/by-slug/draft".to_owned(),
        "/v1/content/article/by-slug/gone".to_owned(),
        "/v1/content/article/by-slug/nothing".to_owned(),
        // A type without a slug field.
        "/v1/content/page/by-slug/anything".to_owned(),
    ] {
        let reply = read(&fx, &uri, None).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(reply.code(), "entry_not_found", "{uri}");
        assert!(reply.headers.get("etag").is_none());
        assert_eq!(reply.headers["vary"], "Authorization, Cookie");
        bodies.push(reply.text);
    }
    bodies.dedup();
    assert_eq!(bodies.len(), 1, "every 404 is the same answer");
    assert_eq!(
        read(&fx, "/v1/content/nothing/x", None).await.code(),
        "content_type_not_found"
    );
    // The singleton is 404 until it is published.
    assert_eq!(
        read(&fx, "/v1/content/settings", None).await.code(),
        "entry_not_found"
    );
    let reply = fx
        .put("/v1/content/manage/singletons/settings/en")
        .header("if-none-match", "*")
        .json(&json!({"fields": {"site_name": "Site"}}))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::CREATED);
    assert_eq!(
        read(&fx, "/v1/content/settings", None).await.code(),
        "entry_not_found"
    );
    let reply = fx
        .post("/v1/content/manage/singletons/settings/en/publish")
        .by(Some("admin"))
        .if_match(1)
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::OK);
    let reply = read(&fx, "/v1/content/settings?locale=fr", None).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body["fields"]["site_name"], "Site");
    assert_eq!(reply.body["sys"]["source_locale"], "en");
    // A singleton takes no list parameters.
    assert_eq!(
        read(&fx, "/v1/content/settings?limit=2", None).await.code(),
        "invalid_query"
    );
}

#[tokio::test]
async fn preview_needs_content_preview_and_is_no_store_without_etag() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", DELIVERY_MODEL).await;
    let draft_only = draft(&fx, "article", json!({"title": "Draft"})).await;
    let changed = published(&fx, "article", json!({"title": "P1"})).await;
    save(&fx, "article", &changed, "en", 1, json!({"title": "P2"})).await;
    let brief = published(&fx, "brief", json!({"title": "Partners"})).await;

    let uri = format!("/v1/content/article/{draft_only}?stage=draft");
    for (who, status, code) in [
        (None, StatusCode::UNAUTHORIZED, "unauthenticated"),
        (Some("nobody"), StatusCode::FORBIDDEN, "unauthorized"),
        // Reading drafts in management is not preview.
        (Some("editor"), StatusCode::FORBIDDEN, "unauthorized"),
        (Some("admin"), StatusCode::FORBIDDEN, "unauthorized"),
    ] {
        let reply = read(&fx, &uri, who).await;
        assert_eq!(reply.status, status, "{who:?}");
        assert_eq!(reply.code(), code);
        assert!(!reply.text.contains("Draft"));
    }
    let reply = read(&fx, &uri, Some("previewer")).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert_eq!(reply.body["fields"]["title"], "Draft");
    assert_eq!(reply.headers["cache-control"], "private, no-store");
    assert_eq!(reply.headers["vary"], "Authorization, Cookie");
    assert!(reply.headers.get("etag").is_none());
    // A matching `If-None-Match` is no shortcut for drafts.
    let again = fx
        .get(&uri)
        .by(Some("previewer"))
        .header("if-none-match", "*")
        .send()
        .await;
    assert_eq!(again.status, StatusCode::OK);
    // Preview shows the newer draft; delivery keeps the published one.
    let preview = read(
        &fx,
        &format!("/v1/content/article/{changed}?stage=draft"),
        Some("previewer"),
    )
    .await;
    assert_eq!(preview.body["fields"]["title"], "P2");
    assert_eq!(preview.body["sys"]["revision"], 2);
    let delivered = read(&fx, &format!("/v1/content/article/{changed}"), None).await;
    assert_eq!(delivered.body["fields"]["title"], "P1");
    assert_eq!(delivered.body["sys"]["revision"], 1);
    // Drafts fall back through the chain too.
    let fallback = read(
        &fx,
        &format!("/v1/content/article/{draft_only}?stage=draft&locale=fr-BE"),
        Some("previewer"),
    )
    .await;
    assert_eq!(fallback.body["sys"]["locale"], "fr-BE");
    assert_eq!(fallback.body["sys"]["source_locale"], "en");
    let strict = read(
        &fx,
        &format!("/v1/content/article/{draft_only}?stage=draft&locale=fr&fallback=false"),
        Some("previewer"),
    )
    .await;
    assert_eq!(strict.code(), "entry_not_found");
    // The preview list holds both drafts, delivery only the published one.
    let preview = read(&fx, "/v1/content/article?stage=draft", Some("previewer")).await;
    assert_eq!(preview.body["items"].as_array().unwrap().len(), 2);
    assert!(preview.headers.get("etag").is_none());
    assert_eq!(preview.headers["cache-control"], "private, no-store");
    let delivered = read(&fx, "/v1/content/article", None).await;
    assert_eq!(delivered.body["items"].as_array().unwrap().len(), 1);
    // The preview list resolves each draft through the chain.
    let preview = read(
        &fx,
        "/v1/content/article?stage=draft&locale=fr-BE",
        Some("previewer"),
    )
    .await;
    let items = preview.body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert!(items.iter().all(|i| i["sys"]["source_locale"] == "en"));
    let preview = read(
        &fx,
        "/v1/content/article?stage=draft&locale=fr-BE&fallback=false",
        Some("previewer"),
    )
    .await;
    assert_eq!(preview.body["items"], json!([]));
    // Preview never lifts a type's own access rule.
    let reply = read(
        &fx,
        &format!("/v1/content/brief/{brief}?stage=draft"),
        Some("previewer"),
    )
    .await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert_eq!(
        read(&fx, "/v1/content/article?stage=drafts", Some("previewer"))
            .await
            .code(),
        "invalid_query"
    );
}

#[tokio::test]
async fn population_respects_access_stage_locale_chain_and_scope() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", DELIVERY_MODEL).await;
    let ada = published(&fx, "author", json!({"name": "Ada"})).await;
    fx.add_variant("author", &ada, "fr", json!({"name": "Ada FR"}))
        .await;
    assert_eq!(
        fx.publish("author", &ada, "fr", 1).await.status,
        StatusCode::OK
    );
    let draft_author = draft(&fx, "author", json!({"name": "Unpublished"})).await;
    let note = published(&fx, "note", json!({"title": "Members"})).await;
    let brief = published(&fx, "brief", json!({"title": "Partners"})).await;
    let linked = published(&fx, "article", json!({"title": "Linked"})).await;
    let draft_article = draft(&fx, "article", json!({"title": "Hidden"})).await;
    let main = published(
        &fx,
        "article",
        json!({
            "title": "Main",
            "author": author_ref(&ada),
            "related": [
                {"id": note, "type": "note"},
                {"id": brief, "type": "brief"},
                {"id": draft_article, "type": "article"},
            ],
            "body": [{"_type": "block", "_key": "b1", "style": "normal",
                      "markDefs": [{"_type": "entry_link", "_key": "l1", "id": linked, "type": "article"}],
                      "children": [{"_type": "span", "_key": "s1", "text": "see", "marks": ["l1"]}]}],
        }),
    )
    .await;
    let uri = format!("/v1/content/article/{main}?populate=author,related,body");

    // Anonymous: public targets only.
    let reply = read(&fx, &uri, None).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    let fields = &reply.body["fields"];
    assert_eq!(fields["author"]["id"], ada.as_str());
    assert_eq!(fields["author"]["entry"]["fields"]["name"], "Ada");
    assert_eq!(fields["author"]["entry"]["sys"]["type"], "author");
    assert_eq!(fields["author"]["entry"]["sys"]["revision"], 1);
    let related = fields["related"].as_array().unwrap();
    for item in related {
        assert_eq!(item["missing"], true, "{item}");
        assert!(item.get("entry").is_none());
    }
    let link = &fields["body"][0]["markDefs"][0];
    assert_eq!(link["entry"]["fields"]["title"], "Linked");
    assert_eq!(reply.headers["cache-control"], PUBLIC_CACHE);
    // Nothing of the hidden targets travels.
    for hidden in ["Members", "Partners", "Hidden"] {
        assert!(!reply.text.contains(hidden), "{hidden}");
    }

    // Any principal reads the note; the brief needs its permission.
    let reply = read(&fx, &uri, Some("nobody")).await;
    let related = reply.body["fields"]["related"].as_array().unwrap().clone();
    assert_eq!(related[0]["entry"]["fields"]["title"], "Members");
    assert_eq!(related[1]["missing"], true);
    assert_eq!(related[2]["missing"], true);
    assert_eq!(reply.headers["cache-control"], PRIVATE_CACHE);
    let reply = read(&fx, &uri, Some("delivery_reader")).await;
    let related = reply.body["fields"]["related"].as_array().unwrap().clone();
    assert_eq!(related[1]["entry"]["fields"]["title"], "Partners");
    // An unpublished target stays missing whoever asks.
    assert_eq!(related[2]["missing"], true);

    // Without `populate` references stay bare, and hidden ones are missing.
    let reply = read(&fx, &format!("/v1/content/article/{main}"), None).await;
    assert_eq!(reply.body["fields"]["author"], author_ref(&ada));
    assert_eq!(reply.body["fields"]["related"][0]["missing"], true);
    assert!(
        reply.body["fields"]["body"][0]["markDefs"][0]
            .get("missing")
            .is_none()
    );

    // The locale chain: `fr-BE` reaches the author's `fr` variant.
    let reply = read(&fx, &format!("{uri}&locale=fr-BE"), None).await;
    let author = &reply.body["fields"]["author"]["entry"];
    assert_eq!(author["sys"]["locale"], "fr-BE");
    assert_eq!(author["sys"]["source_locale"], "fr");
    assert_eq!(author["fields"]["name"], "Ada FR");
    assert_eq!(reply.body["sys"]["source_locale"], "en");

    // The stage: a draft-only author is missing when published, populated
    // with its draft in preview.
    let second = published(
        &fx,
        "article",
        json!({"title": "Second", "author": author_ref(&draft_author)}),
    )
    .await;
    let uri = format!("/v1/content/article/{second}?populate=author");
    let reply = read(&fx, &uri, None).await;
    assert_eq!(reply.body["fields"]["author"]["missing"], true);
    let reply = read(&fx, &format!("{uri}&stage=draft"), Some("previewer")).await;
    assert_eq!(
        reply.body["fields"]["author"]["entry"]["fields"]["name"],
        "Unpublished"
    );

    // Scope: a published row of another scope is never populated, even
    // when a reference names it.
    let org_author = seed_row(&fx.db, "org:acme", "author", r#"{"name": "Org"}"#).await;
    execute(
        &fx.db,
        &format!(
            "INSERT INTO merezha_content.published
               (entry_id, locale, source_locale, scope, content_type, revision, data,
                published_at, first_published_at)
             VALUES ('{org_author}', 'en', 'en', 'org:acme', 'author', 1, '{{\"name\": \"Org\"}}',
                     now(), now());
             UPDATE merezha_content.published
                SET data = jsonb_set(data, '{{author}}', '{{\"id\": \"{org_author}\", \"type\": \"author\"}}')
              WHERE entry_id = '{second}'"
        ),
    )
    .await;
    let reply = read(
        &fx,
        &format!("/v1/content/article/{second}?populate=author"),
        None,
    )
    .await;
    assert_eq!(reply.body["fields"]["author"]["missing"], true);
    assert!(!reply.text.contains("Org"));
    // Nor is it served at the application path, under any route.
    for uri in [
        format!("/v1/content/author/{org_author}"),
        format!("/v1/content/author/{org_author}?stage=draft"),
    ] {
        let reply = read(&fx, &uri, Some("previewer")).await;
        assert_eq!(reply.code(), "entry_not_found", "{uri}");
    }
    let list = read(
        &fx,
        &format!("/v1/content/author?filter[sys.id][eq]={org_author}"),
        None,
    )
    .await;
    assert_eq!(list.body["items"], json!([]));
    // A reference naming another type than its target's is not populated.
    execute(
        &fx.db,
        &format!(
            "UPDATE merezha_content.published
                SET data = jsonb_set(data, '{{author}}', '{{\"id\": \"{ada}\", \"type\": \"article\"}}')
              WHERE entry_id = '{second}'"
        ),
    )
    .await;
    let reply = read(
        &fx,
        &format!("/v1/content/article/{second}?populate=author"),
        None,
    )
    .await;
    assert_eq!(reply.body["fields"]["author"]["missing"], true);
    assert!(!reply.text.contains("Ada"));
}

#[tokio::test]
async fn population_respects_its_depth_and_entry_bounds() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(
        &test,
        "limits = { max_populated_entries = 2 }",
        DELIVERY_MODEL,
    )
    .await;
    let ada = published(&fx, "author", json!({"name": "Ada"})).await;
    for title in ["One", "Two", "Three"] {
        published(
            &fx,
            "article",
            json!({"title": title, "author": author_ref(&ada)}),
        )
        .await;
    }
    // Two articles populate two authors; three exceed the bound and fail
    // rather than truncate.
    let reply = read(&fx, "/v1/content/article?populate=author&limit=2", None).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert_eq!(
        reply.body["items"][1]["fields"]["author"]["entry"]["fields"]["name"],
        "Ada"
    );
    let reply = read(&fx, "/v1/content/article?populate=author", None).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(reply.code(), "populate_limit");
    // Depth: at most `max_populate_depth` (2) segments.
    for (query, detail) in [
        ("populate=author.friend.friend", "too_deep"),
        ("populate=title", "unknown_field"),
        ("populate=author.nothing", "unknown_field"),
        ("populate=", "invalid_value"),
        ("populate=author&fields=title", "not_selected"),
    ] {
        let reply = read(&fx, &format!("/v1/content/article?{query}"), None).await;
        assert_eq!(reply.code(), "invalid_query", "{query}");
        assert_eq!(
            reply.details(),
            [("/populate".to_owned(), detail.to_owned())],
            "{query}"
        );
    }
    assert_eq!(
        read(
            &fx,
            "/v1/content/article?populate=author.friend&limit=1",
            None
        )
        .await
        .status,
        StatusCode::OK
    );
    drop(fx);

    let fx = start(&test, "limits = { max_populate_depth = 0 }", DELIVERY_MODEL).await;
    let reply = read(&fx, "/v1/content/article?populate=author", None).await;
    assert_eq!(
        reply.details(),
        [("/populate".to_owned(), "too_deep".to_owned())]
    );
}

#[tokio::test]
async fn a_reference_cycle_at_maximum_depth_terminates() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "limits = { max_populate_depth = 3 }", DELIVERY_MODEL).await;
    let b = draft(&fx, "author", json!({"name": "B"})).await;
    let a = draft(
        &fx,
        "author",
        json!({"name": "A", "friend": author_ref(&b)}),
    )
    .await;
    save(
        &fx,
        "author",
        &b,
        "en",
        1,
        json!({"name": "B", "friend": author_ref(&a)}),
    )
    .await;
    assert_eq!(
        fx.publish("author", &a, "en", 1).await.status,
        StatusCode::OK
    );
    assert_eq!(
        fx.publish("author", &b, "en", 2).await.status,
        StatusCode::OK
    );
    let reply = tokio::time::timeout(
        Duration::from_secs(10),
        read(
            &fx,
            &format!("/v1/content/author/{a}?populate=friend.friend.friend"),
            None,
        ),
    )
    .await
    .expect("terminates");
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    let level1 = &reply.body["fields"]["friend"]["entry"];
    assert_eq!(level1["fields"]["name"], "B");
    let level2 = &level1["fields"]["friend"]["entry"];
    assert_eq!(level2["fields"]["name"], "A");
    let level3 = &level2["fields"]["friend"]["entry"];
    assert_eq!(level3["fields"]["name"], "B");
    // Past the last segment the reference stays bare: visible, not
    // populated.
    assert_eq!(level3["fields"]["friend"], author_ref(&a));
    // Every level of a list in one batched query each: a full page works.
    let reply = read(
        &fx,
        "/v1/content/author?populate=friend.friend.friend",
        None,
    )
    .await;
    assert_eq!(reply.body["items"].as_array().unwrap().len(), 2);
}

/// The count of registered content statements on `collection`.
fn statements_on(harness: &merezha_telemetry::testing::Harness, collection: &str) -> f64 {
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

#[tokio::test]
async fn a_publish_changes_the_etag_and_an_unchanged_scope_answers_304_from_generations_alone() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = merezha_telemetry::testing::Harness::new();
    let fx = start_metered(&test, "", DELIVERY_MODEL, &harness).await;
    let article = published(&fx, "article", json!({"title": "Hello"})).await;
    let uri = format!("/v1/content/article/{article}");
    let first = read(&fx, &uri, None).await;
    assert_eq!(first.status, StatusCode::OK);
    let tag = first.etag();
    assert!(tag.starts_with("W/\""), "{tag}");
    let state = fx.handle.state().unwrap();
    let fingerprint = &state.model().fingerprint()[..8];
    let generation = generation(&fx.db, "app").await;
    assert!(
        tag.starts_with(&format!("W/\"{fingerprint}-{generation}-")),
        "{tag}"
    );
    assert_eq!(read(&fx, &uri, None).await.etag(), tag, "stable");

    // The 304 path: one statement on `generations`, none on entries.
    let published_before = statements_on(&harness, "merezha_content.published");
    let generations_before = statements_on(&harness, "merezha_content.generations");
    assert!(published_before >= 2.0);
    let reply = fx
        .get(&uri)
        .by(None)
        .header("if-none-match", &tag)
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::NOT_MODIFIED);
    assert_eq!(reply.etag(), tag);
    assert_eq!(reply.headers["cache-control"], PUBLIC_CACHE);
    assert_eq!(reply.headers["vary"], "Authorization, Cookie");
    assert!(reply.text.is_empty());
    assert_eq!(
        statements_on(&harness, "merezha_content.published"),
        published_before
    );
    assert_eq!(statements_on(&harness, "merezha_content.variants"), 0.0);
    assert_eq!(
        statements_on(&harness, "merezha_content.generations"),
        generations_before + 1.0
    );

    // The same with every entry table locked by another session: the 304
    // never waits, the full read does.
    let locker = fx.db.acquire(soon()).await.unwrap();
    locker
        .batch_execute(
            "BEGIN; LOCK TABLE merezha_content.published, merezha_content.variants, \
             merezha_content.entries, merezha_content.refs, merezha_content.revisions \
             IN ACCESS EXCLUSIVE MODE",
        )
        .await
        .unwrap();
    let not_modified = tokio::time::timeout(
        Duration::from_secs(5),
        fx.get(&uri).by(None).header("if-none-match", &tag).send(),
    )
    .await
    .expect("a 304 reads no entry table");
    assert_eq!(not_modified.status, StatusCode::NOT_MODIFIED);
    let full = tokio::time::timeout(Duration::from_millis(500), read(&fx, &uri, None)).await;
    assert!(full.is_err(), "the full read waits for the lock");
    locker.batch_execute("ROLLBACK").await.unwrap();

    // Another scope's generation is not this scope's: the tag holds.
    execute(
        &fx.db,
        "INSERT INTO merezha_content.generations (scope, generation) VALUES ('org:acme', 7)",
    )
    .await;
    let reply = fx
        .get(&uri)
        .by(None)
        .header("if-none-match", &tag)
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::NOT_MODIFIED);

    // A publish anywhere in the scope changes every tag of the scope.
    let list = read(&fx, "/v1/content/article", None).await;
    let list_tag = list.etag();
    assert_ne!(list_tag, tag, "each query has its own tag");
    published(&fx, "article", json!({"title": "Another"})).await;
    let reply = fx
        .get(&uri)
        .by(None)
        .header("if-none-match", &tag)
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_ne!(reply.etag(), tag);
    let reply = fx
        .get("/v1/content/article")
        .by(None)
        .header("if-none-match", &list_tag)
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body["items"].as_array().unwrap().len(), 2);
    // The caller's access class is part of the tag.
    let anonymous = read(&fx, &uri, None).await.etag();
    let principal = read(&fx, &uri, Some("nobody")).await.etag();
    assert_ne!(anonymous, principal);
}

#[tokio::test]
async fn cache_headers_never_let_a_shared_cache_keep_a_private_response() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", DELIVERY_MODEL).await;
    let article = published(&fx, "article", json!({"title": "Hello"})).await;
    let note = published(&fx, "note", json!({"title": "Members"})).await;
    for (uri, who, cache) in [
        (format!("/v1/content/article/{article}"), None, PUBLIC_CACHE),
        ("/v1/content/article".to_owned(), None, PUBLIC_CACHE),
        ("/v1/content/model".to_owned(), None, PUBLIC_CACHE),
        // A principal's response is private, even on a public type.
        (
            format!("/v1/content/article/{article}"),
            Some("nobody"),
            PRIVATE_CACHE,
        ),
        (
            "/v1/content/article".to_owned(),
            Some("editor"),
            PRIVATE_CACHE,
        ),
        (
            "/v1/content/model".to_owned(),
            Some("nobody"),
            PRIVATE_CACHE,
        ),
        (
            format!("/v1/content/note/{note}"),
            Some("nobody"),
            PRIVATE_CACHE,
        ),
        // Errors are never stored.
        (
            "/v1/content/article/x".to_owned(),
            None,
            "private, no-store",
        ),
        ("/v1/content/note".to_owned(), None, "private, no-store"),
    ] {
        let reply = read(&fx, &uri, who).await;
        assert_eq!(reply.headers["cache-control"], cache, "{uri} {who:?}");
        assert_eq!(reply.headers["vary"], "Authorization, Cookie", "{uri}");
    }
    drop(fx);
    let fx = start(
        &test,
        "delivery = { cache_control_public = \"public, max-age=5\", \
                      cache_control_private = \"private, max-age=0\" }",
        DELIVERY_MODEL,
    )
    .await;
    let reply = read(&fx, "/v1/content/article", None).await;
    assert_eq!(reply.headers["cache-control"], "public, max-age=5");
    let reply = read(&fx, "/v1/content/article", Some("nobody")).await;
    assert_eq!(reply.headers["cache-control"], "private, max-age=0");
}

#[tokio::test]
async fn by_slug_prefers_an_own_locale_slug_then_the_chain() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", DELIVERY_MODEL).await;
    // `a` has the slug in `en`, which `fr` and `fr-BE` fall back to; `b`
    // has it as its own `fr` slug.
    let a = published(&fx, "article", json!({"title": "Hello"})).await;
    let b = published(&fx, "article", json!({"title": "Other"})).await;
    fx.add_variant("article", &b, "fr", json!({"title": "Hello"}))
        .await;
    assert_eq!(
        fx.publish("article", &b, "fr", 1).await.status,
        StatusCode::OK
    );
    for (query, expected) in [
        ("locale=en", Some(&a)),
        ("", Some(&a)),
        ("locale=fr", Some(&b)),
        // `b` falls back from `fr`, `a` from `en`: chain order.
        ("locale=fr-BE", Some(&b)),
        ("locale=fr&fallback=false", Some(&b)),
        ("locale=fr-BE&fallback=false", None),
    ] {
        let reply = read(
            &fx,
            &format!("/v1/content/article/by-slug/hello?{query}"),
            None,
        )
        .await;
        match expected {
            Some(id) => {
                assert_eq!(reply.status, StatusCode::OK, "{query}: {:?}", reply.body);
                assert_eq!(reply.body["sys"]["id"], id.as_str(), "{query}");
                assert!(reply.headers.get("etag").is_some());
            }
            None => assert_eq!(reply.code(), "entry_not_found", "{query}"),
        }
    }
    let reply = read(&fx, "/v1/content/article/by-slug/other?locale=fr", None).await;
    // `b`'s `fr` variant has its own slug: `other` is not served in `fr`.
    assert_eq!(reply.code(), "entry_not_found");
    // Preview resolves each entry's draft through the chain, then the slug.
    let c = draft(&fx, "article", json!({"title": "Fresh"})).await;
    let reply = read(
        &fx,
        "/v1/content/article/by-slug/fresh?stage=draft&locale=fr-BE",
        Some("previewer"),
    )
    .await;
    assert_eq!(reply.body["sys"]["id"], c.as_str());
    let reply = read(
        &fx,
        "/v1/content/article/by-slug/hello?stage=draft&locale=fr",
        Some("previewer"),
    )
    .await;
    assert_eq!(reply.body["sys"]["id"], b.as_str());
    assert_eq!(
        read(&fx, "/v1/content/article/by-slug/fresh", None)
            .await
            .code(),
        "entry_not_found"
    );
}

#[tokio::test]
async fn delivery_lists_filter_sort_select_and_page_with_cursors() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", DELIVERY_MODEL).await;
    let mut ids = Vec::new();
    for (title, category, minutes) in [
        ("A", "news", 5),
        ("B", "guide", 3),
        ("C", "news", 9),
        ("D", "news", 1),
        ("E", "guide", 7),
    ] {
        ids.push(
            published(
                &fx,
                "article",
                json!({"title": title, "category": category, "reading_minutes": minutes}),
            )
            .await,
        );
    }
    draft(&fx, "article", json!({"title": "Z", "category": "news"})).await;
    let titles = |reply: &Reply| -> Vec<String> {
        reply.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["fields"]["title"].as_str().unwrap_or("").to_owned())
            .collect()
    };
    // The default order is the newest publish first.
    let reply = read(&fx, "/v1/content/article", None).await;
    assert_eq!(titles(&reply), ["E", "D", "C", "B", "A"]);
    let reply = read(&fx, "/v1/content/article?sort=sys.published_at", None).await;
    assert_eq!(titles(&reply), ["A", "B", "C", "D", "E"]);
    let reply = read(
        &fx,
        "/v1/content/article?filter[category][eq]=news&sort=-reading_minutes",
        None,
    )
    .await;
    assert_eq!(titles(&reply), ["C", "A", "D"]);
    // A cursor walk, two at a time.
    let mut walked = Vec::new();
    let mut uri = "/v1/content/article?sort=reading_minutes&limit=2".to_owned();
    loop {
        let reply = read(&fx, &uri, None).await;
        assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
        assert_eq!(reply.body["page"]["limit"], 2);
        walked.extend(titles(&reply));
        match reply.body["page"]["next_cursor"].as_str() {
            Some(cursor) => {
                uri = format!("/v1/content/article?sort=reading_minutes&limit=2&cursor={cursor}");
                // The cursor belongs to its query.
                let other = read(
                    &fx,
                    &format!("/v1/content/article?sort=-reading_minutes&limit=2&cursor={cursor}"),
                    None,
                )
                .await;
                assert_eq!(other.code(), "invalid_cursor");
                let preview = read(
                    &fx,
                    &format!("/v1/content/article?sort=reading_minutes&limit=2&stage=draft&cursor={cursor}"),
                    Some("previewer"),
                )
                .await;
                assert_eq!(preview.code(), "invalid_cursor");
            }
            None => break,
        }
    }
    assert_eq!(walked, ["D", "B", "A", "E", "C"]);
    // `fields` keeps `sys` and the named fields only.
    let reply = read(&fx, "/v1/content/article?fields=title&limit=1", None).await;
    let item = &reply.body["items"][0];
    assert_eq!(item["fields"].as_object().unwrap().len(), 1);
    assert!(item["sys"]["id"].is_string());
    // System filters of delivery.
    let reply = read(
        &fx,
        &format!(
            "/v1/content/article?filter[sys.id][in]={}&filter[sys.id][in]={}",
            ids[0], ids[1]
        ),
        None,
    )
    .await;
    assert_eq!(titles(&reply).len(), 2);
    let reply = read(
        &fx,
        "/v1/content/article?filter[sys.published_at][gte]=2000-01-01T00:00:00Z",
        None,
    )
    .await;
    assert_eq!(titles(&reply).len(), 5);
    for (query, code) in [
        (
            "filter[sys.updated_at][gte]=2000-01-01T00:00:00Z",
            "invalid_filter",
        ),
        ("sort=sys.updated_at", "invalid_sort"),
        ("filter[slug][eq]=a", "invalid_filter"),
        ("status=draft", "invalid_query"),
        ("fallback=maybe", "invalid_query"),
        ("fields=nope", "invalid_query"),
        ("limit=0", "invalid_query"),
        ("locale=de", "locale_unknown"),
    ] {
        let reply = read(&fx, &format!("/v1/content/article?{query}"), None).await;
        assert_eq!(reply.code(), code, "{query}");
    }
    // Locales: fallback rows by default, own variants only without it.
    fx.add_variant("article", &ids[0], "fr", json!({"title": "A fr"}))
        .await;
    assert_eq!(
        fx.publish("article", &ids[0], "fr", 1).await.status,
        StatusCode::OK
    );
    let reply = read(&fx, "/v1/content/article?locale=fr&sort=title", None).await;
    assert_eq!(titles(&reply), ["A fr", "B", "C", "D", "E"]);
    let reply = read(&fx, "/v1/content/article?locale=fr&fallback=false", None).await;
    assert_eq!(titles(&reply), ["A fr"]);
    assert_eq!(reply.body["items"][0]["sys"]["source_locale"], "fr");
    // A non-localized type serves its default variant in every locale, with
    // or without fallback.
    published(&fx, "page", json!({"title": "About"})).await;
    for query in ["locale=fr-BE", "locale=fr&fallback=false"] {
        let reply = read(&fx, &format!("/v1/content/page?{query}"), None).await;
        assert_eq!(titles(&reply), ["About"], "{query}");
    }
}

#[tokio::test]
async fn the_default_delivery_list_scans_published_list() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", DELIVERY_MODEL).await;
    execute(
        &fx.db,
        "WITH e AS (
           INSERT INTO merezha_content.entries (id, scope, content_type, created_at, created_by)
           SELECT gen_random_uuid(), 'app', 'article', now(), 'sys_test'
             FROM generate_series(1, 3000) RETURNING id
         ), v AS (
           INSERT INTO merezha_content.variants
               (entry_id, locale, scope, content_type, draft_revision, draft_data,
                draft_updated_at, draft_updated_by, published_revision, published_at,
                first_published_at)
           SELECT id, 'en', 'app', 'article', 1,
                  jsonb_build_object('title', md5(id::text),
                                     'reading_minutes', (random() * 599)::int + 1),
                  now(), 'sys_test', 1, now() - random() * interval '30 days', now()
             FROM e RETURNING entry_id, draft_data, published_at
         )
         INSERT INTO merezha_content.published
             (entry_id, locale, source_locale, scope, content_type, revision, data,
              published_at, first_published_at)
         SELECT entry_id, l.code, 'en', 'app', 'article', 1, draft_data, published_at, published_at
           FROM v CROSS JOIN (VALUES ('en'), ('fr'), ('fr-BE')) AS l(code);
         ANALYZE merezha_content.published;",
    )
    .await;
    let state = fx.handle.state().unwrap();
    let derived = |purpose: merezha_content::IndexPurpose| {
        merezha_content::derived_indexes(state.model())
            .into_iter()
            .find(|i| {
                i.field == "reading_minutes"
                    && i.table == merezha_content::IndexTable::Published
                    && i.purpose == purpose
            })
            .unwrap()
            .name
    };
    for (query, expected) in [
        ("", "published_list".to_owned()),
        ("sort=sys.published_at", "published_list".to_owned()),
        ("locale=fr", "published_list".to_owned()),
        (
            "sort=reading_minutes",
            derived(merezha_content::IndexPurpose::Sort),
        ),
        (
            "sort=-reading_minutes",
            derived(merezha_content::IndexPurpose::SortDescending),
        ),
    ] {
        let plan = state.explain_delivery("article", query).await.unwrap();
        assert!(
            plan.contains(&format!("Index Scan using {expected}"))
                || plan.contains(&format!("Index Scan Backward using {expected}")),
            "{query}: {plan}"
        );
        assert!(!plan.contains("Sort Key"), "{query} sorts: {plan}");
    }
}

#[tokio::test]
async fn the_model_endpoints_restrict_delivery_and_guard_management() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", DELIVERY_MODEL).await;
    let types = |reply: &Reply| -> Vec<String> {
        reply.body["types"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    };
    let reply = read(&fx, "/v1/content/model", None).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(types(&reply), ["article", "author", "page", "settings"]);
    assert_eq!(reply.body["version"], 1);
    assert_eq!(reply.body["locales"][2]["code"], "fr-BE");
    let fingerprint = fx.handle.state().unwrap().model().fingerprint().to_owned();
    assert_eq!(reply.body["fingerprint"], fingerprint.as_str());
    // Fields in model order, and no hidden type named anywhere.
    let names: Vec<&str> = reply.body["types"]["article"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "title",
            "slug",
            "category",
            "reading_minutes",
            "author",
            "related",
            "body"
        ]
    );
    assert_eq!(
        reply.body["types"]["article"]["fields"][5]["to"],
        json!(["article"])
    );
    assert!(!reply.text.contains("brief") && !reply.text.contains("note"));
    let reply = read(&fx, "/v1/content/model", Some("nobody")).await;
    assert_eq!(
        types(&reply),
        ["article", "author", "note", "page", "settings"]
    );
    let reply = read(&fx, "/v1/content/model", Some("delivery_reader")).await;
    assert_eq!(
        types(&reply),
        ["article", "author", "brief", "note", "page", "settings"]
    );
    assert_eq!(
        reply.body["types"]["article"]["fields"][5]["to"],
        json!(["article", "note", "brief"])
    );
    assert_eq!(
        read(&fx, "/v1/content/model?x=1", None).await.code(),
        "invalid_query"
    );

    // Management: the full model with `content.model.read`.
    for (who, status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some("editor"), StatusCode::FORBIDDEN),
        (Some("model_reader"), StatusCode::OK),
    ] {
        let reply = read(&fx, "/v1/content/manage/model", who).await;
        assert_eq!(reply.status, status, "{who:?}");
        assert_eq!(reply.headers["cache-control"], "private, no-store");
        if status == StatusCode::OK {
            assert_eq!(types(&reply).len(), 6);
            assert_eq!(reply.body["fingerprint"], fingerprint.as_str());
            assert_eq!(
                reply.body["types"]["brief"]["access"]["permission"],
                "content.delivery.read"
            );
        }
    }
}

#[tokio::test]
async fn management_draft_reads_populate_drafts_of_the_types_the_caller_reads() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", DELIVERY_MODEL).await;
    let ada = draft(&fx, "author", json!({"name": "Ada"})).await;
    let article = draft(
        &fx,
        "article",
        json!({"title": "Main", "author": author_ref(&ada)}),
    )
    .await;
    let uri = format!("{}?populate=author", variant("article", &article, "en"));
    let reply = read(&fx, &uri, Some("editor")).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert_eq!(
        reply.body["fields"]["author"]["entry"]["fields"]["name"],
        "Ada"
    );
    assert_eq!(reply.body["sys"]["status"], "draft");
    assert_eq!(reply.etag(), "\"r1\"");
    assert_eq!(reply.headers["cache-control"], "private, no-store");
    // A caller who reads articles only sees the author as missing.
    let reply = read(&fx, &uri, Some("article_writer")).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body["fields"]["author"]["missing"], true);
    assert!(!reply.text.contains("Ada"));
    // Without `populate` the read is C3's.
    let plain = read(&fx, &variant("article", &article, "en"), Some("editor")).await;
    assert_eq!(plain.body["fields"]["author"], author_ref(&ada));
    for query in ["populate=title", "populate=author.friend.friend"] {
        let reply = read(
            &fx,
            &format!("{}?{query}", variant("article", &article, "en")),
            Some("editor"),
        )
        .await;
        assert_eq!(reply.code(), "invalid_query", "{query}");
    }
}
