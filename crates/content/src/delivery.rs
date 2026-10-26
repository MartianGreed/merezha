//! The delivery API (C5, section 5 delivery routes, caching, section 8):
//! published entries under `/v1/content/{type}`, and under
//! `/v1/content/o/{organization}/{type}` for organization types, read by
//! each type's declared `access`; preview of drafts with `stage=draft` and
//! `content.preview`; `fields`, `populate`, by-slug lookups; asset
//! metadata in place of every asset reference (issue #46,
//! [`crate::populate`]); weak ETags from the scope generations with 304 answered from `generations` alone;
//! cache headers that keep private responses out of shared caches; and
//! the model endpoints, `GET /v1/content/model` (types restricted to those
//! the caller reads) and `GET /v1/content/manage/model`
//! (`content.model.read`).
//!
//! Every statement here is registered ([`crate::statements`]).
use crate::{
    http::{RESOURCE_KIND, entry_id, error_response, page_json, principal, selection},
    json::{optional, string},
    model::{Access, ContentModel, ContentType, Kind, Scope},
    populate::{
        AssetExpansion, Doc, Expanded, Paths, Population, RESPONSE_TOO_LARGE, Read, holds_assets,
        parse_paths, reachable_types, referenced_types,
    },
    publish::{chain, locale_chain},
    query::{self, Built, Params, Source, Split, detail, encode_cursor, hex16, type_literal},
    statements,
    store::{Rejection, Store, sql, transaction},
};
use axum::{
    Router,
    extract::{Path, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
    routing::get,
};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    FieldError,
    ports::{Authorizer, Principal, ResourceScope},
};
use merezha_postgres::tokio_postgres::types::ToSql;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Write as _,
    sync::Arc,
};

/// Permission to read drafts through delivery with `stage=draft`.
pub const PREVIEW: &str = "content.preview";
/// Permission to read the full model through the management API.
pub const MODEL_READ: &str = "content.model.read";
/// `Vary` of every delivery response: a shared cache keys on the
/// credentials, so one caller's response never reaches another.
pub const VARY: &str = "Authorization, Cookie";
/// `Cache-Control` of preview responses.
pub const PREVIEW_CACHE_CONTROL: &str = "private, no-store";

#[derive(Clone)]
pub(crate) struct Delivery {
    pub(crate) store: Store,
    pub(crate) policy: Arc<dyn Authorizer>,
}

/// The delivery router: application types at the root, organization
/// types under `/o/{organization}`.
pub(crate) fn router(store: Store, policy: Arc<dyn Authorizer>) -> Router {
    Router::new()
        .route("/model", get(model))
        .route("/{type}", get(collection))
        .route("/{type}/{id}", get(entry))
        .route("/{type}/by-slug/{slug}", get(by_slug))
        .route("/o/{organization}/model", get(model))
        .route("/o/{organization}/{type}", get(collection))
        .route("/o/{organization}/{type}/{id}", get(entry))
        .route("/o/{organization}/{type}/by-slug/{slug}", get(by_slug))
        .with_state(Delivery { store, policy })
}

type PathParams = Path<HashMap<String, String>>;

/// What a delivery route addresses inside its type.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Address {
    /// A collection's list, or a singleton's entry.
    Type,
    Entry(String),
    Slug(String),
}
impl Address {
    fn canonical(&self) -> String {
        match self {
            Self::Type => "type".to_owned(),
            Self::Entry(id) => format!("entry\u{1f}{id}"),
            Self::Slug(slug) => format!("slug\u{1f}{slug}"),
        }
    }
}

async fn collection(
    State(d): State<Delivery>,
    Path(params): PathParams,
    request: Request,
) -> Response {
    d.respond(params, Address::Type, request).await
}
async fn entry(State(d): State<Delivery>, Path(params): PathParams, request: Request) -> Response {
    let id = params.get("id").cloned().unwrap_or_default();
    d.respond(params, Address::Entry(id), request).await
}
async fn by_slug(
    State(d): State<Delivery>,
    Path(params): PathParams,
    request: Request,
) -> Response {
    let slug = params.get("slug").cloned().unwrap_or_default();
    d.respond(params, Address::Slug(slug), request).await
}

/// An error, never cached and varying like every delivery response.
fn delivery_error(rejection: Rejection) -> Response {
    let mut response = error_response(rejection);
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static(VARY));
    response
}

/// A delivery response with its cache headers.
fn delivery_response(
    status: StatusCode,
    body: Option<String>,
    cache_control: &str,
    etag: Option<&str>,
) -> Response {
    let mut response = match body {
        Some(body) => (status, body).into_response(),
        None => status.into_response(),
    };
    let headers = response.headers_mut();
    if status != StatusCode::NOT_MODIFIED {
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
    }
    if let Ok(value) = HeaderValue::from_str(cache_control) {
        headers.insert(header::CACHE_CONTROL, value);
    }
    headers.insert(header::VARY, HeaderValue::from_static(VARY));
    if let Some(value) = etag.and_then(|e| HeaderValue::from_str(e).ok()) {
        headers.insert(header::ETAG, value);
    }
    response
}

/// Whether `principal` may read published entries of `content_type`
/// (section 2 `access`, owner answer CT1): `public` anyone;
/// `authenticated` any principal, for an organization type one whose
/// tenant is the path's organization `org`; a permission checked in the
/// type's resource scope. Fails `unauthenticated` (no principal where one
/// is needed) or `unauthorized`.
pub(crate) fn access(
    policy: &dyn Authorizer,
    principal: Option<&Principal>,
    content_type: &ContentType,
    org: Option<&Identity>,
) -> Result<(), Failure> {
    let organization = content_type.scope == Scope::Organization;
    if organization && org.is_none() {
        return Err(Failure::permanent("unauthorized"));
    }
    match &content_type.access {
        Access::Public => Ok(()),
        Access::Authenticated => {
            let principal = principal.ok_or_else(|| Failure::permanent("unauthenticated"))?;
            if organization && principal.tenant.as_ref() != org {
                return Err(Failure::permanent("unauthorized"));
            }
            Ok(())
        }
        Access::Permission(name) => {
            let permission = Identity::new(name.clone())?;
            let scope = ResourceScope::Resource {
                tenant: if organization { org.cloned() } else { None },
                kind: Identity::new(RESOURCE_KIND)?,
                id: Identity::new(content_type.name.clone())?,
            };
            policy
                .check(principal, &permission, &scope)
                .map(|_| ())
                .map_err(|denied| denied.failure())
        }
    }
}

/// `content.preview` at `Application` scope, or `Tenant(org)` for an
/// organization type.
pub(crate) fn preview(
    policy: &dyn Authorizer,
    principal: Option<&Principal>,
    content_type: &ContentType,
    org: Option<&Identity>,
) -> Result<(), Failure> {
    let scope = match (content_type.scope, org) {
        (Scope::Application, _) => ResourceScope::Application,
        (Scope::Organization, Some(org)) => ResourceScope::Tenant(org.clone()),
        (Scope::Organization, None) => return Err(Failure::permanent("unauthorized")),
    };
    policy
        .check(principal, &Identity::new(PREVIEW)?, &scope)
        .map(|_| ())
        .map_err(|denied| denied.failure())
}

/// The scope of `content_type`'s entries for a request on `org`'s path:
/// `app`, or `org:<organization>` for organization types (`None`
/// without an organization).
fn type_scope(content_type: &ContentType, org: Option<&Identity>) -> Option<String> {
    match content_type.scope {
        Scope::Application => Some("app".to_owned()),
        Scope::Organization => org.map(|o| format!("org:{}", o.as_str())),
    }
}

/// The types a caller reads on this path, each with its scope: the
/// population targets and the access class of the ETag. Organization
/// types count only on an organization's path, application types
/// everywhere (organization entries may reference them).
fn readable(
    policy: &dyn Authorizer,
    principal: Option<&Principal>,
    model: &ContentModel,
    org: Option<&Identity>,
    read: Read,
) -> BTreeMap<String, String> {
    model
        .types()
        .values()
        .filter_map(|t| {
            let scope = type_scope(t, org)?;
            let allowed = access(policy, principal, t, org).is_ok()
                && (read == Read::Published || preview(policy, principal, t, org).is_ok());
            allowed.then(|| (t.name.clone(), scope))
        })
        .collect()
}

/// The access class an ETag hashes: whether a principal asked, and the
/// types it reads on this path (an anonymous caller included: a policy
/// may grant anonymous reads of a permission type).
fn access_class(principal: bool, readable: &BTreeMap<String, String>) -> String {
    format!(
        "{}\u{1f}{}",
        if principal { "principal" } else { "anonymous" },
        readable.keys().cloned().collect::<Vec<_>>().join(",")
    )
}

/// The scopes whose generations a published read of `content_type` in
/// `scope` depends on: its own, and on an organization's path the
/// application scope too whenever the type can reference entries, since
/// populated entries and the `missing` checks read application rows, or
/// assets, since the expanded metadata of an application asset changes
/// with the application's generation (issue #46).
pub(crate) fn tag_scopes(
    model: &ContentModel,
    content_type: &ContentType,
    scope: &str,
) -> Vec<String> {
    let mut scopes = vec![scope.to_owned()];
    let reads_app = !referenced_types(model, content_type).is_empty()
        || content_type
            .fields
            .iter()
            .any(|f| holds_assets(model, &f.kind, 0));
    if scope != "app" && reads_app {
        scopes.push("app".to_owned());
    }
    scopes
}

/// Whether a published read may carry `delivery.cache_control_public`:
/// no principal, and every type the response could show (the root and
/// every type reachable through its references that the caller reads) is
/// `public`. Decided before reading, so a 304 and a 200 agree.
pub(crate) fn public_cache(
    model: &ContentModel,
    content_type: &ContentType,
    principal: bool,
    readable: &BTreeMap<String, String>,
) -> bool {
    !principal
        && content_type.access == Access::Public
        && reachable_types(model, content_type)
            .iter()
            .filter(|t| readable.contains_key(*t))
            .all(|t| {
                model
                    .content_type(t)
                    .is_some_and(|t| t.access == Access::Public)
            })
}

/// The validated options of a delivery read.
#[derive(Debug)]
struct Plan {
    read: Read,
    locale: String,
    fallback: bool,
    fields: Option<Vec<String>>,
    paths: Paths,
}

/// `stage` from the query string, before access is checked.
fn stage(pairs: &[(String, String)]) -> Result<Read, Rejection> {
    let values: Vec<&str> = pairs
        .iter()
        .filter(|(k, _)| k == "stage")
        .map(|(_, v)| v.as_str())
        .collect();
    match values.as_slice() {
        [] | ["published"] => Ok(Read::Published),
        ["draft"] => Ok(Read::Draft),
        [_] => Err(invalid_query(vec![detail("/stage", "invalid_value")])),
        _ => Err(invalid_query(vec![detail("/stage", "duplicate_parameter")])),
    }
}

fn invalid_query(details: Vec<FieldError>) -> Rejection {
    Rejection {
        failure: Failure::permanent("invalid_query"),
        details,
    }
}

impl Delivery {
    async fn respond(
        &self,
        params: HashMap<String, String>,
        address: Address,
        request: Request,
    ) -> Response {
        let (parts, _) = request.into_parts();
        match self.serve(&parts, &params, address).await {
            Ok(response) => response,
            Err(rejection) => delivery_error(rejection),
        }
    }

    async fn serve(
        &self,
        parts: &Parts,
        params: &HashMap<String, String>,
        address: Address,
    ) -> Result<Response, Rejection> {
        let model = self.store.model().clone();
        let principal = principal(parts);
        let principal = principal.as_deref();
        let org = organization(params)?;
        let type_name = params.get("type").map(String::as_str).unwrap_or("");
        let content_type = model
            .content_type(type_name)
            .filter(|t| (t.scope == Scope::Organization) == org.is_some())
            .ok_or_else(|| Rejection::new(Failure::permanent("content_type_not_found")))?
            .clone();
        let scope = type_scope(&content_type, org.as_ref())
            .ok_or_else(|| Rejection::new(Failure::permanent("content_type_not_found")))?;
        let query_text = parts.uri.query();
        let pairs = query::pairs(query_text)?;
        let read = stage(&pairs)?;
        let policy = &*self.policy;
        access(policy, principal, &content_type, org.as_ref())?;
        if read == Read::Draft {
            preview(policy, principal, &content_type, org.as_ref())?;
        }
        let limits = self.store.state().settings().limits;
        let base_url = self
            .store
            .state()
            .settings()
            .delivery
            .public_base_url
            .clone();
        let listing = address == Address::Type && content_type.kind == Kind::Collection;
        const EXTRA: [&str; 4] = ["locale", "fallback", "populate", "stage"];
        let (mut split, single) = if listing {
            let mut split = query::split(&content_type, query_text, &limits, &EXTRA)?;
            split.check()?;
            let extra = std::mem::take(&mut split.extra);
            (Some(split), extra)
        } else {
            let mut allowed = EXTRA.to_vec();
            allowed.push("fields");
            (None, selection(&content_type, query_text, &allowed)?)
        };
        let fields = match &mut split {
            Some(split) => split.fields.take(),
            None => single
                .get("fields")
                .map(|f| f.split(',').map(str::to_owned).collect()),
        };
        let plan = plan(&model, &content_type, &single, fields, read, &limits)?;
        let readable = readable(policy, principal, &model, org.as_ref(), read);
        let population = Population {
            model: model.clone(),
            read,
            locale: plan.locale.clone(),
            chain: locale_chain(&model, &plan.locale)
                .into_iter()
                .map(str::to_owned)
                .collect(),
            fallback: plan.fallback,
            readable: readable.clone(),
            max_entries: limits.max_populated_entries as usize,
            max_bytes: limits.max_response_bytes,
            mark_all: true,
            assets: Some(AssetExpansion {
                base: base_url.clone(),
            }),
        };
        // The ETag of a published read, without its generations.
        let mut sorted = pairs.clone();
        sorted.sort();
        let class = access_class(principal.is_some(), &readable);
        let mut canonical = format!(
            "{}\n{}\n{scope}\n{}\n{}\n{class}",
            content_type.name,
            content_type.kind.as_str(),
            address.canonical(),
            sorted
                .iter()
                .map(|(k, v)| format!("{k}\u{1f}{v}"))
                .collect::<Vec<_>>()
                .join("\u{1e}"),
        );
        // Asset URLs are absolute under `delivery.public_base_url`, so a
        // new base is a new body (issue #46); unset, tags stay as before.
        if let Some(base) = &base_url {
            canonical.push('\n');
            canonical.push_str(base);
        }
        // An organization response that populates application entries
        // depends on both generations.
        let generations = tag_scopes(&model, &content_type, &scope);
        let tag_base = TagBase {
            fingerprint: model.fingerprint()[..8].to_owned(),
            hash: hex16(&canonical),
            scopes: generations,
            if_none_match: if_none_match(&parts.headers),
        };
        let roots = match (&address, split) {
            (Address::Type, Some(split)) => {
                let statement =
                    list_statement(&model, &content_type, &scope, split, &plan, &limits)?;
                Roots::List(statement)
            }
            (Address::Type, None) => Roots::Singleton,
            (Address::Entry(id), _) => Roots::Entry(entry_id(id)?.hyphenated().to_string()),
            (Address::Slug(slug), _) => Roots::Slug(slug.clone()),
        };
        let cache_control = match read {
            Read::Draft => PREVIEW_CACHE_CONTROL.to_owned(),
            Read::Published => {
                let delivery = &self.store.state().settings().delivery;
                if public_cache(&model, &content_type, principal.is_some(), &readable) {
                    delivery.cache_control_public.clone()
                } else {
                    delivery.cache_control_private.clone()
                }
            }
        };
        let outcome = self
            .run(
                &content_type,
                &scope,
                &plan,
                roots,
                population,
                (read == Read::Published).then_some(tag_base),
            )
            .await?;
        Ok(match outcome {
            Outcome::NotModified(tag) => {
                delivery_response(StatusCode::NOT_MODIFIED, None, &cache_control, Some(&tag))
            }
            Outcome::Found {
                expanded,
                next,
                limit,
                etag,
            } => {
                let max = self.store.state().settings().limits.max_response_bytes;
                let mut items = Vec::with_capacity(expanded.roots.len());
                let mut bytes = 0usize;
                for index in 0..expanded.roots.len() {
                    let item = entry_json(&expanded, index, &content_type, &plan);
                    bytes = bytes.saturating_add(item.len() + 1);
                    if bytes > max {
                        return Err(Failure::permanent(RESPONSE_TOO_LARGE).into());
                    }
                    items.push(item);
                }
                let body = match limit {
                    Some(limit) => page_json(items, next, limit),
                    None => items.into_iter().next().ok_or_else(Rejection::not_found)?,
                };
                delivery_response(StatusCode::OK, Some(body), &cache_control, etag.as_deref())
            }
        })
    }

    /// Read the generations (published), answer 304 on a match, else the
    /// roots and their population, in one transaction: the generations are
    /// read first, so the content is at least as new as the ETag says.
    async fn run(
        &self,
        content_type: &ContentType,
        scope: &str,
        plan: &Plan,
        roots: Roots,
        population: Population,
        tag: Option<TagBase>,
    ) -> Result<Outcome, Rejection> {
        let db = self.store.database().clone();
        let content_type = content_type.clone();
        let scope = scope.to_owned();
        let paths = plan.paths.clone();
        let locale = plan.locale.clone();
        let fallback = plan.fallback;
        let read = plan.read;
        let model = self.store.model().clone();
        let deadline = self.store.deadline();
        transaction(self.store.database(), deadline, move |tx| {
            Box::pin(async move {
                // Issue #56: a deleted organization's path serves nothing,
                // its 304s included.
                crate::organizations::refuse_in(&db, tx, &scope).await?;
                let mut etag = None;
                if let Some(tag) = tag {
                    let rows = db
                        .query(tx, &statements::GENERATIONS, &[&tag.scopes])
                        .await
                        .map_err(|e| sql(&e))?;
                    let found: HashMap<String, i64> =
                        rows.iter().map(|r| (r.get(0), r.get(1))).collect();
                    let generations: Vec<String> = tag
                        .scopes
                        .iter()
                        .map(|s| found.get(s).copied().unwrap_or(0).to_string())
                        .collect();
                    let value = format!(
                        "W/\"{}-{}-{}\"",
                        tag.fingerprint,
                        generations.join("."),
                        tag.hash
                    );
                    if tag.if_none_match.iter().any(|t| weak_eq(t, &value)) {
                        return Ok(Outcome::NotModified(value));
                    }
                    etag = Some(value);
                }
                let own_only = !fallback && content_type.localized;
                let chain: Vec<String> = if own_only {
                    vec![locale.clone()]
                } else {
                    chain(&model, &content_type, &locale)
                        .into_iter()
                        .map(str::to_owned)
                        .collect()
                };
                let serve_fallback = !own_only;
                let type_name = content_type.name.clone();
                let mut next = None;
                let mut limit = None;
                let docs = match (roots, read) {
                    (Roots::List(statement), _) => {
                        let refs: Vec<&(dyn ToSql + Sync)> = statement
                            .params
                            .0
                            .iter()
                            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
                            .collect();
                        let registered = match read {
                            Read::Published => &statements::PUBLISHED_LIST,
                            Read::Draft => &statements::PREVIEW_LIST,
                        };
                        let rows = db
                            .query_text(tx, registered, &statement.sql, &refs)
                            .await
                            .map_err(|e| {
                                if e.code().is_some_and(|c| c.code().starts_with("22")) {
                                    Rejection {
                                        failure: Failure::permanent("invalid_cursor"),
                                        details: vec![detail("/cursor", "invalid_value")],
                                    }
                                } else {
                                    sql(&e)
                                }
                            })?;
                        let page = statement.limit as usize;
                        let mut docs = Vec::with_capacity(rows.len().min(page));
                        let mut last = Vec::new();
                        for row in rows.iter().take(page) {
                            docs.push(Doc::from_row(row)?);
                            last = (0..statement.built.keys)
                                .map(|k| row.get::<_, Option<String>>(8 + k))
                                .collect();
                        }
                        if rows.len() > page
                            && let Some(doc) = docs.last()
                        {
                            next =
                                Some(encode_cursor(&statement.built.fingerprint, &last, &doc.id));
                        }
                        limit = Some(statement.limit);
                        docs
                    }
                    (Roots::Singleton, Read::Published) => db
                        .query_opt(
                            tx,
                            &statements::PUBLISHED_SINGLETON,
                            &[&scope, &type_name, &locale, &serve_fallback],
                        )
                        .await
                        .map_err(|e| sql(&e))?
                        .map(|r| Doc::from_row(&r))
                        .transpose()?
                        .into_iter()
                        .collect(),
                    (Roots::Singleton, Read::Draft) => db
                        .query_opt(
                            tx,
                            &statements::PREVIEW_SINGLETON,
                            &[&scope, &type_name, &chain],
                        )
                        .await
                        .map_err(|e| sql(&e))?
                        .map(|r| Doc::from_row(&r))
                        .transpose()?
                        .into_iter()
                        .collect(),
                    (Roots::Entry(id), Read::Published) => db
                        .query_opt(
                            tx,
                            &statements::PUBLISHED_ENTRY,
                            &[&scope, &type_name, &locale, &id, &serve_fallback],
                        )
                        .await
                        .map_err(|e| sql(&e))?
                        .map(|r| Doc::from_row(&r))
                        .transpose()?
                        .into_iter()
                        .collect(),
                    (Roots::Entry(id), Read::Draft) => db
                        .query_opt(
                            tx,
                            &statements::PREVIEW_ENTRY,
                            &[&scope, &type_name, &id, &chain],
                        )
                        .await
                        .map_err(|e| sql(&e))?
                        .map(|r| Doc::from_row(&r))
                        .transpose()?
                        .into_iter()
                        .collect(),
                    (Roots::Slug(_), _) if crate::store::slug_field(&content_type).is_none() => {
                        Vec::new()
                    }
                    (Roots::Slug(slug), Read::Published) => db
                        .query_opt(
                            tx,
                            &statements::PUBLISHED_SLUG,
                            &[&scope, &type_name, &locale, &slug, &serve_fallback, &chain],
                        )
                        .await
                        .map_err(|e| sql(&e))?
                        .map(|r| Doc::from_row(&r))
                        .transpose()?
                        .into_iter()
                        .collect(),
                    (Roots::Slug(slug), Read::Draft) => db
                        .query_opt(
                            tx,
                            &statements::PREVIEW_SLUG,
                            &[&scope, &type_name, &chain, &slug],
                        )
                        .await
                        .map_err(|e| sql(&e))?
                        .map(|r| Doc::from_row(&r))
                        .transpose()?
                        .into_iter()
                        .collect(),
                };
                if limit.is_none() && docs.is_empty() {
                    // Unpublished and absent alike.
                    return Err(Rejection::not_found());
                }
                let expanded = population.run(&db, tx, docs, &paths).await?;
                Ok(Outcome::Found {
                    expanded,
                    next,
                    limit,
                    etag,
                })
            })
        })
        .await
    }
}

/// The path's organization: a valid identity, else absent like any id.
fn organization(params: &HashMap<String, String>) -> Result<Option<Identity>, Rejection> {
    match params.get("organization") {
        None => Ok(None),
        Some(org) => Identity::new(org.clone())
            .map(Some)
            .map_err(|_| Rejection::new(Failure::permanent("content_type_not_found"))),
    }
}

/// The options shared by every read: `locale` (any configured one; a
/// non-localized type serves its default variant in each), `fallback`,
/// `fields` and `populate`.
fn plan(
    model: &ContentModel,
    content_type: &ContentType,
    single: &HashMap<String, String>,
    fields: Option<Vec<String>>,
    read: Read,
    limits: &crate::settings::Limits,
) -> Result<Plan, Rejection> {
    let mut problems = Vec::new();
    let fallback = match single.get("fallback").map(String::as_str) {
        None | Some("true") => true,
        Some("false") => false,
        Some(_) => {
            problems.push(detail("/fallback", "invalid_value"));
            true
        }
    };
    let paths = match single.get("populate") {
        None => Paths::default(),
        Some(text) => parse_paths(
            model,
            content_type,
            text,
            limits.max_populate_depth,
            fields.as_deref(),
        )
        .unwrap_or_else(|found| {
            problems.extend(found);
            Paths::default()
        }),
    };
    if !problems.is_empty() {
        return Err(invalid_query(problems));
    }
    let locale = single
        .get("locale")
        .cloned()
        .unwrap_or_else(|| model.default_locale().code.clone());
    if !model.locales().iter().any(|l| l.code == locale) {
        return Err(Failure::permanent("locale_unknown").into());
    }
    Ok(Plan {
        read,
        locale,
        fallback,
        fields,
        paths,
    })
}

/// What the ETag of a published read is made of before its generations.
struct TagBase {
    fingerprint: String,
    hash: String,
    scopes: Vec<String>,
    if_none_match: Vec<String>,
}

/// The entity tags of `If-None-Match`.
fn if_none_match(headers: &HeaderMap) -> Vec<String> {
    headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim().to_owned())
        .filter(|t| !t.is_empty())
        .take(32)
        .collect()
}

/// Weak comparison (RFC 9110 §8.8.3.2).
fn weak_eq(a: &str, b: &str) -> bool {
    let opaque = |t: &str| t.strip_prefix("W/").unwrap_or(t).to_owned();
    opaque(a) == opaque(b)
}

enum Roots {
    List(ListStatement),
    Singleton,
    Entry(String),
    Slug(String),
}

enum Outcome {
    NotModified(String),
    Found {
        expanded: Expanded,
        next: Option<String>,
        /// The page size of a list.
        limit: Option<u32>,
        etag: Option<String>,
    },
}

/// A list statement built for one request.
pub(crate) struct ListStatement {
    pub sql: String,
    pub params: Params,
    pub built: Built,
    pub limit: u32,
}

/// The published or preview list of `content_type` in `scope`.
fn list_statement(
    model: &ContentModel,
    content_type: &ContentType,
    scope: &str,
    mut split: Split,
    plan: &Plan,
    limits: &crate::settings::Limits,
) -> Result<ListStatement, Rejection> {
    let page = split.limit;
    let own_only = !plan.fallback && content_type.localized;
    let mut params = Params::default();
    let scope_param = params.push(scope.to_owned());
    let type_literal = type_literal(content_type);
    let stage = match plan.read {
        Read::Published => "published",
        Read::Draft => "draft",
    };
    let context = format!("{scope}\n{stage}\n{}\n{}", plan.locale, !own_only);
    let (sql, built) = match plan.read {
        Read::Published => {
            let locale_param = params.push(plan.locale.clone());
            let built = query::build(
                &mut split,
                content_type,
                limits,
                Source::Published,
                &context,
                &mut params,
            )?;
            let fetch = params.push(i64::from(page) + 1);
            let strict = if own_only {
                " AND p.source_locale = p.locale"
            } else {
                ""
            };
            let sql = format!(
                "SELECT {}{} FROM merezha_content.published p \
                  WHERE p.scope = {scope_param} AND p.content_type = {type_literal} \
                    AND p.locale = {locale_param}{strict}{}{} \
                  ORDER BY {} LIMIT {fetch}",
                statements::PUBLISHED_COLUMNS,
                built.texts,
                built.conditions,
                built.after,
                built.order,
            );
            (sql, built)
        }
        Read::Draft => {
            let chain: Vec<String> = if own_only {
                vec![plan.locale.clone()]
            } else {
                chain(model, content_type, &plan.locale)
                    .into_iter()
                    .map(str::to_owned)
                    .collect()
            };
            let chain_param = params.push(chain);
            let built = query::build(
                &mut split,
                content_type,
                limits,
                Source::Preview,
                &context,
                &mut params,
            )?;
            let fetch = params.push(i64::from(page) + 1);
            let sql = format!(
                "SELECT {}{} FROM (\
                   SELECT DISTINCT ON (v.entry_id) v.entry_id, v.content_type, v.locale, \
                          v.draft_revision, v.draft_data, v.published_at, \
                          v.first_published_at, v.scope \
                     FROM {} \
                    WHERE v.scope = {scope_param} AND v.content_type = {type_literal} \
                      AND v.locale = ANY({chain_param}::text[]) AND NOT v.deleted \
                      AND e.deleted_at IS NULL \
                    ORDER BY v.entry_id, array_position({chain_param}::text[], v.locale)\
                 ) v WHERE true{}{} ORDER BY {} LIMIT {fetch}",
                statements::PREVIEW_COLUMNS,
                built.texts,
                statements::LIVE_VARIANTS,
                built.conditions,
                built.after,
                built.order,
            );
            (sql, built)
        }
    };
    Ok(ListStatement {
        sql,
        params,
        built,
        limit: page,
    })
}

/// One root entry: `sys` (id, type, locale, source locale, revision and
/// publish times) and `fields` in model order, limited to `fields`.
fn entry_json(
    expanded: &Expanded,
    index: usize,
    content_type: &ContentType,
    plan: &Plan,
) -> String {
    let doc = &expanded.roots[index];
    let mut out = String::with_capacity(256);
    let _ = write!(
        out,
        "{{\"sys\":{{\"id\":{},\"type\":{},\"locale\":{},\"source_locale\":{},\"revision\":{},\
         \"published_at\":{},\"first_published_at\":{}}},\"fields\":",
        string(&doc.id),
        string(&doc.content_type),
        string(&plan.locale),
        string(&doc.source_locale),
        doc.revision,
        optional(doc.published_at.as_deref()),
        optional(doc.first_published_at.as_deref()),
    );
    expanded.write_fields(index, content_type, plan.fields.as_deref(), &mut out);
    out.push('}');
    out
}

/// `GET /v1/content/model` and `/o/{organization}/model`: the model
/// restricted to the types the caller reads there.
async fn model(State(d): State<Delivery>, Path(params): PathParams, request: Request) -> Response {
    let (parts, _) = request.into_parts();
    let run = async || -> Result<Response, Rejection> {
        crate::http::selection_empty(parts.uri.query())?;
        let org = organization(&params)?;
        if let Some(org) = &org {
            d.store
                .refuse_deleted(&format!("org:{}", org.as_str()))
                .await?;
        }
        let principal = principal(&parts);
        let model = d.store.model();
        // Organization types count on their organization's path only;
        // application types everywhere, as population reaches them.
        let visible: BTreeSet<String> = readable(
            &*d.policy,
            principal.as_deref(),
            model,
            org.as_ref(),
            Read::Published,
        )
        .into_keys()
        .collect();
        let settings = &d.store.state().settings().delivery;
        let cache = if principal.is_none() {
            &settings.cache_control_public
        } else {
            &settings.cache_control_private
        };
        Ok(delivery_response(
            StatusCode::OK,
            Some(model_json(model, Some(&visible))),
            cache,
            None,
        ))
    };
    run().await.unwrap_or_else(delivery_error)
}

/// `{version, fingerprint, locales, components, types}` with the
/// canonical form's defaults, fields in model order. With `visible`, only
/// those types, the components they embed, and reference targets among
/// them.
pub(crate) fn model_json(model: &ContentModel, visible: Option<&BTreeSet<String>>) -> String {
    let Ok(Value::Object(mut root)) = serde_json::from_str::<Value>(model.canonical_json()) else {
        return "{}".to_owned();
    };
    root.insert(
        "fingerprint".into(),
        Value::String(model.fingerprint().to_owned()),
    );
    if let Some(visible) = visible {
        let mut components = BTreeSet::new();
        let mut pending: Vec<String> = Vec::new();
        for name in visible {
            if let Some(t) = model.content_type(name) {
                for field in &t.fields {
                    pending.extend(nested(&field.kind));
                }
            }
        }
        while let Some(name) = pending.pop() {
            if components.insert(name.clone())
                && let Some(c) = model.component(&name)
            {
                for field in &c.fields {
                    pending.extend(nested(&field.kind));
                }
            }
        }
        if let Some(Value::Object(types)) = root.get_mut("types") {
            types.retain(|name, _| visible.contains(name));
            for value in types.values_mut() {
                restrict_targets(value, visible);
            }
        }
        if let Some(Value::Object(all)) = root.get_mut("components") {
            all.retain(|name, _| components.contains(name));
            for value in all.values_mut() {
                restrict_targets(value, visible);
            }
        }
    }
    let mut out = String::new();
    crate::canonical::write(&Value::Object(root), &mut out);
    out
}

/// Components a field embeds, list items included.
fn nested(kind: &crate::model::FieldKind) -> Vec<String> {
    kind.nested_components()
        .into_iter()
        .map(str::to_owned)
        .collect()
}

/// Drop reference targets (`to`, `entry_link_to`) the caller cannot read,
/// so the restricted model names no hidden type.
fn restrict_targets(value: &mut Value, visible: &BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                if (key == "to" || key == "entry_link_to")
                    && let Value::Array(names) = item
                {
                    names.retain(|n| n.as_str().is_some_and(|n| visible.contains(n)));
                } else {
                    restrict_targets(item, visible);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|i| restrict_targets(i, visible)),
        _ => {}
    }
}

/// `GET /v1/content/manage/model`: the full model with
/// `content.model.read`, checked in the principal's tenant when it has
/// one, else at `Application` scope.
pub(crate) async fn manage_model(
    State(manage): State<crate::http::Manage>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let run = || -> Result<Response, Rejection> {
        let principal = principal(&parts);
        let scope = match principal.as_ref().and_then(|p| p.tenant.clone()) {
            Some(tenant) => ResourceScope::Tenant(tenant),
            None => ResourceScope::Application,
        };
        manage
            .policy
            .check(principal.as_deref(), &Identity::new(MODEL_READ)?, &scope)
            .map_err(|denied| Rejection::new(denied.failure()))?;
        crate::http::selection_empty(parts.uri.query())?;
        Ok(crate::http::json(
            StatusCode::OK,
            model_json(manage.store.model(), None),
        ))
    };
    run().unwrap_or_else(error_response)
}

/// Populate `paths` below one management draft read: drafts of the types
/// the caller reads (`content.entries.read` in each type's resource
/// scope), through the variant's locale chain.
pub(crate) async fn populate_draft(
    manage: &crate::http::Manage,
    parts: &Parts,
    target: &crate::store::Target,
    view: &crate::store::VariantView,
    paths: Paths,
) -> Result<Expanded, Rejection> {
    let principal = principal(parts);
    let model = manage.store.model().clone();
    let permission = Identity::new(crate::http::ENTRIES_READ)?;
    let mut readable = BTreeMap::new();
    for t in model.types().values() {
        let (scope, tenant) = match t.scope {
            Scope::Application => ("app".to_owned(), None),
            Scope::Organization => match &target.tenant {
                Some(tenant) => (target.scope.clone(), Some(tenant.clone())),
                None => continue,
            },
        };
        let check = ResourceScope::Resource {
            tenant,
            kind: Identity::new(RESOURCE_KIND)?,
            id: Identity::new(t.name.clone())?,
        };
        if manage
            .policy
            .check(principal.as_deref(), &permission, &check)
            .is_ok()
        {
            readable.insert(t.name.clone(), scope);
        }
    }
    let limits = manage.store.state().settings().limits;
    let population = Population {
        model: model.clone(),
        read: Read::Draft,
        locale: view.locale.clone(),
        chain: locale_chain(&model, &view.locale)
            .into_iter()
            .map(str::to_owned)
            .collect(),
        fallback: true,
        readable,
        max_entries: limits.max_populated_entries as usize,
        max_bytes: limits.max_response_bytes,
        mark_all: false,
        assets: None,
    };
    let root = Doc {
        id: view.id.clone(),
        content_type: view.content_type.clone(),
        source_locale: view.locale.clone(),
        revision: view.draft_revision,
        data: view.fields.clone(),
        published_at: view.published_at.clone(),
        first_published_at: view.first_published_at.clone(),
        scope: target.scope.clone(),
    };
    let db = manage.store.database().clone();
    transaction(
        manage.store.database(),
        manage.store.deadline(),
        move |tx| Box::pin(async move { population.run(&db, tx, vec![root], &paths).await }),
    )
    .await
}

impl crate::module::ContentState {
    /// `EXPLAIN (COSTS OFF)` of the application-scope published delivery
    /// list of `content_type` for `query`, its parameters bound: the plan
    /// the route runs, for index tests and operators. Not a route.
    #[doc(hidden)]
    pub async fn explain_delivery(
        &self,
        content_type: &str,
        query: &str,
    ) -> Result<String, Rejection> {
        let model = self.model().clone();
        let declared = model
            .content_type(content_type)
            .ok_or_else(|| Rejection::from(Failure::permanent("content_type_not_found")))?;
        let limits = self.settings().limits;
        const EXTRA: [&str; 4] = ["locale", "fallback", "populate", "stage"];
        let mut split = query::split(declared, Some(query), &limits, &EXTRA)?;
        split.check()?;
        let extra = std::mem::take(&mut split.extra);
        let fields = split.fields.take();
        let plan = plan(&model, declared, &extra, fields, Read::Published, &limits)?;
        let statement = list_statement(&model, declared, "app", split, &plan, &limits)?;
        let database = self
            .database()
            .ok_or_else(|| Rejection::from(Failure::transient("content_model_pending")))?
            .clone();
        let text = format!("EXPLAIN (COSTS OFF) {}", statement.sql);
        let params = statement.params;
        let db = database.clone();
        transaction(
            &database,
            std::time::Instant::now() + std::time::Duration::from_secs(10),
            move |tx| {
                Box::pin(async move {
                    let refs: Vec<&(dyn ToSql + Sync)> = params
                        .0
                        .iter()
                        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
                        .collect();
                    let rows = db
                        .query_text(tx, &statements::EXPLAIN_LIST, &text, &refs)
                        .await
                        .map_err(|e| sql(&e))?;
                    Ok(rows
                        .iter()
                        .map(|r| r.get::<_, String>(0))
                        .collect::<Vec<_>>()
                        .join("\n"))
                })
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CONTENT_PERMISSIONS, ModelOptions};
    use merezha_platform::ports::{
        AccessDenied, Allowed, DenyReason, GrantSnapshot, Grants, PrincipalKind,
    };

    const MODEL: &str = r#"
version = 1
locales = [{ code = "en", default = true }]
[types.page]
access = "public"
fields = [{ name = "title", type = "string" }]
[types.member_note]
access = "authenticated"
fields = [{ name = "title", type = "string" }]
[types.partner_brief]
access = { permission = "content.delivery.read" }
fields = [{ name = "title", type = "string" }, { name = "page", type = "reference", to = ["page", "partner_brief"] }]
[types.help]
scope = "organization"
access = "authenticated"
fields = [{ name = "title", type = "string" }]
[types.handbook]
scope = "organization"
access = { permission = "content.delivery.read" }
fields = [{ name = "title", type = "string" }, { name = "page", type = "reference", to = ["page"] }]
[types.digest]
access = "public"
fields = [{ name = "brief", type = "reference", to = ["partner_brief"] }]
"#;

    fn model() -> ContentModel {
        ContentModel::parse(
            MODEL,
            &ModelOptions {
                permissions: &CONTENT_PERMISSIONS,
                media: false,
                organizations: true,
            },
        )
        .unwrap()
    }

    struct Policy;
    impl merezha_application::Authorizer for Policy {
        fn authorize(
            &self,
            _: merezha_domain::MessageKind,
            _: &str,
            _: Option<&str>,
            _: &Value,
            _: &merezha_domain::Metadata,
        ) -> Result<(), Failure> {
            Err(Failure::permanent("unauthorized"))
        }
    }
    impl Authorizer for Policy {
        fn check(
            &self,
            principal: Option<&Principal>,
            permission: &Identity,
            scope: &ResourceScope,
        ) -> Result<Allowed, AccessDenied> {
            match principal {
                None => Err(AccessDenied::new(DenyReason::Unauthenticated)),
                Some(p) => p.check(permission, scope),
            }
        }
    }

    fn id(text: &str) -> Identity {
        Identity::new(text).unwrap()
    }
    fn user(tenant: Option<&str>, grants: GrantSnapshot) -> Principal {
        Principal {
            subject: id("usr_x"),
            kind: PrincipalKind::User,
            user: Some(id("usr_x")),
            service_account: None,
            tenant: tenant.map(id),
            grants: Arc::new(grants),
            session: None,
        }
    }
    fn delivery_read() -> merezha_platform::ports::PermissionSet {
        [id("content.delivery.read")].into_iter().collect()
    }

    #[test]
    fn authenticated_organization_types_serve_only_the_paths_tenant() {
        let model = model();
        let help = model.content_type("help").unwrap();
        let acme = id("acme");
        let code = |p: Option<&Principal>, org: Option<&Identity>| {
            access(&Policy, p, help, org).err().map(|f| f.code)
        };
        assert_eq!(code(None, Some(&acme)).as_deref(), Some("unauthenticated"));
        let member = user(Some("acme"), GrantSnapshot::empty());
        assert_eq!(code(Some(&member), Some(&acme)), None);
        let other = user(Some("globex"), GrantSnapshot::empty());
        assert_eq!(
            code(Some(&other), Some(&acme)).as_deref(),
            Some("unauthorized")
        );
        let none = user(None, GrantSnapshot::empty());
        assert_eq!(
            code(Some(&none), Some(&acme)).as_deref(),
            Some("unauthorized")
        );
        // Never without the path's organization.
        assert_eq!(code(Some(&member), None).as_deref(), Some("unauthorized"));
    }

    #[test]
    fn permission_types_check_the_type_resource_scope() {
        let model = model();
        let brief = model.content_type("partner_brief").unwrap();
        let handbook = model.content_type("handbook").unwrap();
        let note = model.content_type("member_note").unwrap();
        let code = |p: Option<&Principal>, t: &ContentType, org: Option<&Identity>| {
            access(&Policy, p, t, org).err().map(|f| f.code)
        };
        assert_eq!(code(None, brief, None).as_deref(), Some("unauthenticated"));
        let nobody = user(None, GrantSnapshot::empty());
        assert_eq!(
            code(Some(&nobody), brief, None).as_deref(),
            Some("unauthorized")
        );
        // Granted on another type only.
        let other = user(
            None,
            GrantSnapshot::empty().with_application_resource(
                id("content_type"),
                id("page"),
                delivery_read(),
            ),
        );
        assert_eq!(
            code(Some(&other), brief, None).as_deref(),
            Some("unauthorized")
        );
        // On that type, or at application scope.
        let on_type = user(
            None,
            GrantSnapshot::empty().with_application_resource(
                id("content_type"),
                id("partner_brief"),
                delivery_read(),
            ),
        );
        assert_eq!(code(Some(&on_type), brief, None), None);
        let app = user(
            None,
            GrantSnapshot::empty().with_application(Grants::new(vec![], delivery_read())),
        );
        assert_eq!(code(Some(&app), brief, None), None);
        // An authenticated application type serves any principal.
        assert_eq!(code(Some(&nobody), note, None), None);
        assert_eq!(code(None, note, None).as_deref(), Some("unauthenticated"));
        // An organization permission type is checked in the path's tenant.
        let acme = id("acme");
        let tenant = user(
            Some("acme"),
            GrantSnapshot::empty().with_tenant(Grants::new(vec![], delivery_read())),
        );
        assert_eq!(code(Some(&tenant), handbook, Some(&acme)), None);
        assert_eq!(
            code(Some(&tenant), handbook, Some(&id("globex"))).as_deref(),
            Some("unauthorized")
        );
    }

    #[test]
    fn preview_is_checked_at_application_or_tenant_scope() {
        let model = model();
        let page = model.content_type("page").unwrap();
        let help = model.content_type("help").unwrap();
        let previewer = user(
            None,
            GrantSnapshot::empty()
                .with_application(Grants::new(vec![], [id(PREVIEW)].into_iter().collect())),
        );
        assert!(preview(&Policy, Some(&previewer), page, None).is_ok());
        assert_eq!(
            preview(&Policy, None, page, None).unwrap_err().code,
            "unauthenticated"
        );
        // A per-type grant is not a preview grant.
        let typed = user(
            None,
            GrantSnapshot::empty().with_application_resource(
                id("content_type"),
                id("page"),
                [id(PREVIEW)].into_iter().collect(),
            ),
        );
        assert_eq!(
            preview(&Policy, Some(&typed), page, None).unwrap_err().code,
            "unauthorized"
        );
        let tenant = user(
            Some("acme"),
            GrantSnapshot::empty()
                .with_tenant(Grants::new(vec![], [id(PREVIEW)].into_iter().collect())),
        );
        assert!(preview(&Policy, Some(&tenant), help, Some(&id("acme"))).is_ok());
        assert!(preview(&Policy, Some(&tenant), help, Some(&id("globex"))).is_err());
    }

    #[test]
    fn the_restricted_model_names_no_hidden_type() {
        let model = model();
        let anonymous = readable(&Policy, None, &model, None, Read::Published);
        assert_eq!(anonymous.keys().collect::<Vec<_>>(), ["digest", "page"]);
        let visible: BTreeSet<String> = ["page".to_owned(), "partner_brief".to_owned()].into();
        let json: Value = serde_json::from_str(&model_json(&model, Some(&visible))).unwrap();
        let types: Vec<&String> = json["types"].as_object().unwrap().keys().collect();
        assert_eq!(types, ["page", "partner_brief"]);
        assert_eq!(json["fingerprint"], model.fingerprint());
        let only_page: BTreeSet<String> = ["page".to_owned()].into();
        let json: Value = serde_json::from_str(&model_json(&model, Some(&only_page))).unwrap();
        assert!(!json.to_string().contains("partner_brief"));
        assert!(!json.to_string().contains("member_note"));
    }

    #[test]
    fn organization_tags_carry_the_application_generation_when_entries_can_be_referenced() {
        let model = model();
        let tag =
            |name: &str, scope: &str| tag_scopes(&model, model.content_type(name).unwrap(), scope);
        // No reference field: the organization's generation alone.
        assert_eq!(tag("help", "org:acme"), ["org:acme"]);
        // A reference field reads application rows (population and the
        // `missing` checks), with or without `populate`.
        assert_eq!(tag("handbook", "org:acme"), ["org:acme", "app"]);
        assert_eq!(tag("partner_brief", "app"), ["app"]);
    }

    #[test]
    fn public_cache_control_needs_an_anonymous_caller_and_only_public_types() {
        let model = model();
        let page = model.content_type("page").unwrap();
        let digest = model.content_type("digest").unwrap();
        let brief = model.content_type("partner_brief").unwrap();
        let anonymous = readable(&Policy, None, &model, None, Read::Published);
        assert!(public_cache(&model, page, false, &anonymous));
        assert!(!public_cache(&model, page, true, &anonymous));
        assert!(!public_cache(&model, brief, false, &anonymous));
        // A public type whose references reach a type the caller reads
        // that is not public (a policy granting anonymous permission
        // reads, as the loopback one does) is private.
        assert!(public_cache(&model, digest, false, &anonymous));
        let mut granted = anonymous.clone();
        granted.insert("partner_brief".into(), "app".into());
        assert!(!public_cache(&model, digest, false, &granted));
    }

    #[test]
    fn the_access_class_holds_the_readable_types_for_every_caller() {
        let one: BTreeMap<String, String> = [("page".to_owned(), "app".to_owned())].into();
        let mut two = one.clone();
        two.insert("note".into(), "app".into());
        assert_ne!(access_class(false, &one), access_class(false, &two));
        assert_ne!(access_class(true, &one), access_class(true, &two));
        assert_ne!(access_class(false, &one), access_class(true, &one));
    }

    #[test]
    fn etags_compare_weakly() {
        assert!(weak_eq("W/\"a-1-b\"", "W/\"a-1-b\""));
        assert!(weak_eq("\"a-1-b\"", "W/\"a-1-b\""));
        assert!(!weak_eq("W/\"a-2-b\"", "W/\"a-1-b\""));
        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_static("W/\"x\", W/\"y\""),
        );
        assert_eq!(if_none_match(&headers), ["W/\"x\"", "W/\"y\""]);
    }
}
