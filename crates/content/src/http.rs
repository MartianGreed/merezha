//! The management routes of C3 (section 5), nested by the host under
//! `/v1/content`: entries, variants, revisions, restore, referrers and
//! singletons under `/manage`. Every route checks its permission through
//! the host's [`Authorizer`] before reading anything: in the type's
//! resource scope (`Resource { tenant, kind: "content_type", id }`), or
//! at `Application` scope for a type the model does not have, so an
//! unauthorized caller learns nothing about the model. Without the
//! identity block the loopback policy allows them and a public listener
//! denies them.
//!
//! Publishing, deletion, the trash and batches (C4) mount beside them from
//! [`crate::manage`]; delivery is C5's and assets C7's.
use crate::{
    json::{body_object, optional, string, write_fields},
    manage,
    model::{ContentType, Kind, Scope},
    query::{self, decode_cursor, encode_cursor, locale_allowed, pairs},
    store::{
        Actor, Content, Page, Rejection, RevisionItem, Saved, Store, Target, VariantView,
        WriteKind, status,
    },
};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Path, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
    routing::get,
};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    ErrorReply, FieldError,
    edge::{Authenticated, RequestId},
    ports::{Authorizer, Principal, ResourceScope},
};
use serde_json::{Map, Value};
use std::{collections::HashMap, fmt::Write as _, sync::Arc, time::Duration};
use uuid::Uuid;

/// Permission to list and read drafts, revisions and referrers.
pub const ENTRIES_READ: &str = "content.entries.read";
/// Permission to create entries, save and patch drafts, restore and
/// delete a non-default variant.
pub const ENTRIES_WRITE: &str = "content.entries.write";
/// Permission to publish, unpublish and batch publish.
pub const ENTRIES_PUBLISH: &str = "content.entries.publish";
/// Permission to move entries to the trash and undelete them.
pub const ENTRIES_DELETE: &str = "content.entries.delete";
/// Permission to hard-delete entries at once (`purge=true`), with
/// [`ENTRIES_DELETE`].
pub const ENTRIES_PURGE: &str = "content.entries.purge";
/// The resource kind of per-type checks.
pub const RESOURCE_KIND: &str = "content_type";
/// `Cache-Control` of every management response.
pub const NO_STORE: &str = "private, no-store";

#[derive(Clone)]
pub(crate) struct Manage {
    pub(crate) store: Store,
    pub(crate) policy: Arc<dyn Authorizer>,
}

/// The management router for `store`, checked through `policy`.
pub(crate) fn router(store: Store, policy: Arc<dyn Authorizer>) -> Router {
    Router::new()
        .route("/manage/entries/{type}", get(list).post(create))
        .route(
            "/manage/entries/{type}/{id}",
            get(summary).delete(manage::delete_entry),
        )
        .route("/manage/entries/{type}/{id}/referrers", get(referrers))
        .route(
            "/manage/entries/{type}/{id}/undelete",
            axum::routing::post(manage::undelete),
        )
        .route(
            "/manage/entries/{type}/{id}/{locale}",
            get(read_variant)
                .put(put)
                .patch(patch)
                .delete(manage::delete_variant),
        )
        .route(
            "/manage/entries/{type}/{id}/{locale}/publish",
            axum::routing::post(manage::publish),
        )
        .route(
            "/manage/entries/{type}/{id}/{locale}/unpublish",
            axum::routing::post(manage::unpublish),
        )
        .route(
            "/manage/entries/{type}/{id}/{locale}/revisions",
            get(revisions),
        )
        .route(
            "/manage/entries/{type}/{id}/{locale}/revisions/{n}",
            get(revision),
        )
        .route(
            "/manage/entries/{type}/{id}/{locale}/revisions/{n}/restore",
            axum::routing::post(restore),
        )
        .route(
            "/manage/singletons/{type}/{locale}",
            get(read_variant).put(put).patch(patch),
        )
        .route(
            "/manage/singletons/{type}/{locale}/publish",
            axum::routing::post(manage::publish),
        )
        .route(
            "/manage/singletons/{type}/{locale}/unpublish",
            axum::routing::post(manage::unpublish),
        )
        .route(
            "/manage/singletons/{type}/{locale}/revisions",
            get(revisions),
        )
        .route(
            "/manage/singletons/{type}/{locale}/revisions/{n}",
            get(revision),
        )
        .route(
            "/manage/singletons/{type}/{locale}/revisions/{n}/restore",
            axum::routing::post(restore),
        )
        .route("/manage/trash", get(manage::trash))
        .route(
            "/manage/batch/publish",
            axum::routing::post(manage::batch_publish),
        )
        .route(
            "/manage/batch/unpublish",
            axum::routing::post(manage::batch_unpublish),
        )
        .with_state(Manage { store, policy })
}

pub(crate) type Params = Path<HashMap<String, String>>;
pub(crate) type Reply = Result<Response, Rejection>;

pub(crate) fn respond(reply: Reply) -> Response {
    reply.unwrap_or_else(error_response)
}

/// The error shape, with `timeout` (504) for an expired statement bound.
pub(crate) fn error_response(rejection: Rejection) -> Response {
    let mut failure = rejection.failure;
    if failure.code == "database_timeout" {
        failure = Failure::transient("timeout");
    }
    let mut reply = ErrorReply::new(failure.clone()).details(rejection.details);
    if failure.code == "content_model_superseded" || failure.code == "content_model_pending" {
        reply = reply.retry_after(Duration::from_secs(1));
    }
    let mut response = reply.into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    response
}

pub(crate) fn json(status: StatusCode, body: String) -> Response {
    let mut response = (status, body).into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    response
}

fn etag(revision: i64) -> String {
    format!("\"r{revision}\"")
}

pub(crate) fn principal(parts: &Parts) -> Option<Arc<Principal>> {
    parts
        .extensions
        .get::<Authenticated>()
        .and_then(|a| a.0.clone())
}

pub(crate) fn actor(parts: &Parts) -> Actor {
    let correlation = parts.extensions.get::<RequestId>().map(|r| r.0.clone());
    match principal(parts) {
        Some(p) => Actor {
            subject: p.subject.as_str().to_owned(),
            kind: serde_json::to_value(p.kind)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_else(|| "user".to_owned()),
            correlation,
        },
        None => Actor {
            subject: "anonymous".to_owned(),
            kind: "anonymous".to_owned(),
            correlation,
        },
    }
}

impl Manage {
    /// Check `permission` for the request's principal on `type_name` and
    /// resolve the type and scope.
    pub(crate) fn authorize(
        &self,
        parts: &Parts,
        permission: &str,
        type_name: &str,
    ) -> Result<(Target, ContentType), Rejection> {
        let principal = principal(parts);
        let permission = Identity::new(permission)?;
        let model = self.store.model();
        let check = |scope: &ResourceScope| {
            self.policy
                .check(principal.as_deref(), &permission, scope)
                .map(|_| ())
                .map_err(|denied| Rejection::new(denied.failure()))
        };
        let content_type = model.content_type(type_name);
        let (scope, tenant) = resolve_scope(content_type, principal.as_deref(), &check)?;
        let content_type = content_type
            .ok_or_else(|| Rejection::new(Failure::permanent("content_type_not_found")))?;
        Ok((
            Target {
                content_type: content_type.name.clone(),
                scope,
                tenant,
            },
            content_type.clone(),
        ))
    }

    /// Authorize and resolve the variant a route addresses: the entry id
    /// from the path, or the scope's singleton (`None` before its first
    /// save).
    pub(crate) async fn locate(
        &self,
        parts: &Parts,
        permission: &str,
        params: &HashMap<String, String>,
    ) -> Result<(Target, ContentType, Option<Uuid>, String), Rejection> {
        let type_name = params.get("type").map(String::as_str).unwrap_or("");
        let (target, content_type) = self.authorize(parts, permission, type_name)?;
        let locale = params.get("locale").cloned().unwrap_or_default();
        if !locale_allowed(self.store.model(), &content_type, &locale) {
            return Err(Failure::permanent("locale_unknown").into());
        }
        let id = match params.get("id") {
            Some(id) => Some(entry_id(id)?),
            None => {
                if content_type.kind != Kind::Singleton {
                    return Err(Failure::permanent("content_type_not_found").into());
                }
                self.store.singleton(&target).await?
            }
        };
        Ok((target, content_type, id, locale))
    }
}

/// The scope of `content_type` for `principal`, after its permission
/// check (`check`) passed there. A type the model lacks, and an
/// organization type the principal has no tenant for, are both checked at
/// `Application` scope first, so a caller without that grant gets the same
/// answer for either and cannot enumerate type names. Past that check the
/// first is `content_type_not_found` (the caller's `None`), the second
/// `tenant_required`.
pub(crate) fn resolve_scope(
    content_type: Option<&ContentType>,
    principal: Option<&Principal>,
    check: &dyn Fn(&ResourceScope) -> Result<(), Rejection>,
) -> Result<(String, Option<Identity>), Rejection> {
    let Some(content_type) = content_type else {
        check(&ResourceScope::Application)?;
        return Ok((String::new(), None));
    };
    let (scope, tenant) = match content_type.scope {
        Scope::Application => ("app".to_owned(), None),
        Scope::Organization => match principal.and_then(|p| p.tenant.clone()) {
            Some(tenant) => (format!("org:{}", tenant.as_str()), Some(tenant)),
            None => {
                check(&ResourceScope::Application)?;
                return Err(Failure::permanent("tenant_required").into());
            }
        },
    };
    check(&ResourceScope::Resource {
        tenant: tenant.clone(),
        kind: Identity::new(RESOURCE_KIND)?,
        id: Identity::new(content_type.name.clone())?,
    })?;
    Ok((scope, tenant))
}

/// An entry id from a path: a hyphenated UUID, anything else is absent.
pub(crate) fn entry_id(text: &str) -> Result<Uuid, Rejection> {
    Some(text)
        .filter(|t| t.len() == 36)
        .and_then(|t| Uuid::try_parse(t).ok())
        .ok_or_else(Rejection::not_found)
}

pub(crate) async fn read_body(store: &Store, body: Body) -> Result<axum::body::Bytes, Rejection> {
    to_bytes(body, store.state().settings().limits.max_request_bytes)
        .await
        .map_err(|_| Failure::permanent("body_limit").into())
}

/// `{"fields": {...}}` with nothing else.
fn fields_body(bytes: &[u8]) -> Result<Map<String, Value>, Rejection> {
    let mut body = body_object(bytes, &["fields"])?;
    match body.remove("fields") {
        Some(Value::Object(fields)) => Ok(fields),
        _ => Err(Failure::permanent("invalid_json").into()),
    }
}

/// The write preconditions of `PUT`, `PATCH` and restore.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Preconditions {
    /// `If-Match` was sent: `Some(n)` for exactly `"r<n>"`, else `None`,
    /// which matches no revision.
    pub(crate) matches: Option<Option<i64>>,
    /// `If-None-Match: *` was sent.
    pub(crate) none_match: bool,
}
pub(crate) fn preconditions(headers: &HeaderMap) -> Preconditions {
    let matches = headers.get(header::IF_MATCH).map(|value| {
        value
            .to_str()
            .ok()
            .map(str::trim)
            .and_then(|v| v.strip_prefix("\"r"))
            .and_then(|v| v.strip_suffix('"'))
            .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|n| *n > 0)
    });
    let none_match = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim() == "*");
    Preconditions {
        matches,
        none_match,
    }
}
pub(crate) fn precondition_required() -> Rejection {
    Failure::permanent("precondition_required").into()
}

/// `fields=a,b` on single-variant reads.
pub(crate) fn selection(
    content_type: &ContentType,
    query: Option<&str>,
    allowed: &[&str],
) -> Result<HashMap<String, String>, Rejection> {
    let mut out = HashMap::new();
    let mut problems = Vec::new();
    for (key, value) in pairs(query)? {
        if !allowed.contains(&key.as_str()) {
            problems.push(query::detail("", "unknown_parameter"));
        } else if out.insert(key.clone(), value).is_some() {
            problems.push(query::detail(&format!("/{key}"), "duplicate_parameter"));
        }
    }
    if let Some(fields) = out.get("fields")
        && fields.split(',').any(|n| content_type.field(n).is_none())
    {
        problems.push(query::detail("/fields", "unknown_field"));
    }
    if problems.is_empty() {
        Ok(out)
    } else {
        Err(Rejection {
            failure: Failure::permanent("invalid_query"),
            details: problems,
        })
    }
}
fn field_list(query: &HashMap<String, String>) -> Option<Vec<String>> {
    query
        .get("fields")
        .map(|f| f.split(',').map(str::to_owned).collect())
}

/// The management variant document (section 5, entry JSON), fields in
/// model order.
pub(crate) fn variant_json(
    content_type: &ContentType,
    view: &VariantView,
    selection: Option<&[String]>,
    warnings: Option<&[FieldError]>,
) -> String {
    let mut out = String::with_capacity(256);
    let _ = write!(
        out,
        "{{\"sys\":{{\"id\":{},\"type\":{},\"locale\":{},\"status\":{},\"draft_revision\":{},\
         \"published_revision\":{},\"model_version\":{},\"created_at\":{},\"created_by\":{},\
         \"updated_at\":{},\"updated_by\":{},\"published_at\":{},\"first_published_at\":{}}},\
         \"fields\":",
        string(&view.id),
        string(&view.content_type),
        string(&view.locale),
        string(view.status()),
        view.draft_revision,
        view.published_revision
            .map_or_else(|| "null".to_owned(), |r| r.to_string()),
        view.model_version,
        string(&view.created_at),
        string(&view.created_by),
        string(&view.updated_at),
        string(&view.updated_by),
        optional(view.published_at.as_deref()),
        optional(view.first_published_at.as_deref()),
    );
    write_fields(content_type, &view.fields, selection, &mut out);
    if let Some(warnings) = warnings {
        out.push_str(",\"warnings\":");
        out.push_str(&serde_json::to_string(warnings).unwrap_or_else(|_| "[]".to_owned()));
    }
    out.push('}');
    out
}

pub(crate) fn variant_response(
    status: StatusCode,
    content_type: &ContentType,
    view: &VariantView,
    selection: Option<&[String]>,
    location: Option<String>,
    warnings: Option<&[FieldError]>,
) -> Response {
    let mut response = json(
        status,
        variant_json(content_type, view, selection, warnings),
    );
    let headers = response.headers_mut();
    if let Ok(value) = HeaderValue::from_str(&etag(view.draft_revision)) {
        headers.insert(header::ETAG, value);
    }
    if let Some(location) = location.and_then(|l| HeaderValue::from_str(&l).ok()) {
        headers.insert(header::LOCATION, location);
    }
    response
}

fn location(view: &VariantView) -> String {
    format!(
        "/v1/content/manage/entries/{}/{}/{}",
        view.content_type, view.id, view.locale
    )
}

pub(crate) fn page_json(items: Vec<String>, next: Option<String>, limit: u32) -> String {
    format!(
        "{{\"items\":[{}],\"page\":{{\"next_cursor\":{},\"limit\":{limit}}}}}",
        items.join(","),
        optional(next.as_deref())
    )
}

/// `limit` and `cursor` of the history and referrer listings.
pub(crate) fn paging(
    store: &Store,
    query: &HashMap<String, String>,
) -> Result<(u32, Option<String>), Rejection> {
    let limits = store.state().settings().limits;
    let limit = match query.get("limit") {
        None => limits.default_page_size,
        Some(text) => text
            .parse::<u32>()
            .ok()
            .filter(|n| (1..=limits.max_page_size).contains(n))
            .ok_or_else(|| Rejection {
                failure: Failure::permanent("invalid_query"),
                details: vec![query::detail("/limit", "invalid_value")],
            })?,
    };
    Ok((limit, query.get("cursor").cloned()))
}

pub(crate) fn fingerprint(parts: &[&str]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(parts.join("\n").as_bytes());
    digest[..8].iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

async fn list(
    State(manage): State<Manage>,
    Path(type_name): Path<String>,
    request: Request,
) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        let (target, content_type) = manage.authorize(&parts, ENTRIES_READ, &type_name)?;
        let limits = manage.store.state().settings().limits;
        let parsed = query::parse_list(
            manage.store.model(),
            &content_type,
            &target.scope,
            parts.uri.query(),
            &limits,
        )?;
        let fingerprint = parsed.fingerprint.clone();
        let fields = parsed.fields.clone();
        let limit = parsed.limit;
        let page = manage
            .store
            .list(parsed.statement, parsed.params, parsed.keys, limit)
            .await?;
        let items = page
            .items
            .iter()
            .map(|v| variant_json(&content_type, v, fields.as_deref(), None))
            .collect();
        let next = page
            .next
            .map(|(keys, id)| encode_cursor(&fingerprint, &keys, &id));
        Ok(json(StatusCode::OK, page_json(items, next, limit)))
    };
    respond(run.await)
}

async fn create(
    State(manage): State<Manage>,
    Path(type_name): Path<String>,
    request: Request,
) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        let (target, content_type) = manage.authorize(&parts, ENTRIES_WRITE, &type_name)?;
        let bytes = read_body(&manage.store, body).await?;
        let mut body = body_object(&bytes, &["id", "locale", "fields"])?;
        let fields = match body.remove("fields") {
            Some(Value::Object(fields)) => fields,
            _ => return Err(Failure::permanent("invalid_json").into()),
        };
        let model = manage.store.model();
        let locale = match body.remove("locale") {
            None => model.default_locale().code.clone(),
            Some(Value::String(locale)) => locale,
            Some(_) => {
                return Err(Rejection::invalid(vec![query::detail(
                    "/locale",
                    "type_mismatch",
                )]));
            }
        };
        if !locale_allowed(model, &content_type, &locale) {
            return Err(Failure::permanent("locale_unknown").into());
        }
        let id = match body.remove("id") {
            None => manage.store.new_entry_id()?,
            Some(Value::String(id)) => Some(id.as_str())
                .filter(|t| t.len() == 36)
                .and_then(|t| Uuid::try_parse(t).ok())
                .ok_or_else(|| Rejection::invalid(vec![query::detail("/id", "type_mismatch")]))?,
            Some(_) => {
                return Err(Rejection::invalid(vec![query::detail(
                    "/id",
                    "type_mismatch",
                )]));
            }
        };
        let saved = manage
            .store
            .save(
                &target,
                id,
                &locale,
                WriteKind::NewEntry {
                    id,
                    singleton: content_type.kind == Kind::Singleton,
                },
                Content::Replace(fields),
                &actor(&parts),
            )
            .await?;
        Ok(variant_response(
            StatusCode::CREATED,
            &content_type,
            &saved.view,
            None,
            Some(location(&saved.view)),
            None,
        ))
    };
    respond(run.await)
}

async fn summary(State(manage): State<Manage>, Path(params): Params, request: Request) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        let type_name = params.get("type").map(String::as_str).unwrap_or("");
        let (target, _) = manage.authorize(&parts, ENTRIES_READ, type_name)?;
        selection_empty(parts.uri.query())?;
        let id = entry_id(params.get("id").map(String::as_str).unwrap_or(""))?;
        let summary = manage.store.summary(&target, id).await?;
        Ok(json(StatusCode::OK, summary_json(&summary)))
    };
    respond(run.await)
}

/// `GET /entries/{type}/{id}`: the entry and its variants.
pub(crate) fn summary_json(summary: &crate::store::EntrySummary) -> String {
    let variants: Vec<String> = summary
        .variants
        .iter()
        .map(|(locale, draft, published, updated)| {
            format!(
                "{{\"locale\":{},\"status\":{},\"draft_revision\":{draft},\
                 \"published_revision\":{},\"updated_at\":{}}}",
                string(locale),
                string(status(*draft, *published)),
                published.map_or_else(|| "null".to_owned(), |p| p.to_string()),
                string(updated)
            )
        })
        .collect();
    format!(
        "{{\"sys\":{{\"id\":{},\"type\":{},\"created_at\":{},\"deleted_at\":{}}},\
         \"variants\":[{}]}}",
        string(&summary.id),
        string(&summary.content_type),
        string(&summary.created_at),
        optional(summary.deleted_at.as_deref()),
        variants.join(",")
    )
}

pub(crate) fn selection_empty(query: Option<&str>) -> Result<(), Rejection> {
    if pairs(query)?.is_empty() {
        Ok(())
    } else {
        Err(Rejection {
            failure: Failure::permanent("invalid_query"),
            details: vec![query::detail("", "unknown_parameter")],
        })
    }
}

async fn read_variant(
    State(manage): State<Manage>,
    Path(params): Params,
    request: Request,
) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        let (target, content_type, id, locale) =
            manage.locate(&parts, ENTRIES_READ, &params).await?;
        let query = selection(&content_type, parts.uri.query(), &["fields"])?;
        let id = id.ok_or_else(Rejection::not_found)?;
        let view = manage.store.variant(&target, id, &locale).await?;
        Ok(variant_response(
            StatusCode::OK,
            &content_type,
            &view,
            field_list(&query).as_deref(),
            None,
            None,
        ))
    };
    respond(run.await)
}

async fn put(State(manage): State<Manage>, Path(params): Params, request: Request) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        let (target, content_type, id, locale) =
            manage.locate(&parts, ENTRIES_WRITE, &params).await?;
        selection_empty(parts.uri.query())?;
        let conditions = preconditions(&parts.headers);
        let bytes = read_body(&manage.store, body).await?;
        let fields = fields_body(&bytes)?;
        let (id, kind) = match (conditions.matches, conditions.none_match, id) {
            (Some(expected), none_match, Some(id)) => (
                id,
                WriteKind::Update {
                    expected,
                    none_match,
                },
            ),
            (Some(_), _, None) => return Err(Rejection::not_found()),
            (None, true, Some(id)) => (id, WriteKind::NewVariant),
            // The first `PUT` of a singleton creates its entry.
            (None, true, None) => {
                let id = manage.store.new_entry_id()?;
                (
                    id,
                    WriteKind::NewEntry {
                        id,
                        singleton: true,
                    },
                )
            }
            (None, false, _) => return Err(precondition_required()),
        };
        let saved = manage
            .store
            .save(
                &target,
                id,
                &locale,
                kind,
                Content::Replace(fields),
                &actor(&parts),
            )
            .await?;
        Ok(saved_response(&content_type, &saved, false))
    };
    respond(run.await)
}

fn saved_response(content_type: &ContentType, saved: &Saved, warnings: bool) -> Response {
    let (status, location) = if saved.created {
        (StatusCode::CREATED, Some(location(&saved.view)))
    } else {
        (StatusCode::OK, None)
    };
    variant_response(
        status,
        content_type,
        &saved.view,
        None,
        location,
        warnings.then_some(saved.warnings.as_slice()),
    )
}

async fn patch(State(manage): State<Manage>, Path(params): Params, request: Request) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        let (target, content_type, id, locale) =
            manage.locate(&parts, ENTRIES_WRITE, &params).await?;
        selection_empty(parts.uri.query())?;
        let merge = parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(';').next())
            .is_some_and(|v| {
                v.trim()
                    .eq_ignore_ascii_case("application/merge-patch+json")
            });
        if !merge {
            return Err(Failure::permanent("unsupported_media_type").into());
        }
        let conditions = preconditions(&parts.headers);
        let Some(expected) = conditions.matches else {
            return Err(precondition_required());
        };
        let bytes = read_body(&manage.store, body).await?;
        let mut body = body_object(&bytes, &["fields"])?;
        let patch = match body.remove("fields") {
            None => Value::Object(Map::new()),
            Some(value @ Value::Object(_)) => value,
            Some(_) => return Err(Failure::permanent("invalid_json").into()),
        };
        let id = id.ok_or_else(Rejection::not_found)?;
        let saved = manage
            .store
            .save(
                &target,
                id,
                &locale,
                WriteKind::Update {
                    expected,
                    none_match: conditions.none_match,
                },
                Content::Patch(patch),
                &actor(&parts),
            )
            .await?;
        Ok(saved_response(&content_type, &saved, false))
    };
    respond(run.await)
}

fn revision_number(params: &HashMap<String, String>) -> Result<i64, Rejection> {
    params
        .get("n")
        .filter(|n| !n.is_empty() && n.len() <= 18 && n.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|n| n.parse::<i64>().ok())
        .filter(|n| *n > 0)
        .ok_or_else(|| Failure::permanent("revision_not_found").into())
}

fn revision_json(item: &RevisionItem) -> String {
    format!(
        "{{\"revision\":{},\"kind\":{},\"restored_from\":{},\"model_version\":{},\
         \"created_at\":{},\"created_by\":{},\"title\":{},\"published\":{}}}",
        item.revision,
        string(&item.kind),
        item.restored_from
            .map_or_else(|| "null".to_owned(), |r| r.to_string()),
        item.model_version,
        string(&item.created_at),
        string(&item.created_by),
        optional(item.title.as_deref()),
        item.published
    )
}

async fn revisions(
    State(manage): State<Manage>,
    Path(params): Params,
    request: Request,
) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        let (target, content_type, id, locale) =
            manage.locate(&parts, ENTRIES_READ, &params).await?;
        let query = selection(&content_type, parts.uri.query(), &["limit", "cursor"])?;
        let id = id.ok_or_else(Rejection::not_found)?;
        let (limit, cursor) = paging(&manage.store, &query)?;
        let id_text = id.hyphenated().to_string();
        let fp = fingerprint(&[
            "revisions",
            &target.scope,
            &target.content_type,
            &id_text,
            &locale,
        ]);
        let before = match cursor {
            None => None,
            Some(text) => {
                let cursor = decode_cursor(&text, &fp, 1)?;
                let revision = cursor.keys[0]
                    .as_deref()
                    .filter(|_| cursor.id == id_text)
                    .and_then(|k| k.parse::<i64>().ok())
                    .ok_or_else(|| Rejection {
                        failure: Failure::permanent("invalid_cursor"),
                        details: vec![query::detail("/cursor", "invalid_value")],
                    })?;
                Some(revision)
            }
        };
        let Page { items, next } = manage
            .store
            .revisions(&target, id, &locale, before, limit)
            .await?;
        let items = items.iter().map(revision_json).collect();
        let next = next.map(|(keys, id)| encode_cursor(&fp, &keys, &id));
        Ok(json(StatusCode::OK, page_json(items, next, limit)))
    };
    respond(run.await)
}

async fn revision(
    State(manage): State<Manage>,
    Path(params): Params,
    request: Request,
) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        let (target, content_type, id, locale) =
            manage.locate(&parts, ENTRIES_READ, &params).await?;
        selection_empty(parts.uri.query())?;
        let n = revision_number(&params)?;
        let id = id.ok_or_else(Rejection::not_found)?;
        let view = manage.store.revision(&target, id, &locale, n).await?;
        let mut out = String::new();
        let item = &view.item;
        let _ = write!(
            out,
            "{{\"sys\":{{\"id\":{},\"type\":{},\"locale\":{},\"revision\":{},\"kind\":{},\
             \"restored_from\":{},\"model_version\":{},\"created_at\":{},\"created_by\":{},\
             \"published\":{}}},\"fields\":",
            string(&view.id),
            string(&view.content_type),
            string(&view.locale),
            item.revision,
            string(&item.kind),
            item.restored_from
                .map_or_else(|| "null".to_owned(), |r| r.to_string()),
            item.model_version,
            string(&item.created_at),
            string(&item.created_by),
            item.published,
        );
        write_fields(&content_type, &view.fields, None, &mut out);
        out.push('}');
        Ok(json(StatusCode::OK, out))
    };
    respond(run.await)
}

async fn restore(State(manage): State<Manage>, Path(params): Params, request: Request) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        let (target, content_type, id, locale) =
            manage.locate(&parts, ENTRIES_WRITE, &params).await?;
        selection_empty(parts.uri.query())?;
        let n = revision_number(&params)?;
        let conditions = preconditions(&parts.headers);
        let Some(expected) = conditions.matches else {
            return Err(precondition_required());
        };
        let bytes = read_body(&manage.store, body).await?;
        if !bytes.iter().all(u8::is_ascii_whitespace) {
            body_object(&bytes, &[])?;
        }
        let id = id.ok_or_else(Rejection::not_found)?;
        let saved = manage
            .store
            .save(
                &target,
                id,
                &locale,
                WriteKind::Update {
                    expected,
                    none_match: conditions.none_match,
                },
                Content::Restore(n),
                &actor(&parts),
            )
            .await?;
        Ok(saved_response(&content_type, &saved, true))
    };
    respond(run.await)
}

async fn referrers(
    State(manage): State<Manage>,
    Path(params): Params,
    request: Request,
) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        let type_name = params.get("type").map(String::as_str).unwrap_or("");
        let (target, content_type) = manage.authorize(&parts, ENTRIES_READ, type_name)?;
        let query = selection(
            &content_type,
            parts.uri.query(),
            &["stage", "limit", "cursor"],
        )?;
        let stage = match query.get("stage").map(String::as_str) {
            None | Some("draft") => "draft",
            Some("published") => "published",
            Some(_) => {
                return Err(Rejection {
                    failure: Failure::permanent("invalid_query"),
                    details: vec![query::detail("/stage", "invalid_value")],
                });
            }
        };
        let id = entry_id(params.get("id").map(String::as_str).unwrap_or(""))?;
        let (limit, cursor) = paging(&manage.store, &query)?;
        let id_text = id.hyphenated().to_string();
        let fp = fingerprint(&[
            "referrers",
            &target.scope,
            &target.content_type,
            &id_text,
            stage,
        ]);
        let after = match cursor {
            None => None,
            Some(text) => {
                let cursor = decode_cursor(&text, &fp, 2)?;
                let entry = Some(cursor.id.as_str())
                    .filter(|t| t.len() == 36 && Uuid::try_parse(t).is_ok())
                    .map(str::to_owned);
                match (entry, &cursor.keys[0], &cursor.keys[1]) {
                    (Some(entry), Some(locale), Some(path)) => {
                        Some((entry, locale.clone(), path.clone()))
                    }
                    _ => {
                        return Err(Rejection {
                            failure: Failure::permanent("invalid_cursor"),
                            details: vec![query::detail("/cursor", "invalid_value")],
                        });
                    }
                }
            }
        };
        let Page { items, next } = manage
            .store
            .referrers(&target, id, stage, after, limit)
            .await?;
        let items = items
            .iter()
            .map(|r| {
                format!(
                    "{{\"id\":{},\"type\":{},\"locale\":{},\"stage\":{},\"path\":{},\"title\":{}}}",
                    string(&r.id),
                    string(&r.content_type),
                    string(&r.locale),
                    string(&r.stage),
                    string(&r.path),
                    optional(r.title.as_deref())
                )
            })
            .collect();
        let next = next.map(|(keys, id)| encode_cursor(&fp, &keys, &id));
        Ok(json(StatusCode::OK, page_json(items, next, limit)))
    };
    respond(run.await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CONTENT_PERMISSIONS, ContentModel, ModelOptions};
    use merezha_platform::ports::{AccessDenied, DenyReason};

    #[test]
    fn unknown_and_tenantless_organization_types_answer_alike() {
        let model = ContentModel::parse(
            "version = 1\nlocales = [{ code = \"en\", default = true }]\n\
             [types.help]\nscope = \"organization\"\naccess = \"public\"\n\
             fields = [{ name = \"title\", type = \"string\" }]\n",
            &ModelOptions {
                permissions: &CONTENT_PERMISSIONS,
                media: false,
                organizations: true,
            },
        )
        .unwrap();
        let help = model.content_type("help");
        let deny = |_: &ResourceScope| -> Result<(), Rejection> {
            Err(Rejection::new(
                AccessDenied::new(DenyReason::NoGrant).failure(),
            ))
        };
        let allow = |_: &ResourceScope| -> Result<(), Rejection> { Ok(()) };
        // Without the application grant: the same refusal for both.
        assert_eq!(
            resolve_scope(None, None, &deny).unwrap_err(),
            resolve_scope(help, None, &deny).unwrap_err()
        );
        // With it, each is told apart.
        assert_eq!(resolve_scope(None, None, &allow).unwrap().0, "");
        assert_eq!(
            resolve_scope(help, None, &allow).unwrap_err().failure.code,
            "tenant_required"
        );
        // A principal with a tenant is checked in its resource scope.
        let mut principal = Principal::system("t", Default::default()).unwrap();
        principal.tenant = Some(Identity::new("acme").unwrap());
        let seen = std::sync::Mutex::new(Vec::new());
        let record = |scope: &ResourceScope| -> Result<(), Rejection> {
            seen.lock().unwrap().push(scope.clone());
            Ok(())
        };
        let (scope, tenant) = resolve_scope(help, Some(&principal), &record).unwrap();
        assert_eq!(scope, "org:acme");
        assert_eq!(tenant.unwrap().as_str(), "acme");
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [ResourceScope::Resource {
                tenant: Some(Identity::new("acme").unwrap()),
                kind: Identity::new(RESOURCE_KIND).unwrap(),
                id: Identity::new("help").unwrap(),
            }]
        );
    }
}
