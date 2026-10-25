//! The C4 management routes (section 5), mounted by [`crate::http`]:
//! publish and unpublish of entry and singleton variants, the variant
//! delete, the entry delete with `if_unreferenced` and `purge`, undelete,
//! the trash listing and the batches. Each checks its permission in the
//! type's resource scope before reading anything, as the C3 routes do;
//! batches check every item.
use crate::{
    http::{
        ENTRIES_DELETE, ENTRIES_PUBLISH, ENTRIES_PURGE, ENTRIES_READ, ENTRIES_WRITE, Manage,
        Params, actor, entry_id, fingerprint, json, page_json, paging, precondition_required,
        preconditions, principal, read_body, respond, selection, selection_empty, summary_json,
        variant_json, variant_response,
    },
    json::{body_object, optional, string},
    model::ContentType,
    publish::{Expect, Item, in_item},
    query::{self, decode_cursor, encode_cursor, locale_allowed},
    store::{Rejection, Target},
    trash::DeleteOptions,
};
use axum::{
    extract::{Path, Request, State},
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use merezha_domain::{Failure, Identity};
use merezha_platform::ports::ResourceScope;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// `{"revision": n}` or nothing: the revision to publish.
fn publish_body(bytes: &[u8]) -> Result<Option<i64>, Rejection> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    let mut body = body_object(bytes, &["revision"])?;
    match body.remove("revision") {
        None => Ok(None),
        Some(value) => {
            value.as_i64().filter(|n| *n > 0).map(Some).ok_or_else(|| {
                Rejection::invalid(vec![query::detail("/revision", "type_mismatch")])
            })
        }
    }
}

/// An empty body or `{}`.
fn empty_body(bytes: &[u8]) -> Result<(), Rejection> {
    if !bytes.iter().all(u8::is_ascii_whitespace) {
        body_object(bytes, &[])?;
    }
    Ok(())
}

/// `POST .../{locale}/publish` for entries and singletons.
pub(crate) async fn publish(
    State(manage): State<Manage>,
    Path(params): Params,
    request: Request,
) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        let (target, content_type, id, locale) =
            manage.locate(&parts, ENTRIES_PUBLISH, &params).await?;
        selection_empty(parts.uri.query())?;
        let Some(expected) = preconditions(&parts.headers).matches else {
            return Err(precondition_required());
        };
        let revision = publish_body(&read_body(&manage.store, body).await?)?;
        let id = id.ok_or_else(Rejection::not_found)?;
        let item = Item {
            target,
            id,
            locale,
            expect: Expect::Draft(expected),
            revision,
        };
        let mut views = manage
            .store
            .publish(vec![item], &actor(&parts), false)
            .await?;
        let view = views.pop().ok_or_else(Rejection::not_found)?;
        Ok(variant_response(
            StatusCode::OK,
            &content_type,
            &view,
            None,
            None,
            None,
        ))
    };
    respond(run.await)
}

/// `POST .../{locale}/unpublish` for entries and singletons.
pub(crate) async fn unpublish(
    State(manage): State<Manage>,
    Path(params): Params,
    request: Request,
) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        let (target, content_type, id, locale) =
            manage.locate(&parts, ENTRIES_PUBLISH, &params).await?;
        selection_empty(parts.uri.query())?;
        let Some(expected) = preconditions(&parts.headers).matches else {
            return Err(precondition_required());
        };
        empty_body(&read_body(&manage.store, body).await?)?;
        let id = id.ok_or_else(Rejection::not_found)?;
        let item = Item {
            target,
            id,
            locale,
            expect: Expect::Draft(expected),
            revision: None,
        };
        let mut views = manage
            .store
            .unpublish(vec![item], &actor(&parts), false)
            .await?;
        let view = views.pop().ok_or_else(Rejection::not_found)?;
        Ok(variant_response(
            StatusCode::OK,
            &content_type,
            &view,
            None,
            None,
            None,
        ))
    };
    respond(run.await)
}

fn no_content() -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static(crate::http::NO_STORE),
    );
    response
}

/// `DELETE /entries/{type}/{id}/{locale}`: 204.
pub(crate) async fn delete_variant(
    State(manage): State<Manage>,
    Path(params): Params,
    request: Request,
) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        let (target, _, id, locale) = manage.locate(&parts, ENTRIES_WRITE, &params).await?;
        selection_empty(parts.uri.query())?;
        let Some(expected) = preconditions(&parts.headers).matches else {
            return Err(precondition_required());
        };
        empty_body(&read_body(&manage.store, body).await?)?;
        let id = id.ok_or_else(Rejection::not_found)?;
        manage
            .store
            .delete_variant(&target, id, &locale, expected, &actor(&parts))
            .await?;
        Ok(no_content())
    };
    respond(run.await)
}

/// A `true` or `false` query flag, `false` when absent.
fn flag(query: &HashMap<String, String>, name: &str) -> Result<bool, Rejection> {
    match query.get(name).map(String::as_str) {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(_) => Err(Rejection {
            failure: Failure::permanent("invalid_query"),
            details: vec![query::detail(&format!("/{name}"), "invalid_value")],
        }),
    }
}

/// `DELETE /entries/{type}/{id}` with `if_unreferenced` and `purge`: 204.
pub(crate) async fn delete_entry(
    State(manage): State<Manage>,
    Path(params): Params,
    request: Request,
) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        let type_name = params.get("type").map(String::as_str).unwrap_or("");
        let (target, content_type) = manage.authorized(&parts, ENTRIES_DELETE, type_name).await?;
        let query = selection(
            &content_type,
            parts.uri.query(),
            &["if_unreferenced", "purge"],
        )?;
        let options = DeleteOptions {
            if_unreferenced: flag(&query, "if_unreferenced")?,
            purge: flag(&query, "purge")?,
        };
        if options.purge {
            manage.authorize(&parts, ENTRIES_PURGE, type_name)?;
        }
        empty_body(&read_body(&manage.store, body).await?)?;
        let id = entry_id(params.get("id").map(String::as_str).unwrap_or(""))?;
        manage
            .store
            .delete_entry(&target, id, options, &actor(&parts))
            .await?;
        Ok(no_content())
    };
    respond(run.await)
}

/// `POST /entries/{type}/{id}/undelete`: 200 with the entry summary.
pub(crate) async fn undelete(
    State(manage): State<Manage>,
    Path(params): Params,
    request: Request,
) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        let type_name = params.get("type").map(String::as_str).unwrap_or("");
        let (target, _) = manage.authorized(&parts, ENTRIES_DELETE, type_name).await?;
        selection_empty(parts.uri.query())?;
        empty_body(&read_body(&manage.store, body).await?)?;
        let id = entry_id(params.get("id").map(String::as_str).unwrap_or(""))?;
        let summary = manage.store.undelete(&target, id, &actor(&parts)).await?;
        Ok(json(StatusCode::OK, summary_json(&summary)))
    };
    respond(run.await)
}

/// The query keys `GET /trash` takes, checked without a content type.
fn trash_query(query: Option<&str>) -> Result<HashMap<String, String>, Rejection> {
    let mut out = HashMap::new();
    let mut problems = Vec::new();
    for (key, value) in query::pairs(query)? {
        if !["type", "limit", "cursor"].contains(&key.as_str()) {
            problems.push(query::detail("", "unknown_parameter"));
        } else if out.insert(key.clone(), value).is_some() {
            problems.push(query::detail(&format!("/{key}"), "duplicate_parameter"));
        }
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

/// `GET /trash` (`type`, `limit`, `cursor`): trashed entries of the types
/// the caller may read, newest deletion first. With `type`, that type is
/// checked like every entry route; without, each type is checked in its
/// own resource scope and the listing covers those that pass (refused
/// with the first denial when none does).
pub(crate) async fn trash(State(manage): State<Manage>, request: Request) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        let query = trash_query(parts.uri.query())?;
        let allowed: Vec<(String, String)> = match query.get("type") {
            Some(name) => {
                let (target, _) = manage.authorize(&parts, ENTRIES_READ, name)?;
                vec![(target.scope, target.content_type)]
            }
            None => readable_types(&manage, &parts)?,
        };
        // Issue #56: a deleted organization's trash is gone with it.
        let scopes: std::collections::BTreeSet<&str> =
            allowed.iter().map(|(scope, _)| scope.as_str()).collect();
        for scope in scopes {
            manage.store.refuse_deleted(scope).await?;
        }
        let (limit, cursor) = paging(&manage.store, &query)?;
        let mut scope_key: Vec<String> = allowed.iter().map(|(s, t)| format!("{s}/{t}")).collect();
        scope_key.sort();
        let joined = scope_key.join(",");
        let fp = fingerprint(&["trash", &joined]);
        let after = match cursor {
            None => None,
            Some(text) => {
                let cursor = decode_cursor(&text, &fp, 1)?;
                let at = cursor.keys[0]
                    .clone()
                    .filter(|at| query::castable("timestamptz", at))
                    .filter(|_| cursor.id.len() == 36 && Uuid::try_parse(&cursor.id).is_ok());
                match at {
                    Some(at) => Some((at, cursor.id)),
                    None => {
                        return Err(Rejection {
                            failure: Failure::permanent("invalid_cursor"),
                            details: vec![query::detail("/cursor", "invalid_value")],
                        });
                    }
                }
            }
        };
        let page = manage.store.trash(allowed, after, limit).await?;
        let items = page
            .items
            .iter()
            .map(|item| {
                format!(
                    "{{\"id\":{},\"type\":{},\"deleted_at\":{},\"deleted_by\":{},\"title\":{}}}",
                    string(&item.id),
                    string(&item.content_type),
                    string(&item.deleted_at),
                    optional(item.deleted_by.as_deref()),
                    optional(item.title.as_deref())
                )
            })
            .collect();
        let next = page.next.map(|(keys, id)| encode_cursor(&fp, &keys, &id));
        Ok(json(StatusCode::OK, page_json(items, next, limit)))
    };
    respond(run.await)
}

/// Every `(scope, type)` whose entries the caller may read. Organization
/// types need the principal's tenant and are skipped without one.
fn readable_types(manage: &Manage, parts: &Parts) -> Result<Vec<(String, String)>, Rejection> {
    let principal = principal(parts);
    let permission = Identity::new(ENTRIES_READ)?;
    let mut allowed = Vec::new();
    let mut denied: Option<Rejection> = None;
    for content_type in manage.store.model().types().values() {
        let (scope, tenant) = match content_type.scope {
            crate::model::Scope::Application => ("app".to_owned(), None),
            crate::model::Scope::Organization => {
                match principal.as_deref().and_then(|p| p.tenant.clone()) {
                    Some(tenant) => (format!("org:{}", tenant.as_str()), Some(tenant)),
                    None => continue,
                }
            }
        };
        let check = manage.policy.check(
            principal.as_deref(),
            &permission,
            &ResourceScope::Resource {
                tenant,
                kind: Identity::new(crate::http::RESOURCE_KIND)?,
                id: Identity::new(content_type.name.clone())?,
            },
        );
        match check {
            Ok(_) => allowed.push((scope, content_type.name.clone())),
            Err(refused) => {
                denied.get_or_insert_with(|| Rejection::new(refused.failure()));
            }
        }
    }
    match (allowed.is_empty(), denied) {
        (true, Some(denied)) => Err(denied),
        (true, None) => {
            // No type at all: the application-scope check decides.
            manage
                .policy
                .check(
                    principal.as_deref(),
                    &permission,
                    &ResourceScope::Application,
                )
                .map_err(|refused| Rejection::new(refused.failure()))?;
            Ok(allowed)
        }
        _ => Ok(allowed),
    }
}

/// The items of a batch body, each authorized and resolved.
async fn batch_items(
    manage: &Manage,
    parts: &Parts,
    bytes: &[u8],
    with_revision: bool,
) -> Result<Vec<(Item, ContentType)>, Rejection> {
    let mut body = body_object(bytes, &["items"])?;
    let Some(Value::Array(items)) = body.remove("items") else {
        return Err(Failure::permanent("invalid_json").into());
    };
    let max = manage.store.state().settings().limits.max_batch as usize;
    if items.is_empty() || items.len() > max {
        return Err(Rejection::invalid(vec![query::detail(
            "/items",
            if items.is_empty() {
                "min_items"
            } else {
                "max_items"
            },
        )]));
    }
    let keys: &[&str] = if with_revision {
        &["type", "id", "locale", "revision"]
    } else {
        &["type", "id", "locale"]
    };
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(items.len());
    for (index, value) in items.iter().enumerate() {
        let item = batch_item(manage, parts, value, keys, with_revision, &mut seen)
            .await
            .map_err(|r| in_item(index, true, r))?;
        out.push(item);
    }
    Ok(out)
}

async fn batch_item(
    manage: &Manage,
    parts: &Parts,
    value: &Value,
    keys: &[&str],
    with_revision: bool,
    seen: &mut HashSet<(String, Uuid, String)>,
) -> Result<(Item, ContentType), Rejection> {
    let field = |name: &str, code: &str| -> Rejection {
        Rejection::invalid(vec![query::detail(&format!("/{name}"), code)])
    };
    let Value::Object(object) = value else {
        return Err(Rejection::invalid(vec![query::detail("", "type_mismatch")]));
    };
    if object.keys().any(|k| !keys.contains(&k.as_str())) {
        return Err(Rejection::invalid(vec![query::detail("", "unknown_field")]));
    }
    let text = |name: &str| -> Result<&str, Rejection> {
        match object.get(name) {
            None => Err(field(name, "required")),
            Some(Value::String(s)) => Ok(s.as_str()),
            Some(_) => Err(field(name, "type_mismatch")),
        }
    };
    let type_name = text("type")?;
    let (target, content_type): (Target, ContentType) =
        manage.authorize(parts, ENTRIES_PUBLISH, type_name)?;
    let locale = text("locale")?.to_owned();
    if !locale_allowed(manage.store.model(), &content_type, &locale) {
        return Err(Failure::permanent("locale_unknown").into());
    }
    let id = entry_id(text("id")?)?;
    let expect = if with_revision {
        match object.get("revision") {
            None => return Err(field("revision", "required")),
            Some(value) => match value.as_i64().filter(|n| *n > 0) {
                Some(n) => Expect::Draft(Some(n)),
                None => return Err(field("revision", "type_mismatch")),
            },
        }
    } else {
        Expect::Any
    };
    if !seen.insert((target.content_type.clone(), id, locale.clone())) {
        return Err(Rejection::invalid(vec![query::detail(
            "",
            "duplicate_item",
        )]));
    }
    Ok((
        Item {
            target,
            id,
            locale,
            expect,
            revision: None,
        },
        content_type,
    ))
}

fn batch_response(types: &[ContentType], views: &[crate::store::VariantView]) -> Response {
    let items: Vec<String> = types
        .iter()
        .zip(views)
        .map(|(content_type, view)| variant_json(content_type, view, None, None))
        .collect();
    json(
        StatusCode::OK,
        format!("{{\"items\":[{}]}}", items.join(",")),
    )
}

/// `POST /batch/publish`: `{items: [{type, id, locale, revision}]}`, all
/// or none; `revision` stands for `If-Match` and is the one published.
pub(crate) async fn batch_publish(State(manage): State<Manage>, request: Request) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        selection_empty(parts.uri.query())?;
        let bytes = read_body(&manage.store, body).await?;
        let (items, types): (Vec<Item>, Vec<ContentType>) =
            batch_items(&manage, &parts, &bytes, true)
                .await?
                .into_iter()
                .unzip();
        let views = manage.store.publish(items, &actor(&parts), true).await?;
        Ok(batch_response(&types, &views))
    };
    respond(run.await)
}

/// `POST /batch/unpublish`: `{items: [{type, id, locale}]}`, all or none.
pub(crate) async fn batch_unpublish(State(manage): State<Manage>, request: Request) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        selection_empty(parts.uri.query())?;
        let bytes = read_body(&manage.store, body).await?;
        let (items, types): (Vec<Item>, Vec<ContentType>) =
            batch_items(&manage, &parts, &bytes, false)
                .await?
                .into_iter()
                .unzip();
        let views = manage.store.unpublish(items, &actor(&parts), true).await?;
        Ok(batch_response(&types, &views))
    };
    respond(run.await)
}
