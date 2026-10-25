//! The asset routes (section 5), nested by the host under `/v1/content`:
//! management under `/manage/assets` (checked at `Application` scope, the
//! scope of C7's assets) and delivery at `/assets/{id}/{filename}`.
//! Management answers `private, no-store`.
use super::{
    ASSETS_DELETE, ASSETS_READ, ASSETS_WRITE, AssetDelete, AssetFilter, AssetView, Assets, Serve,
    app_target, focal_point, not_found,
    serve::{Disposition, PRIVATE_ASSET_CACHE},
    sniff,
    upload::Upload,
};
use crate::{
    http::{actor, error_response, json, page_json, principal},
    json::body_object,
    query::{self, decode_cursor, encode_cursor, pairs},
    store::Rejection,
};
use axum::{
    Router,
    extract::{Path, Request, State},
    http::{HeaderValue, Method, StatusCode, header, request::Parts},
    response::Response,
    routing::{get, post},
};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    RouteBodyLimit,
    ports::{Authorizer, ResourceScope},
};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

/// The upload route, relative to `/v1/content`.
pub(crate) const UPLOAD_PATH: &str = "/manage/assets";
/// Longest `title`, in Unicode scalar values.
pub const MAX_TITLE_CHARS: usize = 500;

#[derive(Clone)]
struct Routes {
    assets: Assets,
    policy: Arc<dyn Authorizer>,
}

/// The asset routes over `assets`, checked through `policy`.
pub(crate) fn router(assets: Assets, policy: Arc<dyn Authorizer>) -> Router {
    Router::new()
        .route(UPLOAD_PATH, get(list).post(upload))
        .route("/manage/assets/{id}", get(read).patch(patch).delete(delete))
        .route("/manage/assets/{id}/file", get(file))
        .route("/manage/assets/{id}/undelete", post(undelete))
        .route("/assets/{id}/{filename}", get(deliver))
        .with_state(Routes { assets, policy })
}

/// The upload route bounds its own body at `media.max_upload_bytes`.
pub(crate) fn body_limits(max_upload_bytes: u64) -> Vec<RouteBodyLimit> {
    vec![RouteBodyLimit {
        method: Method::POST,
        path: UPLOAD_PATH,
        limit: usize::try_from(max_upload_bytes).unwrap_or(usize::MAX),
    }]
}

/// The error shape, with `Retry-After` on every transient refusal.
fn respond(reply: Result<Response, Rejection>) -> Response {
    reply.unwrap_or_else(|rejection| {
        let transient =
            rejection.failure.classification == merezha_domain::Classification::Transient;
        let mut response = error_response(rejection);
        if transient && !response.headers().contains_key(header::RETRY_AFTER) {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    })
}

impl Routes {
    /// `permission` at `Application` scope for the request's principal.
    fn authorize(&self, parts: &Parts, permission: &str) -> Result<(), Rejection> {
        let principal = principal(parts);
        self.policy
            .check(
                principal.as_deref(),
                &Identity::new(permission)?,
                &ResourceScope::Application,
            )
            .map(|_| ())
            .map_err(|denied| Rejection::new(denied.failure()))
    }
}

/// An asset id from a path: a hyphenated UUID, anything else is absent.
fn asset_id(text: &str) -> Result<String, Rejection> {
    Some(text)
        .filter(|t| t.len() == 36)
        .and_then(|t| Uuid::try_parse(t).ok())
        .map(|id| id.hyphenated().to_string())
        .ok_or_else(not_found)
}

/// Query parameters, each at most once and all in `allowed`.
fn parameters(query: Option<&str>, allowed: &[&str]) -> Result<HashMap<String, String>, Rejection> {
    let mut out = HashMap::new();
    let mut problems = Vec::new();
    for (key, value) in pairs(query)? {
        if !allowed.contains(&key.as_str()) {
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

fn invalid_query(path: &str, code: &str) -> Rejection {
    Rejection {
        failure: Failure::permanent("invalid_query"),
        details: vec![query::detail(path, code)],
    }
}

/// A title: at most [`MAX_TITLE_CHARS`] characters, no control
/// characters.
fn valid_title(title: &str) -> bool {
    title.chars().count() <= MAX_TITLE_CHARS && !title.chars().any(char::is_control)
}

fn asset_response(status: StatusCode, asset: &AssetView, usage: Option<(i64, i64)>) -> Response {
    let mut response = json(status, asset.json(usage));
    if status == StatusCode::CREATED
        && let Ok(location) =
            HeaderValue::from_str(&format!("/v1/content/manage/assets/{}", asset.id))
    {
        response.headers_mut().insert(header::LOCATION, location);
    }
    response
}

async fn upload(State(routes): State<Routes>, request: Request) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        routes.authorize(&parts, ASSETS_WRITE)?;
        let settings = routes.assets.settings();
        // Admission: a slot of `media.max_concurrent_uploads`, held until
        // the upload ends.
        let _slot = routes
            .assets
            .media
            .uploads
            .clone()
            .try_acquire_owned()
            .map_err(|_| Rejection::new(Failure::transient("overloaded")))?;
        let query = parameters(parts.uri.query(), &["filename", "title"])?;
        let raw_name = query
            .get("filename")
            .filter(|name| !name.is_empty())
            .ok_or_else(|| invalid_query("/filename", "required"))?;
        let title = match query.get("title") {
            Some(title) if !valid_title(title) => {
                return Err(invalid_query("/title", "invalid_value"));
            }
            other => other.cloned(),
        };
        // A declared length over the limit fails before any byte is read.
        let declared = parts
            .headers
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok());
        if declared.is_some_and(|len| len > settings.max_upload_bytes) {
            return Err(Rejection::new(Failure::permanent("asset_too_large")));
        }
        let content_type = sniff::declared_type(
            parts
                .headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
        )
        .filter(|t| settings.allowed_types.contains(t))
        .ok_or_else(|| Rejection::new(Failure::permanent("media_type_rejected")))?;
        let filename = sniff::stored_filename(raw_name, &content_type);
        let asset = routes
            .assets
            .upload(
                Upload {
                    content_type,
                    filename,
                    title,
                },
                body,
                &actor(&parts),
            )
            .await?;
        Ok(asset_response(StatusCode::CREATED, &asset, None))
    };
    respond(run.await)
}

async fn list(State(routes): State<Routes>, request: Request) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        routes.authorize(&parts, ASSETS_READ)?;
        let query = parameters(
            parts.uri.query(),
            &["content_type", "filename_prefix", "limit", "cursor"],
        )?;
        let mut filter = AssetFilter::default();
        if let Some(value) = query.get("content_type") {
            match value.strip_suffix("/*") {
                Some(major) if sniff::valid_media_type(&format!("{major}/x")) => {
                    filter.type_prefix = Some(format!("{major}/"));
                }
                None if sniff::valid_media_type(value) => {
                    filter.content_type = Some(value.clone());
                }
                _ => return Err(invalid_query("/content_type", "invalid_value")),
            }
        }
        if let Some(prefix) = query.get("filename_prefix") {
            if prefix.is_empty()
                || prefix.len() > super::MAX_FILENAME_BYTES
                || !prefix
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            {
                return Err(invalid_query("/filename_prefix", "invalid_value"));
            }
            filter.filename_prefix = Some(prefix.clone());
        }
        let limits = routes.assets.state().settings().limits;
        let limit = match query.get("limit") {
            None => limits.default_page_size,
            Some(text) => text
                .parse::<u32>()
                .ok()
                .filter(|n| (1..=limits.max_page_size).contains(n))
                .ok_or_else(|| invalid_query("/limit", "invalid_value"))?,
        };
        let scope = app_target().scope;
        let fp = crate::http::fingerprint(&[
            "assets",
            &scope,
            filter.content_type.as_deref().unwrap_or(""),
            filter.type_prefix.as_deref().unwrap_or(""),
            filter.filename_prefix.as_deref().unwrap_or(""),
        ]);
        let after = match query.get("cursor") {
            None => None,
            Some(text) => {
                let cursor = decode_cursor(text, &fp, 1)?;
                let at = cursor.keys[0]
                    .clone()
                    .filter(|_| cursor.id.len() == 36 && Uuid::try_parse(&cursor.id).is_ok())
                    .ok_or_else(|| invalid_query("/cursor", "invalid_value"))?;
                Some((at, cursor.id))
            }
        };
        let (items, more) = routes.assets.list(&scope, &filter, after, limit).await?;
        let next = if more {
            items
                .last()
                .map(|last| encode_cursor(&fp, &[Some(last.cursor_at.clone())], &last.id))
        } else {
            None
        };
        let items = items.iter().map(|a| a.json(None)).collect();
        Ok(json(StatusCode::OK, page_json(items, next, limit)))
    };
    respond(run.await)
}

async fn read(State(routes): State<Routes>, Path(id): Path<String>, request: Request) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        routes.authorize(&parts, ASSETS_READ)?;
        parameters(parts.uri.query(), &[])?;
        let id = asset_id(&id)?;
        let (asset, usage) = routes.assets.get(&app_target().scope, &id).await?;
        Ok(asset_response(StatusCode::OK, &asset, Some(usage)))
    };
    respond(run.await)
}

async fn patch(State(routes): State<Routes>, Path(id): Path<String>, request: Request) -> Response {
    let run = async {
        let (parts, body) = request.into_parts();
        routes.authorize(&parts, ASSETS_WRITE)?;
        parameters(parts.uri.query(), &[])?;
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
        let id = asset_id(&id)?;
        let bytes = axum::body::to_bytes(
            body,
            routes.assets.state().settings().limits.max_request_bytes,
        )
        .await
        .map_err(|_| Rejection::new(Failure::permanent("body_limit")))?;
        let patch = body_object(&bytes, &["title", "focal_point"])?;
        let scope = app_target().scope;
        let mut problems = Vec::new();
        let title = match patch.get("title") {
            None => None,
            Some(Value::Null) => Some(None),
            Some(Value::String(title)) if valid_title(title) => Some(Some(title.clone())),
            Some(_) => {
                problems.push(query::detail("/title", "invalid_value"));
                None
            }
        };
        let focal = match patch.get("focal_point") {
            None => None,
            Some(Value::Null) => Some(None),
            Some(value) => match focal_point(value) {
                Some(point) => Some(Some(point)),
                None => {
                    problems.push(query::detail("/focal_point", "invalid_value"));
                    None
                }
            },
        };
        if !problems.is_empty() {
            return Err(Rejection::invalid(problems));
        }
        let asset = routes
            .assets
            .patch(&scope, &id, title, focal, &actor(&parts))
            .await?;
        Ok(asset_response(StatusCode::OK, &asset, None))
    };
    respond(run.await)
}

fn flag(query: &HashMap<String, String>, name: &str) -> Result<bool, Rejection> {
    match query.get(name).map(String::as_str) {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(_) => Err(invalid_query(&format!("/{name}"), "invalid_value")),
    }
}

async fn delete(
    State(routes): State<Routes>,
    Path(id): Path<String>,
    request: Request,
) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        routes.authorize(&parts, ASSETS_DELETE)?;
        let query = parameters(parts.uri.query(), &["force", "purge"])?;
        let options = AssetDelete {
            force: flag(&query, "force")?,
            purge: flag(&query, "purge")?,
        };
        let id = asset_id(&id)?;
        routes
            .assets
            .delete(&app_target().scope, &id, options, &actor(&parts))
            .await?;
        let mut response = Response::new(axum::body::Body::empty());
        *response.status_mut() = StatusCode::NO_CONTENT;
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(crate::http::NO_STORE),
        );
        Ok(response)
    };
    respond(run.await)
}

async fn undelete(
    State(routes): State<Routes>,
    Path(id): Path<String>,
    request: Request,
) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        routes.authorize(&parts, ASSETS_DELETE)?;
        parameters(parts.uri.query(), &[])?;
        let id = asset_id(&id)?;
        let asset = routes.assets.undelete(&app_target().scope, &id).await?;
        Ok(asset_response(StatusCode::OK, &asset, None))
    };
    respond(run.await)
}

/// `GET /manage/assets/{id}/file`: any state but purged, always proxied as
/// an attachment.
async fn file(State(routes): State<Routes>, Path(id): Path<String>, request: Request) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        routes.authorize(&parts, ASSETS_READ)?;
        parameters(parts.uri.query(), &[])?;
        let id = asset_id(&id)?;
        let (asset, _) = routes.assets.get(&app_target().scope, &id).await?;
        routes
            .assets
            .proxy(
                &asset,
                &parts.headers,
                Disposition::Attachment,
                crate::http::NO_STORE,
                false,
            )
            .await
    };
    respond(run.await)
}

/// `GET /assets/{id}/{filename}`: delivery, under the asset's referencing
/// types' access.
async fn deliver(
    State(routes): State<Routes>,
    Path((id, filename)): Path<(String, String)>,
    request: Request,
) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        parameters(parts.uri.query(), &[])?;
        let id = asset_id(&id)?;
        let principal = principal(&parts);
        let (asset, public) = routes
            .assets
            .deliverable(
                &app_target().scope,
                &id,
                &filename,
                principal.as_deref(),
                routes.policy.as_ref(),
            )
            .await?;
        if routes.assets.settings().serve == Serve::Redirect {
            return routes.assets.redirect(&asset).await;
        }
        let cache = if public {
            routes.assets.settings().asset_cache_control.clone()
        } else {
            PRIVATE_ASSET_CACHE.to_owned()
        };
        routes
            .assets
            .proxy(&asset, &parts.headers, Disposition::Delivery, &cache, true)
            .await
    };
    let mut response = respond(run.await);
    // Refusals of delivery are not cacheable either way.
    if response.status().is_client_error() || response.status().is_server_error() {
        response.headers_mut().insert(
            header::VARY,
            HeaderValue::from_static("Authorization, Cookie"),
        );
    }
    response
}
