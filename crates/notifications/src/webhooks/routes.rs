//! The webhook routes of NT §7.1: the event-type catalog and, for each
//! registry, the endpoint API. `tenants/{tenant_id}/webhooks…` checks
//! `notifications.webhooks.read` or `.manage` at `Tenant(tenant_id)`
//! scope; `webhooks…` is the application-level registry (NT6), checked at
//! `Application` scope. An endpoint of another registry answers 404, as a
//! missing one does.
//!
//! The secret is in the create and rotate answers only. Every change is
//! audited (never with the secret); PATCH takes the endpoint's `revision`
//! and answers 412 `revision_conflict` on a stale one.
use super::{
    DEFAULT_KEEP_PREVIOUS_HOURS, MAX_FILTERS, MAX_KEEP_PREVIOUS_HOURS, TEST_EVENT_TYPE,
    WebhookSender, code, delivery_id, fanout, signing::SigningSecret, sql, worker,
};
use crate::{
    http::{Ctx, new_id},
    ids,
    notifier::{Core, MODULE_EVENT_TYPES},
};
use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{Path, RawQuery, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    ErrorReply, FieldError,
    edge::{Authenticated, RequestId},
    ports::{OnDuplicate, ResourceScope},
};
use merezha_postgres::classify;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_postgres::Row;
use uuid::Uuid;

/// The permissions of NT §7.1, at `Tenant` scope or, for the
/// application-level registry, `Application`.
pub const READ_PERMISSION: &str = "notifications.webhooks.read";
pub const MANAGE_PERMISSION: &str = "notifications.webhooks.manage";

const DEADLINE: Duration = Duration::from_secs(10);
/// Deliveries per page of the deliveries route.
const PAGE: i64 = 50;
/// Most characters of an endpoint description.
const MAX_DESCRIPTION: usize = 200;
/// `POST …/test` is limited per endpoint and per registry within a
/// sliding window, counted in the database so every instance shares it.
pub const TEST_WINDOW: Duration = Duration::from_secs(60);
pub const TESTS_PER_ENDPOINT: i64 = 5;
pub const TESTS_PER_REGISTRY: i64 = 30;
/// An open delivery untouched this long has lost its job: redeliver
/// accepts it (a job still alive finds a newer generation and stops).
pub const STALE_OPEN: Duration = Duration::from_secs(3600);
/// Attempts shown per delivery in the deliveries route, the latest first
/// kept; `attempt_count` is the total.
pub const ATTEMPTS_SHOWN: i64 = 20;
/// Descriptions of the module event types and of the test event.
const BUILTIN_DESCRIPTIONS: [(&str, &str); 4] = [
    ("content.entry_published", "A content entry was published"),
    (
        "content.entry_unpublished",
        "A content entry was unpublished",
    ),
    ("content.entry_deleted", "A content entry was deleted"),
    (TEST_EVENT_TYPE, "A test event sent from the endpoint API"),
];

pub(crate) fn router(ctx: Ctx) -> Router {
    let registry = |prefix: &str, router: Router<Ctx>| {
        router
            .route(&format!("{prefix}/webhooks"), get(list).post(create))
            .route(
                &format!("{prefix}/webhooks/{{id}}"),
                get(show).patch(update).delete(remove),
            )
            .route(
                &format!("{prefix}/webhooks/{{id}}/rotate-secret"),
                post(rotate),
            )
            .route(&format!("{prefix}/webhooks/{{id}}/enable"), post(enable))
            .route(&format!("{prefix}/webhooks/{{id}}/disable"), post(disable))
            .route(&format!("{prefix}/webhooks/{{id}}/test"), post(test))
            .route(
                &format!("{prefix}/webhooks/{{id}}/deliveries"),
                get(deliveries),
            )
            .route(
                &format!("{prefix}/webhooks/{{id}}/deliveries/{{delivery_id}}/redeliver"),
                post(redeliver),
            )
    };
    let router = Router::new().route("/webhook-event-types", get(event_types));
    let router = registry("", router);
    registry("/tenants/{tenant_id}", router).with_state(ctx)
}

fn reply(failure: Failure) -> Response {
    ErrorReply::new(failure).into_response()
}
fn permanent(code: &str) -> Response {
    reply(Failure::permanent(code))
}
fn field(code: &str, path: &str, reason: &str) -> Response {
    refuse_field(code, path, reason).into_response()
}
fn refuse(code: &str) -> ErrorReply {
    ErrorReply::new(Failure::permanent(code))
}
fn refuse_field(code: &str, path: &str, reason: &str) -> ErrorReply {
    ErrorReply::new(Failure::permanent(code))
        .details(FieldError::new(path, reason).into_iter().collect())
}

/// One registry: a tenant's, or the application's (`None`).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Registry(Option<Identity>);
impl Registry {
    /// The `COALESCE(tenant_id, '')` value of its rows.
    fn key(&self) -> String {
        self.0
            .as_ref()
            .map(|t| t.as_str().to_owned())
            .unwrap_or_default()
    }
    fn tenant(&self) -> Option<String> {
        self.0.as_ref().map(|t| t.as_str().to_owned())
    }
    fn scope(&self) -> ResourceScope {
        match &self.0 {
            Some(tenant) => ResourceScope::Tenant(tenant.clone()),
            None => ResourceScope::Application,
        }
    }
}

/// What every endpoint route resolves first: the registry, the endpoint
/// id when the path has one, the caller's subject, the core and sender.
struct Call {
    registry: Registry,
    id: Option<Uuid>,
    subject: String,
    core: Arc<Core>,
    sender: Arc<WebhookSender>,
}

/// Resolve `params` and check `permission` on the registry. An invalid
/// tenant or endpoint id is 404, as a missing one is.
fn call(
    ctx: &Ctx,
    principal: Option<&Authenticated>,
    params: &BTreeMap<String, String>,
    permission: &str,
) -> Result<Call, ErrorReply> {
    let tenant = match params.get("tenant_id") {
        Some(text) => Some(Identity::new(text.as_str()).map_err(|_| refuse("not_found"))?),
        None => None,
    };
    let registry = Registry(tenant);
    let permission = Identity::new(permission).map_err(ErrorReply::new)?;
    let principal = principal.and_then(|a| a.0.as_deref());
    ctx.authorizer
        .check(principal, &permission, &registry.scope())
        .map_err(|denied| ErrorReply::new(denied.failure()))?;
    let subject = principal
        .map(|p| p.subject.as_str().to_owned())
        .ok_or_else(|| refuse("unauthenticated"))?;
    let id = match params.get("id") {
        Some(text) => Some(Uuid::try_parse(text).map_err(|_| refuse("not_found"))?),
        None => None,
    };
    if ctx.checked.settings.webhooks.is_none() {
        return Err(refuse(code::WEBHOOKS_DISABLED));
    }
    let core = ctx.core()?;
    let sender = core
        .webhooks
        .clone()
        .ok_or_else(|| refuse(code::WEBHOOKS_DISABLED))?;
    Ok(Call {
        registry,
        id,
        subject,
        core,
        sender,
    })
}

fn stamp(value: Option<jiff::Timestamp>) -> Value {
    value.map_or(Value::Null, |t| Value::String(t.to_string()))
}
/// The `Endpoint` shape from a row of `endpoint_columns!`.
fn endpoint(row: &Row) -> Value {
    json!({
        "id": row.get::<_, Uuid>(0).to_string(),
        "url": row.get::<_, String>(1),
        "description": row.get::<_, Option<String>>(2),
        "event_types": row.get::<_, Vec<String>>(3),
        "status": row.get::<_, String>(4),
        "disabled_reason": row.get::<_, Option<String>>(5),
        "failing_since": stamp(row.get(6)),
        "last_success_at": stamp(row.get(7)),
        "revision": row.get::<_, i64>(8),
        "created_at": stamp(row.get(9)),
    })
}

/// The event types endpoints may filter on: the application's, the
/// modules', and `webhook.test`.
fn catalog(ctx: &Ctx) -> Vec<(String, String)> {
    let mut types: Vec<(String, String)> = ctx
        .checked
        .settings
        .webhooks
        .as_ref()
        .map(|w| {
            w.event_types
                .iter()
                .map(|(name, description)| (name.clone(), description.clone()))
                .collect()
        })
        .unwrap_or_default();
    for name in MODULE_EVENT_TYPES {
        let description = BUILTIN_DESCRIPTIONS
            .iter()
            .find(|(n, _)| *n == name)
            .map_or("", |(_, d)| d);
        types.push((name.to_owned(), description.to_owned()));
    }
    types.sort();
    types.dedup_by(|a, b| a.0 == b.0);
    types
}

/// `GET webhook-event-types` (any authenticated principal).
async fn event_types(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
) -> Response {
    if principal.as_deref().and_then(|a| a.0.as_ref()).is_none() {
        return permanent("unauthenticated");
    }
    if ctx.checked.settings.webhooks.is_none() {
        return permanent(code::WEBHOOKS_DISABLED);
    }
    let items: Vec<Value> = catalog(&ctx)
        .into_iter()
        .map(|(name, description)| json!({ "type": name, "description": description }))
        .collect();
    Json(Value::Array(items)).into_response()
}

/// A prefix filter `prefix.*`: one to four segments before the `*`.
fn valid_prefix(prefix: &str) -> bool {
    let segments: Vec<&str> = prefix.split('.').collect();
    (1..=4).contains(&segments.len())
        && segments.iter().enumerate().all(|(i, s)| {
            !s.is_empty()
                && s.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
                && (i > 0 || s.as_bytes()[0].is_ascii_lowercase())
        })
}

/// The filters, checked against the catalog and deduplicated.
fn filters(ctx: &Ctx, given: Vec<String>) -> Result<Vec<String>, ErrorReply> {
    if given.len() > MAX_FILTERS {
        return Err(refuse_field(
            "validation_failed",
            "/event_types",
            "too_many_items",
        ));
    }
    let known = catalog(ctx);
    let mut out: Vec<String> = Vec::new();
    for (index, filter) in given.into_iter().enumerate() {
        let valid = match filter.strip_suffix(".*") {
            Some(prefix) => valid_prefix(prefix),
            None => known.iter().any(|(name, _)| *name == filter),
        };
        if !valid {
            return Err(refuse_field(
                code::UNKNOWN_EVENT_TYPE,
                &format!("/event_types/{index}"),
                code::UNKNOWN_EVENT_TYPE,
            ));
        }
        if !out.contains(&filter) {
            out.push(filter);
        }
    }
    Ok(out)
}

fn description(text: Option<String>) -> Result<Option<String>, ErrorReply> {
    match text {
        Some(text)
            if text.chars().count() > MAX_DESCRIPTION || text.chars().any(char::is_control) =>
        {
            Err(refuse_field(
                "validation_failed",
                "/description",
                "invalid_value",
            ))
        }
        text => Ok(text.filter(|t| !t.is_empty())),
    }
}

/// The URL check of registration: 422 `webhook_url_rejected` with the
/// reason in `details`.
async fn checked_url(sender: &WebhookSender, url: &str) -> Result<String, ErrorReply> {
    sender
        .check_url(url)
        .await
        .map(|url| url.to_string())
        .map_err(|reason| refuse_field(code::URL_REJECTED, "/url", reason))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    url: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    event_types: Vec<String>,
}

/// `POST …webhooks` (manage): 201 `Endpoint + { secret }`.
async fn create(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(params): Path<BTreeMap<String, String>>,
    body: Bytes,
) -> Response {
    let call = match call(&ctx, principal.as_deref(), &params, MANAGE_PERMISSION) {
        Ok(call) => call,
        Err(refusal) => return refusal.into_response(),
    };
    let Ok(body) = serde_json::from_slice::<CreateBody>(&body) else {
        return permanent("invalid_input");
    };
    let description = match description(body.description) {
        Ok(d) => d,
        Err(refusal) => return refusal.into_response(),
    };
    let filters = match filters(&ctx, body.event_types) {
        Ok(f) => f,
        Err(refusal) => return refusal.into_response(),
    };
    let url = match checked_url(&call.sender, &body.url).await {
        Ok(url) => url,
        Err(refusal) => return refusal.into_response(),
    };
    let core = call.core.clone();
    let id = match new_id(&core) {
        Ok(id) => id,
        Err(failure) => return reply(failure),
    };
    let secret = match SigningSecret::generate(core.random.as_ref()) {
        Ok(secret) => secret,
        Err(failure) => return reply(failure),
    };
    let sealed = match call.sender.secrets.seal(id, &secret, core.random.as_ref()) {
        Ok(sealed) => sealed,
        Err(failure) => return reply(failure),
    };
    let limit = i64::from(call.sender.settings.max_endpoints_per_tenant);
    let (key, tenant, subject) = (call.registry.key(), call.registry.tenant(), call.subject);
    let now = core.now();
    let db = core.database.clone();
    let result = db
        .clone()
        .transaction(Instant::now() + DEADLINE, move |tx| {
            Box::pin(async move {
                db.query_one(tx, &sql::REGISTRY_LOCK, &[&key])
                    .await
                    .map_err(|e| classify(&e))?;
                let count: i64 = db
                    .query_one(tx, &sql::REGISTRY_COUNT, &[&key])
                    .await
                    .map_err(|e| classify(&e))?
                    .get(0);
                if count >= limit {
                    return Err(Failure::conflict(code::ENDPOINT_LIMIT));
                }
                let row = db
                    .query_one(
                        tx,
                        &sql::ENDPOINT_INSERT,
                        &[
                            &id,
                            &tenant,
                            &url,
                            &description,
                            &filters,
                            &sealed.ciphertext,
                            &sealed.nonce,
                            &subject,
                            &now,
                            &sealed.key_id,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                worker::audit(&db, tx, id, &tenant, "create", None, &subject, 1, now).await?;
                Ok(endpoint(&row))
            })
        })
        .await;
    match result {
        Ok(mut endpoint) => {
            endpoint["secret"] = Value::String(secret.reveal());
            (StatusCode::CREATED, Json(endpoint)).into_response()
        }
        Err(failure) => reply(failure),
    }
}

/// `GET …webhooks` (read): every endpoint of the registry, oldest first.
async fn list(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(params): Path<BTreeMap<String, String>>,
) -> Response {
    let call = match call(&ctx, principal.as_deref(), &params, READ_PERMISSION) {
        Ok(call) => call,
        Err(refusal) => return refusal.into_response(),
    };
    let limit = i64::from(call.sender.settings.max_endpoints_per_tenant).max(1);
    let key = call.registry.key();
    let result = async {
        let client = call
            .core
            .database
            .acquire(Instant::now() + DEADLINE)
            .await?;
        call.core
            .database
            .query(&client, &sql::ENDPOINT_LIST, &[&key, &limit])
            .await
            .map_err(|e| classify(&e))
    }
    .await;
    match result {
        Ok(rows) => Json(Value::Array(rows.iter().map(endpoint).collect())).into_response(),
        Err(failure) => reply(failure),
    }
}

/// The endpoint `id` of the registry, outside a transaction.
async fn fetch(call: &Call, id: Uuid) -> Result<Option<Row>, Failure> {
    let client = call
        .core
        .database
        .acquire(Instant::now() + DEADLINE)
        .await?;
    call.core
        .database
        .query_opt(&client, &sql::ENDPOINT_GET, &[&id, &call.registry.key()])
        .await
        .map_err(|e| classify(&e))
}

/// `GET …webhooks/{id}` (read).
async fn show(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(params): Path<BTreeMap<String, String>>,
) -> Response {
    let call = match call(&ctx, principal.as_deref(), &params, READ_PERMISSION) {
        Ok(call) => call,
        Err(refusal) => return refusal.into_response(),
    };
    let Some(id) = call.id else {
        return permanent("not_found");
    };
    match fetch(&call, id).await {
        Ok(Some(row)) => Json(endpoint(&row)).into_response(),
        Ok(None) => permanent("not_found"),
        Err(failure) => reply(failure),
    }
}

/// `null` and absent apart: `Some(None)` clears the field.
fn nullable<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateBody {
    #[serde(default)]
    url: Option<String>,
    #[serde(default, deserialize_with = "nullable")]
    description: Option<Option<String>>,
    #[serde(default)]
    event_types: Option<Vec<String>>,
    revision: i64,
}

/// Lock endpoint `id` of the registry: its revision and status.
async fn lock(
    db: &merezha_postgres::Database,
    tx: &mut merezha_postgres::Tx<'_>,
    id: Uuid,
    key: &str,
) -> Result<(i64, String, Option<String>), Failure> {
    let row = db
        .query_opt(tx, &sql::ENDPOINT_LOCK, &[&id, &key])
        .await
        .map_err(|e| classify(&e))?
        .ok_or_else(|| Failure::permanent("not_found"))?;
    Ok((row.get(0), row.get(1), row.get(2)))
}

/// `PATCH …webhooks/{id}` (manage): `{ url?, description?, event_types?,
/// revision }`.
async fn update(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(params): Path<BTreeMap<String, String>>,
    body: Bytes,
) -> Response {
    let call = match call(&ctx, principal.as_deref(), &params, MANAGE_PERMISSION) {
        Ok(call) => call,
        Err(refusal) => return refusal.into_response(),
    };
    let Some(id) = call.id else {
        return permanent("not_found");
    };
    let Ok(body) = serde_json::from_slice::<UpdateBody>(&body) else {
        return permanent("invalid_input");
    };
    let new_description = match body.description {
        Some(text) => match description(text) {
            Ok(d) => Some(d),
            Err(refusal) => return refusal.into_response(),
        },
        None => None,
    };
    let new_filters = match body.event_types {
        Some(given) => match filters(&ctx, given) {
            Ok(f) => Some(f),
            Err(refusal) => return refusal.into_response(),
        },
        None => None,
    };
    let new_url = match &body.url {
        Some(url) => match checked_url(&call.sender, url).await {
            Ok(url) => Some(url),
            Err(refusal) => return refusal.into_response(),
        },
        None => None,
    };
    let (key, subject, revision) = (call.registry.key(), call.subject.clone(), body.revision);
    let core = call.core.clone();
    let now = core.now();
    let db = core.database.clone();
    let result = db
        .clone()
        .transaction(Instant::now() + DEADLINE, move |tx| {
            Box::pin(async move {
                let (current, _, tenant) = lock(&db, tx, id, &key).await?;
                if current != revision {
                    return Err(Failure::conflict("revision_conflict"));
                }
                let row = db
                    .query_one(tx, &sql::ENDPOINT_GET, &[&id, &key])
                    .await
                    .map_err(|e| classify(&e))?;
                let url = new_url.unwrap_or_else(|| row.get(1));
                let description = new_description.unwrap_or_else(|| row.get(2));
                let filters = new_filters.unwrap_or_else(|| row.get(3));
                let row = db
                    .query_one(
                        tx,
                        &sql::ENDPOINT_UPDATE,
                        &[&id, &url, &description, &filters, &now],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                let revision: i64 = row.get(8);
                worker::audit(
                    &db, tx, id, &tenant, "update", None, &subject, revision, now,
                )
                .await?;
                Ok(endpoint(&row))
            })
        })
        .await;
    match result {
        Ok(endpoint) => Json(endpoint).into_response(),
        Err(failure) => reply(failure),
    }
}

/// `DELETE …webhooks/{id}` (manage): its deliveries and attempts go with
/// it; jobs still queued find nothing and finish.
async fn remove(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(params): Path<BTreeMap<String, String>>,
) -> Response {
    let call = match call(&ctx, principal.as_deref(), &params, MANAGE_PERMISSION) {
        Ok(call) => call,
        Err(refusal) => return refusal.into_response(),
    };
    let Some(id) = call.id else {
        return permanent("not_found");
    };
    let (key, subject) = (call.registry.key(), call.subject.clone());
    let now = call.core.now();
    let db = call.core.database.clone();
    let result = db
        .clone()
        .transaction(Instant::now() + DEADLINE, move |tx| {
            Box::pin(async move {
                let row = db
                    .query_opt(tx, &sql::ENDPOINT_DELETE, &[&id, &key])
                    .await
                    .map_err(|e| classify(&e))?
                    .ok_or_else(|| Failure::permanent("not_found"))?;
                let (revision, tenant): (i64, Option<String>) = (row.get(0), row.get(1));
                worker::audit(
                    &db,
                    tx,
                    id,
                    &tenant,
                    "delete",
                    None,
                    &subject,
                    revision + 1,
                    now,
                )
                .await
            })
        })
        .await;
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(failure) => reply(failure),
    }
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RotateBody {
    #[serde(default)]
    keep_previous_hours: Option<u32>,
}

/// `POST …webhooks/{id}/rotate-secret` (manage): a new secret; the
/// current one keeps signing beside it for `keep_previous_hours` (0 to
/// 72, default 24; 0 keeps none).
async fn rotate(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(params): Path<BTreeMap<String, String>>,
    body: Bytes,
) -> Response {
    let call = match call(&ctx, principal.as_deref(), &params, MANAGE_PERMISSION) {
        Ok(call) => call,
        Err(refusal) => return refusal.into_response(),
    };
    let Some(id) = call.id else {
        return permanent("not_found");
    };
    let body = if body.is_empty() {
        RotateBody::default()
    } else {
        match serde_json::from_slice::<RotateBody>(&body) {
            Ok(body) => body,
            Err(_) => return permanent("invalid_input"),
        }
    };
    let hours = body
        .keep_previous_hours
        .unwrap_or(DEFAULT_KEEP_PREVIOUS_HOURS);
    if hours > MAX_KEEP_PREVIOUS_HOURS {
        return field("validation_failed", "/keep_previous_hours", "out_of_range");
    }
    let core = call.core.clone();
    let secret = match SigningSecret::generate(core.random.as_ref()) {
        Ok(secret) => secret,
        Err(failure) => return reply(failure),
    };
    let sealed = match call.sender.secrets.seal(id, &secret, core.random.as_ref()) {
        Ok(sealed) => sealed,
        Err(failure) => return reply(failure),
    };
    let now = core.now();
    let keep = hours > 0;
    let until = now
        .checked_add(jiff::SignedDuration::from_hours(i64::from(hours)))
        .ok()
        .filter(|_| keep);
    let (key, subject) = (call.registry.key(), call.subject.clone());
    let db = core.database.clone();
    let result = db
        .clone()
        .transaction(Instant::now() + DEADLINE, move |tx| {
            Box::pin(async move {
                let (_, _, tenant) = lock(&db, tx, id, &key).await?;
                let revision: i64 = db
                    .query_one(
                        tx,
                        &sql::ENDPOINT_ROTATE,
                        &[
                            &id,
                            &sealed.ciphertext,
                            &sealed.nonce,
                            &keep,
                            &until,
                            &now,
                            &sealed.key_id,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?
                    .get(0);
                worker::audit(
                    &db, tx, id, &tenant, "rotate", None, &subject, revision, now,
                )
                .await
            })
        })
        .await;
    match result {
        Ok(()) => Json(json!({
            "secret": secret.reveal(),
            "previous_expires_at": stamp(until),
        }))
        .into_response(),
        Err(failure) => reply(failure),
    }
}

/// `POST …webhooks/{id}/enable` (manage): active again, `failing_since`
/// cleared. Cancelled deliveries are sent again only through redeliver.
async fn enable(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(params): Path<BTreeMap<String, String>>,
) -> Response {
    let call = match call(&ctx, principal.as_deref(), &params, MANAGE_PERMISSION) {
        Ok(call) => call,
        Err(refusal) => return refusal.into_response(),
    };
    let Some(id) = call.id else {
        return permanent("not_found");
    };
    let (key, subject) = (call.registry.key(), call.subject.clone());
    let now = call.core.now();
    let db = call.core.database.clone();
    let result = db
        .clone()
        .transaction(Instant::now() + DEADLINE, move |tx| {
            Box::pin(async move {
                let (_, _, tenant) = lock(&db, tx, id, &key).await?;
                if let Some(row) = db
                    .query_opt(tx, &sql::ENDPOINT_ENABLE, &[&id, &now])
                    .await
                    .map_err(|e| classify(&e))?
                {
                    let revision: i64 = row.get(0);
                    worker::audit(
                        &db, tx, id, &tenant, "enable", None, &subject, revision, now,
                    )
                    .await?;
                }
                db.query_one(tx, &sql::ENDPOINT_GET, &[&id, &key])
                    .await
                    .map(|row| endpoint(&row))
                    .map_err(|e| classify(&e))
            })
        })
        .await;
    match result {
        Ok(endpoint) => Json(endpoint).into_response(),
        Err(failure) => reply(failure),
    }
}

/// `POST …webhooks/{id}/disable` (manage): `manual`, its queued
/// deliveries cancelled.
async fn disable(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(params): Path<BTreeMap<String, String>>,
) -> Response {
    let call = match call(&ctx, principal.as_deref(), &params, MANAGE_PERMISSION) {
        Ok(call) => call,
        Err(refusal) => return refusal.into_response(),
    };
    let Some(id) = call.id else {
        return permanent("not_found");
    };
    let (key, subject) = (call.registry.key(), call.subject.clone());
    let now = call.core.now();
    let db = call.core.database.clone();
    let result = db
        .clone()
        .transaction(Instant::now() + DEADLINE, move |tx| {
            Box::pin(async move {
                lock(&db, tx, id, &key).await?;
                let disabled = worker::disable(&db, tx, id, "manual", now, None, &subject).await?;
                let row = db
                    .query_one(tx, &sql::ENDPOINT_GET, &[&id, &key])
                    .await
                    .map_err(|e| classify(&e))?;
                Ok((endpoint(&row), disabled.map(|(_, cancelled)| cancelled)))
            })
        })
        .await;
    match result {
        Ok((endpoint, disabled)) => {
            if let Some(cancelled) = disabled {
                worker::disabled(&call.sender, id, "manual", cancelled);
            }
            Json(endpoint).into_response()
        }
        Err(failure) => reply(failure),
    }
}

/// The correlation id of a request: the edge's request id, or a fresh id.
fn correlation(core: &Core, request_id: Option<Extension<RequestId>>) -> Result<Identity, Failure> {
    match request_id.and_then(|Extension(RequestId(id))| Identity::new(id).ok()) {
        Some(id) => Ok(id),
        None => Identity::new(new_id(core)?.simple().to_string()),
    }
}

/// `POST …webhooks/{id}/test` (manage): a `webhook.test` event for this
/// endpoint alone, whatever its filter: 202 `{ delivery_id }`. At most
/// [`TESTS_PER_ENDPOINT`] per endpoint and [`TESTS_PER_REGISTRY`] per
/// registry within [`TEST_WINDOW`], else 429 `rate_limited` with
/// `Retry-After`.
async fn test(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    request_id: Option<Extension<RequestId>>,
    Path(params): Path<BTreeMap<String, String>>,
) -> Response {
    let call = match call(&ctx, principal.as_deref(), &params, MANAGE_PERMISSION) {
        Ok(call) => call,
        Err(refusal) => return refusal.into_response(),
    };
    let Some(id) = call.id else {
        return permanent("not_found");
    };
    let core = call.core.clone();
    let (source_event_id, correlation) = match new_id(&core)
        .map(|n| format!("test:{id}:{}", n.simple()))
        .and_then(|e| correlation(&core, request_id).map(|c| (e, c)))
    {
        Ok(found) => found,
        Err(failure) => return reply(failure),
    };
    let now = core.now();
    let event_id = ids::webhook_event_id(crate::NAME, &source_event_id);
    let delivery = delivery_id(event_id, id);
    let key = call.registry.key();
    let db = core.database.clone();
    let outbox = core.outbox.clone();
    let result = db
        .clone()
        .transaction(Instant::now() + DEADLINE, move |tx| {
            Box::pin(async move {
                let (_, status, tenant) = lock(&db, tx, id, &key).await?;
                if status != "active" {
                    return Err(Failure::conflict(code::ENDPOINT_DISABLED));
                }
                // The registry lock serializes the count across instances.
                db.query_one(tx, &sql::REGISTRY_LOCK, &[&key])
                    .await
                    .map_err(|e| classify(&e))?;
                let window = now
                    .checked_sub(jiff::SignedDuration::try_from(TEST_WINDOW).unwrap_or_default())
                    .unwrap_or(now);
                let counts = db
                    .query_one(tx, &sql::TEST_RATE, &[&id, &key, &window])
                    .await
                    .map_err(|e| classify(&e))?;
                let (endpoint_tests, registry_tests): (i64, i64) = (counts.get(0), counts.get(1));
                if endpoint_tests >= TESTS_PER_ENDPOINT || registry_tests >= TESTS_PER_REGISTRY {
                    return Err(Failure::transient("rate_limited"));
                }
                let event = fanout::Event {
                    id: event_id,
                    source: crate::NAME.to_owned(),
                    source_event_id,
                    tenant: tenant.clone(),
                    event_type: TEST_EVENT_TYPE.to_owned(),
                    occurred_at: now,
                    data: json!({ "endpoint_id": id.to_string() }),
                };
                db.query_opt(
                    tx,
                    &sql::EVENT_INSERT,
                    &[
                        &event.id,
                        &event.source,
                        &event.source_event_id,
                        &event.tenant,
                        &event.event_type,
                        &event.occurred_at,
                        &event.envelope(),
                        &now,
                    ],
                )
                .await
                .map_err(|e| classify(&e))?;
                fanout::insert_deliveries(
                    &db,
                    &outbox,
                    tx,
                    event_id,
                    &[(delivery, id)],
                    &correlation,
                    tenant.as_deref(),
                    now,
                )
                .await
            })
        })
        .await;
    match result {
        Ok(_) => (
            StatusCode::ACCEPTED,
            Json(json!({ "delivery_id": delivery.to_string() })),
        )
            .into_response(),
        Err(failure) if failure.code == "rate_limited" => ErrorReply::new(failure)
            .retry_after(TEST_WINDOW)
            .into_response(),
        Err(failure) => reply(failure),
    }
}

/// A page cursor: the last delivery's creation time and id.
fn encode_cursor(at: jiff::Timestamp, id: Uuid) -> String {
    URL_SAFE_NO_PAD.encode(format!("{}.{}", at.as_microsecond(), id.simple()))
}
fn decode_cursor(text: &str) -> Option<(jiff::Timestamp, Uuid)> {
    let bytes = URL_SAFE_NO_PAD.decode(text).ok()?;
    let text = std::str::from_utf8(&bytes).ok()?;
    let (micros, id) = text.split_once('.')?;
    Some((
        jiff::Timestamp::from_microsecond(micros.parse().ok()?).ok()?,
        Uuid::try_parse(id).ok()?,
    ))
}

/// `GET …webhooks/{id}/deliveries?status=&cursor=` (read): newest first,
/// 50 per page, each with its latest [`ATTEMPTS_SHOWN`] attempts.
async fn deliveries(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(params): Path<BTreeMap<String, String>>,
    RawQuery(query): RawQuery,
) -> Response {
    let call = match call(&ctx, principal.as_deref(), &params, READ_PERMISSION) {
        Ok(call) => call,
        Err(refusal) => return refusal.into_response(),
    };
    let Some(id) = call.id else {
        return permanent("not_found");
    };
    let mut status: Option<String> = None;
    let mut cursor = None;
    for (name, value) in url::form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
        match name.as_ref() {
            "status" if !value.is_empty() => {
                if !matches!(
                    value.as_ref(),
                    "queued" | "sending" | "retrying" | "succeeded" | "failed" | "cancelled"
                ) {
                    return field("invalid_input", "/status", "invalid_value");
                }
                status = Some(value.into_owned());
            }
            "cursor" if !value.is_empty() => match decode_cursor(&value) {
                Some(found) => cursor = Some(found),
                None => return permanent("invalid_cursor"),
            },
            _ => {}
        }
    }
    let result = async {
        if fetch(&call, id).await?.is_none() {
            return Ok(None);
        }
        let db = &call.core.database;
        let client = db.acquire(Instant::now() + DEADLINE).await?;
        let (before, before_id) = (cursor.map(|c| c.0), cursor.map(|c| c.1));
        let rows = db
            .query(
                &client,
                &sql::DELIVERY_PAGE,
                &[&id, &status, &before, &before_id, &(PAGE + 1)],
            )
            .await
            .map_err(|e| classify(&e))?;
        let more = rows.len() as i64 > PAGE;
        let rows = &rows[..rows.len().min(PAGE as usize)];
        let ids: Vec<Uuid> = rows.iter().map(|r| r.get(0)).collect();
        let attempts = db
            .query(&client, &sql::ATTEMPTS_FOR, &[&ids, &ATTEMPTS_SHOWN])
            .await
            .map_err(|e| classify(&e))?;
        let mut by_delivery: BTreeMap<Uuid, Vec<Value>> = BTreeMap::new();
        for a in &attempts {
            by_delivery.entry(a.get(0)).or_default().push(json!({
                "attempt": a.get::<_, i32>(1),
                "outcome": a.get::<_, String>(2),
                "http_status": a.get::<_, Option<i16>>(3),
                "duration_ms": a.get::<_, Option<i32>>(4),
                "started_at": stamp(a.get(5)),
            }));
        }
        let items: Vec<Value> = rows
            .iter()
            .map(|r| {
                let delivery: Uuid = r.get(0);
                json!({
                    "id": delivery.to_string(),
                    "event_type": r.get::<_, String>(1),
                    "status": r.get::<_, String>(2),
                    "attempt_count": r.get::<_, i32>(3),
                    "last_http_status": r.get::<_, Option<i16>>(4),
                    "created_at": stamp(r.get(5)),
                    "attempts": by_delivery.remove(&delivery).unwrap_or_default(),
                })
            })
            .collect();
        let next = more
            .then(|| rows.last())
            .flatten()
            .map(|r| encode_cursor(r.get(5), r.get(0)));
        Ok::<_, Failure>(Some(json!({ "items": items, "next_cursor": next })))
    }
    .await;
    match result {
        Ok(Some(body)) => Json(body).into_response(),
        Ok(None) => permanent("not_found"),
        Err(failure) => reply(failure),
    }
}

/// `POST …webhooks/{id}/deliveries/{delivery_id}/redeliver` (manage): a
/// settled delivery, or an open one untouched for [`STALE_OPEN`], as a new
/// generation, signed again over the same body with the same
/// `webhook-id`: 202 `{ generation }`.
async fn redeliver(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    request_id: Option<Extension<RequestId>>,
    Path(params): Path<BTreeMap<String, String>>,
) -> Response {
    let call = match call(&ctx, principal.as_deref(), &params, MANAGE_PERMISSION) {
        Ok(call) => call,
        Err(refusal) => return refusal.into_response(),
    };
    let (Some(id), Some(Ok(delivery))) = (
        call.id,
        params.get("delivery_id").map(|d| Uuid::try_parse(d)),
    ) else {
        return permanent("not_found");
    };
    let core = call.core.clone();
    let correlation = match correlation(&core, request_id) {
        Ok(c) => c,
        Err(failure) => return reply(failure),
    };
    let key = call.registry.key();
    let now = core.now();
    let stale_before = now
        .checked_sub(jiff::SignedDuration::try_from(STALE_OPEN).unwrap_or_default())
        .unwrap_or(now);
    let db = core.database.clone();
    let outbox = core.outbox.clone();
    let result = db
        .clone()
        .transaction(Instant::now() + DEADLINE, move |tx| {
            Box::pin(async move {
                let row = db
                    .query_opt(
                        tx,
                        &sql::REDELIVER_LOCK,
                        &[&delivery, &id, &key, &stale_before],
                    )
                    .await
                    .map_err(|e| classify(&e))?
                    .ok_or_else(|| Failure::permanent("not_found"))?;
                let (status, purged, endpoint_status, tenant, stale): (
                    String,
                    bool,
                    String,
                    Option<String>,
                    bool,
                ) = (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
                if purged {
                    return Err(Failure::permanent(code::EVENT_BODY_EXPIRED));
                }
                if endpoint_status != "active" {
                    return Err(Failure::conflict(code::ENDPOINT_DISABLED));
                }
                // An open delivery is redelivered only once it has been
                // untouched for `STALE_OPEN`: its job is dead (a job that
                // still runs finds the newer generation and stops).
                if !matches!(status.as_str(), "succeeded" | "failed" | "cancelled") && !stale {
                    return Err(Failure::conflict("not_redeliverable"));
                }
                let generation: i32 = db
                    .query_one(tx, &sql::REDELIVER_SET, &[&delivery, &now])
                    .await
                    .map_err(|e| classify(&e))?
                    .get(0);
                let job = super::DeliverJob {
                    webhook_delivery_id: delivery,
                    endpoint_id: id,
                    generation,
                    busy: 0,
                    busy_since: None,
                };
                let record = fanout::job_record(&job, &correlation, tenant.as_deref())?;
                outbox.append(tx, &[record], OnDuplicate::Ignore).await?;
                Ok(generation)
            })
        })
        .await;
    match result {
        Ok(generation) => (
            StatusCode::ACCEPTED,
            Json(json!({ "generation": generation })),
        )
            .into_response(),
        Err(failure) => reply(failure),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_and_cursors() {
        assert!(valid_prefix("content"));
        assert!(valid_prefix("content.entry"));
        assert!(!valid_prefix(""));
        assert!(!valid_prefix("Content"));
        assert!(!valid_prefix("a.b.c.d.e"));
        assert!(!valid_prefix("content..x"));
        let at = jiff::Timestamp::from_microsecond(1_700_000_000_123_456).unwrap();
        let id = Uuid::from_u128(42);
        assert_eq!(decode_cursor(&encode_cursor(at, id)), Some((at, id)));
        assert_eq!(decode_cursor("!!"), None);
    }
}
