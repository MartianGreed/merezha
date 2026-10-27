//! The inbox REST routes (NT §5.4, §7.1): list, unread count, read,
//! unread, archive and read-all for the calling user.
//!
//! Every change takes its `change_seq` from the user's `inbox_counters`
//! row, locked until commit, and calls `pg_notify` in the same transaction,
//! so a user's changes commit in `change_seq` order (invariant 11). Another
//! user's item answers 404, as a missing one does.
use crate::{http::Ctx, notifier::Core, sql};
use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Path, RawQuery, State},
    response::{IntoResponse, Response},
};
use merezha_domain::Failure;
use merezha_platform::{ErrorReply, edge::Authenticated};
use merezha_postgres::classify;
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Default and largest list page.
const DEFAULT_LIMIT: i64 = 30;
const MAX_LIMIT: i64 = 100;
/// Most items one read-all marks.
const MAX_READ_ALL: i64 = 10_000;
const DEADLINE: Duration = Duration::from_secs(10);

fn reply(code: &str) -> Response {
    ErrorReply::new(Failure::permanent(code)).into_response()
}
fn failure(failure: Failure) -> Response {
    ErrorReply::new(failure).into_response()
}

/// One row as the API shows it (`InboxItem`).
pub(crate) fn item(row: &tokio_postgres::Row) -> Value {
    let id: Uuid = row.get(0);
    let payload: Option<String> = row.get(5);
    let stamp = |i: usize| {
        row.get::<_, Option<jiff::Timestamp>>(i)
            .map(|t| t.to_string())
    };
    json!({
        "id": id.to_string(),
        "topic": row.get::<_, String>(1),
        "title": row.get::<_, String>(2),
        "body": row.get::<_, String>(3),
        "link": row.get::<_, Option<String>>(4),
        "payload": payload.and_then(|p| serde_json::from_str::<Value>(&p).ok()),
        "tenant_id": row.get::<_, Option<String>>(6),
        "seq": row.get::<_, i64>(7),
        "change_seq": row.get::<_, i64>(8),
        "created_at": row.get::<_, jiff::Timestamp>(9).to_string(),
        "read_at": stamp(10),
        "archived_at": stamp(11),
    })
}

/// The query pairs of a request, at most 16.
pub(crate) fn pairs(query: Option<&str>) -> Vec<(String, String)> {
    query
        .map(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .take(16)
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect()
        })
        .unwrap_or_default()
}

/// The tenant in scope (NT §5.4): the principal's active tenant. Items
/// without a tenant are always in scope; a principal without an active
/// tenant sees only those. A `tenant` parameter naming another tenant than
/// the active one is refused (403 `unauthorized`).
fn scope(asked: Option<String>, active: Option<String>) -> Result<Option<String>, ErrorReply> {
    match asked {
        Some(asked) if Some(&asked) != active.as_ref() => {
            Err(ErrorReply::new(Failure::permanent("unauthorized")))
        }
        _ => Ok(active),
    }
}

/// 404 `not_found` when the tenant in scope was deleted (issue #63): from
/// the mark on, its inbox is not served, as content stops serving a
/// deleted organization.
async fn refuse_deleted(core: &Core, scope: Option<&str>) -> Option<Response> {
    let tenant = scope?;
    match crate::tenants::is_deleted(core, tenant, Instant::now() + DEADLINE).await {
        Ok(false) => None,
        Ok(true) => Some(reply("not_found")),
        Err(error) => Some(failure(error)),
    }
}

pub(crate) async fn list(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    RawQuery(query): RawQuery,
) -> Response {
    let (user, active) = match ctx.user(principal.as_deref()) {
        Ok(found) => found,
        Err(reply) => return reply.into_response(),
    };
    let mut cursor: Option<i64> = None;
    let mut limit = DEFAULT_LIMIT;
    let mut unread = false;
    let mut topic: Option<String> = None;
    let mut tenant: Option<String> = None;
    for (key, value) in pairs(query.as_deref()) {
        match key.as_str() {
            "cursor" if !value.is_empty() => match value.parse::<i64>() {
                Ok(seq) if seq > 0 => cursor = Some(seq),
                _ => return reply("invalid_cursor"),
            },
            "cursor" => {}
            "limit" => match value.parse::<i64>() {
                Ok(n) if (1..=MAX_LIMIT).contains(&n) => limit = n,
                _ => return reply("invalid_input"),
            },
            "state" => match value.as_str() {
                "unread" => unread = true,
                "all" => unread = false,
                _ => return reply("invalid_input"),
            },
            "topic" if crate::topics::valid_dotted_name(&value) => topic = Some(value),
            "tenant" if merezha_domain::valid_name(&value) => tenant = Some(value),
            _ => return reply("invalid_input"),
        }
    }
    let scope = match scope(tenant, active) {
        Ok(scope) => scope,
        Err(reply) => return reply.into_response(),
    };
    let Ok(core) = ctx.core() else {
        return reply("unavailable");
    };
    if let Some(refusal) = refuse_deleted(&core, scope.as_deref()).await {
        return refusal;
    }
    let deadline = Instant::now() + DEADLINE;
    let result = async {
        let client = core.database.acquire(deadline).await?;
        let rows = core
            .database
            .query(
                &client,
                &sql::INBOX_LIST,
                &[&user, &unread, &topic, &scope, &cursor, &(limit + 1)],
            )
            .await
            .map_err(|e| classify(&e))?;
        Ok::<_, Failure>(rows)
    }
    .await;
    match result {
        Ok(rows) => {
            let more = rows.len() as i64 > limit;
            let items: Vec<Value> = rows.iter().take(limit as usize).map(item).collect();
            let next = if more {
                rows.get(limit as usize - 1)
                    .map(|r| r.get::<_, i64>(7).to_string())
            } else {
                None
            };
            Json(json!({"items": items, "next_cursor": next})).into_response()
        }
        Err(error) => failure(error),
    }
}

pub(crate) async fn unread_count(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    RawQuery(query): RawQuery,
) -> Response {
    let (user, active) = match ctx.user(principal.as_deref()) {
        Ok(found) => found,
        Err(reply) => return reply.into_response(),
    };
    let mut tenant = None;
    for (key, value) in pairs(query.as_deref()) {
        match key.as_str() {
            "tenant" if merezha_domain::valid_name(&value) => tenant = Some(value),
            _ => return reply("invalid_input"),
        }
    }
    let scope = match scope(tenant, active) {
        Ok(scope) => scope,
        Err(reply) => return reply.into_response(),
    };
    let Ok(core) = ctx.core() else {
        return reply("unavailable");
    };
    if let Some(refusal) = refuse_deleted(&core, scope.as_deref()).await {
        return refusal;
    }
    let deadline = Instant::now() + DEADLINE;
    let result = async {
        let client = core.database.acquire(deadline).await?;
        let row = core
            .database
            .query_one(&client, &sql::INBOX_UNREAD, &[&user, &scope])
            .await
            .map_err(|e| classify(&e))?;
        Ok::<i64, Failure>(row.get(0))
    }
    .await;
    match result {
        Ok(unread) => Json(json!({"unread": unread})).into_response(),
        Err(error) => failure(error),
    }
}

/// What a single-item change does.
#[derive(Clone, Copy)]
enum Change {
    Read,
    Unread,
    Archive,
}
impl Change {
    fn as_str(self) -> &'static str {
        match self {
            Change::Read => "read",
            Change::Unread => "unread",
            Change::Archive => "archive",
        }
    }
}

pub(crate) async fn read(
    state: State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    path: Path<String>,
) -> Response {
    change(state, principal, path, Change::Read).await
}
pub(crate) async fn unread(
    state: State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    path: Path<String>,
) -> Response {
    change(state, principal, path, Change::Unread).await
}
pub(crate) async fn archive(
    state: State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    path: Path<String>,
) -> Response {
    change(state, principal, path, Change::Archive).await
}

/// Lock the user's counter, take the next number and apply the change. A
/// change that changes nothing rolls back, so the counter does not move.
async fn change(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(id): Path<String>,
    change: Change,
) -> Response {
    // A single item outside the active tenant is not found.
    let (user, active) = match ctx.user(principal.as_deref()) {
        Ok(found) => found,
        Err(reply) => return reply.into_response(),
    };
    let Ok(id) = Uuid::try_parse(&id) else {
        return reply("not_found");
    };
    let Ok(core) = ctx.core() else {
        return reply("unavailable");
    };
    if let Some(refusal) = refuse_deleted(&core, active.as_deref()).await {
        return refusal;
    }
    let deadline = Instant::now() + DEADLINE;
    let db = core.database.clone();
    let now = core.now();
    let owner = user.clone();
    let tenant = active.clone();
    let changed = db
        .clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                let Some(counter) = db
                    .query_opt(tx, &sql::COUNTER_BUMP, &[&owner, &1i64])
                    .await
                    .map_err(|e| classify(&e))?
                else {
                    return Err(Failure::permanent("not_found"));
                };
                let seq: i64 = counter.get(0);
                let row = db
                    .query_opt(
                        tx,
                        &sql::INBOX_CHANGE,
                        &[&id, &owner, &change.as_str(), &now, &seq, &tenant],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                match row {
                    Some(row) => {
                        db.query(tx, &sql::INBOX_NOTIFY_ONE, &[&owner])
                            .await
                            .map_err(|e| classify(&e))?;
                        Ok(item(&row))
                    }
                    // Unchanged or missing: roll the counter back.
                    None => Err(Failure::conflict("inbox_unchanged")),
                }
            })
        })
        .await;
    match changed {
        Ok(item) => Json(item).into_response(),
        Err(error) if error.code == "inbox_unchanged" => {
            let client = match core.database.acquire(deadline).await {
                Ok(client) => client,
                Err(error) => return failure(error),
            };
            match core
                .database
                .query_opt(&client, &sql::INBOX_GET, &[&id, &user, &active])
                .await
            {
                Ok(Some(row)) => Json(item(&row)).into_response(),
                Ok(None) => reply("not_found"),
                Err(error) => failure(classify(&error)),
            }
        }
        Err(error) => failure(error),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadAll {
    up_to_seq: Option<i64>,
    tenant: Option<String>,
}

pub(crate) async fn read_all(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    body: Bytes,
) -> Response {
    let (user, active) = match ctx.user(principal.as_deref()) {
        Ok(found) => found,
        Err(reply) => return reply.into_response(),
    };
    let request: ReadAll = if body.is_empty() {
        ReadAll {
            up_to_seq: None,
            tenant: None,
        }
    } else {
        match serde_json::from_slice(&body) {
            Ok(request) => request,
            Err(_) => return reply("invalid_input"),
        }
    };
    if request
        .tenant
        .as_deref()
        .is_some_and(|t| !merezha_domain::valid_name(t))
    {
        return reply("invalid_input");
    }
    let scope = match scope(request.tenant, active) {
        Ok(scope) => scope,
        Err(reply) => return reply.into_response(),
    };
    let Ok(core) = ctx.core() else {
        return reply("unavailable");
    };
    if let Some(refusal) = refuse_deleted(&core, scope.as_deref()).await {
        return refusal;
    }
    let deadline = Instant::now() + DEADLINE;
    let db = core.database.clone();
    let now = core.now();
    let up_to = request.up_to_seq;
    let updated = db
        .clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                let Some(counter) = db
                    .query_opt(tx, &sql::COUNTER_LOCK, &[&user])
                    .await
                    .map_err(|e| classify(&e))?
                else {
                    return Ok(0);
                };
                let last: i64 = counter.get(0);
                let changed = db
                    .execute(
                        tx,
                        &sql::INBOX_READ_ALL,
                        &[&user, &now, &last, &up_to, &scope, &MAX_READ_ALL],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                if changed > 0 {
                    db.execute(tx, &sql::COUNTER_BUMP, &[&user, &(changed as i64)])
                        .await
                        .map_err(|e| classify(&e))?;
                    db.query(tx, &sql::INBOX_NOTIFY_ONE, &[&user])
                        .await
                        .map_err(|e| classify(&e))?;
                }
                Ok(changed)
            })
        })
        .await;
    match updated {
        Ok(updated) => Json(json!({"updated": updated})).into_response(),
        Err(error) => failure(error),
    }
}
