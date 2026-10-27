//! The push routes of NT §7.1: the public VAPID key and the calling
//! user's subscriptions.
//!
//! Subscriptions are tenant-aware like the inbox (NT §5.4): a subscription
//! records the active tenant of the session that made it; listing and
//! deleting see the subscriptions without a tenant and those of the active
//! tenant, and another tenant's subscription answers 404, as a missing one
//! does. Endpoints, `p256dh` and `auth` are never echoed: the list shows
//! the endpoint's origin only.
use super::{HOST_NOT_ALLOWED, PUSH_DISABLED, SUBSCRIPTION_INVALID, b64url_decode, sql};
use crate::http::{Ctx, new_id};
use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use merezha_domain::Failure;
use merezha_platform::{ErrorReply, FieldError, edge::Authenticated};
use merezha_postgres::classify;
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(10);
/// Most characters of a subscription label.
const MAX_LABEL: usize = 64;
/// Most subscriptions the list shows (the per-user limit is at most 100).
const MAX_LIST: i64 = 100;

fn reply(code: &str) -> Response {
    ErrorReply::new(Failure::permanent(code)).into_response()
}
fn invalid(code: &str, path: &str, reason: &str) -> Response {
    ErrorReply::new(Failure::permanent(code))
        .details(FieldError::new(path, reason).into_iter().collect())
        .into_response()
}
fn failed(failure: Failure) -> Response {
    ErrorReply::new(failure).into_response()
}

/// `GET push/vapid-public-key` (public).
pub(crate) async fn public_key(State(ctx): State<Ctx>) -> Response {
    if ctx.checked.settings.push.is_none() {
        return reply(PUSH_DISABLED);
    }
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    match &core.push {
        Some(sender) => Json(json!({
            "public_key": sender.vapid().public_key(),
            "key_id": sender.vapid().key_id(),
        }))
        .into_response(),
        None => reply(PUSH_DISABLED),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeysBody {
    p256dh: String,
    auth: String,
}
/// `PushSubscription.toJSON()` plus `label`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscribeBody {
    endpoint: String,
    #[serde(default, rename = "expirationTime")]
    expiration_time: Option<f64>,
    keys: KeysBody,
    #[serde(default)]
    label: Option<String>,
}

/// `POST push/subscriptions` (self).
pub(crate) async fn subscribe(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    body: Bytes,
) -> Response {
    let (user, tenant) = match ctx.user(principal.as_deref()) {
        Ok(found) => found,
        Err(reply) => return reply.into_response(),
    };
    if ctx.checked.settings.push.is_none() {
        return reply(PUSH_DISABLED);
    }
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    let Some(sender) = core.push.clone() else {
        return reply(PUSH_DISABLED);
    };
    let Ok(body) = serde_json::from_slice::<SubscribeBody>(&body) else {
        return reply("invalid_input");
    };
    let endpoint = match sender.check(&body.endpoint) {
        Ok(endpoint) => endpoint,
        Err(code) if code == HOST_NOT_ALLOWED => {
            return invalid(HOST_NOT_ALLOWED, "/endpoint", "host_not_allowed");
        }
        Err(_) => return invalid(SUBSCRIPTION_INVALID, "/endpoint", "invalid_url"),
    };
    // A valid uncompressed P-256 point, and a 16-byte secret.
    let p256dh = b64url_decode(&body.keys.p256dh).filter(|key| {
        key.len() == 65
            && key[0] == 4
            && web_push_native::p256::PublicKey::from_sec1_bytes(key).is_ok()
    });
    let Some(p256dh) = p256dh else {
        return invalid(SUBSCRIPTION_INVALID, "/keys/p256dh", "invalid_key");
    };
    let Some(auth) = b64url_decode(&body.keys.auth).filter(|a| a.len() == 16) else {
        return invalid(SUBSCRIPTION_INVALID, "/keys/auth", "invalid_key");
    };
    let label = match body.label {
        Some(label) if label.chars().count() > MAX_LABEL || label.chars().any(char::is_control) => {
            return invalid(SUBSCRIPTION_INVALID, "/label", "invalid_value");
        }
        label => label.filter(|l| !l.is_empty()),
    };
    let expires_at = match body.expiration_time {
        None => None,
        Some(millis) if millis.is_finite() => {
            match jiff::Timestamp::from_millisecond(millis as i64) {
                Ok(at) if at > core.now() => Some(at),
                _ => return invalid(SUBSCRIPTION_INVALID, "/expirationTime", "invalid_value"),
            }
        }
        Some(_) => return invalid(SUBSCRIPTION_INVALID, "/expirationTime", "invalid_value"),
    };
    let id = match new_id(&core) {
        Ok(id) => id,
        Err(failure) => return failed(failure),
    };
    let keep = i64::from(
        ctx.checked
            .settings
            .push
            .as_ref()
            .map_or(10, |p| p.max_subscriptions_per_user),
    );
    let key_id = sender.vapid().key_id().to_owned();
    let db = core.database.clone();
    let clock = core.clone();
    let deadline = Instant::now() + DEADLINE;
    let result = db
        .clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                db.query_one(tx, &sql::USER_LOCK, &[&user])
                    .await
                    .map_err(|e| classify(&e))?;
                // No subscription is made in a deleted organization.
                if let Some(tenant) = &tenant
                    && crate::tenants::guard(&db, tx, tenant).await?
                {
                    return Err(Failure::permanent("not_found"));
                }
                // Read after the lock, so this row is the most recently
                // used of the user's when TRIM runs.
                let now = clock.now();
                let row = db
                    .query_one(
                        tx,
                        &sql::UPSERT,
                        &[
                            &id,
                            &user,
                            &tenant,
                            &endpoint.url.as_str(),
                            &endpoint.origin,
                            &p256dh,
                            &auth,
                            &key_id,
                            &label,
                            &expires_at,
                            &now,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                let id: Uuid = row.get(0);
                // The subscription just made is never the one evicted.
                let others = keep - 1;
                let evicted = db
                    .query(tx, &sql::TRIM, &[&user, &others, &id])
                    .await
                    .map_err(|e| classify(&e))?
                    .len();
                Ok((id, row.get::<_, jiff::Timestamp>(1), evicted))
            })
        })
        .await;
    match result {
        Ok((id, created_at, evicted)) => {
            if evicted > 0 {
                // The least recently used subscriptions beyond the limit.
                merezha_telemetry::event!(
                    INFO,
                    "notification.push_subscription_removed",
                    target = "push",
                    reason = "limit"
                );
            }
            (
                StatusCode::CREATED,
                Json(json!({
                    "id": id.to_string(),
                    "key_id": sender.vapid().key_id(),
                    "created_at": created_at.to_string(),
                })),
            )
                .into_response()
        }
        Err(failure) => failed(failure),
    }
}

/// `GET push/subscriptions` (self).
pub(crate) async fn list(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
) -> Response {
    let (user, tenant) = match ctx.user(principal.as_deref()) {
        Ok(found) => found,
        Err(reply) => return reply.into_response(),
    };
    if ctx.checked.settings.push.is_none() {
        return reply(PUSH_DISABLED);
    }
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    let deadline = Instant::now() + DEADLINE;
    let result = async {
        let client = core.database.acquire(deadline).await?;
        core.database
            .query(&client, &sql::LIST, &[&user, &tenant, &MAX_LIST])
            .await
            .map_err(|e| classify(&e))
    }
    .await;
    match result {
        Ok(rows) => {
            let items: Vec<Value> = rows
                .iter()
                .map(|row| {
                    json!({
                        "id": row.get::<_, Uuid>(0).to_string(),
                        "label": row.get::<_, Option<String>>(1),
                        "endpoint_origin": row.get::<_, String>(2),
                        "created_at": row.get::<_, jiff::Timestamp>(3).to_string(),
                        "last_success_at": row
                            .get::<_, Option<jiff::Timestamp>>(4)
                            .map(|t| t.to_string()),
                    })
                })
                .collect();
            Json(Value::Array(items)).into_response()
        }
        Err(failure) => failed(failure),
    }
}

/// `DELETE push/subscriptions/{id}` (self, own subscription in scope).
pub(crate) async fn remove(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(id): Path<String>,
) -> Response {
    let (user, tenant) = match ctx.user(principal.as_deref()) {
        Ok(found) => found,
        Err(reply) => return reply.into_response(),
    };
    if ctx.checked.settings.push.is_none() {
        return reply(PUSH_DISABLED);
    }
    let Ok(id) = Uuid::try_parse(&id) else {
        return reply("not_found");
    };
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    let deadline = Instant::now() + DEADLINE;
    let result = async {
        let client = core.database.acquire(deadline).await?;
        core.database
            .execute(&client, &sql::DELETE_OWN, &[&id, &user, &tenant])
            .await
            .map_err(|e| classify(&e))
    }
    .await;
    match result {
        Ok(0) => reply("not_found"),
        Ok(_) => {
            super::removed(&core.metrics, "user", 1);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(failure) => failed(failure),
    }
}
