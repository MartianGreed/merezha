//! The NT-N2 routes under `/v1/notifications/` (NT §7.1): `send`,
//! `status` and `redeliver`, the inbox, preferences, unsubscribe and
//! suppressions, the NT-N3 Brevo email callback
//! ([`crate::brevo::events`]), and (NT-N6) the push routes of
//! [`crate::push`]. Self routes need a principal of kind `user` and act on its
//! user; application routes check their permission at `Application` scope.
use crate::{
    Checked, ids, inbox,
    keys::Unsubscribe,
    notifier::{BoundNotifier, Core, Slot},
    payload::DeliverPayload,
    sql, topics,
};
use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{Path, RawQuery, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    ErrorReply, FieldError,
    edge::{Authenticated, RequestId},
    ports::{
        Address, Authorizer, Channel, ChannelSelection, Correlation, DedupeKey, Directory,
        EmailAddress, Locale, MessageContext, NotificationRequest, Notifier, OnDuplicate,
        OutboxRecord, OutboxTarget, PhoneNumber, Principal, PrincipalKind, Recipient,
        ResourceScope,
    },
};
use merezha_postgres::classify;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

/// The permissions of NT §7.1 at `Application` scope.
pub const SEND_PERMISSION: &str = "notifications.send";
pub const STATUS_PERMISSION: &str = "notifications.status.read";
pub const ADMIN_PERMISSION: &str = "notifications.admin";
pub const SUPPRESSIONS_PERMISSION: &str = "notifications.suppressions.manage";

/// Furthest `not_before` a send may ask for, in days.
const MAX_NOT_BEFORE_DAYS: i64 = 30;
/// Most entries in one `PUT preferences`.
const MAX_PREFERENCES: usize = 200;
/// Most deliveries `status` lists.
const MAX_STATUS_DELIVERIES: i64 = 1000;
/// Most suppressions one page lists.
const MAX_SUPPRESSIONS_PAGE: i64 = 100;
const DEADLINE: Duration = Duration::from_secs(10);

/// The routes' shared state.
#[derive(Clone)]
pub(crate) struct Ctx {
    pub slot: Slot,
    pub checked: Arc<Checked>,
    pub authorizer: Arc<dyn Authorizer>,
    /// The notifier bound to source `api` (R4a).
    pub api: BoundNotifier,
    pub directory: Arc<dyn Directory>,
}
impl Ctx {
    pub fn core(&self) -> Result<Arc<Core>, ErrorReply> {
        self.slot
            .get()
            .cloned()
            .ok_or_else(|| ErrorReply::new(Failure::transient("unavailable")))
    }
    /// The calling user and its active tenant: 401 without a principal,
    /// 403 for a principal that is not a user.
    pub fn user(
        &self,
        principal: Option<&Authenticated>,
    ) -> Result<(String, Option<String>), ErrorReply> {
        let principal = principal
            .and_then(|a| a.0.as_deref())
            .ok_or_else(|| ErrorReply::new(Failure::permanent("unauthenticated")))?;
        match (&principal.kind, &principal.user) {
            (PrincipalKind::User, Some(user)) => Ok((
                user.as_str().to_owned(),
                principal.tenant.as_ref().map(|t| t.as_str().to_owned()),
            )),
            _ => Err(ErrorReply::new(Failure::permanent("unauthorized"))),
        }
    }
    /// `permission` at `Application` scope.
    fn allow<'a>(
        &self,
        principal: Option<&'a Authenticated>,
        permission: &str,
    ) -> Result<Option<&'a Principal>, ErrorReply> {
        let permission = Identity::new(permission)
            .map_err(|_| ErrorReply::new(Failure::transient("internal")))?;
        let principal = principal.and_then(|a| a.0.as_deref());
        self.authorizer
            .check(principal, &permission, &ResourceScope::Application)
            .map(|_| principal)
            .map_err(|denied| ErrorReply::new(denied.failure()))
    }
}

pub(crate) fn router(ctx: Ctx) -> Router {
    Router::new()
        .route("/send", post(send))
        .route("/status/{id}", get(status))
        .route("/deliveries/{id}/redeliver", post(redeliver))
        .route("/inbox", get(inbox::list))
        .route("/inbox/unread-count", get(inbox::unread_count))
        .route("/inbox/read-all", post(inbox::read_all))
        .route("/inbox/{id}/read", post(inbox::read))
        .route("/inbox/{id}/unread", post(inbox::unread))
        .route("/inbox/{id}/archive", post(inbox::archive))
        .route("/preferences", get(preferences).put(put_preferences))
        .route("/unsubscribe", get(unsubscribe_page).post(unsubscribe))
        .route(
            "/suppressions",
            get(list_suppressions).post(add_suppression),
        )
        .route("/suppressions/{id}", delete(remove_suppression))
        .route(
            crate::brevo::events::PATH,
            post(crate::brevo::events::callback),
        )
        .route(
            "/push/vapid-public-key",
            get(crate::push::routes::public_key),
        )
        .route(
            "/push/subscriptions",
            get(crate::push::routes::list).post(crate::push::routes::subscribe),
        )
        .route(
            "/push/subscriptions/{id}",
            delete(crate::push::routes::remove),
        )
        .with_state(ctx)
}

fn reply(code: &str) -> Response {
    ErrorReply::new(Failure::permanent(code)).into_response()
}
fn detail(code: &str, path: String, reason: &str) -> Response {
    let details = FieldError::new(path, reason).into_iter().collect();
    ErrorReply::new(Failure::permanent(code))
        .details(details)
        .into_response()
}
fn failed(failure: Failure) -> Response {
    ErrorReply::new(failure).into_response()
}

// ----- send -----

/// `{"id": "usr_..."}` or the bare id.
#[derive(Deserialize)]
#[serde(untagged)]
enum UserRef {
    Id(String),
    Object { id: String },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupBody {
    id: String,
    #[serde(default)]
    roles: Option<Vec<String>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RolesBody {
    roles: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AddressBody {
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    phone: Option<String>,
    #[serde(default)]
    locale: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum RecipientBody {
    User(UserRef),
    Tenant(GroupBody),
    Application(RolesBody),
    Address(AddressBody),
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SendBody {
    topic: String,
    recipients: Vec<RecipientBody>,
    #[serde(default)]
    channels: Option<Vec<String>>,
    #[serde(default)]
    template: Option<String>,
    #[serde(default)]
    data: Map<String, Value>,
    #[serde(default)]
    secret_data: Map<String, Value>,
    #[serde(default)]
    locale: Option<String>,
    dedupe_key: String,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    not_before: Option<jiff::Timestamp>,
    #[serde(default)]
    expires_at: Option<jiff::Timestamp>,
    #[serde(default)]
    collapse_key: Option<String>,
}

fn field(code: &str, path: String, reason: &str) -> ErrorReply {
    ErrorReply::new(Failure::permanent(code))
        .details(FieldError::new(path, reason).into_iter().collect())
}
fn coded(code: &str) -> ErrorReply {
    ErrorReply::new(Failure::permanent(code))
}
fn id_of(text: &str, path: String) -> Result<Identity, ErrorReply> {
    Identity::new(text).map_err(|_| field("invalid_recipient", path, "invalid_id"))
}

/// The request a send body describes, without `members`.
fn request_of(
    body: SendBody,
    subject: &Identity,
    correlation: Identity,
) -> Result<NotificationRequest, ErrorReply> {
    let topic = Identity::new(body.topic).map_err(|_| coded("unknown_topic"))?;
    let tenant = body
        .tenant
        .map(|t| {
            Identity::new(t).map_err(|_| field("invalid_input", "/tenant".into(), "invalid_id"))
        })
        .transpose()?;
    let mut recipients = Vec::new();
    for (index, recipient) in body.recipients.into_iter().enumerate() {
        let path = format!("/recipients/{index}");
        let parsed = match recipient {
            RecipientBody::User(UserRef::Id(id) | UserRef::Object { id }) => Recipient::User {
                id: id_of(&id, path)?,
            },
            RecipientBody::Tenant(group) => Recipient::Tenant {
                id: id_of(&group.id, format!("{path}/tenant/id"))?,
                roles: group
                    .roles
                    .map(|roles| {
                        roles
                            .iter()
                            .map(|r| id_of(r, format!("{path}/tenant/roles")))
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?,
            },
            RecipientBody::Application(roles) => Recipient::Application {
                roles: roles
                    .roles
                    .iter()
                    .map(|r| id_of(r, format!("{path}/application/roles")))
                    .collect::<Result<_, _>>()?,
            },
            RecipientBody::Address(address) => {
                let locale = address.locale.map(Locale::new).transpose().map_err(|_| {
                    field(
                        "invalid_locale",
                        format!("{path}/address/locale"),
                        "invalid_locale",
                    )
                })?;
                match (address.email, address.phone) {
                    (Some(email), None) => Recipient::Address(Address::Email {
                        address: EmailAddress::new(email).map_err(|_| {
                            field(
                                "invalid_recipient",
                                format!("{path}/address/email"),
                                "invalid_email",
                            )
                        })?,
                        name: address.name.map(|n| n.chars().take(200).collect()),
                        user: None,
                        locale,
                    }),
                    (None, Some(phone)) => Recipient::Address(Address::Phone {
                        e164: PhoneNumber::new(phone).map_err(|_| {
                            field(
                                "invalid_recipient",
                                format!("{path}/address/phone"),
                                "invalid_phone",
                            )
                        })?,
                        user: None,
                        locale,
                    }),
                    _ => return Err(field("invalid_recipient", path, "one_address")),
                }
            }
        };
        recipients.push(parsed);
    }
    let channels = match body.channels {
        None => ChannelSelection::TopicDefault,
        Some(names) => {
            let mut chosen = Vec::new();
            for (index, name) in names.iter().take(4).enumerate() {
                match topics::channel(name) {
                    Some(channel) => chosen.push(channel),
                    None => {
                        return Err(field(
                            "channel_not_allowed",
                            format!("/channels/{index}"),
                            "invalid_channel",
                        ));
                    }
                }
            }
            if names.len() > 4 {
                return Err(field(
                    "channel_not_allowed",
                    "/channels".into(),
                    "too_many_items",
                ));
            }
            ChannelSelection::Only(chosen)
        }
    };
    let template = body
        .template
        .map(|t| Identity::new(t).map_err(|_| coded("unknown_template")))
        .transpose()?;
    let locale = body
        .locale
        .map(Locale::new)
        .transpose()
        .map_err(|_| coded("invalid_locale"))?;
    // R4a: the client's key is namespaced by the calling principal.
    let dedupe_key = DedupeKey::api(subject, &body.dedupe_key)
        .map_err(|_| field("invalid_dedupe_key", "/dedupe_key".into(), "invalid_value"))?;
    Ok(NotificationRequest {
        topic,
        recipients,
        channels,
        template,
        data: body.data,
        secret_data: body.secret_data,
        locale,
        dedupe_key,
        tenant,
        members: Vec::new(),
        not_before: body.not_before,
        expires_at: body.expires_at,
        collapse_key: body.collapse_key,
        correlation: Correlation {
            correlation_id: correlation,
            causation_id: None,
        },
    })
}

async fn send(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    request_id: Option<Extension<RequestId>>,
    body: Bytes,
) -> Response {
    let principal = match ctx.allow(principal.as_deref(), SEND_PERMISSION) {
        Ok(Some(principal)) => principal.clone(),
        Ok(None) => return reply("unauthenticated"),
        Err(reply) => return reply.into_response(),
    };
    let Ok(body) = serde_json::from_slice::<SendBody>(&body) else {
        return reply("invalid_input");
    };
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    // The edge's request id, or a fresh id without one (AP4).
    let correlation = match request_id.and_then(|Extension(RequestId(id))| Identity::new(id).ok()) {
        Some(id) => id,
        None => match new_id(&core).and_then(|id| Identity::new(id.simple().to_string())) {
            Ok(id) => id,
            Err(error) => return failed(error),
        },
    };
    let mut request = match request_of(body, &principal.subject, correlation) {
        Ok(request) => request,
        Err(reply) => return reply.into_response(),
    };
    // Membership is checked before the transaction opens (R4).
    if let Some(tenant) = request.tenant.clone() {
        let deadline = Instant::now() + DEADLINE;
        for recipient in &request.recipients {
            let Recipient::User { id } = recipient else {
                continue;
            };
            let member =
                tokio::time::timeout_at(deadline.into(), ctx.directory.is_member(id, &tenant))
                    .await;
            match member {
                Ok(Ok(true)) => request.members.push(id.clone()),
                Ok(Ok(false)) => {}
                _ => return failed(Failure::transient("unavailable")),
            }
        }
    }
    // NT §3.1 bounds: at most 30 days ahead, and expiring after it starts.
    let now = core.now();
    let latest = now
        .checked_add(jiff::SignedDuration::from_hours(MAX_NOT_BEFORE_DAYS * 24))
        .unwrap_or(now);
    if request.not_before.is_some_and(|at| at > latest) {
        return field("validation_failed", "/not_before".into(), "too_far").into_response();
    }
    if let (Some(start), Some(end)) = (request.not_before, request.expires_at)
        && end <= start
    {
        return field(
            "validation_failed",
            "/expires_at".into(),
            "not_after_not_before",
        )
        .into_response();
    }
    if let Err(error) = ctx.api.validate(&request) {
        return failed(error.into());
    }
    let notifier = ctx.api.clone();
    let deadline = Instant::now() + DEADLINE;
    let result = core
        .database
        .transaction(deadline, move |tx| {
            Box::pin(async move { notifier.request(tx, request).await.map_err(Failure::from) })
        })
        .await;
    match result {
        Ok(handle) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "notification_id": handle.notification_id.to_string(),
                "duplicate": handle.duplicate,
            })),
        )
            .into_response(),
        Err(error) => failed(error),
    }
}

// ----- status and redeliver -----

fn stamp(value: Option<jiff::Timestamp>) -> Value {
    value.map_or(Value::Null, |t| Value::String(t.to_string()))
}

async fn status(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(id): Path<String>,
) -> Response {
    // `notifications.admin` reads every notification; `status.read` only
    // the caller's own sends, under its `api:{subject}:` namespace (R4a).
    let own = match ctx.allow(principal.as_deref(), ADMIN_PERMISSION) {
        Ok(_) => None,
        Err(_) => match ctx.allow(principal.as_deref(), STATUS_PERMISSION) {
            Ok(Some(principal)) => Some(format!("api:{}:", principal.subject.as_str())),
            Ok(None) => return reply("unauthenticated"),
            Err(reply) => return reply.into_response(),
        },
    };
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
        let db = &core.database;
        let Some(row) = db
            .query_opt(&client, &sql::STATUS_NOTIFICATION, &[&id])
            .await
            .map_err(|e| classify(&e))?
        else {
            return Ok(None);
        };
        let (source, key): (String, String) = (row.get(5), row.get(6));
        if own
            .as_ref()
            .is_some_and(|prefix| source != "api" || !key.starts_with(prefix.as_str()))
        {
            return Ok(None);
        }
        let deliveries = db
            .query(
                &client,
                &sql::STATUS_DELIVERIES,
                &[&id, &MAX_STATUS_DELIVERIES],
            )
            .await
            .map_err(|e| classify(&e))?;
        let deliveries: Vec<Value> = deliveries
            .iter()
            .map(|d| {
                let delivery: Uuid = d.get(0);
                let channel: String = d.get(1);
                let address: Option<String> = d.get(5);
                let target = topics::channel(&channel)
                    .map(|c| ids::mask(c, address.as_deref()))
                    .unwrap_or_default();
                json!({
                    "id": delivery.to_string(),
                    "channel": channel,
                    "status": d.get::<_, String>(2),
                    "status_reason": d.get::<_, Option<String>>(3),
                    "attempt_count": d.get::<_, i32>(4),
                    "target": target,
                    "accepted_at": stamp(d.get(6)),
                    "settled_at": stamp(d.get(7)),
                })
            })
            .collect();
        Ok::<_, Failure>(Some(json!({
            "id": id.to_string(),
            "topic": row.get::<_, String>(0),
            "status": row.get::<_, String>(1),
            "status_reason": row.get::<_, Option<String>>(2),
            "created_at": stamp(row.get(3)),
            "settled_at": stamp(row.get(4)),
            "deliveries": deliveries,
        })))
    }
    .await;
    match result {
        Ok(Some(body)) => Json(body).into_response(),
        Ok(None) => reply("not_found"),
        Err(error) => failed(error),
    }
}

async fn redeliver(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(id): Path<String>,
) -> Response {
    if let Err(reply) = ctx.allow(principal.as_deref(), ADMIN_PERMISSION) {
        return reply.into_response();
    }
    let Ok(id) = Uuid::try_parse(&id) else {
        return reply("not_found");
    };
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    let deadline = Instant::now() + DEADLINE;
    let db = core.database.clone();
    let outbox = core.outbox.clone();
    let now = core.now();
    let result = db
        .clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                let Some(row) = db
                    .query_opt(tx, &sql::REDELIVER_LOCK, &[&id])
                    .await
                    .map_err(|e| classify(&e))?
                else {
                    return Err(Failure::permanent("not_found"));
                };
                let status: String = row.get(0);
                let channel: String = row.get(1);
                let has_secret: bool = row.get(2);
                let purged: bool = row.get(3);
                let tenant: Option<String> = row.get(4);
                let correlation: Option<String> = row.get(5);
                let notification: Uuid = row.get(6);
                // Invariant 5: only from failed, unknown or expired.
                if !matches!(status.as_str(), "failed" | "unknown" | "expired") {
                    return Err(Failure::conflict("not_redeliverable"));
                }
                // R4a: the secret is gone with the flagged payloads.
                if has_secret {
                    return Err(Failure::conflict("secret_purged"));
                }
                if purged {
                    return Err(Failure::permanent("data_purged"));
                }
                let Some((queue, kind)) =
                    topics::channel(&channel).and_then(crate::expand::delivery_queue)
                else {
                    return Err(Failure::conflict("not_redeliverable"));
                };
                let generation: i32 = db
                    .query_one(tx, &sql::REDELIVER_SET, &[&id, &now])
                    .await
                    .map_err(|e| classify(&e))?
                    .get(0);
                db.execute(tx, &sql::NOTIFICATION_REOPEN, &[&notification])
                    .await
                    .map_err(|e| classify(&e))?;
                let correlation_id = correlation
                    .and_then(|c| Identity::new(c).ok())
                    .map_or_else(|| Identity::new(notification.simple().to_string()), Ok)?;
                let record = OutboxRecord {
                    dedupe_key: DedupeKey::new(format!("deliver:{id}:g{generation}"))?,
                    target: OutboxTarget::Job {
                        queue: Identity::new(queue)?,
                        kind: Identity::new(kind)?,
                    },
                    payload: DeliverPayload {
                        delivery_id: id,
                        generation,
                        secret_data: None,
                    }
                    .to_value(),
                    contains_secret: false,
                    run_at: None,
                    priority: 0,
                    context: MessageContext {
                        correlation_id,
                        causation_id: None,
                        tenant: tenant.and_then(|t| Identity::new(t).ok()),
                    },
                };
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
        Err(error) => failed(error),
    }
}

// ----- preferences -----

/// The preferences document of `user` (NT §7.1).
async fn preferences_of(ctx: &Ctx, core: &Core, user: &str) -> Result<Value, Failure> {
    let deadline = Instant::now() + DEADLINE;
    let client = core.database.acquire(deadline).await?;
    let rows = core
        .database
        .query(
            &client,
            &sql::PREFERENCES_LIST,
            &[&user, &((MAX_PREFERENCES * 20) as i64)],
        )
        .await
        .map_err(|e| classify(&e))?;
    let mut stored: BTreeMap<(String, String), bool> = BTreeMap::new();
    for row in rows {
        stored.insert((row.get(0), row.get(1)), row.get(2));
    }
    let timezone: Option<String> = core
        .database
        .query_opt(&client, &sql::SETTINGS_GET, &[&user])
        .await
        .map_err(|e| classify(&e))?
        .and_then(|r| r.get(0));
    let settings = &ctx.checked.settings;
    let topics: Vec<Value> = settings
        .topics
        .values()
        .filter(|t| t.user_configurable)
        .map(|topic| {
            let channels: Vec<Value> = topic
                .channels
                .iter()
                .map(|channel| {
                    let prefs = crate::policy::UserPreferences {
                        topic: stored
                            .get(&(topic.name.clone(), channel.as_str().to_owned()))
                            .copied(),
                        any: stored
                            .get(&("*".into(), channel.as_str().to_owned()))
                            .copied(),
                    };
                    json!({
                        "channel": channel.as_str(),
                        "enabled": crate::policy::user_allows(topic, *channel, prefs).is_ok(),
                        "configurable": settings.channel_available(*channel),
                    })
                })
                .collect();
            json!({
                "topic": topic.name,
                "category": topic.category.as_str(),
                "description": topic.description,
                "channels": channels,
            })
        })
        .collect();
    Ok(json!({"topics": topics, "settings": {"timezone": timezone}}))
}

async fn preferences(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
) -> Response {
    let (user, _) = match ctx.user(principal.as_deref()) {
        Ok(found) => found,
        Err(reply) => return reply.into_response(),
    };
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    match preferences_of(&ctx, &core, &user).await {
        Ok(body) => Json(body).into_response(),
        Err(error) => failed(error),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreferenceEntry {
    topic: String,
    channel: String,
    enabled: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingsBody {
    #[serde(default)]
    timezone: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreferencesBody {
    #[serde(default)]
    preferences: Vec<PreferenceEntry>,
    #[serde(default)]
    settings: Option<SettingsBody>,
}

async fn put_preferences(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    body: Bytes,
) -> Response {
    let (user, _) = match ctx.user(principal.as_deref()) {
        Ok(found) => found,
        Err(reply) => return reply.into_response(),
    };
    let Ok(body) = serde_json::from_slice::<PreferencesBody>(&body) else {
        return reply("invalid_input");
    };
    if body.preferences.len() > MAX_PREFERENCES {
        return detail("validation_failed", "/preferences".into(), "too_many_items");
    }
    let settings = &ctx.checked.settings;
    let mut details = Vec::new();
    let (mut topic_names, mut channels, mut enabled) = (Vec::new(), Vec::new(), Vec::new());
    for (index, entry) in body.preferences.iter().enumerate() {
        let Some(channel) = topics::channel(&entry.channel) else {
            details.extend(FieldError::new(
                format!("/preferences/{index}/channel"),
                "invalid_channel",
            ));
            continue;
        };
        let allowed = entry.topic == "*"
            || settings
                .topics
                .get(&entry.topic)
                .is_some_and(|t| t.user_configurable && t.channels.contains(&channel));
        if !allowed {
            details.extend(FieldError::new(
                format!("/preferences/{index}/topic"),
                "topic_not_configurable",
            ));
            continue;
        }
        topic_names.push(entry.topic.clone());
        channels.push(channel.as_str().to_owned());
        enabled.push(entry.enabled);
    }
    if !details.is_empty() {
        return ErrorReply::new(Failure::permanent("topic_not_configurable"))
            .details(details)
            .into_response();
    }
    let timezone = match &body.settings {
        Some(SettingsBody {
            timezone: Some(zone),
        }) => {
            if zone.len() > 64 || jiff::tz::TimeZone::get(zone).is_err() {
                return detail(
                    "invalid_timezone",
                    "/settings/timezone".into(),
                    "unknown_timezone",
                );
            }
            Some(Some(zone.clone()))
        }
        Some(SettingsBody { timezone: None }) => Some(None),
        None => None,
    };
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    let db = core.database.clone();
    let now = core.now();
    let owner = user.clone();
    let written = db
        .clone()
        .transaction(Instant::now() + DEADLINE, move |tx| {
            Box::pin(async move {
                if !topic_names.is_empty() {
                    db.execute(
                        tx,
                        &sql::PREFERENCES_UPSERT,
                        &[&owner, &now, &topic_names, &channels, &enabled],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                }
                if let Some(zone) = timezone {
                    db.execute(tx, &sql::SETTINGS_UPSERT, &[&owner, &zone, &now])
                        .await
                        .map_err(|e| classify(&e))?;
                }
                Ok(())
            })
        })
        .await;
    if let Err(error) = written {
        return failed(error);
    }
    match preferences_of(&ctx, &core, &user).await {
        Ok(body) => Json(body).into_response(),
        Err(error) => failed(error),
    }
}

// ----- unsubscribe -----

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenBody {
    token: String,
}

/// `POST unsubscribe`: the RFC 8058 one-click form with `?token=`, or JSON
/// `{token}`. No session; every well-formed token answers 200 `{}`, a
/// forged or malformed one 400 `token_invalid`.
async fn unsubscribe(
    State(ctx): State<Ctx>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    let json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    let token = if json {
        serde_json::from_slice::<TokenBody>(&body)
            .ok()
            .map(|b| b.token)
    } else {
        let form = inbox::pairs(std::str::from_utf8(&body).ok());
        let one_click = form
            .iter()
            .any(|(k, v)| k == "List-Unsubscribe" && v == "One-Click");
        let from_form = form
            .iter()
            .find(|(k, _)| k == "token")
            .map(|(_, v)| v.clone());
        let from_query = inbox::pairs(query.as_deref())
            .into_iter()
            .find(|(k, _)| k == "token")
            .map(|(_, v)| v);
        (one_click || from_form.is_some())
            .then(|| from_form.or(from_query))
            .flatten()
    };
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    let Some(claims) = token.and_then(|t| {
        core.keys
            .verify_unsubscribe(&t, |name| configurable(&ctx, name))
    }) else {
        return reply("token_invalid");
    };
    match apply_unsubscribe(&core, &claims).await {
        Ok(()) => Json(json!({})).into_response(),
        Err(error) => failed(error),
    }
}

/// Whether unsubscribe tokens may name `topic`: a configured topic users
/// can turn off.
fn configurable(ctx: &Ctx, topic: &str) -> bool {
    ctx.checked
        .settings
        .topics
        .get(topic)
        .is_some_and(|t| t.user_configurable)
}

/// A user token writes `(user, topic, email, false)`; an address token a
/// topic-scoped `unsubscribed` suppression.
async fn apply_unsubscribe(core: &Core, claims: &Unsubscribe) -> Result<(), Failure> {
    let db = core.database.clone();
    let now = core.now();
    let topic = claims.t.clone();
    let user = claims.u.clone();
    let address = claims.a.clone();
    let id = new_id(core)?;
    db.clone()
        .transaction(Instant::now() + DEADLINE, move |tx| {
            Box::pin(async move {
                match (user, address) {
                    (Some(user), _) => {
                        db.execute(
                            tx,
                            &sql::PREFERENCES_UPSERT,
                            &[&user, &now, &vec![topic], &vec!["email"], &vec![false]],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    }
                    (None, Some(address)) => {
                        db.execute(
                            tx,
                            &sql::SUPPRESSION_INSERT,
                            &[
                                &id,
                                &"email",
                                &address.to_lowercase(),
                                &Some(topic),
                                &"unsubscribed",
                                &None::<Uuid>,
                                &now,
                            ],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    }
                    (None, None) => {}
                }
                Ok(())
            })
        })
        .await
}

/// `GET unsubscribe?token=` changes nothing (mail scanners prefetch links):
/// 303 to `links.unsubscribe_page` when set, else a page with one button.
async fn unsubscribe_page(State(ctx): State<Ctx>, RawQuery(query): RawQuery) -> Response {
    let token = inbox::pairs(query.as_deref())
        .into_iter()
        .find(|(k, _)| k == "token")
        .map(|(_, v)| v);
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    let Some(token) = token.filter(|t| {
        core.keys
            .verify_unsubscribe(t, |name| configurable(&ctx, name))
            .is_some()
    }) else {
        return reply("token_invalid");
    };
    let encoded: String = url::form_urlencoded::byte_serialize(token.as_bytes()).collect();
    if let Some(page) = &ctx.checked.settings.links.unsubscribe_page {
        let separator = if page.contains('?') { '&' } else { '?' };
        let location = format!("{page}{separator}token={encoded}");
        let mut response = StatusCode::SEE_OTHER.into_response();
        if let Ok(value) = HeaderValue::from_str(&location) {
            response.headers_mut().insert(header::LOCATION, value);
        }
        return response;
    }
    let html = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>Unsubscribe</title></head>\
         <body><form method=\"post\" action=\"unsubscribe?token={encoded}\">\
         <input type=\"hidden\" name=\"List-Unsubscribe\" value=\"One-Click\">\
         <button type=\"submit\">Unsubscribe</button></form></body></html>"
    );
    let mut response = html.into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'none'; form-action 'self'"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    response
}

// ----- suppressions -----

/// A UUIDv7 from the clock and the platform CSPRNG.
pub(crate) fn new_id(core: &Core) -> Result<Uuid, Failure> {
    let millis = core
        .clock
        .now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut bits = [0u8; 10];
    core.random.fill(&mut bits)?;
    Ok(uuid::Builder::from_unix_timestamp_millis(millis, &bits).into_uuid())
}

fn suppression(row: &tokio_postgres::Row) -> Value {
    json!({
        "id": row.get::<_, Uuid>(0).to_string(),
        "channel": row.get::<_, String>(1),
        "address": row.get::<_, String>(2),
        "topic": row.get::<_, Option<String>>(3),
        "reason": row.get::<_, String>(4),
        "created_at": row.get::<_, jiff::Timestamp>(5).to_string(),
    })
}

/// A suppression address in its stored form: a lowercased email or E.164.
fn normalize(channel: Channel, address: &str) -> Option<String> {
    match channel {
        Channel::Email => EmailAddress::new(address.to_lowercase())
            .ok()
            .map(|a| a.expose().to_owned()),
        Channel::Sms => PhoneNumber::new(address)
            .ok()
            .map(|p| p.expose().to_owned()),
        _ => None,
    }
}

async fn list_suppressions(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    RawQuery(query): RawQuery,
) -> Response {
    if let Err(reply) = ctx.allow(principal.as_deref(), SUPPRESSIONS_PERMISSION) {
        return reply.into_response();
    }
    let (mut channel, mut address, mut cursor) = (None::<Channel>, None::<String>, None::<Uuid>);
    for (key, value) in inbox::pairs(query.as_deref()) {
        match key.as_str() {
            "channel" => match topics::channel(&value) {
                Some(c @ (Channel::Email | Channel::Sms)) => channel = Some(c),
                _ => return reply("invalid_input"),
            },
            "address" => address = Some(value),
            "cursor" if !value.is_empty() => match Uuid::try_parse(&value) {
                Ok(id) => cursor = Some(id),
                Err(_) => return reply("invalid_cursor"),
            },
            "cursor" => {}
            _ => return reply("invalid_input"),
        }
    }
    let address = match (address, channel) {
        (None, _) => None,
        (Some(address), Some(channel)) => match normalize(channel, &address) {
            Some(address) => Some(address),
            None => return reply("invalid_input"),
        },
        (Some(address), None) => Some(address.to_lowercase()),
    };
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    let channel = channel.map(Channel::as_str);
    let result = async {
        let client = core.database.acquire(Instant::now() + DEADLINE).await?;
        core.database
            .query(
                &client,
                &sql::SUPPRESSIONS_LIST,
                &[&channel, &address, &cursor, &(MAX_SUPPRESSIONS_PAGE + 1)],
            )
            .await
            .map_err(|e| classify(&e))
    }
    .await;
    match result {
        Ok(rows) => {
            let page = MAX_SUPPRESSIONS_PAGE as usize;
            let next = (rows.len() > page)
                .then(|| rows.get(page - 1).map(|r| r.get::<_, Uuid>(0).to_string()))
                .flatten();
            let items: Vec<Value> = rows.iter().take(page).map(suppression).collect();
            Json(json!({"items": items, "next_cursor": next})).into_response()
        }
        Err(error) => failed(error),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SuppressionBody {
    channel: String,
    address: String,
    #[serde(default)]
    topic: Option<String>,
    reason: String,
}

async fn add_suppression(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    body: Bytes,
) -> Response {
    if let Err(reply) = ctx.allow(principal.as_deref(), SUPPRESSIONS_PERMISSION) {
        return reply.into_response();
    }
    let Ok(body) = serde_json::from_slice::<SuppressionBody>(&body) else {
        return reply("invalid_input");
    };
    let Some(channel) =
        topics::channel(&body.channel).filter(|c| matches!(c, Channel::Email | Channel::Sms))
    else {
        return detail("validation_failed", "/channel".into(), "invalid_channel");
    };
    let Some(address) = normalize(channel, &body.address) else {
        return detail("validation_failed", "/address".into(), "invalid_address");
    };
    if body.reason != "manual" {
        return detail("validation_failed", "/reason".into(), "invalid_value");
    }
    if body
        .topic
        .as_deref()
        .is_some_and(|t| !ctx.checked.settings.topics.contains_key(t))
    {
        return detail("validation_failed", "/topic".into(), "unknown_topic");
    }
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    let id = match new_id(&core) {
        Ok(id) => id,
        Err(error) => return failed(error),
    };
    let db = core.database.clone();
    let now = core.now();
    let topic = body.topic;
    let result = db
        .clone()
        .transaction(Instant::now() + DEADLINE, move |tx| {
            Box::pin(async move {
                db.execute(
                    tx,
                    &sql::SUPPRESSION_INSERT,
                    &[
                        &id,
                        &channel.as_str(),
                        &address,
                        &topic,
                        &"manual",
                        &None::<Uuid>,
                        &now,
                    ],
                )
                .await
                .map_err(|e| classify(&e))?;
                let row = db
                    .query_one(
                        tx,
                        &sql::SUPPRESSION_FIND,
                        &[&channel.as_str(), &address, &topic],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                Ok(suppression(&row))
            })
        })
        .await;
    match result {
        Ok(body) => (StatusCode::CREATED, Json(body)).into_response(),
        Err(error) => failed(error),
    }
}

async fn remove_suppression(
    State(ctx): State<Ctx>,
    principal: Option<Extension<Authenticated>>,
    Path(id): Path<String>,
) -> Response {
    if let Err(reply) = ctx.allow(principal.as_deref(), SUPPRESSIONS_PERMISSION) {
        return reply.into_response();
    }
    let Ok(id) = Uuid::try_parse(&id) else {
        return reply("not_found");
    };
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    let result = async {
        let client = core.database.acquire(Instant::now() + DEADLINE).await?;
        core.database
            .execute(&client, &sql::SUPPRESSION_DELETE, &[&id])
            .await
            .map_err(|e| classify(&e))
    }
    .await;
    match result {
        Ok(0) => reply("not_found"),
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => failed(error),
    }
}
