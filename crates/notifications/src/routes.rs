//! The routes of this slice under `/v1/notifications/` (NT §7.1): template
//! listing and preview, and the loopback-only capture listing.
use crate::{
    config::Settings,
    drivers::{CaptureDriver, MAX_CAPTURE_PAGE},
    templates::{PREVIEW_NOTIFICATION_ID, RESERVED_KEYS, RenderContext, TemplateRegistry},
    topics::{self, Category},
};
use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{Path, RawQuery, State},
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    ErrorReply, FieldError,
    edge::{Authenticated, ClientInfo},
    ports::{Authorizer, Channel, Locale, ResourceScope},
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// The permission both preview routes check at `Application` scope.
pub const PREVIEW_PERMISSION: &str = "notifications.templates.preview";
/// `Content-Security-Policy` of `preview.html` (NT §4).
pub const PREVIEW_CSP: &str =
    "default-src 'none'; style-src 'unsafe-inline'; img-src https: data:; sandbox";

#[derive(Clone)]
pub(crate) struct RouteState {
    pub settings: Arc<Settings>,
    pub registry: Arc<TemplateRegistry>,
    pub authorizer: Arc<dyn Authorizer>,
    pub captures: Option<CaptureDriver>,
}

pub(crate) fn router(state: RouteState) -> Router {
    Router::new()
        .route("/templates", get(list_templates))
        .route("/templates/{name}/preview", post(preview))
        .route("/templates/{name}/preview.html", get(preview_html))
        .route("/dev/captures", get(captures))
        .with_state(state)
}

fn reply(code: &str) -> Response {
    ErrorReply::new(Failure::permanent(code)).into_response()
}

/// 404 when previews are off, else the permission check.
fn preview_gate(state: &RouteState, principal: Option<&Authenticated>) -> Result<(), ErrorReply> {
    if !state.settings.templates.preview {
        return Err(ErrorReply::new(Failure::permanent("not_found")));
    }
    let permission = Identity::new(PREVIEW_PERMISSION)
        .map_err(|_| ErrorReply::new(Failure::transient("internal")))?;
    let principal = principal.and_then(|a| a.0.as_deref());
    state
        .authorizer
        .check(principal, &permission, &ResourceScope::Application)
        .map(|_| ())
        .map_err(|denied| ErrorReply::new(denied.failure()))
}

async fn list_templates(
    State(state): State<RouteState>,
    principal: Option<Extension<Authenticated>>,
) -> Response {
    if let Err(reply) = preview_gate(&state, principal.as_deref()) {
        return reply.into_response();
    }
    let default = state.registry.default_locale().to_owned();
    let items: Vec<Value> = state
        .registry
        .templates()
        .map(|info| {
            json!({
                "name": info.name,
                "description": info.description,
                "locales": info.locales(),
                "channels": info.channels(&state.registry, &default)
                    .iter().map(|c| c.as_str()).collect::<Vec<_>>(),
            })
        })
        .collect();
    Json(items).into_response()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreviewRequest {
    locale: Option<String>,
    data: Option<Map<String, Value>>,
    channels: Option<Vec<String>>,
}

/// The topic that uses `template`, for its category and name.
fn topic_of<'a>(settings: &'a Settings, template: &str) -> Option<&'a topics::Topic> {
    settings.topics.values().find(|t| t.template == template)
}

/// Render the requested channels and answer every failure at once.
fn render_all(
    state: &RouteState,
    name: &str,
    locale: &str,
    channels: &[Channel],
    data: &Map<String, Value>,
    secret: &Map<String, Value>,
) -> Result<Map<String, Value>, ErrorReply> {
    let topic = topic_of(&state.settings, name);
    let category = topic.map_or(Category::Transactional, |t| t.category);
    let mut out = Map::new();
    let mut details = Vec::new();
    for channel in channels {
        let unsubscribe = state.registry.preview_unsubscribe(category, *channel);
        let context = RenderContext {
            topic: topic.map_or(name, |t| t.name.as_str()),
            category,
            notification_id: PREVIEW_NOTIFICATION_ID,
            data,
            secret,
            recipient_name: Some("Ada Lovelace"),
            timezone: Some("UTC"),
            unsubscribe: unsubscribe.as_deref(),
        };
        match state.registry.render(name, *channel, locale, &context) {
            Ok(rendered) => {
                out.insert(channel.as_str().to_owned(), rendered.to_json());
            }
            Err(error) => {
                if let Ok(detail) =
                    FieldError::new(format!("/channels/{}", channel.as_str()), error.code)
                {
                    details.push(detail);
                }
            }
        }
    }
    if details.is_empty() {
        Ok(out)
    } else {
        Err(ErrorReply::new(Failure::permanent("render_failed")).details(details))
    }
}

fn resolve_locale(state: &RouteState, requested: Option<&str>) -> Result<String, ErrorReply> {
    match requested {
        Some(locale) => {
            Locale::new(locale).map_err(ErrorReply::new)?;
            Ok(state.registry.resolve_locale([locale]))
        }
        None => Ok(state.registry.default_locale().to_owned()),
    }
}

async fn preview(
    State(state): State<RouteState>,
    principal: Option<Extension<Authenticated>>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(reply) = preview_gate(&state, principal.as_deref()) {
        return reply.into_response();
    }
    let Some(info) = state.registry.get(&name) else {
        return reply("not_found");
    };
    let request: PreviewRequest = if body.is_empty() {
        PreviewRequest {
            locale: None,
            data: None,
            channels: None,
        }
    } else {
        match serde_json::from_slice(&body) {
            Ok(request) => request,
            Err(_) => return reply("invalid_input"),
        }
    };
    let locale = match resolve_locale(&state, request.locale.as_deref()) {
        Ok(locale) => locale,
        Err(reply) => return reply.into_response(),
    };
    let available = info.channels(&state.registry, &locale);
    let channels = match &request.channels {
        None => available,
        Some(names) => {
            let mut chosen = Vec::new();
            let mut details = Vec::new();
            for (index, name) in names.iter().take(4).enumerate() {
                match topics::channel(name) {
                    Some(channel) if available.contains(&channel) => chosen.push(channel),
                    Some(_) => details.push((index, "channel_not_available")),
                    None => details.push((index, "invalid_channel")),
                }
            }
            if names.len() > 4 {
                details.push((4, "too_many_items"));
            }
            if !details.is_empty() {
                let details = details
                    .into_iter()
                    .filter_map(|(i, code)| FieldError::new(format!("/channels/{i}"), code).ok())
                    .collect();
                return ErrorReply::new(Failure::permanent("channel_not_allowed"))
                    .details(details)
                    .into_response();
            }
            chosen
        }
    };
    let (data, secret) = match request.data {
        None => (info.preview.clone(), info.preview_secret.clone()),
        Some(mut data) => {
            let secret = match data.remove("secret") {
                Some(Value::Object(secret)) => secret,
                Some(_) => return reply("reserved_data_key"),
                None => Map::new(),
            };
            if RESERVED_KEYS.iter().any(|k| data.contains_key(*k)) {
                return reply("reserved_data_key");
            }
            let size = Value::Object(data.clone()).to_string().len()
                + Value::Object(secret.clone()).to_string().len();
            if size > state.settings.limits.max_data_bytes {
                return reply("data_too_large");
            }
            let missing: Vec<FieldError> = info
                .required
                .iter()
                .filter(|k| !data.contains_key(*k))
                .filter_map(|k| FieldError::new(format!("/data/{}", pointer(k)), "required").ok())
                .collect();
            if !missing.is_empty() {
                return ErrorReply::new(Failure::permanent("render_failed"))
                    .details(missing)
                    .into_response();
            }
            (data, secret)
        }
    };
    match render_all(&state, &name, &locale, &channels, &data, &secret) {
        Ok(rendered) => {
            // Previews may hold secret data; nothing caches them.
            let mut response = Json(Value::Object(rendered)).into_response();
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            response
        }
        Err(reply) => reply.into_response(),
    }
}

/// A JSON pointer token: `~` and `/` escaped.
fn pointer(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}

fn query_pairs(query: Option<&str>) -> Vec<(String, String)> {
    query
        .map(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .take(16)
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect()
        })
        .unwrap_or_default()
}

async fn preview_html(
    State(state): State<RouteState>,
    principal: Option<Extension<Authenticated>>,
    Path(name): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    if let Err(reply) = preview_gate(&state, principal.as_deref()) {
        return reply.into_response();
    }
    let Some(info) = state.registry.get(&name) else {
        return reply("not_found");
    };
    let pairs = query_pairs(query.as_deref());
    let requested = pairs
        .iter()
        .find(|(k, _)| k == "locale")
        .map(|(_, v)| v.as_str());
    let locale = match resolve_locale(&state, requested) {
        Ok(locale) => locale,
        Err(reply) => return reply.into_response(),
    };
    if !info
        .channels(&state.registry, &locale)
        .contains(&Channel::Email)
    {
        return reply("not_found");
    }
    let rendered = match render_all(
        &state,
        &name,
        &locale,
        &[Channel::Email],
        &info.preview,
        &info.preview_secret,
    ) {
        Ok(rendered) => rendered,
        Err(reply) => return reply.into_response(),
    };
    let email = &rendered["email"];
    let html = match email["html"].as_str() {
        Some(html) => html.to_owned(),
        None => format!(
            "<!doctype html><pre>{}</pre>",
            escape(email["text"].as_str().unwrap_or_default())
        ),
    };
    let mut response = html.into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(PREVIEW_CSP),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Loopback only, and only while a capture driver is active; 404 otherwise.
/// Whether a `Host` header names this machine: `localhost`, `127.0.0.1`
/// or `[::1]`, with an optional port. A page on another origin that
/// rebinds its name to 127.0.0.1 still sends its own name.
pub fn loopback_host_header(value: &str) -> bool {
    let host = if let Some(rest) = value.strip_prefix('[') {
        match rest.split_once(']') {
            Some((inner, port))
                if port.is_empty() || port.strip_prefix(':').is_some_and(valid_port) =>
            {
                return inner == "::1";
            }
            _ => return false,
        }
    } else {
        match value.rsplit_once(':') {
            Some((host, port)) if valid_port(port) => host,
            Some(_) => return false,
            None => value,
        }
    };
    host.eq_ignore_ascii_case("localhost") || host == "127.0.0.1"
}
fn valid_port(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) && text.parse::<u16>().is_ok()
}

async fn captures(
    State(state): State<RouteState>,
    client: Option<Extension<ClientInfo>>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    // The peer itself, not a forwarded address: a trusted proxy may relay
    // requests from anywhere.
    let loopback = client.as_ref().is_some_and(|c| {
        !c.via_trusted_proxy && c.address.is_some_and(|a| a.to_canonical().is_loopback())
    });
    let host = headers
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .is_some_and(loopback_host_header);
    let Some(driver) = state.captures.as_ref().filter(|_| loopback && host) else {
        return reply("not_found");
    };
    let mut channel = None;
    let mut limit = 20;
    for (key, value) in query_pairs(query.as_deref()) {
        match key.as_str() {
            "channel" => match topics::channel(&value) {
                Some(c @ (Channel::Email | Channel::Sms)) => channel = Some(c),
                _ => return reply("invalid_input"),
            },
            "limit" => match value.parse::<u32>() {
                Ok(n) if (1..=MAX_CAPTURE_PAGE).contains(&n) => limit = n,
                _ => return reply("invalid_input"),
            },
            _ => return reply("invalid_input"),
        }
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    match driver.list(channel, limit, deadline).await {
        Ok(items) => Json(items.iter().map(|c| c.to_json()).collect::<Vec<_>>()).into_response(),
        Err(failure) => ErrorReply::new(failure).into_response(),
    }
}
