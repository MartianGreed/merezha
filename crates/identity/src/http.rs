//! The PR 1 routes under `/v1/identity` (section 4). Bodies are JSON
//! (`415 unsupported_media_type` otherwise) of at most 16 KiB
//! (`413 body_limit`); every response carries `Cache-Control: no-store`;
//! anonymous unsafe requests pass the Origin check; errors never echo
//! input.
use crate::{
    core::{ApiError, Client, Core, Transport},
    flows::{Binding, Caller, CookieAction, Reply},
    normalize::{self, Email},
    password::Password,
    session::caller,
};
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use merezha_platform::{
    ErrorReply,
    edge::{Authenticated, ClientInfo, RequestId},
};
use secrecy::ExposeSecret;
use serde_json::{Map, Value};
use std::sync::Arc;

/// Largest request body.
pub const MAX_BODY_BYTES: usize = 16 * 1024;
/// `limit` bounds of `GET /me/security-events`.
const DEFAULT_PAGE: i64 = 20;
const MAX_PAGE: i64 = 100;

pub(crate) type Shared = State<Arc<Core>>;

/// The identity router, nested under `/v1/identity` by the host.
pub fn router(core: Arc<Core>) -> Router {
    Router::new()
        .route("/registrations", post(register))
        .route("/email-verifications", post(resend_verification))
        .route("/email-verifications/confirm", post(confirm_verification))
        .route("/sessions", post(sign_in).get(list_sessions))
        .route("/session", get(current_session).delete(sign_out))
        .route("/session/reauthenticate", post(reauthenticate))
        .route("/sessions/revoke-all", post(revoke_all))
        .route("/sessions/{session_id}", delete(revoke_session))
        .route("/magic-links", post(request_magic_link))
        .route("/magic-links/consume", post(consume_magic_link))
        .route("/password-resets", post(request_password_reset))
        .route("/password-resets/complete", post(complete_password_reset))
        .route("/me", get(me).patch(update_me).delete(delete_me))
        .route("/me/password", axum::routing::put(change_password))
        .route("/me/email-changes", post(request_email_change))
        .route("/me/email-changes/confirm", post(confirm_email_change))
        .route("/me/security-events", get(security_events))
        .merge(crate::admin_http::routes())
        .merge(crate::oauth::routes::routes())
        .merge(crate::passkey_routes())
        .fallback(|| async { failure(ApiError::code("not_found")) })
        .method_not_allowed_fallback(|| async { failure(ApiError::code("method_not_allowed")) })
        .layer(axum::middleware::map_response(no_store))
        .with_state(core)
}

async fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

pub(crate) fn failure(error: ApiError) -> Response {
    let mut reply = ErrorReply::new(error.failure).details(error.details);
    if let Some(after) = error.retry_after {
        reply = reply.retry_after(after);
    }
    reply.into_response()
}

/// A request's facts: the client, the principal and the headers.
pub(crate) struct Ctx {
    pub(crate) parts: Parts,
    pub(crate) client: Client,
    pub(crate) caller: Option<Caller>,
    pub(crate) authenticated: bool,
}
impl Ctx {
    pub(crate) fn new(parts: Parts) -> Self {
        let principal = parts
            .extensions
            .get::<Authenticated>()
            .and_then(|a| a.0.clone());
        let client = Client {
            ip: parts.extensions.get::<ClientInfo>().and_then(|c| c.address),
            user_agent: Client::user_agent(
                parts
                    .headers
                    .get(header::USER_AGENT)
                    .and_then(|v| v.to_str().ok()),
            ),
            request_id: parts.extensions.get::<RequestId>().map(|r| r.0.clone()),
        };
        Self {
            caller: caller(principal.as_deref()),
            authenticated: principal.is_some(),
            parts,
            client,
        }
    }
    /// The signed-in caller, or `401 unauthenticated`.
    pub(crate) fn session(&self) -> Result<Caller, ApiError> {
        self.caller.ok_or_else(|| ApiError::code("unauthenticated"))
    }
    /// The Origin check of anonymous unsafe requests (login CSRF);
    /// authenticated ones passed the edge's cookie checks already.
    pub(crate) fn anonymous_origin(&self, core: &Core) -> Result<(), ApiError> {
        if self.authenticated || core.origin_allowed(&self.parts.headers) {
            Ok(())
        } else {
            Err(ApiError::code("origin_rejected"))
        }
    }
}

/// A JSON object body being validated: every problem becomes one detail.
pub(crate) struct Fields {
    map: Map<String, Value>,
    details: Vec<(String, &'static str)>,
}
impl Fields {
    pub(crate) fn has(&self, name: &str) -> bool {
        self.map.contains_key(name)
    }
    pub(crate) fn take(&mut self, name: &str) -> Option<Value> {
        self.map.remove(name)
    }
    pub(crate) fn problem(&mut self, name: &str, code: &'static str) {
        self.details.push((format!("/{name}"), code));
    }
    /// A required string.
    pub(crate) fn string(&mut self, name: &str) -> Option<String> {
        match self.take(name) {
            Some(Value::String(s)) => Some(s),
            None | Some(Value::Null) => {
                self.problem(name, "required");
                None
            }
            Some(_) => {
                self.problem(name, "invalid");
                None
            }
        }
    }
    /// An optional string: absent is `None`, `null` is `Some(None)`.
    pub(crate) fn nullable(&mut self, name: &str) -> Option<Option<String>> {
        match self.take(name) {
            None => None,
            Some(Value::Null) => Some(None),
            Some(Value::String(s)) => Some(Some(s)),
            Some(_) => {
                self.problem(name, "invalid");
                None
            }
        }
    }
    pub(crate) fn email(&mut self, name: &str) -> Option<Email> {
        let text = self.string(name)?;
        match normalize::email(&text) {
            Ok(email) => Some(email),
            Err(code) => {
                self.problem(name, code);
                None
            }
        }
    }
    fn password(&mut self, name: &str) -> Option<Password> {
        let text = zeroize::Zeroizing::new(self.string(name)?);
        match Password::new(&text) {
            Ok(password) => Some(password),
            Err(code) => {
                self.problem(name, code);
                None
            }
        }
    }
    pub(crate) fn transport(&mut self) -> Option<Transport> {
        let text = self.nullable("transport").flatten()?;
        let parsed = Transport::parse(&text);
        if parsed.is_none() {
            self.problem("transport", "invalid");
        }
        parsed
    }
    pub(crate) fn boolean(&mut self, name: &str) -> Option<bool> {
        match self.take(name) {
            None | Some(Value::Null) => None,
            Some(Value::Bool(b)) => Some(b),
            Some(_) => {
                self.problem(name, "invalid");
                None
            }
        }
    }
    /// Fail with every problem, unknown fields included.
    fn finish(mut self) -> Result<(), ApiError> {
        let unknown: Vec<String> = self.map.keys().cloned().collect();
        for name in unknown {
            self.problem(&name, "unknown");
        }
        if self.details.is_empty() {
            return Ok(());
        }
        let details = self
            .details
            .iter()
            .map(|(path, code)| (path.as_str(), *code))
            .collect();
        Err(ApiError::invalid(details))
    }
}

/// Split the request, require JSON within the size limit and parse it as
/// an object.
pub(crate) async fn json(request: Request) -> Result<(Ctx, Fields), ApiError> {
    json_within(request, MAX_BODY_BYTES).await
}
/// [`json`] with a body of at most `limit` bytes.
pub(crate) async fn json_within(request: Request, limit: usize) -> Result<(Ctx, Fields), ApiError> {
    let (parts, body) = request.into_parts();
    let ctx = Ctx::new(parts);
    let json_type = ctx
        .parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/json"));
    if !json_type {
        return Err(ApiError::code("unsupported_media_type"));
    }
    let declared = ctx
        .parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());
    if declared.is_some_and(|n| n > limit) {
        return Err(ApiError::code("body_limit"));
    }
    let bytes = axum::body::to_bytes(body, limit)
        .await
        .map_err(|_| ApiError::code("body_limit"))?;
    let map = match serde_json::from_slice::<Value>(&bytes) {
        Ok(Value::Object(map)) => map,
        _ => return Err(ApiError::code("invalid_input")),
    };
    Ok((
        ctx,
        Fields {
            map,
            details: Vec::new(),
        },
    ))
}

pub(crate) fn respond(core: &Core, result: Result<Reply, ApiError>) -> Response {
    let reply = match result {
        Ok(reply) => reply,
        Err(error) => return failure(error),
    };
    let status = StatusCode::from_u16(reply.status).unwrap_or(StatusCode::OK);
    let mut response = match reply.body {
        Some(body) => (
            status,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            serde_json::to_vec(&body).unwrap_or_default(),
        )
            .into_response(),
        None => (status, Body::empty()).into_response(),
    };
    let cookie = match reply.cookie {
        CookieAction::Keep => None,
        CookieAction::Set { token, expires } => core.set_cookie(token.expose_secret(), expires),
        CookieAction::Clear => Some(core.clear_cookie()),
    };
    if let Some(cookie) = cookie {
        response.headers_mut().append(header::SET_COOKIE, cookie);
    }
    let binding = match &reply.binding {
        None => None,
        Some(Binding::Set(value)) => core.binding_cookie(Some(value.expose_secret())),
        Some(Binding::Clear) => core.binding_cookie(None),
    };
    if let Some(binding) = binding {
        response.headers_mut().append(header::SET_COOKIE, binding);
    }
    let oauth = match &reply.oauth_binding {
        None => None,
        Some(Binding::Set(value)) => core.oauth_binding_cookie(Some(value.expose_secret())),
        Some(Binding::Clear) => core.oauth_binding_cookie(None),
    };
    if let Some(oauth) = oauth {
        response.headers_mut().append(header::SET_COOKIE, oauth);
    }
    response
}

/// Run a JSON handler body: parse, check, call.
macro_rules! anonymous {
    ($core:ident, $request:ident, |$ctx:ident, $fields:ident| $body:expr) => {{
        let result = async {
            let (ctx, mut fields) = json($request).await?;
            ctx.anonymous_origin(&$core)?;
            let $ctx = ctx;
            let $fields = &mut fields;
            $body
        }
        .await;
        respond(&$core, result)
    }};
}

async fn register(State(core): Shared, request: Request) -> Response {
    anonymous!(core, request, |ctx, f| {
        let email = f.email("email");
        let password = f.password("password");
        let display_name = match f.nullable("display_name").flatten() {
            Some(name) => match normalize::display_name(&name) {
                Ok(name) => Some(name),
                Err(code) => {
                    f.problem("display_name", code);
                    None
                }
            },
            None => None,
        };
        finish(f)?;
        let binding =
            crate::session::cookie(&ctx.parts.headers, core.settings.binding_cookie_name());
        core.register(
            &ctx.client,
            required(email)?,
            required(password)?,
            display_name,
            binding,
        )
        .await
    })
}

async fn resend_verification(State(core): Shared, request: Request) -> Response {
    anonymous!(core, request, |ctx, f| {
        let email = f.email("email");
        finish(f)?;
        core.resend_verification(&ctx.client, required(email)?)
            .await
    })
}

async fn confirm_verification(State(core): Shared, request: Request) -> Response {
    anonymous!(core, request, |ctx, f| {
        let token = f.string("token");
        let transport = f.transport();
        finish(f)?;
        let binding =
            crate::session::cookie(&ctx.parts.headers, core.settings.binding_cookie_name());
        core.confirm_verification(&ctx.client, &required(token)?, transport, binding)
            .await
    })
}

async fn sign_in(State(core): Shared, request: Request) -> Response {
    anonymous!(core, request, |ctx, f| {
        let email = f.email("email");
        let password = f.password("password");
        let transport = f.transport();
        finish(f)?;
        core.sign_in(
            &ctx.client,
            required(email)?,
            required(password)?,
            transport,
        )
        .await
    })
}

async fn request_magic_link(State(core): Shared, request: Request) -> Response {
    anonymous!(core, request, |ctx, f| {
        let email = f.email("email");
        let return_to = match f.nullable("return_to").flatten() {
            Some(path) => match normalize::return_to(&path) {
                Ok(path) => Some(path),
                Err(code) => {
                    f.problem("return_to", code);
                    None
                }
            },
            None => None,
        };
        finish(f)?;
        core.request_magic_link(&ctx.client, required(email)?, return_to)
            .await
    })
}

async fn consume_magic_link(State(core): Shared, request: Request) -> Response {
    anonymous!(core, request, |ctx, f| {
        let token = f.string("token");
        let transport = f.transport();
        finish(f)?;
        core.consume_magic_link(&ctx.client, &required(token)?, transport)
            .await
    })
}

async fn request_password_reset(State(core): Shared, request: Request) -> Response {
    anonymous!(core, request, |ctx, f| {
        let email = f.email("email");
        finish(f)?;
        core.request_password_reset(&ctx.client, required(email)?)
            .await
    })
}

async fn complete_password_reset(State(core): Shared, request: Request) -> Response {
    anonymous!(core, request, |ctx, f| {
        let token = f.string("token");
        let password = f.password("new_password");
        let transport = f.transport();
        finish(f)?;
        core.complete_password_reset(
            &ctx.client,
            &required(token)?,
            required(password)?,
            transport,
        )
        .await
    })
}

async fn confirm_email_change(State(core): Shared, request: Request) -> Response {
    anonymous!(core, request, |ctx, f| {
        let token = f.string("token");
        finish(f)?;
        core.confirm_email_change(&ctx.client, ctx.caller.as_ref(), &required(token)?)
            .await
    })
}

/// A session route with a JSON body.
macro_rules! signed_in {
    ($core:ident, $request:ident, |$ctx:ident, $caller:ident, $fields:ident| $body:expr) => {{
        let result = async {
            let (ctx, mut fields) = json($request).await?;
            let $caller = ctx.session()?;
            let $ctx = ctx;
            let $fields = &mut fields;
            $body
        }
        .await;
        respond(&$core, result)
    }};
}
/// A session route without a body.
macro_rules! bodyless {
    ($core:ident, $request:ident, |$ctx:ident, $caller:ident| $body:expr) => {{
        let result = async {
            let (parts, _) = $request.into_parts();
            let $ctx = Ctx::new(parts);
            let $caller = $ctx.session()?;
            $body
        }
        .await;
        respond(&$core, result)
    }};
}

async fn current_session(State(core): Shared, request: Request) -> Response {
    bodyless!(core, request, |ctx, caller| {
        let _ = &ctx;
        core.current_session(&caller).await
    })
}

async fn sign_out(State(core): Shared, request: Request) -> Response {
    bodyless!(core, request, |ctx, caller| core
        .sign_out(&ctx.client, &caller)
        .await)
}

async fn reauthenticate(State(core): Shared, request: Request) -> Response {
    signed_in!(core, request, |ctx, caller, f| {
        let password = f.password("password");
        finish(f)?;
        core.reauthenticate(&ctx.client, &caller, required(password)?)
            .await
    })
}

async fn list_sessions(State(core): Shared, request: Request) -> Response {
    bodyless!(core, request, |ctx, caller| {
        let _ = &ctx;
        core.list_sessions(&caller).await
    })
}

async fn revoke_session(
    State(core): Shared,
    axum::extract::Path(id): axum::extract::Path<String>,
    request: Request,
) -> Response {
    bodyless!(core, request, |ctx, caller| core
        .revoke_session(&ctx.client, &caller, &id)
        .await)
}

async fn revoke_all(State(core): Shared, request: Request) -> Response {
    signed_in!(core, request, |ctx, caller, f| {
        let keep = f.boolean("keep_current").unwrap_or(false);
        finish(f)?;
        core.revoke_all(&ctx.client, &caller, keep).await
    })
}

/// `GET /me`: `user` access, a session or a key whose owner is a user.
async fn me(State(core): Shared, request: Request) -> Response {
    let (parts, _) = request.into_parts();
    let user = parts
        .extensions
        .get::<Authenticated>()
        .and_then(|a| a.0.as_deref())
        .and_then(crate::grants::principal_user);
    let result = match user {
        Some(user) => core.me(user).await,
        None => Err(ApiError::code("unauthenticated")),
    };
    respond(&core, result)
}

async fn update_me(State(core): Shared, request: Request) -> Response {
    signed_in!(core, request, |ctx, caller, f| {
        let _ = &ctx;
        let display_name = match f.nullable("display_name") {
            Some(Some(name)) => match normalize::display_name(&name) {
                Ok(name) => Some(Some(name)),
                Err(code) => {
                    f.problem("display_name", code);
                    None
                }
            },
            other => other,
        };
        let locale = match f.nullable("locale") {
            Some(Some(tag)) => match normalize::locale(&tag) {
                Ok(tag) => Some(Some(tag)),
                Err(code) => {
                    f.problem("locale", code);
                    None
                }
            },
            other => other,
        };
        finish(f)?;
        core.update_me(&caller, display_name, locale).await
    })
}

async fn change_password(State(core): Shared, request: Request) -> Response {
    signed_in!(core, request, |ctx, caller, f| {
        let password = f.password("new_password");
        finish(f)?;
        core.change_password(&ctx.client, &caller, required(password)?)
            .await
    })
}

async fn request_email_change(State(core): Shared, request: Request) -> Response {
    signed_in!(core, request, |ctx, caller, f| {
        let email = f.email("new_email");
        finish(f)?;
        core.request_email_change(&ctx.client, &caller, required(email)?)
            .await
    })
}

async fn delete_me(State(core): Shared, request: Request) -> Response {
    signed_in!(core, request, |ctx, caller, f| {
        let email = f.email("confirm_email");
        finish(f)?;
        core.delete_me(&ctx.client, &caller, required(email)?).await
    })
}

async fn security_events(State(core): Shared, request: Request) -> Response {
    bodyless!(core, request, |ctx, caller| {
        let (cursor, limit) = page(ctx.parts.uri.query())?;
        core.security_events(&caller, cursor, limit).await
    })
}

/// `?cursor&limit` of the security event listing.
fn page(query: Option<&str>) -> Result<(Option<uuid::Uuid>, i64), ApiError> {
    let mut cursor = None;
    let mut limit = DEFAULT_PAGE;
    let mut details = Vec::new();
    for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            "cursor" => match uuid::Uuid::try_parse(value) {
                Ok(id) => cursor = Some(id),
                Err(_) => details.push(("/cursor", "invalid")),
            },
            "limit" => match value.parse::<i64>() {
                Ok(n) if (1..=MAX_PAGE).contains(&n) => limit = n,
                _ => details.push(("/limit", "invalid")),
            },
            _ => details.push(("/query", "unknown")),
        }
    }
    if details.is_empty() {
        Ok((cursor, limit))
    } else {
        Err(ApiError::invalid(details))
    }
}

pub(crate) fn finish(fields: &mut Fields) -> Result<(), ApiError> {
    std::mem::replace(
        fields,
        Fields {
            map: Map::new(),
            details: Vec::new(),
        },
    )
    .finish()
}
/// A field whose absence `finish` already reported.
pub(crate) fn required<T>(value: Option<T>) -> Result<T, ApiError> {
    value.ok_or_else(|| ApiError::invalid(vec![]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listing_queries_are_bounded() {
        assert_eq!(page(None).unwrap(), (None, DEFAULT_PAGE));
        let id = uuid::Uuid::from_u128(7);
        assert_eq!(
            page(Some(&format!("cursor={}&limit=5", id.hyphenated()))).unwrap(),
            (Some(id), 5)
        );
        for bad in ["limit=0", "limit=101", "limit=x", "cursor=nope", "other=1"] {
            assert_eq!(
                page(Some(bad)).unwrap_err().failure.code,
                "validation_failed",
                "{bad}"
            );
        }
    }
}
