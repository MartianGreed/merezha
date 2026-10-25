//! The OAuth routes of section 4: `GET oauth/providers`,
//! `POST oauth/{provider}/start`, `GET oauth/{provider}/callback`,
//! `GET me/oauth-identities` and `DELETE me/oauth-identities/{id}`. The
//! callback is a browser navigation, so it answers `303 See Other`, never
//! JSON.
use super::flows::{CallbackQuery, Intent};
use crate::{
    core::Core,
    http::{Ctx, Shared, finish, json, respond},
    normalize,
    session::cookie,
};
use axum::{
    Router,
    extract::{Path, Request, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use secrecy::ExposeSecret;
use std::sync::Arc;
use tracing::Instrument;

pub(crate) fn routes() -> Router<Arc<Core>> {
    Router::new()
        .route("/oauth/providers", get(providers))
        .route("/oauth/{provider}/start", post(start))
        .route("/oauth/{provider}/callback", get(callback))
        .route("/me/oauth-identities", get(identities))
        .route("/me/oauth-identities/{id}", delete(unlink))
}

async fn providers(State(core): Shared) -> Response {
    respond(&core, Ok(core.oauth_providers()))
}

async fn start(State(core): Shared, Path(provider): Path<String>, request: Request) -> Response {
    let result = async {
        let (ctx, mut fields) = json(request).await?;
        let f = &mut fields;
        let intent = match f.string("intent") {
            Some(text) => match Intent::parse(&text) {
                Some(intent) => Some(intent),
                None => {
                    f.problem("intent", "invalid");
                    None
                }
            },
            None => None,
        };
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
        let intent = crate::http::required(intent)?;
        // A sign-in is anonymous (the Origin check stops login CSRF); a
        // link is the signed-in caller's, whose cookie passed the edge's
        // Origin and CSRF checks.
        let caller = match intent {
            Intent::SignIn => {
                ctx.anonymous_origin(&core)?;
                ctx.caller
            }
            Intent::Link => Some(ctx.session()?),
        };
        let presented = cookie(&ctx.parts.headers, core.settings.oauth_cookie_name());
        core.oauth_start(&ctx.client, caller, &provider, intent, return_to, presented)
            .await
    }
    .instrument(tracing::info_span!(
        target: merezha_telemetry::TARGET,
        "identity login.oauth",
        otel.name = "identity login.oauth"
    ))
    .await;
    respond(&core, result)
}

async fn callback(State(core): Shared, Path(provider): Path<String>, request: Request) -> Response {
    let (parts, _) = request.into_parts();
    let ctx = Ctx::new(parts);
    let query = CallbackQuery::parse(ctx.parts.uri.query());
    let binding = cookie(&ctx.parts.headers, core.settings.oauth_cookie_name());
    let registration = cookie(&ctx.parts.headers, core.settings.binding_cookie_name());
    let outcome = core
        .oauth_callback(&ctx.client, &provider, query, binding, registration)
        .instrument(tracing::info_span!(
            target: merezha_telemetry::TARGET,
            "identity oauth.callback",
            otel.name = "identity oauth.callback"
        ))
        .await;
    let mut response = StatusCode::SEE_OTHER.into_response();
    let headers = response.headers_mut();
    if let Ok(location) = HeaderValue::from_str(&outcome.location) {
        headers.insert(header::LOCATION, location);
    }
    // The code and state stay out of the next page's `Referer`.
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    // Only this browser's own callback clears its binding, once no other
    // tab's flow is pending; a forged callback leaves it alone.
    if outcome.clear_binding
        && let Some(clear) = core.oauth_binding_cookie(None)
    {
        headers.append(header::SET_COOKIE, clear);
    }
    if let Some(session) = outcome.session
        && let Some(set) = core.set_cookie(
            session.token.expose_secret(),
            session.row.absolute_expires_at,
        )
    {
        headers.append(header::SET_COOKIE, set);
    }
    if let Some(binding) = outcome.verify_binding
        && let Some(set) = core.binding_cookie(Some(binding.expose_secret()))
    {
        headers.append(header::SET_COOKIE, set);
    }
    response
}

async fn identities(State(core): Shared, request: Request) -> Response {
    let (parts, _) = request.into_parts();
    let ctx = Ctx::new(parts);
    let result = async { core.oauth_identities(&ctx.session()?).await }.await;
    respond(&core, result)
}

async fn unlink(State(core): Shared, Path(id): Path<String>, request: Request) -> Response {
    let (parts, _) = request.into_parts();
    let ctx = Ctx::new(parts);
    let result = async {
        let caller = ctx.session()?;
        core.unlink_identity(&ctx.client, &caller, &id).await
    }
    .await;
    respond(&core, result)
}
