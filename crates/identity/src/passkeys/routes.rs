//! The passkey routes of section 4. The verification routes take bodies of
//! up to 64 KiB (`413 body_limit` beyond); every other one keeps 16 KiB.
use super::MAX_VERIFY_BODY;
use crate::{
    core::{ApiError, Core},
    http::{Ctx, Fields, Shared, finish, json, json_within, required, respond},
};
use axum::{
    Router,
    extract::{Path, Request, State},
    response::Response,
    routing::{delete, get, post},
};
use merezha_platform::RouteBodyLimit;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tracing::Instrument;

/// The verification routes, relative to `/v1/identity`.
pub const VERIFY_ROUTES: [&str; 3] = [
    "/passkeys/registrations/verify",
    "/passkeys/authentications/verify",
    "/session/reauthenticate/passkey/verify",
];

/// The edge's body limits for [`VERIFY_ROUTES`] (`Module::body_limits`).
pub fn body_limits() -> Vec<RouteBodyLimit> {
    VERIFY_ROUTES
        .iter()
        .map(|path| RouteBodyLimit {
            method: axum::http::Method::POST,
            path,
            limit: MAX_VERIFY_BODY,
        })
        .collect()
}

pub(crate) fn routes() -> Router<Arc<Core>> {
    Router::new()
        .route(
            "/passkeys/registrations/options",
            post(registration_options),
        )
        .route("/passkeys/registrations/verify", post(registration_verify))
        .route(
            "/passkeys/authentications/options",
            post(authentication_options),
        )
        .route(
            "/passkeys/authentications/verify",
            post(authentication_verify),
        )
        .route(
            "/session/reauthenticate/passkey/options",
            post(reauth_options),
        )
        .route(
            "/session/reauthenticate/passkey/verify",
            post(reauth_verify),
        )
        .route("/me/passkeys", get(list))
        .route("/me/passkeys/{id}", delete(remove).patch(rename))
}

/// The WebAuthn response in `credential`, parsed into its type.
fn credential<T: DeserializeOwned>(f: &mut Fields) -> Option<T> {
    match f.take("credential") {
        Some(value @ serde_json::Value::Object(_)) => match serde_json::from_value(value) {
            Ok(parsed) => Some(parsed),
            Err(_) => {
                f.problem("credential", "invalid");
                None
            }
        },
        None | Some(serde_json::Value::Null) => {
            f.problem("credential", "required");
            None
        }
        Some(_) => {
            f.problem("credential", "invalid");
            None
        }
    }
}

/// An optional label of 1 to 64 scalar values without control characters.
fn label(f: &mut Fields) -> Option<Option<String>> {
    let label = f.nullable("label")?;
    match label {
        None => Some(None),
        Some(text) => {
            let text =
                unicode_normalization::UnicodeNormalization::nfc(text.trim()).collect::<String>();
            let count = text.chars().count();
            if (1..=super::MAX_LABEL).contains(&count) && !text.chars().any(char::is_control) {
                Some(Some(text))
            } else {
                f.problem("label", "invalid");
                None
            }
        }
    }
}

/// A body that may be empty, or an empty JSON object.
async fn bodyless(request: Request) -> Result<Ctx, ApiError> {
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, crate::http::MAX_BODY_BYTES)
        .await
        .map_err(|_| ApiError::code("body_limit"))?;
    if !bytes.is_empty()
        && !matches!(serde_json::from_slice::<serde_json::Value>(&bytes),
                     Ok(serde_json::Value::Object(map)) if map.is_empty())
    {
        return Err(ApiError::invalid(vec![("", "unknown")]));
    }
    Ok(Ctx::new(parts))
}

async fn registration_options(State(core): Shared, request: Request) -> Response {
    let result = async {
        let ctx = bodyless(request).await?;
        let caller = ctx.session()?;
        core.passkey_registration_options(&ctx.client, &caller)
            .await
    }
    .await;
    respond(&core, result)
}

async fn registration_verify(State(core): Shared, request: Request) -> Response {
    let result = async {
        let (ctx, mut fields) = json_within(request, MAX_VERIFY_BODY).await?;
        let caller = ctx.session()?;
        let f = &mut fields;
        let ceremony = f.string("ceremony_id");
        let credential = credential(f);
        let label = label(f).flatten();
        finish(f)?;
        core.passkey_registration_verify(
            &ctx.client,
            &caller,
            &required(ceremony)?,
            required(credential)?,
            label,
        )
        .await
    }
    .await;
    respond(&core, result)
}

async fn authentication_options(State(core): Shared, request: Request) -> Response {
    let result = async {
        let (ctx, mut fields) = json(request).await?;
        ctx.anonymous_origin(&core)?;
        let f = &mut fields;
        let email = if f.has("email") {
            match f.nullable("email") {
                Some(Some(text)) => match crate::normalize::email(&text) {
                    Ok(email) => Some(email),
                    Err(code) => {
                        f.problem("email", code);
                        None
                    }
                },
                _ => None,
            }
        } else {
            None
        };
        finish(f)?;
        core.passkey_authentication_options(&ctx.client, email)
            .await
    }
    .await;
    respond(&core, result)
}

async fn authentication_verify(State(core): Shared, request: Request) -> Response {
    let result = async {
        let (ctx, mut fields) = json_within(request, MAX_VERIFY_BODY).await?;
        ctx.anonymous_origin(&core)?;
        let f = &mut fields;
        let ceremony = f.string("ceremony_id");
        let credential = credential(f);
        let transport = f.transport();
        finish(f)?;
        core.passkey_authentication_verify(
            &ctx.client,
            &required(ceremony)?,
            required(credential)?,
            transport,
        )
        .await
    }
    .instrument(tracing::info_span!(
        target: merezha_telemetry::TARGET,
        "identity login.passkey",
        otel.name = "identity login.passkey"
    ))
    .await;
    respond(&core, result)
}

async fn reauth_options(State(core): Shared, request: Request) -> Response {
    let result = async {
        let ctx = bodyless(request).await?;
        let caller = ctx.session()?;
        core.passkey_reauth_options(&ctx.client, &caller).await
    }
    .await;
    respond(&core, result)
}

async fn reauth_verify(State(core): Shared, request: Request) -> Response {
    let result = async {
        let (ctx, mut fields) = json_within(request, MAX_VERIFY_BODY).await?;
        let caller = ctx.session()?;
        let f = &mut fields;
        let ceremony = f.string("ceremony_id");
        let credential = credential(f);
        finish(f)?;
        core.passkey_reauth_verify(
            &ctx.client,
            &caller,
            &required(ceremony)?,
            required(credential)?,
        )
        .await
    }
    .await;
    respond(&core, result)
}

async fn list(State(core): Shared, request: Request) -> Response {
    let (parts, _) = request.into_parts();
    let ctx = Ctx::new(parts);
    let result = async { core.list_passkeys(&ctx.session()?).await }.await;
    respond(&core, result)
}

async fn rename(State(core): Shared, Path(id): Path<String>, request: Request) -> Response {
    let result = async {
        let (ctx, mut fields) = json(request).await?;
        let caller = ctx.session()?;
        let f = &mut fields;
        if !f.has("label") {
            f.problem("label", "required");
        }
        let label = label(f);
        finish(f)?;
        core.rename_passkey(&ctx.client, &caller, &id, required(label)?)
            .await
    }
    .await;
    respond(&core, result)
}

async fn remove(State(core): Shared, Path(id): Path<String>, request: Request) -> Response {
    let (parts, _) = request.into_parts();
    let ctx = Ctx::new(parts);
    let result = async {
        let caller = ctx.session()?;
        core.remove_passkey(&ctx.client, &caller, &id).await
    }
    .await;
    respond(&core, result)
}
