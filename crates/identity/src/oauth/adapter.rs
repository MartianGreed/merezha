//! The provider adapter: `openidconnect` for Google and generic OIDC,
//! `oauth2` plus two API calls for GitHub, over the platform's outbound
//! client. This is the only module that sees `chrono` (R14): the two
//! crates require it in their claim types, and nothing it returns carries
//! one.
//!
//! The HTTP client follows no redirects, retries nothing, waits at most
//! `oauth.http_timeout` per call and reads at most 1 MiB (section 6). ID
//! tokens are checked against the provider's key set (refetched once on an
//! unknown `kid`), with `RS256`, `ES256` and `EdDSA` only, `iss`, `aud`,
//! `azp`, `exp`, `iat` with five minutes of skew, and `nonce`.
use super::{
    DISCOVERY_TTL, GITHUB_API, GITHUB_AUTHORIZE, GITHUB_TOKEN, IAT_SKEW, JWKS_REFETCH_INTERVAL,
    MAX_RESPONSE_BYTES, MAX_SUBJECT_BYTES, Profile, Provider, ProviderError,
};
use crate::config::ProviderKind;
use base64::Engine;
use jiff::Timestamp;
use merezha_domain::Failure;
use merezha_platform::OutboundHttp;
use openidconnect::{
    AsyncHttpClient, AuthUrl, AuthorizationCode, ClaimsVerificationError, ClientId, ClientSecret,
    CsrfToken, HttpRequest, HttpResponse, IssuerUrl, Nonce, OAuth2TokenResponse, PkceCodeChallenge,
    PkceCodeVerifier, RedirectUrl, RequestTokenError, Scope, SignatureVerificationError,
    TokenResponse, TokenUrl,
    core::{
        CoreAuthenticationFlow, CoreClient, CoreIdTokenClaims, CoreIdTokenVerifier,
        CoreJsonWebKeySet, CoreJwsSigningAlgorithm,
    },
};
use secrecy::{ExposeSecret, SecretString};
use std::{
    future::Future,
    pin::Pin,
    time::{Duration, Instant},
};

/// A failed outbound call, carrying the platform's classification.
#[derive(Debug)]
pub struct HttpError(pub Failure);
impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "outbound call failed: {}", self.0.code)
    }
}
impl std::error::Error for HttpError {}

/// The platform client as `openidconnect`'s and `oauth2`'s
/// `AsyncHttpClient`.
pub struct Http<'a> {
    pub http: &'a OutboundHttp,
    pub timeout: Duration,
}
impl<'c> AsyncHttpClient<'c> for Http<'_> {
    type Error = HttpError;
    type Future = Pin<Box<dyn Future<Output = Result<HttpResponse, HttpError>> + Send + 'c>>;
    fn call(&'c self, request: HttpRequest) -> Self::Future {
        Box::pin(async move {
            let request = reqwest::Request::try_from(request)
                .map_err(|_| HttpError(Failure::permanent("invalid_request")))?;
            let response = self
                .http
                .send(
                    request,
                    Instant::now() + self.timeout,
                    MAX_RESPONSE_BYTES,
                    None,
                )
                .await
                .map_err(HttpError)?;
            let mut built = HttpResponse::new(response.body);
            *built.status_mut() = response.status;
            *built.headers_mut() = response.headers;
            Ok(built)
        })
    }
}

/// The RFC 9207 field of a discovery document: a provider that sets it
/// sends `iss` with every authorization response, and the callback then
/// requires it.
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Rfc9207 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    authorization_response_iss_parameter_supported: Option<bool>,
}
impl openidconnect::AdditionalProviderMetadata for Rfc9207 {}

/// Core provider metadata with the RFC 9207 field.
type Metadata = openidconnect::ProviderMetadata<
    Rfc9207,
    openidconnect::core::CoreAuthDisplay,
    openidconnect::core::CoreClientAuthMethod,
    openidconnect::core::CoreClaimName,
    openidconnect::core::CoreClaimType,
    openidconnect::core::CoreGrantType,
    openidconnect::core::CoreJweContentEncryptionAlgorithm,
    openidconnect::core::CoreJweKeyManagementAlgorithm,
    openidconnect::core::CoreJsonWebKey,
    openidconnect::core::CoreResponseMode,
    openidconnect::core::CoreResponseType,
    openidconnect::core::CoreSubjectIdentifierType,
>;

/// A provider's discovery document with its key set, as last fetched.
struct Cached {
    metadata: Metadata,
    fetched: Timestamp,
    /// The last key set refetch an unknown `kid` forced.
    forced: Option<Instant>,
}

#[derive(Default)]
struct CacheState {
    cached: Option<Cached>,
    /// The last failed discovery: callers within [`DISCOVERY_RETRY`] get
    /// `provider_unavailable` at once instead of each waiting out the
    /// timeout again.
    failed: Option<Timestamp>,
}

/// How long a failed discovery is remembered.
pub const DISCOVERY_RETRY: Duration = Duration::from_secs(30);

/// The per-provider cache. Fetches are serialized, so a cold cache costs
/// one round trip however many sign-ins wait on it: the waiters share the
/// result, a success for [`DISCOVERY_TTL`] and a failure for
/// [`DISCOVERY_RETRY`]. Times come from the platform clock.
#[derive(Default)]
pub(crate) struct Cache {
    state: tokio::sync::Mutex<CacheState>,
}
impl Cache {
    async fn metadata(
        &self,
        provider: &Provider,
        http: &Http<'_>,
        now: Timestamp,
    ) -> Result<Metadata, ProviderError> {
        let mut state = self.state.lock().await;
        if let Some(cached) = state.cached.as_ref()
            && now < crate::core::plus(cached.fetched, DISCOVERY_TTL)
        {
            return Ok(cached.metadata.clone());
        }
        if state
            .failed
            .is_some_and(|at| now < crate::core::plus(at, DISCOVERY_RETRY))
        {
            return Err(ProviderError::Unavailable);
        }
        let issuer = IssuerUrl::new(provider.issuer().unwrap_or_default().to_owned())
            .map_err(|_| ProviderError::Unavailable)?;
        // Discovery checks the document's `issuer` against the configured
        // one, then fetches `jwks_uri`.
        let metadata = match Metadata::discover_async(issuer, http).await {
            Ok(metadata) => metadata,
            Err(_) => {
                state.failed = Some(now);
                return Err(ProviderError::Unavailable);
            }
        };
        *state = CacheState {
            cached: Some(Cached {
                metadata: metadata.clone(),
                fetched: now,
                forced: None,
            }),
            failed: None,
        };
        Ok(metadata)
    }
    /// Refetch the key set after an unknown `kid`, at most once per
    /// [`JWKS_REFETCH_INTERVAL`]; `None` when throttled.
    async fn refetch_keys(
        &self,
        http: &Http<'_>,
    ) -> Result<Option<CoreJsonWebKeySet>, ProviderError> {
        let mut state = self.state.lock().await;
        let Some(cached) = state.cached.as_mut() else {
            return Ok(None);
        };
        if cached
            .forced
            .is_some_and(|at| at.elapsed() < JWKS_REFETCH_INTERVAL)
        {
            return Ok(None);
        }
        cached.forced = Some(Instant::now());
        let keys = CoreJsonWebKeySet::fetch_async(cached.metadata.jwks_uri(), http)
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        cached.metadata = cached.metadata.clone().set_jwks(keys.clone());
        Ok(Some(keys))
    }
}

/// What a flow started with: everything the callback needs back.
pub struct FlowSecrets<'a> {
    pub state: &'a str,
    pub pkce_verifier: &'a str,
    pub nonce: Option<&'a str>,
}

fn redirect(uri: &str) -> Result<RedirectUrl, ProviderError> {
    RedirectUrl::new(uri.to_owned()).map_err(|_| ProviderError::Failed)
}
fn client_id(provider: &Provider) -> ClientId {
    ClientId::new(provider.settings.client_id.clone())
}
fn client_secret(provider: &Provider) -> ClientSecret {
    ClientSecret::new(provider.settings.client_secret.expose().to_owned())
}
fn scopes(provider: &Provider) -> impl Iterator<Item = Scope> + '_ {
    // `openidconnect` adds `openid` itself.
    provider
        .settings
        .scopes
        .iter()
        .filter(|s| provider.kind() == ProviderKind::Github || *s != "openid")
        .map(|s| Scope::new(s.clone()))
}

/// Whether the provider's discovery document advertises RFC 9207, so its
/// callbacks must carry `iss`. GitHub has none.
pub async fn requires_iss(
    provider: &Provider,
    http: &Http<'_>,
    now: Timestamp,
) -> Result<bool, ProviderError> {
    if provider.kind() == ProviderKind::Github {
        return Ok(false);
    }
    let metadata = provider.cache.metadata(provider, http, now).await?;
    Ok(metadata
        .additional_metadata()
        .authorization_response_iss_parameter_supported
        == Some(true))
}

/// The provider's authorization URL for a new flow. OIDC providers need
/// their discovery document (`provider_unavailable` when unreachable).
pub async fn authorization_url(
    provider: &Provider,
    http: &Http<'_>,
    redirect_uri: &str,
    flow: FlowSecrets<'_>,
    now: Timestamp,
) -> Result<String, ProviderError> {
    let challenge = PkceCodeChallenge::from_code_verifier_sha256(&PkceCodeVerifier::new(
        flow.pkce_verifier.to_owned(),
    ));
    let state = CsrfToken::new(flow.state.to_owned());
    match provider.kind() {
        ProviderKind::Github => {
            let client = github_client(provider, redirect_uri)?;
            let (url, _) = client
                .authorize_url(move || state)
                .add_scopes(scopes(provider))
                .set_pkce_challenge(challenge)
                .url();
            Ok(url.to_string())
        }
        ProviderKind::Google | ProviderKind::Oidc => {
            let metadata = provider.cache.metadata(provider, http, now).await?;
            let nonce = Nonce::new(flow.nonce.unwrap_or_default().to_owned());
            let client = CoreClient::from_provider_metadata(
                metadata,
                client_id(provider),
                Some(client_secret(provider)),
            )
            .set_redirect_uri(redirect(redirect_uri)?);
            let (url, _, _) = client
                .authorize_url(
                    CoreAuthenticationFlow::AuthorizationCode,
                    move || state,
                    move || nonce,
                )
                .add_scopes(scopes(provider))
                .set_pkce_challenge(challenge)
                .url();
            Ok(url.to_string())
        }
    }
}

fn github_client(
    provider: &Provider,
    redirect_uri: &str,
) -> Result<
    oauth2::basic::BasicClient<
        oauth2::EndpointSet,
        oauth2::EndpointNotSet,
        oauth2::EndpointNotSet,
        oauth2::EndpointNotSet,
        oauth2::EndpointSet,
    >,
    ProviderError,
> {
    let e = &provider.endpoints;
    let auth = AuthUrl::new(
        e.authorization
            .as_deref()
            .unwrap_or(GITHUB_AUTHORIZE)
            .to_owned(),
    )
    .map_err(|_| ProviderError::Failed)?;
    let token = TokenUrl::new(e.token.as_deref().unwrap_or(GITHUB_TOKEN).to_owned())
        .map_err(|_| ProviderError::Failed)?;
    Ok(oauth2::basic::BasicClient::new(client_id(provider))
        .set_client_secret(client_secret(provider))
        .set_auth_uri(auth)
        .set_token_uri(token)
        .set_redirect_uri(redirect(redirect_uri)?))
}

fn token_error<T: openidconnect::ErrorResponse + 'static>(
    error: &RequestTokenError<HttpError, T>,
) -> ProviderError {
    match error {
        RequestTokenError::Request(_) => ProviderError::Unavailable,
        _ => ProviderError::Failed,
    }
}

/// Exchange `code` and read who signed in. The provider's tokens are
/// dropped before this returns; identity never stores or forwards them.
pub async fn exchange(
    provider: &Provider,
    http: &Http<'_>,
    redirect_uri: &str,
    code: &str,
    flow: FlowSecrets<'_>,
    now: Timestamp,
) -> Result<Profile, ProviderError> {
    let verifier = PkceCodeVerifier::new(flow.pkce_verifier.to_owned());
    let code = AuthorizationCode::new(code.to_owned());
    match provider.kind() {
        ProviderKind::Github => {
            let client = github_client(provider, redirect_uri)?;
            let token = client
                .exchange_code(code)
                .set_pkce_verifier(verifier)
                .request_async(http)
                .await
                .map_err(|e| token_error(&e))?;
            let access = SecretString::from(token.access_token().secret().clone());
            drop(token);
            github_profile(provider, http, &access).await
        }
        ProviderKind::Google | ProviderKind::Oidc => {
            let Some(nonce) = flow.nonce else {
                return Err(ProviderError::Failed);
            };
            let metadata = provider.cache.metadata(provider, http, now).await?;
            let issuer = metadata.issuer().clone();
            let keys = metadata.jwks().clone();
            let client = CoreClient::from_provider_metadata(
                metadata,
                client_id(provider),
                Some(client_secret(provider)),
            )
            .set_redirect_uri(redirect(redirect_uri)?);
            let token = client
                .exchange_code(code)
                .map_err(|_| ProviderError::Failed)?
                .set_pkce_verifier(verifier)
                .request_async(http)
                .await
                .map_err(|e| token_error(&e))?;
            let id_token = token.id_token().ok_or(ProviderError::Failed)?.clone();
            drop(token);
            let nonce = Nonce::new(nonce.to_owned());
            let verify = |keys: CoreJsonWebKeySet| {
                let verifier = id_token_verifier(provider, issuer.clone(), keys, now);
                id_token.claims(&verifier, &nonce).cloned()
            };
            let claims = match verify(keys) {
                Ok(claims) => claims,
                // An unknown `kid`: the provider may have rotated its keys.
                Err(ClaimsVerificationError::SignatureVerification(
                    SignatureVerificationError::NoMatchingKey,
                )) => match provider.cache.refetch_keys(http).await? {
                    Some(keys) => verify(keys).map_err(|_| ProviderError::Failed)?,
                    None => return Err(ProviderError::Failed),
                },
                Err(_) => return Err(ProviderError::Failed),
            };
            oidc_profile(provider, &claims, &id_token.to_string())
        }
    }
}

fn chrono_time(ts: Timestamp) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(ts.as_second(), ts.subsec_nanosecond().max(0) as u32)
        .unwrap_or_default()
}

/// The ID token checks of section 6, on the platform clock.
fn id_token_verifier(
    provider: &Provider,
    issuer: IssuerUrl,
    keys: CoreJsonWebKeySet,
    now: Timestamp,
) -> CoreIdTokenVerifier<'static> {
    let current = chrono_time(now);
    let latest = chrono_time(crate::core::plus(now, IAT_SKEW));
    CoreIdTokenVerifier::new_confidential_client(
        client_id(provider),
        client_secret(provider),
        issuer,
        keys,
    )
    // No `none`, no HMAC: a confidential client would otherwise accept
    // HS256 keyed with its own secret.
    .set_allowed_algs([
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
        CoreJwsSigningAlgorithm::EcdsaP256Sha256,
        CoreJwsSigningAlgorithm::EdDsa,
    ])
    .set_time_fn(move || current)
    .set_issue_time_verifier_fn(move |iat| {
        if iat > latest {
            Err("issued in the future".to_owned())
        } else {
            Ok(())
        }
    })
}

/// The `hd` claim of a verified ID token's payload.
fn hosted_domain(compact: &str) -> Option<String> {
    let payload = compact.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value.get("hd")?.as_str().map(str::to_owned)
}

fn oidc_profile(
    provider: &Provider,
    claims: &CoreIdTokenClaims,
    compact: &str,
) -> Result<Profile, ProviderError> {
    // `azp`, which `openidconnect` leaves to the client: when present it
    // must name this client. (Extra audiences never get here:
    // `openidconnect` refuses audiences other than this client.)
    match claims.authorized_party() {
        Some(azp) if azp.as_str() != provider.settings.client_id => {
            return Err(ProviderError::Failed);
        }
        _ => {}
    }
    let subject = claims.subject().as_str();
    if subject.is_empty() || subject.len() > MAX_SUBJECT_BYTES {
        return Err(ProviderError::Failed);
    }
    let hosted = &provider.settings.allowed_hosted_domains;
    if provider.kind() == ProviderKind::Google
        && !hosted.is_empty()
        && !hosted_domain(compact).is_some_and(|hd| hosted.contains(&hd))
    {
        return Err(ProviderError::Failed);
    }
    let verified = claims.email_verified() == Some(true);
    let email = claims.email().map(|e| e.as_str().to_owned());
    Ok(Profile {
        subject: subject.to_owned(),
        // Google's address counts only when Google asserts it verified.
        email: if provider.kind() == ProviderKind::Google && !verified {
            None
        } else {
            email
        },
        email_verified: verified,
        display_name: claims
            .name()
            .and_then(|n| n.get(None))
            .map(|n| n.as_str().to_owned()),
    })
}

/// `GET {api}/user` and `GET {api}/user/emails` with the access token.
async fn github_profile(
    provider: &Provider,
    http: &Http<'_>,
    access: &SecretString,
) -> Result<Profile, ProviderError> {
    let api = provider
        .endpoints
        .api
        .as_deref()
        .unwrap_or(GITHUB_API)
        .trim_end_matches('/');
    let user = github_get(http, &format!("{api}/user"), access).await?;
    let subject = match user.get("id") {
        Some(serde_json::Value::Number(n)) if n.is_u64() => n.to_string(),
        _ => return Err(ProviderError::Failed),
    };
    let emails = github_get(http, &format!("{api}/user/emails"), access).await?;
    let entries: Vec<(String, bool, bool)> = emails
        .as_array()
        .ok_or(ProviderError::Failed)?
        .iter()
        .filter_map(|e| {
            Some((
                e.get("email")?.as_str()?.to_owned(),
                e.get("primary")?.as_bool()?,
                e.get("verified")?.as_bool()?,
            ))
        })
        .collect();
    let email = super::github_email(&entries);
    Ok(Profile {
        subject,
        email_verified: email.is_some(),
        email,
        display_name: user.get("name").and_then(|n| n.as_str()).map(str::to_owned),
    })
}

async fn github_get(
    http: &Http<'_>,
    url: &str,
    access: &SecretString,
) -> Result<serde_json::Value, ProviderError> {
    let request = http
        .http
        .client()
        .get(url)
        .bearer_auth(access.expose_secret())
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header("x-github-api-version", "2022-11-28")
        .build()
        .map_err(|_| ProviderError::Failed)?;
    let response = http
        .http
        .send(
            request,
            Instant::now() + http.timeout,
            MAX_RESPONSE_BYTES,
            None,
        )
        .await
        .map_err(|_| ProviderError::Unavailable)?;
    if response.status.is_server_error() {
        return Err(ProviderError::Unavailable);
    }
    if !response.status.is_success() {
        return Err(ProviderError::Failed);
    }
    serde_json::from_slice(&response.body).map_err(|_| ProviderError::Failed)
}
