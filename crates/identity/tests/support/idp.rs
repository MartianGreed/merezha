//! An in-process identity provider for the OAuth tests: an OIDC provider
//! (discovery, JWKS, token endpoint signing ID tokens with test-only keys
//! generated at start) and a GitHub API double (token endpoint, `/user`,
//! `/user/emails`), both on one loopback axum server. The tests drive the
//! real `openidconnect` and `oauth2` code paths of the identity block
//! against it.
#![allow(dead_code)]
use axum::{
    Router,
    body::Bytes,
    extract::{RawQuery, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use openssl::{
    hash::MessageDigest,
    pkey::{PKey, Private},
    rsa::Rsa,
    sign::Signer,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

pub const CLIENT_ID: &str = "merezha-client";
/// The secret `OAUTH_SECRET` resolves to in the test projects.
pub const CLIENT_SECRET: &str = "a-test-oauth-client-secret";

/// A test-only signing key: RS256 or EdDSA (Ed25519).
#[derive(Clone)]
pub struct TestKey {
    pub kid: String,
    pub key: PKey<Private>,
    pub rsa: bool,
}
impl TestKey {
    pub fn rsa(kid: &str) -> Self {
        Self {
            kid: kid.to_owned(),
            key: PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap(),
            rsa: true,
        }
    }
    pub fn ed25519(kid: &str) -> Self {
        Self {
            kid: kid.to_owned(),
            key: PKey::generate_ed25519().unwrap(),
            rsa: false,
        }
    }
    pub fn alg(&self) -> &'static str {
        if self.rsa { "RS256" } else { "EdDSA" }
    }
    pub fn jwk(&self) -> Value {
        if self.rsa {
            let rsa = self.key.rsa().unwrap();
            json!({"kty": "RSA", "kid": self.kid, "use": "sig", "alg": "RS256",
                   "n": B64.encode(rsa.n().to_vec()), "e": B64.encode(rsa.e().to_vec())})
        } else {
            json!({"kty": "OKP", "crv": "Ed25519", "kid": self.kid, "use": "sig", "alg": "EdDSA",
                   "x": B64.encode(self.key.raw_public_key().unwrap())})
        }
    }
    /// A compact JWS of `claims`.
    pub fn sign(&self, claims: &Value) -> String {
        let header = json!({"alg": self.alg(), "kid": self.kid, "typ": "JWT"});
        let input = format!(
            "{}.{}",
            B64.encode(header.to_string()),
            B64.encode(claims.to_string())
        );
        let signature = if self.rsa {
            let mut signer = Signer::new(MessageDigest::sha256(), &self.key).unwrap();
            signer.update(input.as_bytes()).unwrap();
            signer.sign_to_vec().unwrap()
        } else {
            let mut signer = Signer::new_without_digest(&self.key).unwrap();
            signer.sign_oneshot_to_vec(input.as_bytes()).unwrap()
        };
        format!("{input}.{}", B64.encode(signature))
    }
}

/// An unsigned token (`alg = none`).
pub fn unsigned(claims: &Value) -> String {
    format!(
        "{}.{}.",
        B64.encode(json!({"alg": "none", "typ": "JWT"}).to_string()),
        B64.encode(claims.to_string())
    )
}
/// A token MACed with the client secret (`alg = HS256`), which a
/// confidential client would accept if HMAC were allowed.
pub fn hs256(claims: &Value) -> String {
    use hmac::{KeyInit, Mac};
    let input = format!(
        "{}.{}",
        B64.encode(json!({"alg": "HS256", "typ": "JWT"}).to_string()),
        B64.encode(claims.to_string())
    );
    let mut mac = hmac::Hmac::<Sha256>::new_from_slice(CLIENT_SECRET.as_bytes()).unwrap();
    mac.update(input.as_bytes());
    format!("{input}.{}", B64.encode(mac.finalize().into_bytes()))
}

/// How the ID token of a grant is made.
#[derive(Clone)]
pub enum Token {
    /// Signed with the provider's current signing key.
    Signed,
    /// Signed with this key, published or not.
    With(TestKey),
    /// Exactly this compact token.
    Raw(String),
}

/// One authorization the test granted: the claims the ID token carries
/// and what the token request must match.
#[derive(Clone)]
pub struct Grant {
    pub claims: Value,
    pub challenge: String,
    pub redirect_uri: String,
    pub token: Token,
}

/// What discovery answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Discovery {
    Normal,
    /// A document over 1 MiB.
    Huge,
    /// `302` to another path, which must never be fetched.
    Redirect,
    /// `500`.
    Broken,
}

pub struct IdpState {
    pub issuer: String,
    /// Published in the key set.
    pub keys: Vec<TestKey>,
    /// Index of the signing key in `keys`.
    pub signer: usize,
    pub codes: HashMap<String, Grant>,
    pub discovery: Discovery,
    pub jwks_delay: Duration,
    pub discovery_hits: usize,
    pub jwks_hits: usize,
    pub token_hits: usize,
    pub redirect_target_hits: usize,
    /// GitHub: the `/user` id and the `/user/emails` entries.
    pub github_id: u64,
    pub github_name: Option<String>,
    pub github_emails: Vec<(String, bool, bool)>,
    next_code: u64,
}

/// The running provider.
#[derive(Clone)]
pub struct Idp {
    pub base: String,
    pub state: Arc<Mutex<IdpState>>,
}

/// The parameters of an authorization URL the block produced.
#[derive(Clone, Debug)]
pub struct Authorization {
    pub state: String,
    pub nonce: Option<String>,
    pub challenge: String,
    pub redirect_uri: String,
    pub client_id: String,
    pub scope: String,
    pub method: String,
    pub url: String,
}
impl Authorization {
    pub fn parse(url: &str) -> Self {
        let query = url.split_once('?').map_or("", |(_, q)| q);
        let params: HashMap<String, String> = oauth2::url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        Self {
            state: params["state"].clone(),
            nonce: params.get("nonce").cloned(),
            challenge: params["code_challenge"].clone(),
            redirect_uri: params["redirect_uri"].clone(),
            client_id: params["client_id"].clone(),
            scope: params.get("scope").cloned().unwrap_or_default(),
            method: params["code_challenge_method"].clone(),
            url: url.to_owned(),
        }
    }
}

impl Idp {
    /// Start a provider on a loopback port with one RS256 signing key.
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(IdpState {
            issuer: base.clone(),
            keys: vec![TestKey::rsa("rsa-1")],
            signer: 0,
            codes: HashMap::new(),
            discovery: Discovery::Normal,
            jwks_delay: Duration::ZERO,
            discovery_hits: 0,
            jwks_hits: 0,
            token_hits: 0,
            redirect_target_hits: 0,
            github_id: 4242,
            github_name: Some("Octo Cat".into()),
            github_emails: Vec::new(),
            next_code: 0,
        }));
        let app = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route(
                "/elsewhere/.well-known/openid-configuration",
                get(redirect_target),
            )
            .route("/jwks", get(jwks))
            .route("/token", post(token))
            .route("/login/oauth/access_token", post(github_token))
            .route("/api/user", get(github_user))
            .route("/api/user/emails", get(github_emails))
            .with_state(state.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self { base, state }
    }
    pub fn with<T>(&self, f: impl FnOnce(&mut IdpState) -> T) -> T {
        f(&mut self.state.lock().unwrap())
    }
    /// The GitHub endpoint overrides of this server.
    pub fn github_endpoints(&self) -> merezha_identity::oauth::ProviderEndpoints {
        merezha_identity::oauth::ProviderEndpoints {
            authorization: Some(format!("{}/login/oauth/authorize", self.base)),
            token: Some(format!("{}/login/oauth/access_token", self.base)),
            api: Some(format!("{}/api", self.base)),
            issuer: None,
        }
    }
    /// The claims of a normal ID token for `authorization` at `now`
    /// (seconds), merged with `extra`.
    pub fn claims(&self, authorization: &Authorization, now: i64, extra: Value) -> Value {
        let mut claims = json!({
            "iss": self.base,
            "aud": CLIENT_ID,
            "iat": now,
            "exp": now + 300,
        });
        if let Some(nonce) = &authorization.nonce {
            claims["nonce"] = Value::String(nonce.clone());
        }
        if let (Some(map), Value::Object(extra)) = (claims.as_object_mut(), extra) {
            for (k, v) in extra {
                if v.is_null() {
                    map.remove(&k);
                } else {
                    map.insert(k, v);
                }
            }
        }
        claims
    }
    /// Grant `authorization` with these claims and token; the code.
    pub fn grant(&self, authorization: &Authorization, claims: Value, token: Token) -> String {
        self.with(|s| {
            s.next_code += 1;
            let code = format!("code-{}", s.next_code);
            s.codes.insert(
                code.clone(),
                Grant {
                    claims,
                    challenge: authorization.challenge.clone(),
                    redirect_uri: authorization.redirect_uri.clone(),
                    token,
                },
            );
            code
        })
    }
}

async fn discovery(State(state): State<Arc<Mutex<IdpState>>>) -> Response {
    let (mode, issuer) = {
        let mut s = state.lock().unwrap();
        s.discovery_hits += 1;
        (s.discovery, s.issuer.clone())
    };
    let base = issuer.clone();
    match mode {
        Discovery::Broken => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        Discovery::Redirect => (
            StatusCode::FOUND,
            [(
                header::LOCATION,
                format!("{base}/elsewhere/.well-known/openid-configuration"),
            )],
        )
            .into_response(),
        Discovery::Normal | Discovery::Huge => {
            let mut document = json!({
                "issuer": issuer,
                "authorization_endpoint": format!("{base}/authorize"),
                "token_endpoint": format!("{base}/token"),
                "jwks_uri": format!("{base}/jwks"),
                "response_types_supported": ["code"],
                "subject_types_supported": ["public"],
                "id_token_signing_alg_values_supported": ["RS256", "EdDSA"],
            });
            if mode == Discovery::Huge {
                document["padding"] = Value::String("x".repeat(1024 * 1024 + 1));
            }
            axum::Json(document).into_response()
        }
    }
}

async fn redirect_target(State(state): State<Arc<Mutex<IdpState>>>) -> Response {
    state.lock().unwrap().redirect_target_hits += 1;
    StatusCode::NOT_FOUND.into_response()
}

async fn jwks(State(state): State<Arc<Mutex<IdpState>>>) -> Response {
    let (delay, keys) = {
        let mut s = state.lock().unwrap();
        s.jwks_hits += 1;
        (
            s.jwks_delay,
            s.keys.iter().map(TestKey::jwk).collect::<Vec<_>>(),
        )
    };
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    axum::Json(json!({ "keys": keys })).into_response()
}

fn form(body: &Bytes) -> HashMap<String, String> {
    oauth2::url::form_urlencoded::parse(body)
        .into_owned()
        .collect()
}
fn token_error(code: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        axum::Json(json!({ "error": code })),
    )
        .into_response()
}
/// The client authenticated with HTTP Basic, the code exists for this
/// redirect URI and the PKCE verifier matches its S256 challenge.
#[allow(clippy::result_large_err)]
fn redeem(state: &Mutex<IdpState>, headers: &HeaderMap, body: &Bytes) -> Result<Grant, Response> {
    let params = form(body);
    let expected = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{CLIENT_ID}:{CLIENT_SECRET}"))
    );
    if headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        != Some(expected.as_str())
    {
        return Err(token_error("invalid_client"));
    }
    let mut s = state.lock().unwrap();
    s.token_hits += 1;
    if params.get("grant_type").map(String::as_str) != Some("authorization_code") {
        return Err(token_error("unsupported_grant_type"));
    }
    let Some(grant) = params.get("code").and_then(|c| s.codes.remove(c)) else {
        return Err(token_error("invalid_grant"));
    };
    let verifier = params.get("code_verifier").cloned().unwrap_or_default();
    if B64.encode(Sha256::digest(verifier.as_bytes())) != grant.challenge
        || params.get("redirect_uri") != Some(&grant.redirect_uri)
    {
        return Err(token_error("invalid_grant"));
    }
    Ok(grant)
}

async fn token(
    State(state): State<Arc<Mutex<IdpState>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let grant = match redeem(&state, &headers, &body) {
        Ok(grant) => grant,
        Err(response) => return response,
    };
    let id_token = {
        let s = state.lock().unwrap();
        match &grant.token {
            Token::Signed => s.keys[s.signer].sign(&grant.claims),
            Token::With(key) => key.sign(&grant.claims),
            Token::Raw(raw) => raw.clone(),
        }
    };
    axum::Json(json!({
        "access_token": "provider-access-token",
        "token_type": "Bearer",
        "expires_in": 3600,
        "refresh_token": "provider-refresh-token",
        "id_token": id_token,
    }))
    .into_response()
}

async fn github_token(
    State(state): State<Arc<Mutex<IdpState>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(response) = redeem(&state, &headers, &body) {
        return response;
    }
    axum::Json(
        json!({"access_token": "gho_testtoken", "token_type": "bearer",
                      "scope": "read:user,user:email"}),
    )
    .into_response()
}

fn github_authorized(headers: &HeaderMap) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        == Some("Bearer gho_testtoken")
}

async fn github_user(State(state): State<Arc<Mutex<IdpState>>>, headers: HeaderMap) -> Response {
    if !github_authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let s = state.lock().unwrap();
    axum::Json(json!({"id": s.github_id, "login": "octocat", "name": s.github_name}))
        .into_response()
}

async fn github_emails(
    State(state): State<Arc<Mutex<IdpState>>>,
    headers: HeaderMap,
    _: RawQuery,
) -> Response {
    if !github_authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let s = state.lock().unwrap();
    let entries: Vec<Value> = s
        .github_emails
        .iter()
        .map(|(email, primary, verified)| {
            json!({"email": email, "primary": primary, "verified": verified, "visibility": null})
        })
        .collect();
    axum::Json(Value::Array(entries)).into_response()
}
