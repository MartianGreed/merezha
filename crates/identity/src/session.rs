//! Session authentication at the edge (section 4): the `Authenticator`
//! installed in the host, the Origin and CSRF checks of cookie requests and
//! the session cookie itself.
use crate::{
    core::{Core, Transport, plus, session_row, sql},
    flows::Caller,
    grants::{OrgFacts, Owner, header_organization},
    ids,
    secrets::{API_KEY_PREFIX, RawToken, SESSION_PREFIX},
};
use axum::http::{HeaderMap, HeaderValue, Method, header};
use jiff::Timestamp;
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    BoxFuture,
    ports::{
        Authenticator, Principal, PrincipalKind, RequestHead, ResourceScope, SessionFacts,
        SessionTransport,
    },
};
use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use tracing::Instrument;
use uuid::Uuid;

/// Bound on the authentication query.
const AUTHENTICATE_DEADLINE: Duration = Duration::from_secs(2);
/// The CSRF header of cookie requests.
pub const CSRF_HEADER: &str = "x-csrf-token";
/// The header that selects the active organization for one request; it
/// wins over the session's (section 4).
pub const ORGANIZATION_HEADER: &str = "merezha-organization";

/// The organization `Merezha-Organization` names: `None` without the
/// header, `not_found` for a value that is no organization id.
fn requested_organization(head: &RequestHead) -> Result<Option<Uuid>, Failure> {
    match head.headers.get(ORGANIZATION_HEADER) {
        None => Ok(None),
        Some(value) => {
            let text = value
                .to_str()
                .map_err(|_| Failure::permanent("not_found"))?;
            header_organization(text).map(Some)
        }
    }
}

/// `POST`, `PUT`, `PATCH` and `DELETE`.
pub fn unsafe_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

/// The value of the session cookie `name` in the request's `Cookie`
/// headers, if any.
pub fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|line| line.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(key, _)| *key == name)
        .map(|(_, value)| value.trim())
}

impl Core {
    /// The Origin check (section 4): `Origin` is one of `allowed_origins`,
    /// or absent with `Sec-Fetch-Site: same-origin`.
    pub fn origin_allowed(&self, headers: &HeaderMap) -> bool {
        match headers.get(header::ORIGIN) {
            Some(origin) => origin
                .to_str()
                .is_ok_and(|o| self.settings.allowed_origins.iter().any(|a| a == o)),
            None => headers
                .get("sec-fetch-site")
                .is_some_and(|v| v.as_bytes() == b"same-origin"),
        }
    }
    /// `Set-Cookie` for a session token until `expires`.
    pub fn set_cookie(&self, token: &str, expires: Timestamp) -> Option<HeaderValue> {
        let max_age = (expires.as_second() - self.now().as_second()).max(0);
        let secure = if self.settings.dev_cookie {
            ""
        } else {
            "; Secure"
        };
        HeaderValue::from_str(&format!(
            "{}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}{secure}",
            self.settings.cookie_name()
        ))
        .ok()
    }
    /// `Set-Cookie` for the binding cookie, for the verification token's
    /// lifetime, or clearing it when `value` is `None`.
    pub fn binding_cookie(&self, value: Option<&str>) -> Option<HeaderValue> {
        let secure = if self.settings.dev_cookie {
            ""
        } else {
            "; Secure"
        };
        let max_age = match value {
            Some(_) => self.settings.tokens.email_verification_ttl.as_secs(),
            None => 0,
        };
        HeaderValue::from_str(&format!(
            "{}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}{secure}",
            self.settings.binding_cookie_name(),
            value.unwrap_or("")
        ))
        .ok()
    }
    /// `Set-Cookie` for the OAuth flow binding cookie, for the flow's
    /// lifetime, or clearing it when `value` is `None` (section 6).
    pub fn oauth_binding_cookie(&self, value: Option<&str>) -> Option<HeaderValue> {
        let secure = if self.settings.dev_cookie {
            ""
        } else {
            "; Secure"
        };
        let max_age = match value {
            Some(_) => self.settings.oauth.flow_ttl.as_secs(),
            None => 0,
        };
        HeaderValue::from_str(&format!(
            "{}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}{secure}",
            self.settings.oauth_cookie_name(),
            value.unwrap_or("")
        ))
        .ok()
    }
    /// `Set-Cookie` that clears the session cookie.
    pub fn clear_cookie(&self) -> HeaderValue {
        let secure = if self.settings.dev_cookie {
            ""
        } else {
            "; Secure"
        };
        HeaderValue::from_str(&format!(
            "{}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0{secure}",
            self.settings.cookie_name()
        ))
        .unwrap_or_else(|_| HeaderValue::from_static("merezha_session=; Path=/; Max-Age=0"))
    }

    /// Resolve the request's credential (section 4). No credential is
    /// anonymous; a cookie and an `Authorization` header together are
    /// `ambiguous_credentials`; a malformed, unknown, revoked or expired one
    /// is `unauthenticated`, never anonymous; a database failure is
    /// `unavailable`. Cookie requests with an unsafe method also pass the
    /// Origin check (`origin_rejected`) and the CSRF check (`csrf_failed`).
    /// `Merezha-Organization` selects the active organization, over the
    /// session's own: one the caller belongs to, or any with
    /// `identity.organization.enter` at `Application`; otherwise
    /// `404 not_found`.
    pub async fn authenticate(
        self: &Arc<Self>,
        head: &RequestHead,
    ) -> Result<Option<Arc<Principal>>, Failure> {
        let cookie = cookie(&head.headers, self.settings.cookie_name());
        let authorization = head.headers.get(header::AUTHORIZATION);
        let (token, transport) = match (cookie, authorization) {
            (None, None) => {
                self.metrics.authentication("none", "ok");
                return Ok(None);
            }
            (Some(_), Some(_)) => return Err(Failure::permanent("ambiguous_credentials")),
            (Some(value), None) => (value.to_owned(), Transport::Cookie),
            (None, Some(value)) => {
                let text = value.to_str().unwrap_or("");
                let Some((scheme, credential)) = text.split_once(' ') else {
                    return Err(self.invalid("session"));
                };
                if !scheme.eq_ignore_ascii_case("bearer") {
                    return Err(self.invalid("session"));
                }
                let credential = credential.trim();
                if credential.starts_with(API_KEY_PREFIX) {
                    let organization = requested_organization(head)?;
                    return self
                        .authenticate_key(head, credential, organization)
                        .instrument(tracing::info_span!(
                            target: merezha_telemetry::TARGET,
                            "identity api_key.resolve",
                            otel.name = "identity api_key.resolve"
                        ))
                        .await
                        .map(|p| Some(Arc::new(p)));
                }
                (credential.to_owned(), Transport::Bearer)
            }
        };
        let cookie_write = transport == Transport::Cookie && unsafe_method(&head.method);
        if cookie_write && !self.origin_allowed(&head.headers) {
            return Err(Failure::permanent("origin_rejected"));
        }
        let Some(raw) = RawToken::decode_with(SESSION_PREFIX, &token) else {
            return Err(self.invalid("session"));
        };
        let requested = requested_organization(head)?;
        let digest = raw.digest();
        let d: &[u8] = &digest;
        let deadline = Instant::now() + AUTHENTICATE_DEADLINE;
        let unavailable = |_: Failure| Failure::transient("unavailable");
        let client = self.db().acquire(deadline).await.map_err(unavailable)?;
        let row = tokio::time::timeout_at(
            deadline.into(),
            self.db().query_opt(
                &client,
                &crate::sql::SESSION_AUTHENTICATE,
                &[&d, &requested],
            ),
        )
        .await
        .map_err(|_| Failure::transient("unavailable"))?
        .map_err(|e| unavailable(sql(e)))?;
        let Some(row) = row else {
            return Err(self.invalid("session"));
        };
        let session = session_row(&row, 0);
        let status: String = row.get(12);
        let verified: Option<Timestamp> = row.get(13);
        let now = self.now();
        if !session.alive(now) {
            self.metrics.authentication("session", "expired");
            return Err(Failure::permanent("unauthenticated"));
        }
        if status != "active"
            || session.transport != transport
            || (self.settings.require_verified_email && verified.is_none())
        {
            self.metrics.authentication("session", "revoked");
            return Err(Failure::permanent("unauthenticated"));
        }
        if cookie_write {
            let presented = head
                .headers
                .get(CSRF_HEADER)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if !self.keys.csrf_matches(session.id, presented) {
                return Err(Failure::permanent("csrf_failed"));
            }
        }
        // Idle expiry slides, writing at most once per touch interval; the
        // compare on `last_seen_at` keeps concurrent touches to one write.
        let s = &self.settings.sessions;
        if plus(session.last_seen_at, s.touch_interval) <= now {
            let idle = plus(now, s.idle_timeout).min(session.absolute_expires_at);
            let _ = tokio::time::timeout_at(
                deadline.into(),
                self.db().execute(
                    &client,
                    &crate::sql::SESSION_TOUCH,
                    &[&session.id, &now, &idle, &session.last_seen_at],
                ),
            )
            .await;
        }
        let version: i64 = row.get(14);
        let organization: Option<(Uuid, i64)> = match (
            row.get::<_, Option<Uuid>>(15),
            row.get::<_, Option<i64>>(16),
        ) {
            (Some(id), Some(version)) => Some((id, version)),
            _ => None,
        };
        let membership: Option<Uuid> = row.get(17);
        if requested.is_some() && organization.is_none() {
            return Err(Failure::permanent("not_found"));
        }
        let facts = SessionFacts {
            id: ids::identity(ids::SESSION, session.id),
            method: Identity::new(session.method.clone())?,
            authenticated_at: session.authenticated_at,
            transport: match transport {
                Transport::Cookie => SessionTransport::Cookie,
                Transport::Bearer => SessionTransport::Bearer,
            },
        };
        let owner = Owner::User(session.user_id);
        let work = async {
            let (tenant, org) = match (organization, membership) {
                (None, _) => (None, None),
                (Some((id, version)), Some(membership)) => (
                    Some(id),
                    Some(OrgFacts {
                        id,
                        version,
                        holder: Some(membership),
                    }),
                ),
                (Some((id, version)), None) => {
                    let base = self
                        .snapshot(&client, session.id, owner, version, None, None)
                        .await?;
                    let outside = self.user_principal(session.user_id, None, base, None);
                    if self.may_enter(&outside) {
                        self.entered(session.user_id, session.id, id);
                        (
                            Some(id),
                            Some(OrgFacts {
                                id,
                                version,
                                holder: None,
                            }),
                        )
                    } else if requested.is_some() {
                        return Err(Failure::permanent("not_found"));
                    } else {
                        // The session's organization is no longer the
                        // caller's: act without one.
                        (None, None)
                    }
                }
            };
            let grants = self
                .snapshot(&client, session.id, owner, version, org, None)
                .await?;
            Ok::<_, Failure>(self.user_principal(session.user_id, tenant, grants, Some(facts)))
        };
        let principal = tokio::time::timeout_at(deadline.into(), work)
            .await
            .map_err(|_| Failure::transient("unavailable"))?
            .map_err(|failure| match failure.code.as_str() {
                "not_found" | "unauthenticated" => failure,
                _ => Failure::transient("unavailable"),
            })?;
        self.metrics.authentication("session", "ok");
        Ok(Some(Arc::new(principal)))
    }
    /// Whether `principal` may act in an organization it does not belong
    /// to: `identity.organization.enter` at `Application` (break-glass
    /// included).
    pub(crate) fn may_enter(&self, principal: &Principal) -> bool {
        Identity::new("identity.organization.enter")
            .is_ok_and(|enter| principal.check(&enter, &ResourceScope::Application).is_ok())
    }
    pub(crate) fn invalid(&self, credential: &'static str) -> Failure {
        self.metrics.authentication(credential, "invalid");
        Failure::permanent("unauthenticated")
    }
}

/// The caller of a session route, from the principal the edge
/// authenticated.
pub fn caller(principal: Option<&Principal>) -> Option<Caller> {
    let principal = principal?;
    if principal.kind != PrincipalKind::User {
        return None;
    }
    let user = ids::parse(ids::USER, principal.user.as_ref()?.as_str()).ok()?;
    let facts = principal.session.as_ref()?;
    Some(Caller {
        user,
        session: ids::parse(ids::SESSION, facts.id.as_str()).ok()?,
        transport: match facts.transport {
            SessionTransport::Cookie => Transport::Cookie,
            SessionTransport::Bearer => Transport::Bearer,
        },
        authenticated_at: facts.authenticated_at,
    })
}

/// The `Authenticator` the identity block installs in the host. It exists
/// before the block starts, so the host can take it at construction, and
/// answers `unavailable` for credentials until the block is running.
#[derive(Clone, Default)]
pub struct SessionAuthenticator {
    core: Arc<OnceLock<Arc<Core>>>,
}
impl std::fmt::Debug for SessionAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionAuthenticator")
            .field("started", &self.core.get().is_some())
            .finish()
    }
}
impl SessionAuthenticator {
    pub(crate) fn new(core: Arc<OnceLock<Arc<Core>>>) -> Self {
        Self { core }
    }
}
impl Authenticator for SessionAuthenticator {
    fn authenticate<'a>(
        &'a self,
        request: &'a RequestHead,
    ) -> BoxFuture<'a, Result<Option<Arc<Principal>>, Failure>> {
        Box::pin(
            async move {
                match self.core.get() {
                    Some(core) => core.authenticate(request).await,
                    None if request.headers.contains_key(header::AUTHORIZATION)
                        || request.headers.contains_key(header::COOKIE) =>
                    {
                        Err(Failure::transient("unavailable"))
                    }
                    None => Ok(None),
                }
            }
            .instrument(tracing::info_span!(
                target: merezha_telemetry::TARGET,
                "identity session.resolve",
                otel.name = "identity session.resolve"
            )),
        )
    }
    fn rejection_headers(&self, request: &RequestHead, failure: &Failure) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(core) = self.core.get()
            && failure.code == "unauthenticated"
            && cookie(&request.headers, core.settings.cookie_name()).is_some()
        {
            headers.insert(header::SET_COOKIE, core.clear_cookie());
        }
        // A key presented while `api_key.invalid.ip` is exhausted: retry
        // after one emission interval of the policy.
        if let Some(core) = self.core.get()
            && failure.code == "rate_limited"
        {
            let rate = core.settings.rate(&crate::gcra::API_KEY_INVALID_IP);
            let seconds = (rate.interval_us() as u64).div_ceil(1_000_000).max(1);
            if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
                headers.insert(header::RETRY_AFTER, value);
            }
        }
        headers
    }
}
