//! The OAuth use cases of section 4: the provider list, starting a flow,
//! the callback with the linking rules, the caller's identities and
//! unlinking.
use super::{
    Profile, Provider, ProviderError,
    adapter::{self, FlowSecrets, Http},
    redirect_uri,
};
use crate::{
    config::{ProviderKind, Registration},
    core::{
        Actor, ApiError, Audit, Client, Core, Method, NewSession, Purpose, Subject, Transport,
        UserRow, event, plus, sql, topic,
    },
    flows::{Binding, Caller, Reply},
    gcra, ids,
    link::{AssertedEmail, LinkDecision, LinkInput, decide_link},
    normalize::{self, Email},
    secrets::RawToken,
};
use jiff::Timestamp;
use merezha_domain::Failure;
use merezha_platform::Tx;
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

/// Why a flow was started.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Intent {
    SignIn,
    /// Attach an identity to the signed-in caller.
    Link,
}
impl Intent {
    pub fn as_str(self) -> &'static str {
        match self {
            Intent::SignIn => "sign_in",
            Intent::Link => "link",
        }
    }
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "sign_in" => Some(Intent::SignIn),
            "link" => Some(Intent::Link),
            _ => None,
        }
    }
}

/// The provider's redirect back, as the callback reads it.
#[derive(Clone, Default)]
pub struct CallbackQuery {
    pub code: Option<String>,
    pub state: Option<String>,
    pub iss: Option<String>,
    pub error: Option<String>,
}
impl std::fmt::Debug for CallbackQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CallbackQuery(<redacted>)")
    }
}
/// Longest `code` read from the callback.
const MAX_CODE_BYTES: usize = 2048;
impl CallbackQuery {
    /// Parse the query string; a repeated, oversized or undecodable
    /// parameter fails, so the callback answers `invalid_state`.
    pub fn parse(query: Option<&str>) -> Option<Self> {
        let mut parsed = Self::default();
        for (key, value) in oauth2::url::form_urlencoded::parse(query.unwrap_or("").as_bytes()) {
            let (slot, limit) = match key.as_ref() {
                "code" => (&mut parsed.code, MAX_CODE_BYTES),
                "state" => (&mut parsed.state, 128),
                "iss" => (&mut parsed.iss, 512),
                "error" => (&mut parsed.error, 128),
                _ => continue,
            };
            if slot.is_some() || value.len() > limit {
                return None;
            }
            *slot = Some(value.into_owned());
        }
        Some(parsed)
    }
}

/// Where the callback sends the browser, and the cookies it sets.
pub struct Callback {
    pub location: String,
    /// A new session (sign-in), for the session cookie.
    pub session: Option<NewSession>,
    /// The registration binding cookie of an account created unverified:
    /// its verification link, clicked in this browser, keeps the identity.
    pub verify_binding: Option<SecretString>,
    /// Clear the flow binding cookie: the callback was this browser's and
    /// no other flow of it is pending.
    pub clear_binding: bool,
}
impl std::fmt::Debug for Callback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Callback")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

/// An `oauth_flows` row, consumed.
struct FlowRow {
    binding_hash: Vec<u8>,
    provider: String,
    intent: String,
    user_id: Option<Uuid>,
    session_id: Option<Uuid>,
    pkce_verifier: SecretString,
    nonce: Option<SecretString>,
    return_to: Option<String>,
    expires_at: Timestamp,
}

/// What the linking transaction committed.
struct Linked {
    user: UserRow,
    session: Option<NewSession>,
    verify_binding: Option<RawToken>,
    /// Committed, but the browser lands on the error page with this code
    /// (an account created unverified while verification is required).
    refused: Option<&'static str>,
}

/// A failed callback: its fragment code, and whether it named a live
/// flow (only those are audited).
#[derive(Clone, Copy, Debug)]
struct Failed {
    code: &'static str,
    audit: bool,
}
impl Failed {
    fn known(code: &'static str) -> Self {
        Self { code, audit: true }
    }
    fn unknown(code: &'static str) -> Self {
        Self { code, audit: false }
    }
}

fn provider_unavailable(_: ProviderError) -> ApiError {
    Failure::transient("provider_unavailable").into()
}

/// The `identity.outcome` of a failed callback (TE's closed set).
fn outcome(code: &str) -> &'static str {
    match code {
        "provider_unavailable" | "oauth_failed" => "provider_error",
        "email_unverified" => "unverified",
        "account_disabled" | "registration_closed" => "locked",
        "invalid_state" => "expired",
        _ => "invalid_credentials",
    }
}

impl Core {
    fn oauth_http(&self) -> Http<'_> {
        Http {
            http: self.ctx.http(),
            timeout: self.settings.oauth.http_timeout,
        }
    }
    /// `{app_origin}{path}` of a stored `return_to`, else
    /// `links.sign_in_redirect`.
    fn landing(&self, return_to: Option<&str>) -> String {
        match return_to {
            Some(path) => format!("{}{path}", self.settings.app_origin),
            None => self.settings.links.sign_in_redirect.clone(),
        }
    }
    /// `links.oauth_error#error=<code>`.
    fn oauth_error(&self, code: &str) -> String {
        format!("{}#error={code}", self.settings.links.oauth_error)
    }

    /// `GET /oauth/providers`.
    pub fn oauth_providers(&self) -> Reply {
        let items: Vec<Value> = self
            .oauth
            .iter()
            .map(|p| {
                json!({
                    "id": p.name(),
                    "kind": p.kind().as_str(),
                    "display_name": p.settings.display_name,
                })
            })
            .collect();
        Reply::json(200, json!({ "items": items }))
    }

    /// `POST /oauth/{provider}/start`: a flow row with `state`, a PKCE S256
    /// verifier and, for OIDC, a nonce; the browser gets the binding
    /// cookie (a well-formed one it presents is kept, so two tabs can sign
    /// in at once) and the provider's authorization URL.
    pub async fn oauth_start(
        self: &Arc<Self>,
        client: &Client,
        caller: Option<Caller>,
        name: &str,
        intent: Intent,
        return_to: Option<String>,
        presented: Option<&str>,
    ) -> Result<Reply, ApiError> {
        let provider = self
            .oauth
            .get(name)
            .cloned()
            .ok_or_else(|| ApiError::code("not_found"))?;
        // Linking adds a way into the account: a recent session, as for
        // passkey registration.
        let linking = match intent {
            Intent::Link => {
                let caller = caller.ok_or_else(|| ApiError::code("unauthenticated"))?;
                self.require_recent(&caller)?;
                Some(caller)
            }
            Intent::SignIn => None,
        };
        self.charge(gcra::OAUTH_IP, Subject::Ip(client.ip), client)
            .await?;
        let random = self.ctx.random();
        let state = RawToken::generate(random)?;
        let binding = match presented.and_then(RawToken::decode) {
            Some(binding) => binding,
            None => RawToken::generate(random)?,
        };
        let verifier = RawToken::generate(random)?.encode();
        let nonce = match provider.kind() {
            ProviderKind::Github => None,
            _ => Some(RawToken::generate(random)?.encode()),
        };
        let redirect = redirect_uri(&self.settings.api_base_url, provider.name());
        let encoded_state = state.encode();
        let url = adapter::authorization_url(
            &provider,
            &self.oauth_http(),
            &redirect,
            FlowSecrets {
                state: encoded_state.expose_secret(),
                pkce_verifier: verifier.expose_secret(),
                nonce: nonce.as_ref().map(|n| n.expose_secret()),
            },
            self.now(),
        )
        .await
        .map_err(provider_unavailable)?;
        let now = self.now();
        let expires = plus(now, self.settings.oauth.flow_ttl);
        let state_hash = state.digest();
        let binding_hash = binding.digest();
        let core = self.clone();
        let provider_name = provider.name().to_owned();
        self.in_tx(move |tx| {
            Box::pin(async move {
                let s: &[u8] = &state_hash;
                let b: &[u8] = &binding_hash;
                core.db()
                    .execute(
                        &*tx,
                        &crate::sql::OAUTH_FLOW_INSERT,
                        &[
                            &s,
                            &b,
                            &provider_name,
                            &intent.as_str(),
                            &linking.map(|c| c.user),
                            &linking.map(|c| c.session),
                            &verifier.expose_secret(),
                            &nonce.as_ref().map(|n| n.expose_secret()),
                            &return_to,
                            &now,
                            &expires,
                        ],
                    )
                    .await
                    .map_err(sql)?;
                Ok(())
            })
        })
        .await?;
        let mut reply = Reply::json(200, json!({ "authorization_url": url }));
        reply.oauth_binding = Some(Binding::Set(binding.encode()));
        Ok(reply)
    }

    /// `GET /oauth/{provider}/callback`: consume the flow, check the
    /// binding cookie and `iss`, exchange the code, verify the ID token and
    /// apply the linking rules. Always a redirect: to `return_to` or
    /// `links.sign_in_redirect`, or to `links.oauth_error#error=<code>`.
    /// Failed callbacks are charged to `oauth_callback.ip`, checked first.
    /// `registration` is the registration binding cookie the browser
    /// presents, reused by a sign-up that waits for verification.
    pub async fn oauth_callback(
        self: &Arc<Self>,
        client: &Client,
        name: &str,
        query: Option<CallbackQuery>,
        binding: Option<&str>,
        registration: Option<&str>,
    ) -> Callback {
        let provider = self.oauth.get(name).cloned();
        if self
            .peek(gcra::OAUTH_CALLBACK_IP, Subject::Ip(client.ip), client)
            .await
            .is_err()
        {
            self.metrics.login_with(
                "oauth",
                "rate_limited",
                provider.as_ref().map_or("unknown", |p| p.name()),
            );
            return Callback {
                location: self.oauth_error("rate_limited"),
                session: None,
                verify_binding: None,
                clear_binding: false,
            };
        }
        let mut clear = false;
        let result = match (&provider, query) {
            (Some(provider), Some(query)) => {
                self.callback_with(client, provider, query, binding, registration, &mut clear)
                    .await
            }
            _ => Err(Failed::unknown("invalid_state")),
        };
        let label = provider.as_ref().map_or("unknown", |p| p.name());
        match result {
            Ok((mut callback, None)) => {
                self.metrics.login_with("oauth", "success", label);
                callback.clear_binding = clear;
                callback
            }
            Ok((mut callback, Some(code))) => {
                self.failed_callback(client, provider.as_deref(), Failed::known(code))
                    .await;
                callback.clear_binding = clear;
                callback
            }
            Err(failed) => {
                // Past the junk budget, a callback naming no live flow is
                // answered `rate_limited`.
                let failed = if !failed.audit
                    && self
                        .peek(
                            gcra::OAUTH_CALLBACK_UNKNOWN_IP,
                            Subject::Ip(client.ip),
                            client,
                        )
                        .await
                        .is_err()
                {
                    Failed::unknown("rate_limited")
                } else {
                    failed
                };
                self.failed_callback(client, provider.as_deref(), failed)
                    .await;
                Callback {
                    location: self.oauth_error(failed.code),
                    session: None,
                    verify_binding: None,
                    clear_binding: clear,
                }
            }
        }
    }
    /// The metric always; the audit row only for a callback that named a
    /// live flow (an unknown or forged `state` is noise an attacker makes
    /// for free, so it stays a counter); the charge of the IP's budget.
    async fn failed_callback(
        self: &Arc<Self>,
        client: &Client,
        provider: Option<&Provider>,
        failed: Failed,
    ) {
        let label = provider.map_or("unknown", |p| p.name());
        let code = failed.code;
        self.metrics.login_with("oauth", outcome(code), label);
        merezha_telemetry::event!(
            DEBUG,
            "identity.login_failed",
            method = "oauth",
            outcome = outcome(code)
        );
        // Junk (no live flow) fills its own, looser bucket, so it never
        // locks the same IP's legitimate callbacks out.
        let policy = if failed.audit {
            gcra::OAUTH_CALLBACK_IP
        } else {
            gcra::OAUTH_CALLBACK_UNKNOWN_IP
        };
        self.charge_failure(policy, Subject::Ip(client.ip)).await;
        if !failed.audit {
            return;
        }
        let mut audit = Audit::new("oauth.sign_in_failed", Actor::Anonymous)
            .failed()
            .detail("reason", code);
        if let Some(provider) = provider {
            audit = audit.detail("provider", provider.name().to_owned());
        }
        self.audit_alone(audit, client).await;
    }

    async fn callback_with(
        self: &Arc<Self>,
        client: &Client,
        provider: &Arc<Provider>,
        query: CallbackQuery,
        binding: Option<&str>,
        registration: Option<&str>,
        clear: &mut bool,
    ) -> Result<(Callback, Option<&'static str>), Failed> {
        let state = query
            .state
            .as_deref()
            .and_then(RawToken::decode)
            .ok_or(Failed::unknown("invalid_state"))?;
        // Consumed first, in its own transaction: a replayed or parallel
        // callback with the same `state` finds nothing.
        let flow = self
            .consume_flow(&state)
            .await
            .map_err(|_| Failed::known("oauth_failed"))?
            .ok_or(Failed::unknown("invalid_state"))?;
        // The browser that started the flow presents its binding cookie;
        // a callback finished in another browser (login CSRF, code
        // injection) does not, and leaves that browser's cookie alone.
        let presented = binding.and_then(RawToken::decode).map(|b| b.digest());
        let bound = presented.is_some_and(|digest| {
            bool::from(subtle::ConstantTimeEq::ct_eq(
                flow.binding_hash.as_slice(),
                &digest[..],
            ))
        });
        if !bound {
            return Err(Failed::known("invalid_state"));
        }
        // Cleared once no other flow of this browser is pending, so two
        // tabs both complete.
        *clear = self
            .pending_flows(&flow.binding_hash)
            .await
            .is_ok_and(|n| n == 0);
        if flow.provider != provider.name() || flow.expires_at <= self.now() {
            return Err(Failed::known("invalid_state"));
        }
        if let Some(error) = &query.error {
            return Err(Failed::known(if error == "access_denied" {
                "oauth_denied"
            } else {
                "oauth_failed"
            }));
        }
        // RFC 9207: an `iss` naming another issuer is a mix-up, and a
        // provider that advertises the parameter must send it.
        match (&query.iss, provider.issuer()) {
            (Some(iss), Some(issuer)) if iss != issuer => {
                return Err(Failed::known("oauth_failed"));
            }
            (None, Some(_)) => {
                let required = adapter::requires_iss(provider, &self.oauth_http(), self.now())
                    .await
                    .map_err(|e| Failed::known(e.code()))?;
                if required {
                    return Err(Failed::known("oauth_failed"));
                }
            }
            _ => {}
        }
        let code = query.code.as_deref().ok_or(Failed::known("oauth_failed"))?;
        let redirect = redirect_uri(&self.settings.api_base_url, provider.name());
        let profile = adapter::exchange(
            provider,
            &self.oauth_http(),
            &redirect,
            code,
            FlowSecrets {
                state: "",
                pkce_verifier: flow.pkce_verifier.expose_secret(),
                nonce: flow.nonce.as_ref().map(|n| n.expose_secret()),
            },
            self.now(),
        )
        .await
        .map_err(|e| Failed::known(e.code()))?;
        let intent = Intent::parse(&flow.intent).ok_or(Failed::known("invalid_state"))?;
        let linked = self
            .link_profile(
                client,
                provider,
                &flow,
                intent,
                profile,
                registration.map(str::to_owned),
            )
            .await
            .map_err(|e| {
                Failed::known(match e.failure.code.as_str() {
                    "invalid_state" => "invalid_state",
                    _ => "oauth_failed",
                })
            })?
            .map_err(Failed::known)?;
        let location = match linked.refused {
            Some(code) => self.oauth_error(code),
            None => self.landing(flow.return_to.as_deref()),
        };
        let _ = linked.user;
        Ok((
            Callback {
                location,
                session: linked.session,
                verify_binding: linked.verify_binding.map(|b| b.encode()),
                clear_binding: false,
            },
            linked.refused,
        ))
    }

    /// Live flows of a browser binding.
    async fn pending_flows(&self, binding: &[u8]) -> Result<i64, Failure> {
        let client = self.db().acquire(self.deadline()).await?;
        Ok(self
            .db()
            .query_one(
                &client,
                &crate::sql::OAUTH_FLOWS_OF_BINDING,
                &[&binding, &self.now()],
            )
            .await
            .map_err(sql)?
            .get(0))
    }

    async fn consume_flow(self: &Arc<Self>, state: &RawToken) -> Result<Option<FlowRow>, ApiError> {
        let digest = state.digest();
        let core = self.clone();
        self.in_tx(move |tx| {
            Box::pin(async move {
                let d: &[u8] = &digest;
                let row = core
                    .db()
                    .query_opt(&*tx, &crate::sql::OAUTH_FLOW_CONSUME, &[&d])
                    .await
                    .map_err(sql)?;
                Ok(row.map(|r| FlowRow {
                    binding_hash: r.get(0),
                    provider: r.get(1),
                    intent: r.get(2),
                    user_id: r.get(3),
                    session_id: r.get(4),
                    pkce_verifier: SecretString::from(r.get::<_, String>(5)),
                    nonce: r.get::<_, Option<String>>(6).map(SecretString::from),
                    return_to: r.get(7),
                    expires_at: r.get(8),
                }))
            })
        })
        .await
    }

    /// Section 6's linking rules in one transaction, under the user locks.
    /// A lost race (the identity or the address taken meanwhile) decides
    /// again from fresh reads.
    async fn link_profile(
        self: &Arc<Self>,
        client: &Client,
        provider: &Arc<Provider>,
        flow: &FlowRow,
        intent: Intent,
        profile: Profile,
        registration: Option<String>,
    ) -> Result<Result<Linked, &'static str>, ApiError> {
        let email = profile
            .email
            .as_deref()
            .and_then(|e| normalize::email(e).ok());
        let asserted = match &email {
            None => AssertedEmail::Absent,
            Some(_) if provider.settings.trust_email && profile.email_verified => {
                AssertedEmail::Trusted
            }
            Some(_) => AssertedEmail::Unverified,
        };
        let core = self.clone();
        let client = client.clone();
        let provider = provider.clone();
        let signed_in = match intent {
            Intent::Link => match (flow.user_id, flow.session_id) {
                (Some(user), Some(session)) => Some((user, session)),
                _ => return Ok(Err("invalid_state")),
            },
            Intent::SignIn => None,
        };
        self.in_tx(move |tx| {
            Box::pin(async move {
                let link = LinkCtx {
                    core: &core,
                    client: &client,
                    provider: &provider,
                    profile: &profile,
                    email: email.as_ref(),
                    asserted,
                    registration: registration.as_deref(),
                };
                for _ in 0..3 {
                    if let Some(outcome) = link.attempt(tx, signed_in).await? {
                        return Ok(outcome);
                    }
                }
                Err(Failure::conflict("conflict").into())
            })
        })
        .await
    }

    // ----- the caller's identities -----

    /// `GET /me/oauth-identities`.
    pub async fn oauth_identities(self: &Arc<Self>, caller: &Caller) -> Result<Reply, ApiError> {
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(
                &client,
                &crate::sql::OAUTH_IDENTITIES_OF_USER,
                &[&caller.user],
            )
            .await
            .map_err(sql)?;
        let items: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "id": ids::render(ids::OAUTH_IDENTITY, r.get(0)),
                    "provider": r.get::<_, String>(1),
                    "email": r.get::<_, Option<String>>(2),
                    "created_at": r.get::<_, Timestamp>(3).to_string(),
                    "last_used_at": r.get::<_, Option<Timestamp>>(4).map(|t| t.to_string()),
                })
            })
            .collect();
        Ok(Reply::json(200, json!({ "items": items })))
    }

    /// `DELETE /me/oauth-identities/{id}` (recent session): refused
    /// `409 last_credential` when nothing else would sign the user in.
    pub async fn unlink_identity(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
        id: &str,
    ) -> Result<Reply, ApiError> {
        self.require_recent(caller)?;
        let target = ids::parse(ids::OAUTH_IDENTITY, id)?;
        let core = self.clone();
        let client = client.clone();
        let caller = *caller;
        let outcome: Result<(), ApiError> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    if core.user_by_id(tx, caller.user, true).await?.is_none() {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    }
                    if !core
                        .keeps_a_credential(tx, caller.user, None, Some(target))
                        .await?
                    {
                        let owned = core
                            .db()
                            .query(&*tx, &crate::sql::OAUTH_IDENTITIES_OF_USER, &[&caller.user])
                            .await
                            .map_err(sql)?
                            .iter()
                            .any(|r| r.get::<_, Uuid>(0) == target);
                        return Ok(Err(if owned {
                            ApiError::conflict("last_credential")
                        } else {
                            ApiError::code("not_found")
                        }));
                    }
                    let Some(row) = core
                        .db()
                        .query_opt(
                            &*tx,
                            &crate::sql::OAUTH_IDENTITY_DELETE,
                            &[&target, &caller.user],
                        )
                        .await
                        .map_err(sql)?
                    else {
                        return Ok(Err(ApiError::code("not_found")));
                    };
                    let provider: String = row.get(0);
                    core.audit(
                        tx,
                        Audit::new("oauth.unlinked", Actor::User(caller.user))
                            .detail("provider", provider),
                        &client,
                    )
                    .await?;
                    Ok(Ok(()))
                })
            })
            .await?;
        outcome?;
        Ok(Reply::empty(crate::flows::CookieAction::Keep))
    }

    /// Whether user `user`, without passkey `passkey` or identity
    /// `identity`, keeps a way to sign in under this configuration: a
    /// password with password sign-in on, a passkey with passkeys on, an
    /// identity of a configured provider, or a verified address with magic
    /// links on (section 1: control of the verified email is a credential).
    pub(crate) async fn keeps_a_credential(
        &self,
        tx: &mut Tx<'_>,
        user: Uuid,
        passkey: Option<Uuid>,
        identity: Option<Uuid>,
    ) -> Result<bool, Failure> {
        let Some(row) = self
            .db()
            .query_opt(
                &*tx,
                &crate::sql::CREDENTIALS_OF_USER,
                &[&user, &passkey, &identity],
            )
            .await
            .map_err(sql)?
        else {
            return Ok(false);
        };
        let password: bool = row.get(0);
        let passkeys: i64 = row.get(1);
        let providers: Vec<String> = row.get(2);
        let verified: bool = row.get(3);
        Ok((password && self.settings.password.is_some())
            || (passkeys > 0 && self.passkeys_enabled())
            || providers.iter().any(|p| self.oauth.contains(p))
            || (verified && self.settings.magic_links.is_some()))
    }

    /// Passkeys are configured and compiled in.
    pub fn passkeys_enabled(&self) -> bool {
        cfg!(feature = "passkeys") && self.settings.passkeys.is_some()
    }
}

/// One linking attempt's context.
struct LinkCtx<'a> {
    core: &'a Arc<Core>,
    client: &'a Client,
    provider: &'a Arc<Provider>,
    profile: &'a Profile,
    email: Option<&'a Email>,
    asserted: AssertedEmail,
    /// The registration binding cookie the browser presents.
    registration: Option<&'a str>,
}
impl LinkCtx<'_> {
    /// The trusted assertion names exactly this user's address.
    fn trusted_for(&self, user: &UserRow) -> bool {
        self.asserted == AssertedEmail::Trusted
            && self
                .email
                .is_some_and(|e| e.normalized() == user.email_normalized)
    }
    /// The identity of `(provider, subject)`: its id, user and issuer.
    async fn identity(&self, tx: &mut Tx<'_>) -> Result<Option<(Uuid, Uuid, String)>, Failure> {
        Ok(self
            .core
            .db()
            .query_opt(
                &*tx,
                &crate::sql::OAUTH_IDENTITY_BY_SUBJECT,
                &[&self.provider.name(), &self.profile.subject],
            )
            .await
            .map_err(sql)?
            .map(|r| (r.get(0), r.get(1), r.get(2))))
    }
    /// Refresh the stored assertion of the identity of `user`; `false`
    /// when it is not (or no longer) theirs.
    async fn touch(&self, tx: &mut Tx<'_>, user: Uuid) -> Result<bool, Failure> {
        let now = self.core.now();
        Ok(self
            .core
            .db()
            .query_opt(
                &*tx,
                &crate::sql::OAUTH_IDENTITY_TOUCH,
                &[
                    &self.provider.name(),
                    &self.profile.subject,
                    &self.email.map(Email::normalized),
                    &(self.asserted == AssertedEmail::Trusted),
                    &now,
                    &user,
                    &self.provider.identity_issuer(),
                ],
            )
            .await
            .map_err(sql)?
            .is_some())
    }
    /// Link the identity to `user` (locked by the caller); `false` when
    /// `(provider, subject)` or `(user, provider)` is taken. `notify`, the
    /// existing account, gets the `identity.sign_in_method_added` notice.
    async fn insert(
        &self,
        tx: &mut Tx<'_>,
        user: Uuid,
        notify: Option<&UserRow>,
    ) -> Result<bool, Failure> {
        let id = self.core.new_id()?;
        let now = self.core.now();
        let inserted = self
            .core
            .db()
            .query_opt(
                &*tx,
                &crate::sql::OAUTH_IDENTITY_INSERT,
                &[
                    &id,
                    &user,
                    &self.provider.name(),
                    &self.profile.subject,
                    &self.email.map(Email::normalized),
                    &(self.asserted == AssertedEmail::Trusted),
                    &now,
                    &self.provider.identity_issuer(),
                ],
            )
            .await
            .map_err(sql)?
            .is_some();
        if inserted {
            self.core
                .audit(
                    tx,
                    Audit::new("oauth.linked", Actor::User(user))
                        .target_of("oauth_identity", id)
                        .detail("provider", self.provider.name().to_owned()),
                    self.client,
                )
                .await?;
            if let Some(owner) = notify {
                self.core
                    .method_added(
                        tx,
                        owner,
                        "oauth",
                        Some(&self.provider.settings.display_name),
                        self.client,
                    )
                    .await?;
            }
        }
        Ok(inserted)
    }
    /// A sign-in session for `user` (locked), and the new sign-in notice.
    async fn session(&self, tx: &mut Tx<'_>, user: &UserRow) -> Result<NewSession, Failure> {
        let session = self
            .core
            .create_session(
                tx,
                user.id,
                Method::OAuth,
                Transport::Cookie,
                self.client,
                None,
                true,
            )
            .await?;
        if self.core.settings.notify_new_sign_in {
            self.core
                .notice(tx, topic::NEW_SIGN_IN, user, self.client)
                .await?;
        }
        Ok(session)
    }
    fn signed_in(user: UserRow, session: NewSession) -> Result<Linked, &'static str> {
        Ok(Linked {
            user,
            session: Some(session),
            verify_binding: None,
            refused: None,
        })
    }

    /// One decision and its execution; `None` asks for another attempt
    /// from fresh reads (a race was lost).
    async fn attempt(
        &self,
        tx: &mut Tx<'_>,
        signed_in: Option<(Uuid, Uuid)>,
    ) -> Result<Option<Result<Linked, &'static str>>, ApiError> {
        let core = self.core;
        let linked = self.identity(tx).await?;
        // An identity linked under another issuer is another principal:
        // a changed issuer never re-maps its subjects onto accounts.
        if linked
            .as_ref()
            .is_some_and(|(_, _, issuer)| *issuer != self.provider.identity_issuer())
        {
            return Ok(Some(Err("oauth_failed")));
        }
        let existing = match self.email {
            Some(email) => core.user_by_email(tx, email.normalized(), false).await?,
            None => None,
        };
        let settings = &core.settings;
        let input = LinkInput {
            linked_user: linked.as_ref().map(|(_, user, _)| *user),
            signed_in_user: signed_in.map(|(user, _)| user),
            email: self.asserted,
            existing_user: existing.as_ref().map(|u| (u.id, u.verified())),
            link_by_verified_email: settings.oauth.link_by_verified_email,
            registration_open: settings.registration == Registration::Open,
            allow_sign_up: self.provider.settings.allow_sign_up,
        };
        match decide_link(&input) {
            LinkDecision::Refuse(code) => Ok(Some(Err(code))),
            LinkDecision::SignIn(id) => {
                let Some(mut user) = core.user_by_id(tx, id, true).await? else {
                    return Ok(None);
                };
                if !self.touch(tx, user.id).await? {
                    return Ok(None);
                }
                if !user.active() {
                    return Ok(Some(Err("account_disabled")));
                }
                if !user.verified() {
                    if self.trusted_for(&user) {
                        core.mark_verified(tx, &mut user, self.client).await?;
                    } else if settings.require_verified_email {
                        return Ok(Some(Err("email_unverified")));
                    }
                }
                let session = self.session(tx, &user).await?;
                Ok(Some(Self::signed_in(user, session)))
            }
            LinkDecision::Attach(id) => {
                let Some((_, session_id)) = signed_in else {
                    return Ok(Some(Err("invalid_state")));
                };
                let Some(mut user) = core.user_by_id(tx, id, true).await? else {
                    return Ok(Some(Err("invalid_state")));
                };
                // The session that started the link must still be alive,
                // and still recent: the provider round trip does not
                // stretch the reauthentication window.
                match core.hold_session(tx, session_id).await? {
                    Some(s) if s.user_id == user.id && s.alive(core.now()) => {
                        let window = settings.sessions.reauthentication_window;
                        if plus(s.authenticated_at, window) < core.now() {
                            return Ok(Some(Err("reauthentication_required")));
                        }
                    }
                    _ => return Ok(Some(Err("invalid_state"))),
                }
                if !user.active() {
                    return Ok(Some(Err("account_disabled")));
                }
                if linked.is_some() {
                    if !self.touch(tx, user.id).await? {
                        return Ok(None);
                    }
                } else if !self.insert(tx, user.id, Some(&user)).await? {
                    // Still unlinked: the user holds another identity of
                    // this provider (one per provider per user).
                    return Ok(match self.identity(tx).await? {
                        None => Some(Err("identity_already_linked")),
                        Some(_) => None,
                    });
                }
                if !user.verified() && self.trusted_for(&user) {
                    core.mark_verified(tx, &mut user, self.client).await?;
                }
                Ok(Some(Ok(Linked {
                    user,
                    session: None,
                    verify_binding: None,
                    refused: None,
                })))
            }
            decision @ (LinkDecision::LinkAndSignIn(id) | LinkDecision::ClaimAndLink(id)) => {
                let claim = matches!(decision, LinkDecision::ClaimAndLink(_));
                let Some(mut user) = core.user_by_id(tx, id, true).await? else {
                    return Ok(None);
                };
                // Still the holder of the address, verified as decided.
                if !self.trusted_for(&user) || user.verified() == claim {
                    return Ok(None);
                }
                if !user.active() {
                    return Ok(Some(Err("account_disabled")));
                }
                if claim {
                    // Section 2: the provider proved the address, so whoever
                    // registered it keeps nothing. Never "the registering
                    // browser".
                    core.claim_unverified(tx, &mut user, self.client).await?;
                    core.mark_verified(tx, &mut user, self.client).await?;
                }
                if !self.insert(tx, user.id, Some(&user)).await? {
                    return Ok(None);
                }
                let session = self.session(tx, &user).await?;
                Ok(Some(Self::signed_in(user, session)))
            }
            LinkDecision::CreateUser => {
                let Some(email) = self.email else {
                    return Ok(Some(Err("email_unverified")));
                };
                let verified = self.asserted == AssertedEmail::Trusted;
                // A browser that already holds a well-formed registration
                // binding keeps it.
                let binding = if verified {
                    None
                } else {
                    Some(match self.registration.and_then(RawToken::decode) {
                        Some(binding) => binding,
                        None => RawToken::generate(core.ctx.random())?,
                    })
                };
                let display_name = self
                    .profile
                    .display_name
                    .as_deref()
                    .and_then(|n| normalize::display_name(n).ok());
                let Some(id) = core
                    .insert_user(
                        tx,
                        email,
                        verified,
                        display_name.as_deref(),
                        binding.as_ref().map(RawToken::digest),
                    )
                    .await?
                else {
                    // Registered meanwhile: decide again.
                    return Ok(None);
                };
                if !self.insert(tx, id, None).await? {
                    // The identity was linked meanwhile: roll the new
                    // account back.
                    return Err(Failure::conflict("conflict").into());
                }
                let user = core
                    .user_by_id(tx, id, true)
                    .await?
                    .ok_or_else(|| Failure::transient("internal"))?;
                core.audit(
                    tx,
                    Audit::new("user.registered", Actor::User(id)).detail("method", "oauth"),
                    self.client,
                )
                .await?;
                core.publish(
                    tx,
                    event::USER_REGISTERED,
                    format!("user:{}:registered", id.simple()),
                    json!({"user_id": ids::render(ids::USER, id), "method": "oauth",
                           "registered_at": user.created_at.to_string()}),
                    self.client,
                )
                .await?;
                if verified || !settings.require_verified_email {
                    let session = self.session(tx, &user).await?;
                    return Ok(Some(Self::signed_in(user, session)));
                }
                // An address the provider is not trusted for: the account
                // waits for its verification link, which keeps the
                // identity when clicked in this browser (the binding).
                let (token_id, token, expires) = core
                    .issue_token(
                        tx,
                        Purpose::EmailVerification,
                        email.normalized(),
                        Some(id),
                        email,
                        None,
                        crate::core::Bound::default(),
                        settings.tokens.email_verification_ttl,
                    )
                    .await?;
                core.notify(
                    tx,
                    topic::VERIFY_EMAIL,
                    Core::address(email.display(), Some(&user))?,
                    Core::data(Some(&user), Some(expires)),
                    Some((
                        Core::link(&settings.links.verify_email, &token),
                        "email_verification",
                    )),
                    token_id,
                    Some(expires),
                    self.client,
                )
                .await?;
                Ok(Some(Ok(Linked {
                    user,
                    session: None,
                    verify_binding: binding,
                    refused: Some("email_unverified"),
                })))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_queries_are_bounded_and_single_valued() {
        let q = CallbackQuery::parse(Some("code=abc&state=xyz&iss=https%3A%2F%2Fid.example&x=1"))
            .unwrap();
        assert_eq!(q.code.as_deref(), Some("abc"));
        assert_eq!(q.state.as_deref(), Some("xyz"));
        assert_eq!(q.iss.as_deref(), Some("https://id.example"));
        assert!(q.error.is_none());
        assert!(CallbackQuery::parse(Some("state=a&state=b")).is_none());
        let long = format!("code={}", "a".repeat(MAX_CODE_BYTES + 1));
        assert!(CallbackQuery::parse(Some(&long)).is_none());
        let none = CallbackQuery::parse(None).unwrap();
        assert!(none.code.is_none() && none.state.is_none());
        assert_eq!(format!("{q:?}"), "CallbackQuery(<redacted>)");
    }

    #[test]
    fn intents_round_trip() {
        for intent in [Intent::SignIn, Intent::Link] {
            assert_eq!(Intent::parse(intent.as_str()), Some(intent));
        }
        assert_eq!(Intent::parse("other"), None);
    }
}
