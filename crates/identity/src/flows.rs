//! The PR 1 use cases of section 4: registration, verification, password
//! sign-in, sessions, reauthentication, magic links, password reset and
//! change, email change, account deletion and security events. Each runs
//! in one transaction with its audit row, its notification requests and
//! its integration events (section 2).
use crate::{
    config::Registration,
    core::{
        Actor, ApiError, Audit, Client, ConsumedToken, Core, Method, NewSession, Purpose,
        SessionRow, Subject, Transport, UserRow, event, plus, session_row, sql, topic,
    },
    gcra,
    hasher::needs_rehash,
    ids,
    normalize::{self, Email},
    password::Password,
    secrets::RawToken,
};
use jiff::Timestamp;
use merezha_domain::Failure;
use merezha_platform::{Tx, ports::Recipient};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

/// The signed-in caller of a session route.
#[derive(Clone, Copy, Debug)]
pub struct Caller {
    pub user: Uuid,
    pub session: Uuid,
    pub transport: Transport,
    pub authenticated_at: Timestamp,
}

/// What a use case answers: a status, a body and what to do with the
/// session cookie.
#[derive(Debug)]
pub struct Reply {
    pub status: u16,
    pub body: Option<Value>,
    pub cookie: CookieAction,
    /// The registration binding cookie (section 2, pre-hijacking).
    pub binding: Option<Binding>,
    /// The OAuth flow binding cookie (section 6).
    pub oauth_binding: Option<Binding>,
}
/// What to do with the binding cookie a registration or a verification
/// resend sets in the browser that asked.
#[derive(Debug)]
pub enum Binding {
    /// Set it to this value for the verification token's lifetime.
    Set(SecretString),
    Clear,
}
#[derive(Debug)]
pub enum CookieAction {
    Keep,
    /// Set the session cookie to `token` until the absolute expiry.
    Set {
        token: SecretString,
        expires: Timestamp,
    },
    Clear,
}
impl Reply {
    pub(crate) fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            body: Some(body),
            cookie: CookieAction::Keep,
            binding: None,
            oauth_binding: None,
        }
    }
    pub(crate) fn accepted(status: &str) -> Self {
        Self::json(202, json!({ "status": status }))
    }
    pub(crate) fn empty(cookie: CookieAction) -> Self {
        Self {
            status: 204,
            body: None,
            cookie,
            binding: None,
            oauth_binding: None,
        }
    }
}

/// An outcome a transaction commits before refusing, so a consumed or
/// expired token stays deleted.
pub(crate) type Committed<T> = Result<T, ApiError>;

fn invalid_token() -> ApiError {
    ApiError::code("invalid_token")
}

impl Core {
    /// `{session, user?, csrf_token}` for a new session, with the token in
    /// the body (bearer) or the cookie.
    pub(crate) fn session_reply(
        &self,
        status: u16,
        new: NewSession,
        user: Option<&UserRow>,
    ) -> Reply {
        let mut session = new.row.json(true);
        let cookie = match new.row.transport {
            Transport::Bearer => {
                session["token"] = Value::String(new.token.expose_secret().to_owned());
                CookieAction::Keep
            }
            Transport::Cookie => CookieAction::Set {
                token: new.token,
                expires: new.row.absolute_expires_at,
            },
        };
        let mut body = json!({ "session": session, "csrf_token": self.csrf(new.row.id) });
        if let Some(user) = user {
            body["user"] = user.json();
        }
        Reply {
            status,
            body: Some(body),
            cookie,
            binding: None,
            oauth_binding: None,
        }
    }
    /// The browser's binding cookie when it presents a well-formed one,
    /// else a new one.
    fn binding(&self, presented: Option<&str>) -> Result<RawToken, Failure> {
        match presented.and_then(RawToken::decode) {
            Some(binding) => Ok(binding),
            None => RawToken::generate(self.ctx.random()),
        }
    }
    pub(crate) fn transport(&self, requested: Option<Transport>) -> Result<Transport, ApiError> {
        match requested.unwrap_or(Transport::Cookie) {
            Transport::Bearer if !self.settings.sessions.bearer => {
                Err(ApiError::invalid(vec![("/transport", "unsupported")]))
            }
            transport => Ok(transport),
        }
    }
    fn require_password(
        &self,
    ) -> Result<(&crate::hasher::Hasher, &crate::password::PasswordPolicy), ApiError> {
        match (&self.hasher, &self.policy) {
            (Some(hasher), Some(policy)) => Ok((hasher, policy)),
            _ => Err(ApiError::code("not_found")),
        }
    }
    /// The reauthentication window (section 4).
    pub fn require_recent(&self, caller: &Caller) -> Result<(), ApiError> {
        let window = self.settings.sessions.reauthentication_window;
        if plus(caller.authenticated_at, window) < self.now() {
            Err(ApiError::code("reauthentication_required"))
        } else {
            Ok(())
        }
    }
    /// Email-sending routes: per IP, then per address and IP, then the
    /// shared per-address cap once the address-and-IP budget admitted the
    /// request (section 7). Naming a victim's address from other IPs spends
    /// those IPs' budgets.
    pub(crate) async fn email_limits(
        self: &Arc<Self>,
        email: &Email,
        client: &Client,
    ) -> Result<(), ApiError> {
        self.charge(gcra::EMAIL_IP, Subject::Ip(client.ip), client)
            .await?;
        self.charge(
            gcra::EMAIL_ADDRESS_IP,
            Subject::EmailIp(email.normalized(), client.ip),
            client,
        )
        .await?;
        self.charge(
            gcra::EMAIL_ADDRESS,
            Subject::Email(email.normalized()),
            client,
        )
        .await
    }
    /// Insert a user, or `None` when the address is taken (a concurrent
    /// registration won).
    pub(crate) async fn insert_user(
        &self,
        tx: &mut Tx<'_>,
        email: &Email,
        verified: bool,
        display_name: Option<&str>,
        browser: Option<[u8; 32]>,
    ) -> Result<Option<Uuid>, Failure> {
        let browser: Option<&[u8]> = browser.as_ref().map(|b| b.as_slice());
        let id = self.new_id()?;
        let now = self.now();
        let verified_at = verified.then_some(now);
        let row = self
            .db()
            .query_opt(
                &*tx,
                &crate::sql::USER_INSERT,
                &[
                    &id,
                    &email.display(),
                    &email.normalized(),
                    &verified_at,
                    &display_name,
                    &now,
                    &browser,
                ],
            )
            .await
            .map_err(sql)?;
        Ok(row.map(|r| r.get(0)))
    }
    async fn set_password(&self, tx: &mut Tx<'_>, user: Uuid, phc: &str) -> Result<(), Failure> {
        self.db()
            .execute(&*tx, &crate::sql::PASSWORD_SET, &[&user, &phc, &self.now()])
            .await
            .map_err(sql)?;
        Ok(())
    }
    pub(crate) async fn mark_verified(
        &self,
        tx: &mut Tx<'_>,
        user: &mut UserRow,
        client: &Client,
    ) -> Result<(), Failure> {
        if user.verified() {
            return Ok(());
        }
        let now = self.now();
        self.db()
            .execute(&*tx, &crate::sql::USER_VERIFY, &[&user.id, &now])
            .await
            .map_err(sql)?;
        user.verified_at = Some(now);
        self.audit(
            tx,
            Audit::new("user.email_verified", Actor::User(user.id)),
            client,
        )
        .await?;
        self.publish(
            tx,
            event::USER_EMAIL_VERIFIED,
            format!(
                "user:{}:verified:{}",
                user.id.simple(),
                now.as_microsecond()
            ),
            json!({"user_id": ids::render(ids::USER, user.id), "verified_at": now.to_string()}),
            client,
        )
        .await
    }
    pub(crate) async fn delete_sessions(
        &self,
        tx: &mut Tx<'_>,
        user: Uuid,
        keep: Option<Uuid>,
    ) -> Result<u64, Failure> {
        self.db()
            .execute(&*tx, &crate::sql::SESSIONS_OF_USER_DELETE, &[&user, &keep])
            .await
            .map_err(sql)
    }
    pub(crate) async fn delete_tokens(&self, tx: &mut Tx<'_>, user: Uuid) -> Result<(), Failure> {
        self.db()
            .execute(&*tx, &crate::sql::TOKENS_OF_USER_DELETE, &[&user])
            .await
            .map_err(sql)?;
        Ok(())
    }
    /// A user notice through the `Directory` (`User` recipient).
    pub(crate) async fn notice(
        &self,
        tx: &mut Tx<'_>,
        topic: &'static str,
        user: &UserRow,
        client: &Client,
    ) -> Result<(), Failure> {
        self.notify(
            tx,
            topic,
            Recipient::User {
                id: user.identity(),
            },
            Core::data(Some(user), None),
            None,
            self.new_id()?,
            None,
            client,
        )
        .await
    }
    /// Charge `token.ip` when a token route refused.
    pub(crate) async fn token_failed<T>(
        self: &Arc<Self>,
        result: Result<T, ApiError>,
        client: &Client,
    ) -> Result<T, ApiError> {
        if let Err(error) = &result
            && error.failure.code == "invalid_token"
        {
            self.charge_failure(gcra::TOKEN_IP, Subject::Ip(client.ip))
                .await;
        }
        result
    }

    // ----- registration -----

    /// `POST /registrations`: `202` for every valid request. A new address
    /// gets an account, a password and `identity.verify_email`; an existing
    /// one gets `identity.account_exists` and nothing else. Both paths hash
    /// once, so neither the answer nor its timing tells them apart.
    pub async fn register(
        self: &Arc<Self>,
        client: &Client,
        email: Email,
        password: Password,
        display_name: Option<String>,
        binding: Option<&str>,
    ) -> Result<Reply, ApiError> {
        let (hasher, policy) = self.require_password()?;
        if self.settings.registration != Registration::Open {
            return Err(ApiError::code("registration_closed"));
        }
        self.charge(gcra::REGISTRATION_IP, Subject::Ip(client.ip), client)
            .await?;
        self.email_limits(&email, client).await?;
        policy
            .check(&password, &email)
            .map_err(|e| ApiError::code(e.code()))?;
        let phc = hasher.hash(&password, self.ctx.random()).await?;
        drop(password);
        // Both paths set a binding cookie, so the answers stay identical; only
        // a new account stores its digest on the verification token. A
        // browser that already holds one keeps it.
        let binding = self.binding(binding)?;
        let browser = binding.digest();
        let core = self.clone();
        let client = client.clone();
        self.in_tx(move |tx| {
            Box::pin(async move {
                let existing = core.user_by_email(tx, email.normalized(), true).await?;
                let created = match existing {
                    Some(_) => None,
                    None => {
                        core.insert_user(tx, &email, false, display_name.as_deref(), Some(browser))
                            .await?
                    }
                };
                let Some(id) = created else {
                    let user = core.user_by_email(tx, email.normalized(), true).await?;
                    let dedupe = core.new_id()?;
                    core.notify(
                        tx,
                        topic::ACCOUNT_EXISTS,
                        Core::address(email.display(), user.as_ref())?,
                        Core::data(user.as_ref(), None),
                        None,
                        dedupe,
                        None,
                        &client,
                    )
                    .await?;
                    return Ok(());
                };
                core.set_password(tx, id, &phc).await?;
                let user = core
                    .user_by_id(tx, id, false)
                    .await?
                    .ok_or_else(|| Failure::transient("internal"))?;
                let (token_id, token, expires) = core
                    .issue_token(
                        tx,
                        Purpose::EmailVerification,
                        email.normalized(),
                        Some(id),
                        &email,
                        None,
                        crate::core::Bound::default(),
                        core.settings.tokens.email_verification_ttl,
                    )
                    .await?;
                core.notify(
                    tx,
                    topic::VERIFY_EMAIL,
                    Core::address(email.display(), Some(&user))?,
                    Core::data(Some(&user), Some(expires)),
                    Some((
                        Core::link(&core.settings.links.verify_email, &token),
                        "email_verification",
                    )),
                    token_id,
                    Some(expires),
                    &client,
                )
                .await?;
                core.audit(
                    tx,
                    Audit::new("user.registered", Actor::User(id)).detail("method", "password"),
                    &client,
                )
                .await?;
                core.publish(
                    tx,
                    event::USER_REGISTERED,
                    format!("user:{}:registered", id.simple()),
                    json!({"user_id": ids::render(ids::USER, id), "method": "password",
                           "registered_at": user.created_at.to_string()}),
                    &client,
                )
                .await?;
                Ok(())
            })
        })
        .await?;
        let mut reply = Reply::accepted("pending_verification");
        reply.binding = Some(Binding::Set(binding.encode()));
        Ok(reply)
    }

    /// `POST /email-verifications`: validate, rate-limit and queue one
    /// `identity.link_request` job (section 7), the same work for every
    /// address; the job sends a new link to an unverified, active account
    /// and nothing otherwise. Always `202`. It neither sets nor replaces the
    /// binding cookie: only the registering browser, bound on the user row,
    /// verifies without a claim, whoever asks for the resend.
    pub async fn resend_verification(
        self: &Arc<Self>,
        client: &Client,
        email: Email,
    ) -> Result<Reply, ApiError> {
        self.email_limits(&email, client).await?;
        self.request_link(
            crate::links::LinkPurpose::Verification,
            client,
            &email,
            None,
        )
        .await?;
        Ok(Reply::accepted("sent_if_eligible"))
    }

    /// `POST /email-verifications/confirm`: the token proves inbox control,
    /// so it marks the address verified and signs the user in. A click that
    /// carries the binding cookie of the browser that registered verifies
    /// the account as it is. Any other click on an unverified account is an
    /// ownership claim (section 2, pre-hijacking): the password and every
    /// other credential and session go, and the new session answers
    /// `"password_unset": true` so the client asks for a password.
    pub async fn confirm_verification(
        self: &Arc<Self>,
        client: &Client,
        token: &str,
        transport: Option<Transport>,
        binding: Option<&str>,
    ) -> Result<Reply, ApiError> {
        let transport = self.transport(transport)?;
        self.peek(gcra::TOKEN_IP, Subject::Ip(client.ip), client)
            .await?;
        let browser = binding.and_then(RawToken::decode).map(|b| b.digest());
        let result = self.verify_with(client, token, transport, browser).await;
        self.token_failed(result, client).await
    }
    async fn verify_with(
        self: &Arc<Self>,
        client: &Client,
        token: &str,
        transport: Transport,
        browser: Option<[u8; 32]>,
    ) -> Result<Reply, ApiError> {
        let token = RawToken::decode(token).ok_or_else(invalid_token)?;
        let core = self.clone();
        let client = client.clone();
        let outcome: Committed<(NewSession, UserRow, bool)> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let Some(consumed) = core
                        .consume_token(tx, Purpose::EmailVerification, &token)
                        .await?
                    else {
                        return Ok(Err(invalid_token()));
                    };
                    let Some(mut user) = core.token_user(tx, &consumed).await? else {
                        return Ok(Err(invalid_token()));
                    };
                    if !user.active() {
                        return Ok(Err(ApiError::code("account_disabled")));
                    }
                    // Only the binding stored at registration counts.
                    let stored: Option<Vec<u8>> = core
                        .db()
                        .query_one(&*tx, &crate::sql::USER_BINDING, &[&user.id])
                        .await
                        .map_err(sql)?
                        .get(0);
                    let same_browser = match (stored, browser) {
                        (Some(stored), Some(presented)) => bool::from(
                            subtle::ConstantTimeEq::ct_eq(stored.as_slice(), &presented[..]),
                        ),
                        _ => false,
                    };
                    let claimed = !user.verified() && !same_browser;
                    if claimed {
                        core.claim_unverified(tx, &mut user, &client).await?;
                    }
                    core.mark_verified(tx, &mut user, &client).await?;
                    let session = core
                        .create_session(
                            tx,
                            user.id,
                            Method::EmailToken,
                            transport,
                            &client,
                            None,
                            true,
                        )
                        .await?;
                    Ok(Ok((session, user, claimed)))
                })
            })
            .await?;
        let (session, user, _claimed) = outcome?;
        let password_unset = user.phc.is_none();
        let mut reply = self.session_reply(201, session, Some(&user));
        if let Some(body) = reply.body.as_mut() {
            body["session"]["password_unset"] = Value::Bool(password_unset);
        }
        reply.binding = Some(Binding::Clear);
        Ok(reply)
    }
    /// The locked user a consumed token names, when its address still is
    /// the one the token was sent to.
    async fn token_user(
        &self,
        tx: &mut Tx<'_>,
        consumed: &ConsumedToken,
    ) -> Result<Option<UserRow>, Failure> {
        let user = match consumed.user_id {
            Some(id) => self.user_by_id(tx, id, true).await?,
            None => {
                self.user_by_email(tx, &consumed.email_normalized, true)
                    .await?
            }
        };
        Ok(user.filter(|u| u.email_normalized == consumed.email_normalized))
    }

    // ----- password sign-in -----

    /// `POST /sessions`. Unknown address and wrong password answer the same
    /// `invalid_credentials` after one Argon2id verification each;
    /// `email_unverified` and `account_disabled` only after the password
    /// matched.
    pub async fn sign_in(
        self: &Arc<Self>,
        client: &Client,
        email: Email,
        password: Password,
        transport: Option<Transport>,
    ) -> Result<Reply, ApiError> {
        let (hasher, _) = self.require_password()?;
        let transport = self.transport(transport)?;
        if let Err(error) = self
            .charge(gcra::SIGN_IN_IP, Subject::Ip(client.ip), client)
            .await
        {
            self.metrics.login("password", "rate_limited");
            return Err(error);
        }
        let account_ip = Subject::EmailIp(email.normalized(), client.ip);
        let account = Subject::Email(email.normalized());
        let user = self.read_user_by_email(email.normalized()).await?;
        // Held from the peek to the charge, around the Argon2id work and one
        // statement on each side (section 7): at most `hash_concurrency`
        // attempts pass a nearly empty bucket.
        let gate = hasher.admit().await?;
        if let Err(error) = self
            .peek_many(
                &[
                    (gcra::SIGN_IN_ACCOUNT_IP, account_ip),
                    (gcra::SIGN_IN_ACCOUNT, account),
                ],
                client,
            )
            .await
        {
            drop(gate);
            self.metrics.login("password", "locked");
            return Err(error);
        }
        let matched = hasher
            .verify(&password, user.as_ref().and_then(|u| u.phc.as_deref()))
            .await?;
        if !matched {
            self.charge_failure(gcra::SIGN_IN_ACCOUNT_IP, account_ip)
                .await;
            self.charge_failure(gcra::SIGN_IN_ACCOUNT, account).await;
            drop(gate);
            let audit = Audit::new("session.sign_in_failed", Actor::Anonymous)
                .failed()
                .detail("method", "password")
                .detail("reason", "invalid_credentials");
            let audit = match &user {
                Some(user) => audit.target(user.id),
                None => audit.subject(self.keys.subject(email.normalized())),
            };
            self.audit_alone(audit, client).await;
            self.metrics.login("password", "invalid_credentials");
            merezha_telemetry::event!(
                DEBUG,
                "identity.login_failed",
                method = "password",
                outcome = "invalid_credentials"
            );
            return Err(ApiError::code("invalid_credentials"));
        }
        drop(gate);
        let Some(user) = user else {
            // The dummy never matches.
            return Err(ApiError::code("invalid_credentials"));
        };
        if self.settings.require_verified_email && !user.verified() {
            self.metrics.login("password", "unverified");
            return Err(ApiError::code("email_unverified"));
        }
        if !user.active() {
            self.metrics.login("password", "locked");
            return Err(ApiError::code("account_disabled"));
        }
        let old_phc = user.phc.clone().unwrap_or_default();
        let rehashed = if needs_rehash(&old_phc, hasher.cost()) {
            Some(hasher.hash(&password, self.ctx.random()).await?)
        } else {
            None
        };
        drop(password);
        let core = self.clone();
        let client_owned = client.clone();
        let outcome: Committed<(NewSession, UserRow)> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let Some(user) = core.user_by_id(tx, user.id, true).await? else {
                        return Ok(Err(ApiError::code("invalid_credentials")));
                    };
                    // The password changed since it was verified.
                    if user.phc.as_deref() != Some(old_phc.as_str()) || !user.active() {
                        return Ok(Err(ApiError::code("invalid_credentials")));
                    }
                    if let Some(phc) = rehashed {
                        core.set_password(tx, user.id, &phc).await?;
                    }
                    let session = core
                        .create_session(
                            tx,
                            user.id,
                            Method::Password,
                            transport,
                            &client_owned,
                            None,
                            true,
                        )
                        .await?;
                    if core.settings.notify_new_sign_in {
                        core.notice(tx, topic::NEW_SIGN_IN, &user, &client_owned)
                            .await?;
                    }
                    Ok(Ok((session, user)))
                })
            })
            .await?;
        let (session, user) = outcome?;
        self.metrics.login("password", "success");
        Ok(self.session_reply(201, session, Some(&user)))
    }

    // ----- sessions -----

    /// The caller's live session row and user.
    async fn caller_rows(
        self: &Arc<Self>,
        caller: &Caller,
    ) -> Result<(SessionRow, UserRow), ApiError> {
        let core = self.clone();
        let caller = *caller;
        let outcome: Committed<(SessionRow, UserRow)> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let session = core.session(tx, caller.session).await?;
                    let user = core.user_by_id(tx, caller.user, false).await?;
                    match (session, user) {
                        (Some(s), Some(u)) if s.alive(core.now()) => Ok(Ok((s, u))),
                        _ => Ok(Err(ApiError::code("unauthenticated"))),
                    }
                })
            })
            .await?;
        outcome
    }

    /// `GET /session`.
    pub async fn current_session(self: &Arc<Self>, caller: &Caller) -> Result<Reply, ApiError> {
        let (session, user) = self.caller_rows(caller).await?;
        Ok(Reply::json(
            200,
            json!({"session": session.json(true), "user": user.json(), "csrf_token": self.csrf(session.id)}),
        ))
    }

    /// `DELETE /session`: sign out.
    pub async fn sign_out(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
    ) -> Result<Reply, ApiError> {
        let core = self.clone();
        let client = client.clone();
        let caller = *caller;
        self.in_tx(move |tx| {
            Box::pin(async move {
                let revoked = core
                    .db()
                    .execute(
                        &*tx,
                        &crate::sql::SESSION_DELETE_OWN,
                        &[&caller.session, &caller.user],
                    )
                    .await
                    .map_err(sql)?;
                if revoked > 0 {
                    core.audit(
                        tx,
                        Audit::new("session.revoked", Actor::User(caller.user))
                            .detail("reason", "logout"),
                        &client,
                    )
                    .await?;
                }
                core.metrics.sessions_revoked("logout", revoked);
                Ok(())
            })
        })
        .await?;
        Ok(Reply::empty(CookieAction::Clear))
    }

    /// `POST /session/reauthenticate`: prove the password again; the
    /// session is rotated.
    pub async fn reauthenticate(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
        password: Password,
    ) -> Result<Reply, ApiError> {
        let (hasher, _) = self.require_password()?;
        let subject = Subject::Session(caller.session);
        let user = self.read_user(caller.user).await?;
        let gate = hasher.admit().await?;
        self.peek_many(&[(gcra::REAUTH_SESSION, subject)], client)
            .await?;
        let matched = hasher
            .verify(&password, user.as_ref().and_then(|u| u.phc.as_deref()))
            .await?;
        if !matched {
            self.charge_failure(gcra::REAUTH_SESSION, subject).await;
            drop(gate);
            self.audit_alone(
                Audit::new("session.reauthenticated", Actor::User(caller.user))
                    .failed()
                    .detail("reason", "invalid_credentials"),
                client,
            )
            .await;
            return Err(ApiError::code("invalid_credentials"));
        }
        let core = self.clone();
        let client = client.clone();
        let caller = *caller;
        let outcome: Committed<NewSession> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    if core.user_by_id(tx, caller.user, true).await?.is_none() {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    }
                    let Some(new) = core
                        .rotate_session(tx, caller.session, Method::Password, &client)
                        .await?
                    else {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    };
                    core.audit(
                        tx,
                        Audit::new("session.reauthenticated", Actor::User(caller.user))
                            .detail("method", "password"),
                        &client,
                    )
                    .await?;
                    Ok(Ok(new))
                })
            })
            .await?;
        Ok(self.session_reply(200, outcome?, None))
    }

    /// `GET /sessions`: the caller's live sessions.
    pub async fn list_sessions(self: &Arc<Self>, caller: &Caller) -> Result<Reply, ApiError> {
        let client = self.db().acquire(self.deadline()).await?;
        let now = self.now();
        let cap = i64::from(self.settings.sessions.max_per_user);
        let rows = self
            .db()
            .query(
                &client,
                &crate::sql::SESSION_LIST,
                &[&caller.user, &now, &cap],
            )
            .await
            .map_err(sql)?;
        let items: Vec<Value> = rows
            .iter()
            .map(|r| {
                let row = session_row(r, 0);
                let current = row.id == caller.session;
                row.listing(current)
            })
            .collect();
        Ok(Reply::json(200, json!({ "items": items })))
    }

    /// `DELETE /sessions/{session_id}`.
    pub async fn revoke_session(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
        id: &str,
    ) -> Result<Reply, ApiError> {
        let target = ids::parse(ids::SESSION, id)?;
        let core = self.clone();
        let client = client.clone();
        let caller = *caller;
        let outcome: Committed<()> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let revoked = core
                        .db()
                        .execute(
                            &*tx,
                            &crate::sql::SESSION_DELETE_OWN,
                            &[&target, &caller.user],
                        )
                        .await
                        .map_err(sql)?;
                    if revoked == 0 {
                        return Ok(Err(ApiError::code("not_found")));
                    }
                    core.audit(
                        tx,
                        Audit::new("session.revoked", Actor::User(caller.user))
                            .detail("reason", "revoked"),
                        &client,
                    )
                    .await?;
                    core.metrics.sessions_revoked("logout", revoked);
                    Ok(Ok(()))
                })
            })
            .await?;
        outcome?;
        Ok(Reply::empty(if target == caller.session {
            CookieAction::Clear
        } else {
            CookieAction::Keep
        }))
    }

    /// `POST /sessions/revoke-all`: log out everywhere, optionally keeping
    /// the current session.
    pub async fn revoke_all(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
        keep_current: bool,
    ) -> Result<Reply, ApiError> {
        let core = self.clone();
        let client = client.clone();
        let caller = *caller;
        let revoked = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let keep = keep_current.then_some(caller.session);
                    let revoked = core.delete_sessions(tx, caller.user, keep).await?;
                    core.audit(
                        tx,
                        Audit::new("session.revoked_all", Actor::User(caller.user))
                            .detail("count", revoked)
                            .detail("keep_current", keep_current),
                        &client,
                    )
                    .await?;
                    core.metrics.sessions_revoked("logout", revoked);
                    Ok(revoked)
                })
            })
            .await?;
        let mut reply = Reply::json(200, json!({ "revoked": revoked }));
        if !keep_current {
            reply.cookie = CookieAction::Clear;
        }
        Ok(reply)
    }

    // ----- magic links -----

    /// `POST /magic-links`: validate, rate-limit and queue one
    /// `identity.link_request` job, the same work for every address. The
    /// job sends a known active account a sign-in link and an unknown
    /// address a sign-up link when registration is open and
    /// `magic_links.sign_up` is on; anything else nothing. Always `202`.
    pub async fn request_magic_link(
        self: &Arc<Self>,
        client: &Client,
        email: Email,
        return_to: Option<String>,
    ) -> Result<Reply, ApiError> {
        if self.settings.magic_links.is_none() {
            return Err(ApiError::code("not_found"));
        }
        self.email_limits(&email, client).await?;
        self.request_link(
            crate::links::LinkPurpose::MagicLink,
            client,
            &email,
            return_to.as_deref(),
        )
        .await?;
        Ok(Reply::accepted("sent_if_eligible"))
    }

    /// `POST /magic-links/consume`: sign in, creating the account when the
    /// link was a sign-up link, and claim an unverified account under the
    /// pre-hijacking rule.
    pub async fn consume_magic_link(
        self: &Arc<Self>,
        client: &Client,
        token: &str,
        transport: Option<Transport>,
    ) -> Result<Reply, ApiError> {
        if self.settings.magic_links.is_none() {
            return Err(ApiError::code("not_found"));
        }
        let transport = self.transport(transport)?;
        self.peek(gcra::TOKEN_IP, Subject::Ip(client.ip), client)
            .await?;
        let result = self.magic_with(client, token, transport).await;
        self.token_failed(result, client).await
    }
    async fn magic_with(
        self: &Arc<Self>,
        client: &Client,
        token: &str,
        transport: Transport,
    ) -> Result<Reply, ApiError> {
        let token = RawToken::decode(token).ok_or_else(invalid_token)?;
        let core = self.clone();
        let client_owned = client.clone();
        let outcome: Committed<(NewSession, UserRow, Option<String>)> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let client = client_owned;
                    let Some(consumed) = core.consume_token(tx, Purpose::MagicLink, &token).await? else {
                        return Ok(Err(invalid_token()));
                    };
                    let mut user = match core.token_user(tx, &consumed).await? {
                        Some(user) => user,
                        None if consumed.user_id.is_some() => return Ok(Err(invalid_token())),
                        None => {
                            let open = core.settings.registration == Registration::Open
                                && core.settings.magic_links == Some(true);
                            if !open {
                                return Ok(Err(ApiError::code("registration_closed")));
                            }
                            let Ok(email) = normalize::email(&consumed.email) else {
                                return Ok(Err(invalid_token()));
                            };
                            let (id, created) = match core.insert_user(tx, &email, true, None, None).await? {
                                Some(id) => (id, true),
                                // Registered meanwhile: sign in to that account.
                                None => match core.user_by_email(tx, email.normalized(), true).await? {
                                    Some(user) => (user.id, false),
                                    None => return Ok(Err(invalid_token())),
                                },
                            };
                            let user = core
                                .user_by_id(tx, id, true)
                                .await?
                                .ok_or_else(|| Failure::transient("internal"))?;
                            if created {
                                core.audit(
                                    tx,
                                    Audit::new("user.registered", Actor::User(id)).detail("method", "magic_link"),
                                    &client,
                                )
                                .await?;
                                core.publish(
                                    tx,
                                    event::USER_REGISTERED,
                                    format!("user:{}:registered", id.simple()),
                                    json!({"user_id": ids::render(ids::USER, id), "method": "magic_link",
                                           "registered_at": user.created_at.to_string()}),
                                    &client,
                                )
                                .await?;
                            }
                            user
                        }
                    };
                    if !user.active() {
                        return Ok(Err(ApiError::code("account_disabled")));
                    }
                    if !user.verified() {
                        core.claim_unverified(tx, &mut user, &client).await?;
                        core.mark_verified(tx, &mut user, &client).await?;
                    }
                    let session = core
                        .create_session(tx, user.id, Method::MagicLink, transport, &client, None, true)
                        .await?;
                    Ok(Ok((session, user, consumed.return_to)))
                })
            })
            .await?;
        let (session, user, return_to) = outcome?;
        let mut reply = self.session_reply(201, session, Some(&user));
        if let Some(body) = reply.body.as_mut() {
            body["return_to"] = return_to
                .map(|path| Value::String(format!("{}{path}", self.settings.app_origin)))
                .unwrap_or(Value::Null);
        }
        self.metrics.login("magic_link", "success");
        Ok(reply)
    }

    // ----- password reset -----

    /// `POST /password-resets`: validate, rate-limit and queue one
    /// `identity.link_request` job, the same work for every address; the
    /// job sends a reset link to a known active account and nothing
    /// otherwise. Always `202`.
    pub async fn request_password_reset(
        self: &Arc<Self>,
        client: &Client,
        email: Email,
    ) -> Result<Reply, ApiError> {
        self.require_password()?;
        self.email_limits(&email, client).await?;
        self.request_link(
            crate::links::LinkPurpose::PasswordReset,
            client,
            &email,
            None,
        )
        .await?;
        Ok(Reply::accepted("sent_if_eligible"))
    }

    /// `POST /password-resets/complete`: set the password, revoke every
    /// session, verify the address (claiming an unverified account first)
    /// and sign in. A password the policy refuses leaves the token usable.
    pub async fn complete_password_reset(
        self: &Arc<Self>,
        client: &Client,
        token: &str,
        password: Password,
        transport: Option<Transport>,
    ) -> Result<Reply, ApiError> {
        let (hasher, policy) = self.require_password()?;
        let transport = self.transport(transport)?;
        self.peek(gcra::TOKEN_IP, Subject::Ip(client.ip), client)
            .await?;
        let result = async {
            let token = RawToken::decode(token).ok_or_else(invalid_token)?;
            let email = self
                .pending_token_email(Purpose::PasswordReset, &token)
                .await?;
            policy
                .check(&password, &email)
                .map_err(|e| ApiError::code(e.code()))?;
            let phc = hasher.hash(&password, self.ctx.random()).await?;
            self.reset_with(client, token, phc, transport).await
        }
        .await;
        self.token_failed(result, client).await
    }
    /// The address a live token was sent to, without consuming it.
    async fn pending_token_email(
        &self,
        purpose: Purpose,
        token: &RawToken,
    ) -> Result<Email, ApiError> {
        let digest = token.digest();
        let d: &[u8] = &digest;
        let client = self.db().acquire(self.deadline()).await?;
        let row = self
            .db()
            .query_opt(&client, &crate::sql::TOKEN_PEEK, &[&d, &purpose.as_str()])
            .await
            .map_err(sql)?;
        match row {
            Some(row) if row.get::<_, Timestamp>(1) > self.now() => {
                normalize::email(row.get(0)).map_err(|_| invalid_token())
            }
            _ => Err(invalid_token()),
        }
    }
    async fn reset_with(
        self: &Arc<Self>,
        client: &Client,
        token: RawToken,
        phc: String,
        transport: Transport,
    ) -> Result<Reply, ApiError> {
        let core = self.clone();
        let client = client.clone();
        let outcome: Committed<(NewSession, UserRow)> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let Some(consumed) = core
                        .consume_token(tx, Purpose::PasswordReset, &token)
                        .await?
                    else {
                        return Ok(Err(invalid_token()));
                    };
                    let Some(mut user) = core.token_user(tx, &consumed).await? else {
                        return Ok(Err(invalid_token()));
                    };
                    if !user.active() {
                        return Ok(Err(ApiError::code("account_disabled")));
                    }
                    if !user.verified() {
                        core.claim_unverified(tx, &mut user, &client).await?;
                        core.mark_verified(tx, &mut user, &client).await?;
                    }
                    core.set_password(tx, user.id, &phc).await?;
                    user.phc = Some(phc);
                    let revoked = core.delete_sessions(tx, user.id, None).await?;
                    core.metrics.sessions_revoked("password_change", revoked);
                    core.delete_tokens(tx, user.id).await?;
                    core.audit(
                        tx,
                        Audit::new("user.password_reset", Actor::User(user.id)),
                        &client,
                    )
                    .await?;
                    core.notice(tx, topic::PASSWORD_CHANGED, &user, &client)
                        .await?;
                    let session = core
                        .create_session(
                            tx,
                            user.id,
                            Method::Password,
                            transport,
                            &client,
                            None,
                            true,
                        )
                        .await?;
                    Ok(Ok((session, user)))
                })
            })
            .await?;
        let (session, user) = outcome?;
        Ok(self.session_reply(201, session, Some(&user)))
    }

    // ----- the caller's account -----

    /// `GET /me`.
    pub async fn me(self: &Arc<Self>, user: Uuid) -> Result<Reply, ApiError> {
        let user = self
            .read_user(user)
            .await?
            .ok_or_else(|| ApiError::code("unauthenticated"))?;
        Ok(Reply::json(200, json!({ "user": user.json() })))
    }

    /// `PATCH /me`: `display_name` and `locale`; `null` clears one.
    pub async fn update_me(
        self: &Arc<Self>,
        caller: &Caller,
        display_name: Option<Option<String>>,
        locale: Option<Option<String>>,
    ) -> Result<Reply, ApiError> {
        let core = self.clone();
        let caller = *caller;
        let user = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let now = core.now();
                    core.db()
                        .execute(
                            &*tx,
                            &crate::sql::USER_PROFILE,
                            &[
                                &caller.user,
                                &display_name.is_some(),
                                &display_name.flatten(),
                                &locale.is_some(),
                                &locale.flatten(),
                                &now,
                            ],
                        )
                        .await
                        .map_err(sql)?;
                    core.user_by_id(tx, caller.user, false)
                        .await?
                        .ok_or_else(|| ApiError::code("unauthenticated"))
                })
            })
            .await?;
        Ok(Reply::json(200, json!({ "user": user.json() })))
    }

    /// `PUT /me/password` (recent session): set a new password, rotate the
    /// current session and revoke every other one.
    pub async fn change_password(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
        password: Password,
    ) -> Result<Reply, ApiError> {
        let (hasher, policy) = self.require_password()?;
        self.require_recent(caller)?;
        let user = self
            .read_user(caller.user)
            .await?
            .ok_or_else(|| ApiError::code("unauthenticated"))?;
        let email = normalize::email(&user.email).map_err(|_| Failure::transient("internal"))?;
        policy
            .check(&password, &email)
            .map_err(|e| ApiError::code(e.code()))?;
        let phc = hasher.hash(&password, self.ctx.random()).await?;
        drop(password);
        let core = self.clone();
        let client = client.clone();
        let caller = *caller;
        let outcome: Committed<NewSession> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let Some(user) = core.user_by_id(tx, caller.user, true).await? else {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    };
                    let Some(new) = core
                        .rotate_session(tx, caller.session, Method::Password, &client)
                        .await?
                    else {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    };
                    core.set_password(tx, user.id, &phc).await?;
                    // Every token goes, except an email change the rotated
                    // session itself asked for.
                    core.db()
                        .execute(
                            &*tx,
                            &crate::sql::TOKENS_OF_USER_DELETE_KEEPING,
                            &[&user.id, &new.row.id],
                        )
                        .await
                        .map_err(sql)?;
                    let revoked = core.delete_sessions(tx, user.id, Some(new.row.id)).await?;
                    core.metrics.sessions_revoked("password_change", revoked);
                    core.audit(
                        tx,
                        Audit::new("user.password_changed", Actor::User(user.id)),
                        &client,
                    )
                    .await?;
                    core.notice(tx, topic::PASSWORD_CHANGED, &user, &client)
                        .await?;
                    Ok(Ok(new))
                })
            })
            .await?;
        Ok(self.session_reply(200, outcome?, None))
    }

    /// `POST /me/email-changes` (recent session): a confirmation link to
    /// the new address, bound to the requesting session, and a notice to
    /// the current address. Always `202`.
    pub async fn request_email_change(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
        new_email: Email,
    ) -> Result<Reply, ApiError> {
        self.require_recent(caller)?;
        self.email_limits(&new_email, client).await?;
        let core = self.clone();
        let client = client.clone();
        let caller = *caller;
        self.in_tx(move |tx| {
            Box::pin(async move {
                let Some(user) = core.user_by_id(tx, caller.user, true).await? else {
                    return Err(ApiError::code("unauthenticated"));
                };
                if user.email_normalized == new_email.normalized() {
                    return Ok(());
                }
                let (token_id, token, expires) = core
                    .issue_token(
                        tx,
                        Purpose::EmailChange,
                        &user.id.simple().to_string(),
                        Some(user.id),
                        &new_email,
                        None,
                        crate::core::Bound {
                            session: Some(caller.session),
                        },
                        core.settings.tokens.email_change_ttl,
                    )
                    .await?;
                core.notify(
                    tx,
                    topic::EMAIL_CHANGE_CONFIRM,
                    Core::address(new_email.display(), Some(&user))?,
                    Core::data(Some(&user), Some(expires)),
                    Some((
                        Core::link(&core.settings.links.email_change, &token),
                        "email_change",
                    )),
                    token_id,
                    Some(expires),
                    &client,
                )
                .await?;
                // The notice goes to the address the account has now.
                core.notify(
                    tx,
                    topic::EMAIL_CHANGE_NOTICE,
                    Core::address(&user.email, Some(&user))?,
                    Core::data(Some(&user), None),
                    None,
                    core.new_id()?,
                    None,
                    &client,
                )
                .await?;
                core.audit(
                    tx,
                    Audit::new("user.email_change_requested", Actor::User(user.id)),
                    &client,
                )
                .await?;
                Ok(())
            })
        })
        .await?;
        Ok(Reply::accepted("sent_if_eligible"))
    }

    /// `POST /me/email-changes/confirm`: only the session that requested
    /// the change confirms it (`401 sign_in_required` otherwise, nothing
    /// changes and the token stays usable). Swap the address when it is
    /// still free (`409 email_taken` otherwise), rotate that session and
    /// revoke the others.
    pub async fn confirm_email_change(
        self: &Arc<Self>,
        client: &Client,
        caller: Option<&Caller>,
        token: &str,
    ) -> Result<Reply, ApiError> {
        self.peek(gcra::TOKEN_IP, Subject::Ip(client.ip), client)
            .await?;
        let result = self.email_change_with(client, caller.copied(), token).await;
        self.token_failed(result, client).await
    }
    async fn email_change_with(
        self: &Arc<Self>,
        client: &Client,
        caller: Option<Caller>,
        token: &str,
    ) -> Result<Reply, ApiError> {
        let token = RawToken::decode(token).ok_or_else(invalid_token)?;
        let core = self.clone();
        let client = client.clone();
        let outcome: Committed<(UserRow, Option<NewSession>)> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    // The token is bound to the session that asked: any
                    // other caller changes nothing and keeps the token.
                    if let Some(pending) = core.peek_token(tx, Purpose::EmailChange, &token).await?
                        && pending.expires_at > core.now()
                        && caller.map(|c| c.session) != pending.session
                    {
                        return Err(ApiError::code("sign_in_required"));
                    }
                    let Some(consumed) = core.consume_token(tx, Purpose::EmailChange, &token).await? else {
                        return Ok(Err(invalid_token()));
                    };
                    let Some(id) = consumed.user_id else {
                        return Ok(Err(invalid_token()));
                    };
                    let Some(mut user) = core.user_by_id(tx, id, true).await? else {
                        return Ok(Err(invalid_token()));
                    };
                    if consumed.subject_key != user.id.simple().to_string() {
                        return Ok(Err(invalid_token()));
                    }
                    let taken = core.db().query_opt(&*tx, &crate::sql::USER_EMAIL_TAKEN,
                            &[&consumed.email_normalized, &user.id],
                        )
                        .await
                        .map_err(sql)?;
                    if taken.is_some() {
                        return Ok(Err(ApiError::conflict("email_taken")));
                    }
                    let now = core.now();
                    core.db().execute(&*tx, &crate::sql::USER_SET_EMAIL,
                        &[&user.id, &consumed.email, &consumed.email_normalized, &now],
                    )
                    .await
                    .map_err(|e| {
                        let failure = sql(e);
                        if failure.code == "unique_violation" {
                            Failure::conflict("email_taken")
                        } else {
                            failure
                        }
                    })?;
                    user.email = consumed.email.clone();
                    user.email_normalized = consumed.email_normalized.clone();
                    user.verified_at = Some(now);
                    core.delete_tokens(tx, user.id).await?;
                    let rotated = match caller {
                        Some(caller) if caller.user == user.id && Some(caller.session) == consumed.session => {
                            core.rotate_session(tx, caller.session, Method::EmailToken, &client).await?
                        }
                        _ => None,
                    };
                    let revoked = core.delete_sessions(tx, user.id, rotated.as_ref().map(|n| n.row.id)).await?;
                    core.metrics.sessions_revoked("password_change", revoked);
                    core.audit(tx, Audit::new("user.email_changed", Actor::User(user.id)), &client)
                        .await?;
                    core.publish(
                        tx,
                        event::USER_EMAIL_CHANGED,
                        format!("user:{}:email:{}", user.id.simple(), consumed.id.simple()),
                        json!({"user_id": ids::render(ids::USER, user.id), "changed_at": now.to_string()}),
                        &client,
                    )
                    .await?;
                    Ok(Ok((user, rotated)))
                })
            })
            .await?;
        let (user, rotated) = outcome?;
        match rotated {
            Some(new) => Ok(self.session_reply(200, new, Some(&user))),
            None => Ok(Reply::json(200, json!({ "user": user.json() }))),
        }
    }

    /// `DELETE /me` (recent session): hard-delete the account, refused
    /// `409 sole_owner` while the user is the only owner of an
    /// organization. Audit rows keep the user id;
    /// `identity.membership_removed` (reason `user_deleted`) and
    /// `identity.user_deleted` tell other blocks to erase what they hold.
    pub async fn delete_me(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
        confirm: Email,
    ) -> Result<Reply, ApiError> {
        self.require_recent(caller)?;
        let core = self.clone();
        let client = client.clone();
        let caller = *caller;
        let outcome: Committed<()> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let Some(user) = core.user_by_id(tx, caller.user, true).await? else {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    };
                    if user.email_normalized != confirm.normalized() {
                        return Ok(Err(ApiError::invalid(vec![("/confirm_email", "mismatch")])));
                    }
                    // The last owner of an organization cannot leave it
                    // without one.
                    if let Err(error) = core.before_user_deleted(tx, user.id, &client).await {
                        return if error.failure.code == "sole_owner" {
                            Ok(Err(error))
                        } else {
                            Err(error)
                        };
                    }
                    let now = core.now();
                    core.audit(tx, Audit::new("user.deleted", Actor::User(user.id)), &client)
                        .await?;
                    core.publish(
                        tx,
                        event::USER_DELETED,
                        format!("user:{}:deleted", user.id.simple()),
                        json!({"user_id": ids::render(ids::USER, user.id), "deleted_at": now.to_string()}),
                        &client,
                    )
                    .await?;
                    let sessions = core.db().execute(&*tx, &crate::sql::SESSIONS_OF_USER_DELETE, &[&user.id, &None::<Uuid>])
                        .await
                        .map_err(sql)?;
                    core.metrics.sessions_revoked("logout", sessions);
                    // The service account keys it minted outlive the row
                    // (no foreign key): revoke them first.
                    core.revoke_everything_minted(tx, user.id, "minter_deleted", Actor::User(user.id), &client)
                        .await?;
                    core.db().execute(&*tx, &crate::sql::USER_DELETE, &[&user.id])
                        .await
                        .map_err(sql)?;
                    Ok(Ok(()))
                })
            })
            .await?;
        outcome?;
        Ok(Reply::empty(CookieAction::Clear))
    }

    /// `GET /me/security-events`: the caller's audit rows, newest first.
    pub async fn security_events(
        self: &Arc<Self>,
        caller: &Caller,
        cursor: Option<Uuid>,
        limit: i64,
    ) -> Result<Reply, ApiError> {
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(
                &client,
                &crate::sql::AUDIT_LIST,
                &[&caller.user, &cursor, &(limit + 1)],
            )
            .await
            .map_err(sql)?;
        let more = rows.len() as i64 > limit;
        let items: Vec<Value> = rows
            .iter()
            .take(limit as usize)
            .map(|r| {
                let ip: Option<std::net::IpAddr> = r.get(4);
                json!({
                    "id": r.get::<_, Uuid>(0).hyphenated().to_string(),
                    "occurred_at": r.get::<_, Timestamp>(1).to_string(),
                    "action": r.get::<_, String>(2),
                    "outcome": r.get::<_, String>(3),
                    "ip": ip.map(crate::core::truncate_ip),
                    "user_agent": r.get::<_, Option<String>>(5),
                    "details": r.get::<_, Value>(6),
                })
            })
            .collect();
        let next = if more {
            items
                .last()
                .and_then(|i| i["id"].as_str().map(str::to_owned))
        } else {
            None
        };
        Ok(Reply::json(
            200,
            json!({ "items": items, "next_cursor": next }),
        ))
    }
}
