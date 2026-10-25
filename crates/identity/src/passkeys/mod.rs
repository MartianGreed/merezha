//! WebAuthn passkeys with `webauthn-rs` (section 6), behind the `passkeys`
//! cargo feature: registration by a signed-in user with a recent session,
//! email-first and discoverable (conditional UI) sign-in, reauthentication
//! and management. User verification is required, attestation is none, and
//! the RP id and origins come from configuration only.
//!
//! Ceremony state (`PasskeyRegistration`, `PasskeyAuthentication`,
//! `DiscoverableAuthentication`) is serialised into `webauthn_ceremonies`
//! and never leaves the server, the case `danger-allow-state-serialisation`
//! documents as safe. A ceremony is consumed by `DELETE ... RETURNING`
//! before it is verified, is bound to its user and session, and lives
//! `passkeys.ceremony_ttl`.
pub mod routes;

use crate::{
    config::PasskeySettings,
    core::{
        Actor, ApiError, Audit, Client, Core, Method, NewSession, Subject, Transport, UserRow,
        plus, sql, topic,
    },
    flows::{Caller, CookieAction, Reply},
    gcra, ids,
    normalize::Email,
};
use base64::Engine;
use jiff::Timestamp;
use merezha_domain::Failure;
use merezha_platform::Tx;
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;
use webauthn_rs::prelude::{
    AuthenticationResult, DiscoverableAuthentication, DiscoverableKey, Passkey,
    PasskeyAuthentication, PasskeyRegistration, PublicKeyCredential, RegisterPublicKeyCredential,
    Url, Webauthn, WebauthnBuilder, WebauthnError,
};

/// Largest body of the verification routes (section 4).
pub const MAX_VERIFY_BODY: usize = 64 * 1024;
/// Longest passkey label, in scalar values.
pub const MAX_LABEL: usize = 64;

/// The relying party of `settings`: RP id, name and every origin, built
/// once at start.
pub fn relying_party(settings: &PasskeySettings) -> Result<Webauthn, Failure> {
    let invalid = |_| Failure::permanent("passkeys_config");
    let primary = Url::parse(
        settings
            .origins
            .first()
            .ok_or(Failure::permanent("passkeys_config"))?,
    )
    .map_err(invalid)?;
    let mut builder = WebauthnBuilder::new(&settings.rp_id, &primary)
        .map_err(|_| Failure::permanent("passkeys_config"))?
        .rp_name(&settings.rp_name)
        .timeout(settings.ceremony_ttl);
    for origin in settings.origins.iter().skip(1) {
        builder = builder.append_allowed_origin(&Url::parse(origin).map_err(invalid)?);
    }
    builder
        .build()
        .map_err(|_| Failure::permanent("passkeys_config"))
}

/// A `passkeys` row.
#[derive(Clone)]
struct PasskeyRow {
    id: Uuid,
    credential_id: Vec<u8>,
    passkey: Passkey,
    label: Option<String>,
    aaguid: Option<Uuid>,
    backup_eligible: bool,
    backup_state: bool,
    discoverable: Option<bool>,
    transports: Vec<String>,
    revision: i64,
    created_at: Timestamp,
    last_used_at: Option<Timestamp>,
    user_id: Uuid,
}
fn passkey_row(row: &tokio_postgres::Row) -> Result<PasskeyRow, Failure> {
    let passkey: Value = row.get(2);
    Ok(PasskeyRow {
        id: row.get(0),
        credential_id: row.get(1),
        passkey: serde_json::from_value(passkey).map_err(|_| Failure::transient("internal"))?,
        label: row.get(3),
        aaguid: row.get(4),
        backup_eligible: row.get(5),
        backup_state: row.get(6),
        discoverable: row.get(7),
        transports: row.get(8),
        revision: row.get(9),
        created_at: row.get(10),
        last_used_at: row.get(11),
        user_id: row.get(12),
    })
}
impl PasskeyRow {
    /// The `passkey` shape of section 4.
    fn json(&self) -> Value {
        json!({
            "id": ids::render(ids::PASSKEY, self.id),
            "label": self.label,
            "aaguid": self.aaguid.map(|a| a.hyphenated().to_string()),
            "backup_eligible": self.backup_eligible,
            "backup_state": self.backup_state,
            "discoverable": self.discoverable,
            "transports": self.transports,
            "created_at": self.created_at.to_string(),
            "last_used_at": self.last_used_at.map(|t| t.to_string()),
        })
    }
}

/// What the stored `Passkey` says of its backup flags.
fn backup_flags(passkey: &Value) -> (bool, bool) {
    let cred = &passkey["cred"];
    (
        cred["backup_eligible"].as_bool().unwrap_or(false),
        cred["backup_state"].as_bool().unwrap_or(false),
    )
}
/// The AAGUID of an attested registration, when nonzero.
fn aaguid(passkey: &Value) -> Option<Uuid> {
    let metadata = &passkey["cred"]["attestation"]["metadata"];
    let text = metadata
        .get("Packed")
        .or_else(|| metadata.get("Tpm"))?
        .get("aaguid")?
        .as_str()?;
    Uuid::try_parse(text).ok().filter(|a| !a.is_nil())
}
/// The transports a credential reported, bounded.
fn transports(passkey: &Value) -> Vec<String> {
    passkey["cred"]["transports"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|t| t.as_str())
                .filter(|t| t.len() <= 16)
                .take(8)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// A consumed `webauthn_ceremonies` row.
struct Ceremony {
    purpose: String,
    user_id: Option<Uuid>,
    session_id: Option<Uuid>,
    state: Value,
}

fn invalid_ceremony() -> ApiError {
    ApiError::code("invalid_ceremony")
}
fn invalid_credentials() -> ApiError {
    ApiError::code("invalid_credentials")
}
fn internal<E>(_: E) -> ApiError {
    Failure::transient("internal").into()
}

/// Remove the `transports` hints and `extensions` of an options object,
/// so an email-first answer has the same shape for real and decoy
/// credentials.
fn uniform_request(public_key: &mut Value) {
    if let Some(map) = public_key.as_object_mut() {
        map.remove("extensions");
    }
    if let Some(items) = public_key
        .get_mut("allowCredentials")
        .and_then(Value::as_array_mut)
    {
        for item in items {
            if let Some(map) = item.as_object_mut() {
                map.remove("transports");
            }
        }
    }
}

impl Core {
    fn relying(&self) -> Result<&Webauthn, ApiError> {
        self.webauthn
            .as_deref()
            .ok_or_else(|| ApiError::code("not_found"))
    }
    fn passkey_settings(&self) -> Result<&PasskeySettings, ApiError> {
        self.settings
            .passkeys
            .as_ref()
            .ok_or_else(|| ApiError::code("not_found"))
    }
    /// A random (UUIDv4) ceremony id: ceremonies are not time ordered and
    /// their id is the client's only handle on them.
    fn ceremony_id(&self) -> Result<Uuid, Failure> {
        let mut bytes = [0u8; 16];
        self.ctx.random().fill(&mut bytes)?;
        Ok(uuid::Builder::from_random_bytes(bytes).into_uuid())
    }
    async fn passkeys_of(&self, tx: &mut Tx<'_>, user: Uuid) -> Result<Vec<PasskeyRow>, Failure> {
        self.db()
            .query(&*tx, &crate::sql::PASSKEYS_OF_USER, &[&user])
            .await
            .map_err(sql)?
            .iter()
            .map(passkey_row)
            .collect()
    }
    async fn read_passkeys(&self, user: Uuid) -> Result<Vec<PasskeyRow>, Failure> {
        let client = self.db().acquire(self.deadline()).await?;
        self.db()
            .query(&client, &crate::sql::PASSKEYS_OF_USER, &[&user])
            .await
            .map_err(sql)?
            .iter()
            .map(passkey_row)
            .collect()
    }
    async fn store_ceremony(
        &self,
        tx: &mut Tx<'_>,
        purpose: &str,
        user: Option<Uuid>,
        session: Option<Uuid>,
        state: Value,
    ) -> Result<Uuid, Failure> {
        let id = self.ceremony_id()?;
        let now = self.now();
        let ttl = self
            .settings
            .passkeys
            .as_ref()
            .map_or(std::time::Duration::from_secs(300), |p| p.ceremony_ttl);
        self.db()
            .execute(
                &*tx,
                &crate::sql::CEREMONY_INSERT,
                &[
                    &id,
                    &purpose,
                    &user,
                    &session,
                    &state,
                    &now,
                    &plus(now, ttl),
                ],
            )
            .await
            .map_err(sql)?;
        Ok(id)
    }
    /// Consume ceremony `id` of one of `purposes` in its own transaction:
    /// single use whatever the verification says. Unknown, expired, of
    /// another purpose: `None`.
    async fn take_ceremony(
        self: &Arc<Self>,
        id: &str,
        purposes: &'static [&'static str],
    ) -> Result<Option<Ceremony>, ApiError> {
        let Ok(id) = ids::parse(ids::CEREMONY, id) else {
            return Ok(None);
        };
        let core = self.clone();
        let purposes: Vec<&str> = purposes.to_vec();
        let row = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    Ok(core
                        .db()
                        .query_opt(&*tx, &crate::sql::CEREMONY_CONSUME, &[&id, &purposes])
                        .await
                        .map_err(sql)?)
                })
            })
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        if row.get::<_, Timestamp>(4) <= self.now() {
            return Ok(None);
        }
        Ok(Some(Ceremony {
            purpose: row.get(0),
            user_id: row.get(1),
            session_id: row.get(2),
            state: row.get(3),
        }))
    }
    /// Write the authenticator's new counter and backup flags back with a
    /// compare-and-swap on `revision`; a lost race merges into a fresh
    /// read (counters only ever grow) and retries once. The caller holds
    /// the user lock, which every passkey writer takes.
    async fn record_use(
        &self,
        tx: &mut Tx<'_>,
        row: &PasskeyRow,
        result: &AuthenticationResult,
    ) -> Result<(), Failure> {
        let mut current = row.clone();
        for _ in 0..2 {
            let mut passkey = current.passkey.clone();
            passkey.update_credential(result);
            let value =
                serde_json::to_value(&passkey).map_err(|_| Failure::transient("internal"))?;
            let (eligible, state) = backup_flags(&value);
            let written = self
                .db()
                .execute(
                    &*tx,
                    &crate::sql::PASSKEY_UPDATE,
                    &[
                        &current.id,
                        &current.revision,
                        &value,
                        &eligible,
                        &state,
                        &self.now(),
                    ],
                )
                .await
                .map_err(sql)?;
            if written == 1 {
                return Ok(());
            }
            let fresh = self
                .db()
                .query_opt(&*tx, &crate::sql::PASSKEY_GET, &[&row.id, &row.user_id])
                .await
                .map_err(sql)?;
            match fresh {
                Some(fresh) => current = passkey_row(&fresh)?,
                // Removed meanwhile: nothing to update.
                None => return Ok(()),
            }
        }
        Err(Failure::conflict("conflict"))
    }

    // ----- registration -----

    /// `POST /passkeys/registrations/options` (recent session).
    pub async fn passkey_registration_options(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
    ) -> Result<Reply, ApiError> {
        self.relying()?;
        let max = i64::from(self.passkey_settings()?.max_per_user);
        self.require_recent(caller)?;
        self.charge(gcra::PASSKEY_IP, Subject::Ip(client.ip), client)
            .await?;
        let core = self.clone();
        let caller = *caller;
        let outcome: Result<(Uuid, Value), ApiError> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let Some(user) = core.user_by_id(tx, caller.user, true).await? else {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    };
                    let existing = core.passkeys_of(tx, user.id).await?;
                    if existing.len() as i64 >= max {
                        return Ok(Err(ApiError::code("passkey_limit")));
                    }
                    let handle: Uuid = core
                        .db()
                        .query_one(
                            &*tx,
                            &crate::sql::USER_WEBAUTHN_HANDLE,
                            &[&user.id, &core.ceremony_id()?],
                        )
                        .await
                        .map_err(sql)?
                        .get(0);
                    let exclude = existing
                        .iter()
                        .map(|p| p.passkey.cred_id().clone())
                        .collect();
                    let display = user
                        .display_name
                        .clone()
                        .unwrap_or_else(|| user.email.clone());
                    let (challenge, state) = core
                        .relying()?
                        .start_passkey_registration(handle, &user.email, &display, Some(exclude))
                        .map_err(internal)?;
                    let state = serde_json::to_value(&state).map_err(internal)?;
                    let id = core
                        .store_ceremony(
                            tx,
                            "registration",
                            Some(user.id),
                            Some(caller.session),
                            state,
                        )
                        .await?;
                    let challenge = serde_json::to_value(&challenge).map_err(internal)?;
                    Ok(Ok((id, challenge["publicKey"].clone())))
                })
            })
            .await?;
        let (id, public_key) = outcome?;
        Ok(Reply::json(
            200,
            json!({"ceremony_id": ids::render(ids::CEREMONY, id), "public_key": public_key}),
        ))
    }

    /// `POST /passkeys/registrations/verify` (recent session): the
    /// ceremony must be the caller's, user and session.
    pub async fn passkey_registration_verify(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
        ceremony: &str,
        credential: RegisterPublicKeyCredential,
        label: Option<String>,
    ) -> Result<Reply, ApiError> {
        let webauthn = self.relying()?;
        let max = i64::from(self.passkey_settings()?.max_per_user);
        self.require_recent(caller)?;
        let ceremony = self
            .take_ceremony(ceremony, &["registration"])
            .await?
            .filter(|c| c.user_id == Some(caller.user) && c.session_id == Some(caller.session))
            .ok_or_else(invalid_ceremony)?;
        let state: PasskeyRegistration =
            serde_json::from_value(ceremony.state).map_err(|_| invalid_ceremony())?;
        let passkey = webauthn
            .finish_passkey_registration(&credential, &state)
            .map_err(|_| ApiError::code("passkey_rejected"))?;
        let value = serde_json::to_value(&passkey).map_err(internal)?;
        let credential_id: Vec<u8> = passkey.cred_id().as_ref().to_vec();
        let discoverable = credential.extensions.cred_props.as_ref().and_then(|p| p.rk);
        let core = self.clone();
        let client = client.clone();
        let caller = *caller;
        let outcome: Result<PasskeyRow, ApiError> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    if core.user_by_id(tx, caller.user, true).await?.is_none() {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    }
                    let count: i64 = core
                        .db()
                        .query_one(&*tx, &crate::sql::PASSKEYS_COUNT, &[&caller.user])
                        .await
                        .map_err(sql)?
                        .get(0);
                    if count >= max {
                        return Ok(Err(ApiError::code("passkey_limit")));
                    }
                    let id = core.new_id()?;
                    let now = core.now();
                    let (eligible, state) = backup_flags(&value);
                    let row = PasskeyRow {
                        id,
                        credential_id,
                        passkey,
                        label,
                        aaguid: aaguid(&value),
                        backup_eligible: eligible,
                        backup_state: state,
                        discoverable,
                        transports: transports(&value),
                        revision: 0,
                        created_at: now,
                        last_used_at: None,
                        user_id: caller.user,
                    };
                    let inserted = core
                        .db()
                        .query_opt(
                            &*tx,
                            &crate::sql::PASSKEY_INSERT,
                            &[
                                &row.id,
                                &row.user_id,
                                &row.credential_id,
                                &value,
                                &row.label,
                                &row.aaguid,
                                &row.backup_eligible,
                                &row.backup_state,
                                &row.discoverable,
                                &row.transports,
                                &now,
                            ],
                        )
                        .await
                        .map_err(sql)?;
                    if inserted.is_none() {
                        return Ok(Err(ApiError::conflict("passkey_exists")));
                    }
                    core.audit(
                        tx,
                        Audit::new("passkey.registered", Actor::User(caller.user))
                            .target_of("passkey", id),
                        &client,
                    )
                    .await?;
                    Ok(Ok(row))
                })
            })
            .await?;
        Ok(Reply::json(201, json!({ "passkey": outcome?.json() })))
    }

    // ----- authentication -----

    /// `POST /passkeys/authentications/options`: with `email`, the user's
    /// credential ids (decoys for an unknown address or a user without
    /// passkeys, the same shape either way); without, a discoverable
    /// (conditional UI) ceremony.
    pub async fn passkey_authentication_options(
        self: &Arc<Self>,
        client: &Client,
        email: Option<Email>,
    ) -> Result<Reply, ApiError> {
        let webauthn = self.relying()?;
        let discoverable = self.passkey_settings()?.discoverable;
        self.charge(gcra::PASSKEY_IP, Subject::Ip(client.ip), client)
            .await?;
        let (purpose, user, mut challenge, state) = match email {
            None if !discoverable => return Err(ApiError::invalid(vec![("/email", "required")])),
            None => {
                let (challenge, state) = webauthn
                    .start_discoverable_authentication()
                    .map_err(internal)?;
                (
                    "discoverable",
                    None,
                    serde_json::to_value(&challenge).map_err(internal)?,
                    serde_json::to_value(&state).map_err(internal)?,
                )
            }
            Some(email) => {
                let user = self.read_user_by_email(email.normalized()).await?;
                let passkeys = match &user {
                    Some(user) => self.read_passkeys(user.id).await?,
                    None => Vec::new(),
                };
                if passkeys.is_empty() {
                    // Decoys: stable per address, so a repeat looks the same.
                    let (challenge, state) = webauthn
                        .start_discoverable_authentication()
                        .map_err(internal)?;
                    let mut challenge = serde_json::to_value(&challenge).map_err(internal)?;
                    let decoys: Vec<Value> = self
                        .keys
                        .passkey_decoys(email.normalized())
                        .iter()
                        .map(|id| {
                            json!({"type": "public-key",
                                   "id": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(id)})
                        })
                        .collect();
                    challenge["publicKey"]["allowCredentials"] = Value::Array(decoys);
                    if let Some(map) = challenge.as_object_mut() {
                        map.remove("mediation");
                    }
                    (
                        "authentication",
                        None,
                        challenge,
                        serde_json::to_value(&state).map_err(internal)?,
                    )
                } else {
                    let keys: Vec<Passkey> = passkeys.into_iter().map(|p| p.passkey).collect();
                    let (challenge, state) = webauthn
                        .start_passkey_authentication(&keys)
                        .map_err(internal)?;
                    (
                        "authentication",
                        user.map(|u| u.id),
                        serde_json::to_value(&challenge).map_err(internal)?,
                        serde_json::to_value(&state).map_err(internal)?,
                    )
                }
            }
        };
        if purpose == "authentication" {
            uniform_request(&mut challenge["publicKey"]);
        }
        let core = self.clone();
        let id = self
            .in_tx(move |tx| {
                Box::pin(
                    async move { Ok(core.store_ceremony(tx, purpose, user, None, state).await?) },
                )
            })
            .await?;
        Ok(Reply::json(
            200,
            json!({
                "ceremony_id": ids::render(ids::CEREMONY, id),
                "public_key": challenge["publicKey"],
                "mediation": challenge.get("mediation").cloned().unwrap_or(Value::Null),
            }),
        ))
    }

    /// Verify an assertion against ceremony `ceremony`: the passkey it
    /// used, and the result to record. A counter regression on a
    /// credential with a nonzero counter is audited and refused.
    async fn verify_assertion(
        self: &Arc<Self>,
        client: &Client,
        ceremony: Ceremony,
        credential: &PublicKeyCredential,
    ) -> Result<(PasskeyRow, AuthenticationResult), ApiError> {
        let webauthn = self.relying()?;
        let (rows, result) = match ceremony.purpose.as_str() {
            "discoverable" => {
                let state: DiscoverableAuthentication =
                    serde_json::from_value(ceremony.state).map_err(|_| invalid_ceremony())?;
                let (handle, credential_id) = webauthn
                    .identify_discoverable_authentication(credential)
                    .map_err(|_| invalid_credentials())?;
                let client_db = self.db().acquire(self.deadline()).await?;
                let row = self
                    .db()
                    .query_opt(
                        &client_db,
                        &crate::sql::PASSKEY_BY_CREDENTIAL,
                        &[&credential_id],
                    )
                    .await
                    .map_err(sql)?;
                drop(client_db);
                let Some(row) = row else {
                    return Err(invalid_credentials());
                };
                // The user handle the authenticator returned must be the
                // credential's own user's.
                if row.get::<_, Option<Uuid>>(13) != Some(handle) {
                    return Err(invalid_credentials());
                }
                let row = passkey_row(&row)?;
                let key = DiscoverableKey::from(&row.passkey);
                let result = webauthn.finish_discoverable_authentication(credential, state, &[key]);
                (vec![row], result)
            }
            _ => {
                let state: PasskeyAuthentication =
                    serde_json::from_value(ceremony.state).map_err(|_| invalid_ceremony())?;
                let Some(user) = ceremony.user_id else {
                    return Err(invalid_credentials());
                };
                let rows = self.read_passkeys(user).await?;
                let result = webauthn.finish_passkey_authentication(credential, &state);
                (rows, result)
            }
        };
        let used = credential.get_credential_id();
        let row = rows.iter().find(|r| r.credential_id == used).cloned();
        match result {
            Ok(result) => match rows
                .into_iter()
                .find(|r| r.passkey.cred_id() == result.cred_id())
            {
                Some(row) => Ok((row, result)),
                None => Err(invalid_credentials()),
            },
            Err(WebauthnError::CredentialPossibleCompromise) => {
                if let Some(row) = row {
                    self.audit_alone(
                        Audit::new("passkey.counter_regression", Actor::User(row.user_id))
                            .denied()
                            .target_of("passkey", row.id),
                        client,
                    )
                    .await;
                }
                Err(invalid_credentials())
            }
            Err(_) => Err(invalid_credentials()),
        }
    }

    /// `POST /passkeys/authentications/verify`: sign in with session
    /// method `passkey`.
    pub async fn passkey_authentication_verify(
        self: &Arc<Self>,
        client: &Client,
        ceremony: &str,
        credential: PublicKeyCredential,
        transport: Option<Transport>,
    ) -> Result<Reply, ApiError> {
        self.relying()?;
        let transport = self.transport(transport)?;
        let result = async {
            let ceremony = self
                .take_ceremony(ceremony, &["authentication", "discoverable"])
                .await?
                .ok_or_else(invalid_ceremony)?;
            self.verify_assertion(client, ceremony, &credential).await
        }
        .await;
        let (row, result) = match result {
            Ok(found) => found,
            Err(error) => {
                if error.failure.code == "invalid_credentials" {
                    self.audit_alone(
                        Audit::new("session.sign_in_failed", Actor::Anonymous)
                            .failed()
                            .detail("method", "passkey")
                            .detail("reason", "invalid_credentials"),
                        client,
                    )
                    .await;
                    merezha_telemetry::event!(
                        DEBUG,
                        "identity.login_failed",
                        method = "passkey",
                        outcome = "invalid_credentials"
                    );
                    self.metrics.login("passkey", "invalid_credentials");
                }
                return Err(error);
            }
        };
        let core = self.clone();
        let client_owned = client.clone();
        let outcome: Result<(NewSession, UserRow), ApiError> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let client = client_owned;
                    let Some(user) = core.user_by_id(tx, row.user_id, true).await? else {
                        return Ok(Err(invalid_credentials()));
                    };
                    if !user.active() {
                        return Ok(Err(ApiError::code("account_disabled")));
                    }
                    if core.settings.require_verified_email && !user.verified() {
                        return Ok(Err(ApiError::code("email_unverified")));
                    }
                    core.record_use(tx, &row, &result).await?;
                    let session = core
                        .create_session(
                            tx,
                            user.id,
                            Method::Passkey,
                            transport,
                            &client,
                            None,
                            true,
                        )
                        .await?;
                    if core.settings.notify_new_sign_in {
                        core.notice(tx, topic::NEW_SIGN_IN, &user, &client).await?;
                    }
                    Ok(Ok((session, user)))
                })
            })
            .await?;
        let (session, user) = outcome?;
        self.metrics.login("passkey", "success");
        Ok(self.session_reply(201, session, Some(&user)))
    }

    // ----- reauthentication -----

    /// `POST /session/reauthenticate/passkey/options`: a ceremony over the
    /// caller's passkeys, bound to the caller's session.
    pub async fn passkey_reauth_options(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
    ) -> Result<Reply, ApiError> {
        let webauthn = self.relying()?;
        self.charge(gcra::PASSKEY_IP, Subject::Ip(client.ip), client)
            .await?;
        let keys: Vec<Passkey> = self
            .read_passkeys(caller.user)
            .await?
            .into_iter()
            .map(|p| p.passkey)
            .collect();
        if keys.is_empty() {
            return Err(ApiError::code("not_found"));
        }
        let (challenge, state) = webauthn
            .start_passkey_authentication(&keys)
            .map_err(internal)?;
        let state = serde_json::to_value(&state).map_err(internal)?;
        let core = self.clone();
        let caller = *caller;
        let id = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    Ok(core
                        .store_ceremony(
                            tx,
                            "reauthentication",
                            Some(caller.user),
                            Some(caller.session),
                            state,
                        )
                        .await?)
                })
            })
            .await?;
        let challenge = serde_json::to_value(&challenge).map_err(internal)?;
        Ok(Reply::json(
            200,
            json!({"ceremony_id": ids::render(ids::CEREMONY, id), "public_key": challenge["publicKey"]}),
        ))
    }

    /// `POST /session/reauthenticate/passkey/verify`: prove a passkey again;
    /// the session is rotated with method `passkey`. Failures charge
    /// `reauth.session` like a wrong password.
    pub async fn passkey_reauth_verify(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
        ceremony: &str,
        credential: PublicKeyCredential,
    ) -> Result<Reply, ApiError> {
        self.relying()?;
        let subject = Subject::Session(caller.session);
        self.peek_many(&[(gcra::REAUTH_SESSION, subject)], client)
            .await?;
        let found = async {
            let ceremony = self
                .take_ceremony(ceremony, &["reauthentication"])
                .await?
                .filter(|c| c.user_id == Some(caller.user) && c.session_id == Some(caller.session))
                .ok_or_else(invalid_ceremony)?;
            self.verify_assertion(client, ceremony, &credential).await
        }
        .await;
        let (row, result) = match found {
            Ok(found) => found,
            Err(error) => {
                if error.failure.code == "invalid_credentials" {
                    self.charge_failure(gcra::REAUTH_SESSION, subject).await;
                    self.audit_alone(
                        Audit::new("session.reauthenticated", Actor::User(caller.user))
                            .failed()
                            .detail("method", "passkey")
                            .detail("reason", "invalid_credentials"),
                        client,
                    )
                    .await;
                }
                return Err(error);
            }
        };
        let core = self.clone();
        let client = client.clone();
        let caller = *caller;
        let outcome: Result<NewSession, ApiError> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    if core.user_by_id(tx, caller.user, true).await?.is_none()
                        || row.user_id != caller.user
                    {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    }
                    core.record_use(tx, &row, &result).await?;
                    let Some(new) = core
                        .rotate_session(tx, caller.session, Method::Passkey, &client)
                        .await?
                    else {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    };
                    core.audit(
                        tx,
                        Audit::new("session.reauthenticated", Actor::User(caller.user))
                            .detail("method", "passkey"),
                        &client,
                    )
                    .await?;
                    Ok(Ok(new))
                })
            })
            .await?;
        Ok(self.session_reply(200, outcome?, None))
    }

    // ----- management -----

    /// `GET /me/passkeys`.
    pub async fn list_passkeys(self: &Arc<Self>, caller: &Caller) -> Result<Reply, ApiError> {
        self.relying()?;
        let items: Vec<Value> = self
            .read_passkeys(caller.user)
            .await?
            .iter()
            .map(PasskeyRow::json)
            .collect();
        Ok(Reply::json(200, json!({ "items": items })))
    }

    /// `PATCH /me/passkeys/{id}`: the label.
    pub async fn rename_passkey(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
        id: &str,
        label: Option<String>,
    ) -> Result<Reply, ApiError> {
        self.relying()?;
        let target = ids::parse(ids::PASSKEY, id)?;
        let core = self.clone();
        let client = client.clone();
        let caller = *caller;
        let outcome: Result<PasskeyRow, ApiError> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    if core.user_by_id(tx, caller.user, true).await?.is_none() {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    }
                    let renamed = core
                        .db()
                        .execute(
                            &*tx,
                            &crate::sql::PASSKEY_RENAME,
                            &[&target, &caller.user, &label],
                        )
                        .await
                        .map_err(sql)?;
                    if renamed == 0 {
                        return Ok(Err(ApiError::code("not_found")));
                    }
                    let row = core
                        .db()
                        .query_one(&*tx, &crate::sql::PASSKEY_GET, &[&target, &caller.user])
                        .await
                        .map_err(sql)?;
                    core.audit(
                        tx,
                        Audit::new("passkey.renamed", Actor::User(caller.user))
                            .target_of("passkey", target),
                        &client,
                    )
                    .await?;
                    Ok(Ok(passkey_row(&row)?))
                })
            })
            .await?;
        Ok(Reply::json(200, json!({ "passkey": outcome?.json() })))
    }

    /// `DELETE /me/passkeys/{id}` (recent session): refused
    /// `409 last_credential` when nothing else would sign the user in.
    pub async fn remove_passkey(
        self: &Arc<Self>,
        client: &Client,
        caller: &Caller,
        id: &str,
    ) -> Result<Reply, ApiError> {
        self.relying()?;
        self.require_recent(caller)?;
        let target = ids::parse(ids::PASSKEY, id)?;
        let core = self.clone();
        let client = client.clone();
        let caller = *caller;
        let outcome: Result<(), ApiError> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    if core.user_by_id(tx, caller.user, true).await?.is_none() {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    }
                    let exists = core
                        .db()
                        .query_opt(&*tx, &crate::sql::PASSKEY_GET, &[&target, &caller.user])
                        .await
                        .map_err(sql)?
                        .is_some();
                    if !exists {
                        return Ok(Err(ApiError::code("not_found")));
                    }
                    if !core
                        .keeps_a_credential(tx, caller.user, Some(target), None)
                        .await?
                    {
                        return Ok(Err(ApiError::conflict("last_credential")));
                    }
                    core.db()
                        .execute(&*tx, &crate::sql::PASSKEY_DELETE, &[&target, &caller.user])
                        .await
                        .map_err(sql)?;
                    core.audit(
                        tx,
                        Audit::new("passkey.removed", Actor::User(caller.user))
                            .target_of("passkey", target),
                        &client,
                    )
                    .await?;
                    Ok(Ok(()))
                })
            })
            .await?;
        outcome?;
        Ok(Reply::empty(CookieAction::Keep))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_first_answers_have_one_shape() {
        let mut real = json!({"challenge": "c", "allowCredentials": [
            {"type": "public-key", "id": "abc", "transports": ["internal"]}],
            "extensions": {"uvm": true}, "userVerification": "required"});
        uniform_request(&mut real);
        assert_eq!(
            real,
            json!({"challenge": "c", "allowCredentials": [{"type": "public-key", "id": "abc"}],
                   "userVerification": "required"})
        );
    }

    #[test]
    fn stored_passkey_fields_are_read_defensively() {
        let value = json!({"cred": {"backup_eligible": true, "backup_state": false,
            "transports": ["internal", "hybrid"],
            "attestation": {"metadata": {"Packed": {"aaguid": "adce0002-35bc-c60a-648b-0b25f1f05503"}}}}});
        assert_eq!(backup_flags(&value), (true, false));
        assert_eq!(transports(&value), vec!["internal", "hybrid"]);
        assert!(aaguid(&value).is_some());
        let none = json!({"cred": {"attestation": {"metadata": "None"},
            "transports": null}});
        assert_eq!(backup_flags(&none), (false, false));
        assert!(transports(&none).is_empty());
        assert_eq!(aaguid(&none), None);
        let zero = json!({"cred": {"attestation": {"metadata": {"Packed": {"aaguid": "00000000-0000-0000-0000-000000000000"}}}}});
        assert_eq!(aaguid(&zero), None);
    }
}
