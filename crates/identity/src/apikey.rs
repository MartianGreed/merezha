//! API keys (sections 3 and 4): the `mzk_` format, and their resolution
//! into principals. A key's power is its list intersected with its owner's
//! live grants at the key's scope, checked on every request, cache hit or
//! not: a revoked or expired key, an inactive owner, and a user's tenant key
//! without its owner's membership never become a principal.
use crate::{
    core::{Actor, Audit, Client, Core, Subject, sql},
    gcra,
    grants::{KeyFacts, OrgFacts, Owner},
    ids,
    secrets::{API_KEY_PREFIX, RawToken},
};
use axum::http::header;
use jiff::Timestamp;
use merezha_domain::Failure;
use merezha_platform::{
    Random,
    edge::ClientInfo,
    ports::{Principal, RequestHead},
};
use merezha_postgres::Session;
use secrecy::{ExposeSecret, SecretString};
use std::{sync::Arc, time::Instant};
use subtle::ConstantTimeEq;
use uuid::Uuid;

/// Length of the public id, base32 of 10 random bytes.
pub const PUBLIC_ID_LEN: usize = 16;
const BASE32: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// A freshly minted key: its public id, the secret's digest and the full
/// `mzk_<public id>_<secret>` value, shown once.
pub struct NewKey {
    pub public_id: String,
    pub digest: [u8; 32],
    pub value: SecretString,
}
impl std::fmt::Debug for NewKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NewKey")
            .field("public_id", &self.public_id)
            .finish_non_exhaustive()
    }
}
/// Mint a key from the platform CSPRNG.
pub fn mint(random: &dyn Random) -> Result<NewKey, Failure> {
    let mut id = [0u8; 10];
    random.fill(&mut id)?;
    let public_id = base32(&id);
    let secret = RawToken::generate(random)?;
    Ok(NewKey {
        value: SecretString::from(format!(
            "{API_KEY_PREFIX}{public_id}_{}",
            secret.encode().expose_secret()
        )),
        digest: secret.digest(),
        public_id,
    })
}
/// Lowercase RFC 4648 base32 without padding of exactly 10 bytes.
fn base32(bytes: &[u8; 10]) -> String {
    let mut out = String::with_capacity(PUBLIC_ID_LEN);
    let mut buffer: u64 = 0;
    let mut bits = 0;
    for byte in bytes {
        buffer = (buffer << 8) | u64::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(BASE32[((buffer >> bits) & 31) as usize] as char);
        }
    }
    out
}
/// Split a presented `mzk_<16 base32>_<43 base64url>` into the public id
/// and the secret; `None` for anything else.
pub fn parse(value: &str) -> Option<(&str, RawToken)> {
    let rest = value.strip_prefix(API_KEY_PREFIX)?;
    let (public_id, secret) = rest.split_once('_')?;
    if public_id.len() != PUBLIC_ID_LEN || !public_id.bytes().all(|b| BASE32.contains(&b)) {
        return None;
    }
    Some((public_id, RawToken::decode(secret)?))
}

/// One `KEY_AUTHENTICATE` / `KEY_RESOLVE` row.
struct KeyRow {
    id: Uuid,
    secret_hash: Vec<u8>,
    organization: Option<Uuid>,
    user: Option<Uuid>,
    service_account: Option<Uuid>,
    permissions: Vec<String>,
    expires_at: Timestamp,
    revoked_at: Option<Timestamp>,
    last_used_at: Option<Timestamp>,
    user_status: Option<String>,
    user_verified: Option<Timestamp>,
    user_version: Option<i64>,
    service_status: Option<String>,
    service_version: Option<i64>,
    service_organization: Option<Uuid>,
    org: Option<Uuid>,
    org_version: Option<i64>,
    membership: Option<Uuid>,
}
fn key_row(row: &tokio_postgres::Row) -> KeyRow {
    KeyRow {
        id: row.get(0),
        secret_hash: row.get(1),
        organization: row.get(2),
        user: row.get(3),
        service_account: row.get(4),
        permissions: row.get(5),
        expires_at: row.get(6),
        revoked_at: row.get(7),
        last_used_at: row.get(8),
        user_status: row.get(9),
        user_verified: row.get(10),
        user_version: row.get(11),
        service_status: row.get(12),
        service_version: row.get(13),
        service_organization: row.get(14),
        org: row.get(15),
        org_version: row.get(16),
        membership: row.get(17),
    }
}

/// Why a key was refused: every one is `401 unauthenticated` except a
/// request naming an organization the key cannot act in (`404`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refusal {
    Revoked,
    Expired,
    Owner,
    NotFound,
}

impl Core {
    /// Authenticate a presented `mzk_` key. Failed attempts charge
    /// `api_key.invalid.ip`, checked before the lookup; a use after
    /// revocation is audited `api_key.used_after_revocation`.
    pub(crate) async fn authenticate_key(
        self: &Arc<Self>,
        head: &RequestHead,
        value: &str,
        organization: Option<Uuid>,
    ) -> Result<Principal, Failure> {
        let client = Client {
            ip: head.extensions.get::<ClientInfo>().and_then(|c| c.address),
            user_agent: Client::user_agent(
                head.headers
                    .get(header::USER_AGENT)
                    .and_then(|v| v.to_str().ok()),
            ),
            request_id: None,
        };
        let subject = Subject::Ip(client.ip);
        if let Err(error) = self.peek(gcra::API_KEY_INVALID_IP, subject, &client).await {
            return Err(error.failure);
        }
        let failed = |core: &Arc<Self>| {
            let core = core.clone();
            async move {
                core.charge_failure(gcra::API_KEY_INVALID_IP, subject).await;
            }
        };
        let Some((public_id, secret)) = parse(value) else {
            failed(self).await;
            return Err(self.invalid("api_key"));
        };
        let deadline = Instant::now() + std::time::Duration::from_secs(2);
        let unavailable = |_: Failure| Failure::transient("unavailable");
        let session = self.db().acquire(deadline).await.map_err(unavailable)?;
        let row = tokio::time::timeout_at(
            deadline.into(),
            self.db().query_opt(
                &session,
                &crate::sql::KEY_AUTHENTICATE,
                &[&public_id, &organization],
            ),
        )
        .await
        .map_err(|_| Failure::transient("unavailable"))?
        .map_err(|e| unavailable(sql(e)))?;
        let Some(row) = row else {
            failed(self).await;
            return Err(self.invalid("api_key"));
        };
        let row = key_row(&row);
        let digest = secret.digest();
        if !bool::from(row.secret_hash.as_slice().ct_eq(&digest[..])) {
            failed(self).await;
            return Err(self.invalid("api_key"));
        }
        let resolved = tokio::time::timeout_at(
            deadline.into(),
            self.key_principal_from(&session, &row, organization),
        )
        .await
        .map_err(|_| Failure::transient("unavailable"))?;
        match resolved {
            Ok(principal) => {
                let now = self.now();
                let touch = self.settings.api_keys.touch_interval;
                if row
                    .last_used_at
                    .is_none_or(|at| crate::core::plus(at, touch) <= now)
                {
                    let cutoff = crate::core::minus(now, touch);
                    let _ = self
                        .db()
                        .execute(&session, &crate::sql::KEY_TOUCH, &[&row.id, &now, &cutoff])
                        .await;
                }
                self.metrics.authentication("api_key", "ok");
                Ok(principal)
            }
            Err(Ok(refusal)) => {
                drop(session);
                match refusal {
                    Refusal::Revoked => {
                        self.metrics.authentication("api_key", "revoked");
                        failed(self).await;
                        self.audit_alone(
                            Audit::new("api_key.used_after_revocation", Actor::ApiKey(row.id))
                                .denied()
                                .target_of("api_key", row.id),
                            &client,
                        )
                        .await;
                        Err(Failure::permanent("unauthenticated"))
                    }
                    Refusal::Expired => {
                        self.metrics.authentication("api_key", "expired");
                        Err(Failure::permanent("unauthenticated"))
                    }
                    Refusal::Owner => {
                        self.metrics.authentication("api_key", "revoked");
                        Err(Failure::permanent("unauthenticated"))
                    }
                    Refusal::NotFound => Err(Failure::permanent("not_found")),
                }
            }
            Err(Err(failure)) => Err(match failure.code.as_str() {
                "not_found" | "unauthenticated" => failure,
                _ => Failure::transient("unavailable"),
            }),
        }
    }

    /// The live principal of key `key` acting in `organization`, read in
    /// the caller's transaction; `None` when the key no longer
    /// authenticates there (revoked, expired, owner inactive, membership
    /// gone, another organization).
    pub(crate) async fn live_key(
        self: &Arc<Self>,
        session: &impl Session,
        key: Uuid,
        organization: Uuid,
    ) -> Result<Option<Principal>, Failure> {
        let row = self
            .db()
            .query_opt(
                session,
                &crate::sql::KEY_RESOLVE,
                &[&key, &Some(organization)],
            )
            .await
            .map_err(sql)?;
        let Some(row) = row else {
            return Ok(None);
        };
        match self
            .key_principal_from(session, &key_row(&row), Some(organization))
            .await
        {
            Ok(principal) => Ok(Some(principal)),
            Err(Ok(_)) => Ok(None),
            Err(Err(failure)) => Err(failure),
        }
    }

    /// Every rule of section 3 on a key row, then its principal.
    async fn key_principal_from(
        self: &Arc<Self>,
        session: &impl Session,
        row: &KeyRow,
        requested: Option<Uuid>,
    ) -> Result<Principal, Result<Refusal, Failure>> {
        let now = self.now();
        if row.revoked_at.is_some() {
            return Err(Ok(Refusal::Revoked));
        }
        if row.expires_at <= now {
            return Err(Ok(Refusal::Expired));
        }
        let (owner, version) = match (row.user, row.service_account) {
            (Some(user), None) => {
                let active = row.user_status.as_deref() == Some("active")
                    && (!self.settings.require_verified_email || row.user_verified.is_some());
                if !active {
                    return Err(Ok(Refusal::Owner));
                }
                (Owner::User(user), row.user_version.unwrap_or(0))
            }
            (None, Some(service)) => {
                if row.service_status.as_deref() != Some("active")
                    // A service account's keys share its scope.
                    || row.service_organization != row.organization
                {
                    return Err(Ok(Refusal::Owner));
                }
                (
                    Owner::ServiceAccount {
                        id: service,
                        organization: row.service_organization,
                    },
                    row.service_version.unwrap_or(0),
                )
            }
            _ => return Err(Ok(Refusal::Owner)),
        };
        let key = KeyFacts {
            grants: row.permissions.clone(),
            organization: row.organization,
        };
        let org_facts = |holder| -> Option<OrgFacts> {
            Some(OrgFacts {
                id: row.org?,
                version: row.org_version?,
                holder,
            })
        };
        let (tenant, org) = match row.organization {
            // A tenant key acts in its own organization only.
            Some(pinned) => {
                if requested.is_some_and(|r| r != pinned) {
                    return Err(Ok(Refusal::NotFound));
                }
                let holder = match owner {
                    Owner::User(_) => match row.membership {
                        Some(membership) => membership,
                        // Without its owner's live membership a user's
                        // tenant key is no credential at all.
                        None => return Err(Ok(Refusal::Owner)),
                    },
                    Owner::ServiceAccount { id, .. } => id,
                };
                let Some(facts) = org_facts(Some(holder)) else {
                    return Err(Ok(Refusal::Owner));
                };
                (Some(pinned), Some(facts))
            }
            // An application key holds its owner's application grants
            // only: it never takes tenant grants, and a header naming an
            // organization is refused unless the key may enter any.
            None => match requested {
                None => (None, None),
                Some(requested) => {
                    if row.org.is_none() {
                        return Err(Ok(Refusal::NotFound));
                    }
                    let base = self
                        .snapshot(session, row.id, owner, version, None, Some(&key))
                        .await
                        .map_err(Err)?;
                    let outside = self.key_principal(row.id, owner, None, base);
                    if !self.may_enter(&outside) {
                        return Err(Ok(Refusal::NotFound));
                    }
                    let user = match owner {
                        Owner::User(user) => Some(user),
                        Owner::ServiceAccount { .. } => None,
                    };
                    self.entered_by_key(user, row.id, requested);
                    (Some(requested), org_facts(None))
                }
            },
        };
        let grants = self
            .snapshot(session, row.id, owner, version, org, Some(&key))
            .await
            .map_err(Err)?;
        Ok(self.key_principal(row.id, owner, tenant, grants))
    }
}

/// The rendered key id.
pub fn render(id: Uuid) -> String {
    ids::render(ids::API_KEY, id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_have_the_scannable_format_and_parse_strictly() {
        let key = mint(&merezha_platform::OsRandom).unwrap();
        let value = key.value.expose_secret();
        assert!(value.starts_with("mzk_"));
        assert_eq!(value.len(), 4 + 16 + 1 + 43);
        assert!(key.public_id.bytes().all(|b| BASE32.contains(&b)));
        let (public_id, secret) = parse(value).unwrap();
        assert_eq!(public_id, key.public_id);
        assert_eq!(secret.digest(), key.digest);
        assert!(!format!("{key:?}").contains(&value[21..]));
        for bad in [
            "",
            "mzk_",
            &value.replace("mzk_", "mzs_"),
            &value[..value.len() - 1],
            &value.replacen('_', "-", 2),
            &format!("mzk_{}_{}", "A".repeat(16), &value[21..]),
            &format!("mzk_{}1_{}", &key.public_id[..15], &value[21..]),
        ] {
            assert!(parse(bad).is_none(), "{bad}");
        }
        assert_eq!(base32(&[0; 10]), "a".repeat(16));
        assert_eq!(base32(&[0xff; 10]), "7".repeat(16));
    }
}
