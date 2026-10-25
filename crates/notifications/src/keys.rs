//! Keys derived from `secret_key` (NT §7.2) and the signed unsubscribe
//! token (NT §6).
//!
//! `k_unsub = HKDF-SHA256(secret_key, info = "merezha-notify/unsubscribe/v1")`
//! (RFC 5869 with an all-zero salt, as identity derives its keys). Tokens
//! signed with `secret_key_previous` still verify. A token never expires:
//! RFC 8058 links must keep working.
use crate::config::KeyMaterial;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = hmac::Hmac<Sha256>;

/// HKDF info of the unsubscribe key.
pub const UNSUBSCRIBE_INFO: &[u8] = b"merezha-notify/unsubscribe/v1";
/// Longest token accepted.
pub const MAX_TOKEN_BYTES: usize = 2048;

/// A 32-byte derived key. `Debug` never shows it.
#[derive(Clone)]
pub struct Key([u8; 32]);
impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Key(<redacted>)")
    }
}
impl Key {
    fn mac(&self, message: &[u8]) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(&self.0)
            .unwrap_or_else(|_| unreachable!("HMAC takes keys of any length"));
        mac.update(message);
        mac.finalize().into_bytes().into()
    }
}

/// HKDF-SHA256 with an all-zero salt and one output block.
pub fn hkdf(ikm: &[u8], info: &[u8]) -> Key {
    let mut extract = HmacSha256::new_from_slice(&[0u8; 32])
        .unwrap_or_else(|_| unreachable!("HMAC takes keys of any length"));
    extract.update(ikm);
    let prk: [u8; 32] = extract.finalize().into_bytes().into();
    let mut expand = HmacSha256::new_from_slice(&prk)
        .unwrap_or_else(|_| unreachable!("HMAC takes keys of any length"));
    expand.update(info);
    expand.update(&[1]);
    Key(expand.finalize().into_bytes().into())
}

/// What an unsubscribe token names: a user or an address, a topic and the
/// channel (always `email`). `Debug` hides the address.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Unsubscribe {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub u: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub a: Option<String>,
    pub t: String,
    pub c: String,
    pub iat: i64,
}
impl std::fmt::Debug for Unsubscribe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Unsubscribe")
            .field("u", &self.u)
            .field("t", &self.t)
            .finish_non_exhaustive()
    }
}

/// Signs and verifies unsubscribe tokens with the current key, and verifies
/// with the previous one too.
#[derive(Clone, Debug)]
pub struct Keys {
    unsubscribe: Key,
    previous_unsubscribe: Option<Key>,
}
impl Keys {
    pub fn new(secret_key: &KeyMaterial, previous: Option<&KeyMaterial>) -> Self {
        Self {
            unsubscribe: hkdf(secret_key.expose(), UNSUBSCRIBE_INFO),
            previous_unsubscribe: previous.map(|k| hkdf(k.expose(), UNSUBSCRIBE_INFO)),
        }
    }
    /// `v1.<b64url(payload)>.<b64url(HMAC-SHA256(k_unsub, "v1." + b64url(payload)))>`.
    pub fn unsubscribe_token(&self, claims: &Unsubscribe) -> String {
        let payload = serde_json::to_vec(claims).unwrap_or_default();
        let signed = format!("v1.{}", URL_SAFE_NO_PAD.encode(payload));
        let mac = self.unsubscribe.mac(signed.as_bytes());
        format!("{signed}.{}", URL_SAFE_NO_PAD.encode(mac))
    }
    /// The claims of a well-formed token whose MAC verifies under either
    /// key; `None` otherwise (forged, truncated, another version).
    pub fn verify_unsubscribe(&self, token: &str) -> Option<Unsubscribe> {
        if token.len() > MAX_TOKEN_BYTES {
            return None;
        }
        let (signed, mac) = token.rsplit_once('.')?;
        let payload = signed.strip_prefix("v1.")?;
        let mac = URL_SAFE_NO_PAD.decode(mac).ok()?;
        let verified = std::iter::once(&self.unsubscribe)
            .chain(self.previous_unsubscribe.as_ref())
            .any(|key| bool::from(key.mac(signed.as_bytes()).ct_eq(mac.as_slice())));
        if !verified {
            return None;
        }
        let claims: Unsubscribe =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()?;
        let one_target = claims.u.is_some() != claims.a.is_some();
        (one_target && claims.c == "email" && crate::topics::valid_dotted_name(&claims.t))
            .then_some(claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn material(byte: u8) -> KeyMaterial {
        crate::config::KeyMaterial::for_tests(vec![byte; 32])
    }

    #[test]
    fn tokens_verify_with_either_key_and_forgeries_fail() {
        let claims = Unsubscribe {
            u: Some("usr_1".into()),
            a: None,
            t: "tasks.digest".into(),
            c: "email".into(),
            iat: 1,
        };
        let old = Keys::new(&material(1), None);
        let token = old.unsubscribe_token(&claims);
        assert_eq!(old.verify_unsubscribe(&token), Some(claims.clone()));
        let rotated = Keys::new(&material(2), Some(&material(1)));
        assert_eq!(rotated.verify_unsubscribe(&token), Some(claims.clone()));
        assert_eq!(
            Keys::new(&material(3), None).verify_unsubscribe(&token),
            None
        );
        let (signed, mac) = token.rsplit_once('.').unwrap();
        let mut forged = mac.as_bytes().to_vec();
        forged[0] = if forged[0] == b'A' { b'B' } else { b'A' };
        let forged = format!("{signed}.{}", String::from_utf8(forged).unwrap());
        assert_eq!(old.verify_unsubscribe(&forged), None);
        assert_eq!(old.verify_unsubscribe("v2.e30.AAAA"), None);
        assert_eq!(old.verify_unsubscribe(""), None);
    }
}
