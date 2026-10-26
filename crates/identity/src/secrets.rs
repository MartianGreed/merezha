//! Secret values and the keys derived from `secret_key` (section 7).
//!
//! Session tokens and one-time tokens are 32 bytes from the platform
//! CSPRNG, stored only as SHA-256 digests. `secret_key` derives four
//! purpose keys with HKDF-SHA256: `csrf`, `ratelimit` (rate-limit bucket
//! keys), `audit-subject` (HMAC of emails for unknown accounts) and
//! `passkey-decoy` (the decoy credential ids of unknown addresses). Secret
//! types print `[redacted]`, have no `Serialize` and zeroize on drop.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{KeyInit, Mac};
use merezha_domain::Failure;
use merezha_platform::Random;
use secrecy::{ExposeSecret, SecretBox, SecretString};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;
use zeroize::Zeroize;

type HmacSha256 = hmac::Hmac<Sha256>;

/// Prefix of session tokens: `mzs_` + 43 base64url characters.
pub const SESSION_PREFIX: &str = "mzs_";
/// Prefix of API keys (ID PR 2).
pub const API_KEY_PREFIX: &str = "mzk_";
/// Length of 32 bytes in unpadded base64url.
const ENCODED_LEN: usize = 43;
/// The CSRF token's domain separation tag.
const CSRF_TAG: &[u8] = b"merezha.csrf.v1";

/// 32 random bytes. `Debug` prints `[redacted]`.
pub struct RawToken(SecretBox<[u8; 32]>);
impl RawToken {
    /// Fresh bytes from `random`; fails transient `random_unavailable`.
    pub fn generate(random: &dyn Random) -> Result<Self, Failure> {
        let mut bytes = Box::new([0u8; 32]);
        random.fill(bytes.as_mut())?;
        Ok(Self(SecretBox::new(bytes)))
    }
    /// The SHA-256 digest stored in the database.
    pub fn digest(&self) -> [u8; 32] {
        Sha256::digest(self.0.expose_secret()).into()
    }
    /// Unpadded base64url, the form in links and cookies.
    pub fn encode(&self) -> SecretString {
        SecretString::from(URL_SAFE_NO_PAD.encode(self.0.expose_secret()))
    }
    /// `prefix` followed by [`RawToken::encode`].
    pub fn encode_with(&self, prefix: &str) -> SecretString {
        SecretString::from(format!("{prefix}{}", self.encode().expose_secret()))
    }
    /// Decode the 43-character form; `None` for anything else.
    pub fn decode(text: &str) -> Option<Self> {
        if text.len() != ENCODED_LEN {
            return None;
        }
        let mut decoded = URL_SAFE_NO_PAD.decode(text).ok()?;
        let result = <[u8; 32]>::try_from(decoded.as_slice())
            .ok()
            .map(|bytes| Self(SecretBox::new(Box::new(bytes))));
        decoded.zeroize();
        result
    }
    /// Decode `prefix` + the 43-character form.
    pub fn decode_with(prefix: &str, text: &str) -> Option<Self> {
        Self::decode(text.strip_prefix(prefix)?)
    }
}
impl std::fmt::Debug for RawToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[redacted]")
    }
}

/// A 32-byte key. `Debug` prints `[redacted]`; zeroized on drop.
#[derive(Clone)]
pub struct Key([u8; 32]);
impl Drop for Key {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}
impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[redacted]")
    }
}
impl Key {
    /// HMAC-SHA256 of `parts`, concatenated.
    pub fn mac(&self, parts: &[&[u8]]) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(&self.0)
            .unwrap_or_else(|_| unreachable!("HMAC takes keys of any length"));
        for part in parts {
            mac.update(part);
        }
        mac.finalize().into_bytes().into()
    }
}

/// HKDF-SHA256 (RFC 5869) with an all-zero salt and `info` as the label.
pub fn hkdf(ikm: &[u8], info: &[u8]) -> Key {
    let mut extract = HmacSha256::new_from_slice(&[0u8; 32])
        .unwrap_or_else(|_| unreachable!("HMAC takes keys of any length"));
    extract.update(ikm);
    let mut prk: [u8; 32] = extract.finalize().into_bytes().into();
    let mut expand = HmacSha256::new_from_slice(&prk)
        .unwrap_or_else(|_| unreachable!("HMAC takes keys of any length"));
    expand.update(info);
    expand.update(&[1]);
    let okm: [u8; 32] = expand.finalize().into_bytes().into();
    prk.zeroize();
    Key(okm)
}

/// The purpose keys of `secret_key` and, for CSRF only, of
/// `secret_key_previous` during a rotation.
#[derive(Clone, Debug)]
pub struct Keys {
    pub csrf: Key,
    pub csrf_previous: Option<Key>,
    pub ratelimit: Key,
    pub audit_subject: Key,
    /// HMAC key of the decoy credential ids passkey options return for
    /// unknown addresses (section 4).
    pub passkey_decoy: Key,
}
impl Keys {
    pub const CSRF_LABEL: &'static [u8] = b"merezha/identity/csrf";
    pub const RATELIMIT_LABEL: &'static [u8] = b"merezha/identity/ratelimit";
    pub const AUDIT_LABEL: &'static [u8] = b"merezha/identity/audit-subject";
    pub const PASSKEY_DECOY_LABEL: &'static [u8] = b"merezha/identity/passkey-decoy";
    pub fn derive(secret: &[u8], previous: Option<&[u8]>) -> Self {
        Self {
            csrf: hkdf(secret, Self::CSRF_LABEL),
            csrf_previous: previous.map(|p| hkdf(p, Self::CSRF_LABEL)),
            ratelimit: hkdf(secret, Self::RATELIMIT_LABEL),
            audit_subject: hkdf(secret, Self::AUDIT_LABEL),
            passkey_decoy: hkdf(secret, Self::PASSKEY_DECOY_LABEL),
        }
    }
    /// `base64url(HMAC-SHA256(csrf_key, "merezha.csrf.v1" || session_id))`.
    pub fn csrf_token(&self, session: Uuid) -> String {
        URL_SAFE_NO_PAD.encode(self.csrf.mac(&[CSRF_TAG, session.as_bytes()]))
    }
    /// Constant-time check of a presented CSRF token against the current
    /// key and, during a rotation, the previous one.
    pub fn csrf_matches(&self, session: Uuid, presented: &str) -> bool {
        let Ok(decoded) = URL_SAFE_NO_PAD.decode(presented) else {
            return false;
        };
        let current = self.csrf.mac(&[CSRF_TAG, session.as_bytes()]);
        let mut ok: bool = current.as_slice().ct_eq(decoded.as_slice()).into();
        if let Some(previous) = &self.csrf_previous {
            let old = previous.mac(&[CSRF_TAG, session.as_bytes()]);
            ok |= bool::from(old.as_slice().ct_eq(decoded.as_slice()));
        }
        ok
    }
    /// `HMAC-SHA256(ratelimit_key, policy || 0x00 || subject)`.
    pub fn bucket(&self, policy: &str, subject: &[u8]) -> [u8; 32] {
        self.ratelimit.mac(&[policy.as_bytes(), &[0], subject])
    }
    /// HMAC of a normalized email, for audit rows about unknown accounts.
    pub fn subject(&self, email_normalized: &str) -> [u8; 32] {
        self.audit_subject.mac(&[email_normalized.as_bytes()])
    }
    /// One or two decoy credential ids of `email_normalized`, stable
    /// across requests, shaped like real ones (section 4): 16, 32 or 64
    /// bytes, the lengths common authenticators use.
    pub fn passkey_decoys(&self, email_normalized: &str) -> Vec<Vec<u8>> {
        let seed = self.passkey_decoy.mac(&[b"0", email_normalized.as_bytes()]);
        let count = 1 + usize::from(seed[0] & 1);
        (0..count)
            .map(|i| {
                let len = [16, 32, 64][usize::from(seed[1 + i]) % 3];
                let mut id = self
                    .passkey_decoy
                    .mac(&[&[b'1' + i as u8], email_normalized.as_bytes()])
                    .to_vec();
                id.extend(
                    self.passkey_decoy
                        .mac(&[&[b'a' + i as u8], email_normalized.as_bytes()]),
                );
                id.truncate(len);
                id
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_encode_decode_and_hide() {
        let token = RawToken::generate(&merezha_platform::OsRandom).unwrap();
        let text = token.encode_with(SESSION_PREFIX);
        assert_eq!(text.expose_secret().len(), 4 + ENCODED_LEN);
        let back = RawToken::decode_with(SESSION_PREFIX, text.expose_secret()).unwrap();
        assert_eq!(back.digest(), token.digest());
        assert_eq!(format!("{token:?}"), "[redacted]");
        assert!(!format!("{text:?}").contains(&text.expose_secret()[4..]));
        for bad in [
            "",
            "mzs_",
            &text.expose_secret()[1..],
            &format!("{}=", text.expose_secret()),
            &text.expose_secret().replace("mzs_", "mzk_"),
        ] {
            assert!(
                RawToken::decode_with(SESSION_PREFIX, bad).is_none(),
                "{bad}"
            );
        }
        let other = RawToken::generate(&merezha_platform::OsRandom).unwrap();
        assert_ne!(other.digest(), token.digest());
    }

    #[test]
    fn hkdf_matches_rfc_5869_and_keys_are_separated() {
        // RFC 5869 test case 3: zero-length salt and info, 42-byte output;
        // our expansion yields its first 32 bytes.
        let ikm = [0x0b_u8; 22];
        let okm = hkdf(&ikm, b"");
        let expected = "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d";
        let hex: String = okm.0.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, expected);
        let keys = Keys::derive(&[7; 32], None);
        assert_ne!(keys.csrf.0, keys.ratelimit.0);
        assert_ne!(keys.ratelimit.0, keys.audit_subject.0);
        assert_eq!(format!("{keys:?}").matches("[redacted]").count(), 4);
        assert_ne!(keys.audit_subject.0, keys.passkey_decoy.0);
        assert_eq!(
            keys.passkey_decoys("ada@example.com"),
            keys.passkey_decoys("ada@example.com")
        );
        assert_ne!(
            keys.passkey_decoys("ada@example.com")[0],
            keys.passkey_decoys("bob@example.com")[0]
        );
        // Decoys take the common real lengths, and vary by address.
        let mut lengths = std::collections::BTreeSet::new();
        let mut counts = std::collections::BTreeSet::new();
        for n in 0..64 {
            let decoys = keys.passkey_decoys(&format!("user{n}@example.com"));
            counts.insert(decoys.len());
            for id in decoys {
                assert!([16, 32, 64].contains(&id.len()));
                lengths.insert(id.len());
            }
        }
        assert_eq!(lengths.len(), 3);
        assert_eq!(counts, [1, 2].into_iter().collect());
    }

    #[test]
    fn csrf_tokens_bind_the_session_and_survive_rotation() {
        let session = Uuid::from_u128(42);
        let old = Keys::derive(&[1; 32], None);
        let token = old.csrf_token(session);
        assert!(old.csrf_matches(session, &token));
        assert!(!old.csrf_matches(Uuid::from_u128(43), &token));
        assert!(!old.csrf_matches(session, "garbage"));
        assert!(!old.csrf_matches(session, ""));
        let rotated = Keys::derive(&[2; 32], Some(&[1; 32]));
        assert!(
            rotated.csrf_matches(session, &token),
            "previous key accepted"
        );
        assert!(rotated.csrf_matches(session, &rotated.csrf_token(session)));
        let fresh = Keys::derive(&[2; 32], None);
        assert!(!fresh.csrf_matches(session, &token));
        assert_ne!(
            old.bucket("sign_in.ip", b"1.2.3.4"),
            old.bucket("sign_in.account", b"1.2.3.4")
        );
    }
}
