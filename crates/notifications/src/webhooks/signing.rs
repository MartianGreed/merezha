//! Standard Webhooks signatures (NT §5.5).
//!
//! The signed content is `{webhook-id}.{webhook-timestamp}.{body}`, the
//! signature `v1,<base64(HMAC-SHA256(secret bytes, content))>`. During a
//! rotation window the header carries one `v1,` signature per secret,
//! separated by a space, and a receiver accepts the request when any of
//! them verifies. A secret is `whsec_` plus base64 of its bytes; the block
//! makes 32-byte secrets.
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{KeyInit, Mac};
use merezha_domain::Failure;
use merezha_platform::Random;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use uuid::Uuid;

type HmacSha256 = hmac::Hmac<Sha256>;

/// The prefix of a signing secret.
pub const SECRET_PREFIX: &str = "whsec_";
/// Bytes of a secret the block makes.
pub const SECRET_BYTES: usize = 32;
/// Most bytes of a secret [`SigningSecret::parse`] accepts.
const MAX_SECRET_BYTES: usize = 64;
/// Most signatures one header may carry for [`verify`].
const MAX_SIGNATURES: usize = 8;

/// The bytes of an endpoint's signing secret. `Debug` never shows them,
/// there is no `Display` ([`SigningSecret::reveal`] is the one way to the
/// text, used by the create and rotate answers only), and they are zeroed
/// on drop.
#[derive(Clone, PartialEq, Eq)]
pub struct SigningSecret(Vec<u8>);
/// The bytes are wiped when the secret is dropped.
impl Drop for SigningSecret {
    fn drop(&mut self) {
        zeroize::Zeroize::zeroize(&mut self.0);
    }
}
impl std::fmt::Debug for SigningSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SigningSecret(<redacted>)")
    }
}
impl SigningSecret {
    /// A fresh 32-byte secret.
    pub fn generate(random: &dyn Random) -> Result<Self, Failure> {
        let mut bytes = vec![0u8; SECRET_BYTES];
        random.fill(&mut bytes)?;
        Ok(Self(bytes))
    }
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
    /// `whsec_<base64>`, or the bare base64 (as Standard Webhooks
    /// libraries accept): `None` when it does not decode or is empty or
    /// longer than 64 bytes.
    pub fn parse(text: &str) -> Option<Self> {
        let encoded = text.strip_prefix(SECRET_PREFIX).unwrap_or(text);
        let bytes = STANDARD.decode(encoded).ok()?;
        (!bytes.is_empty() && bytes.len() <= MAX_SECRET_BYTES).then_some(Self(bytes))
    }
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
    /// `whsec_<base64>`: what the create and rotate routes show once.
    pub fn reveal(&self) -> String {
        format!("{SECRET_PREFIX}{}", STANDARD.encode(&self.0))
    }
}

/// `msg_<delivery id as 32 hex>`: stable across the attempts and
/// generations of one delivery, so receivers deduplicate on it.
pub fn webhook_id(delivery: Uuid) -> String {
    format!("msg_{}", delivery.simple())
}

fn mac(secret: &[u8], id: &str, timestamp: i64, body: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(secret)
        .unwrap_or_else(|_| unreachable!("HMAC takes keys of any length"));
    mac.update(id.as_bytes());
    mac.update(b".");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    mac.finalize().into_bytes().into()
}

/// One `v1,<base64>` signature.
pub fn signature(secret: &SigningSecret, id: &str, timestamp: i64, body: &[u8]) -> String {
    format!(
        "v1,{}",
        STANDARD.encode(mac(secret.expose(), id, timestamp, body))
    )
}

/// The `webhook-signature` header: one signature per secret, the current
/// one first, separated by a space.
pub fn header(secrets: &[&SigningSecret], id: &str, timestamp: i64, body: &[u8]) -> String {
    secrets
        .iter()
        .map(|secret| signature(secret, id, timestamp, body))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether one of the `v1,` signatures of `header` is `secret`'s over
/// `id`, `timestamp` and `body`, compared in constant time. The timestamp
/// tolerance is the receiver's (5 minutes recommended).
pub fn verify(secret: &SigningSecret, id: &str, timestamp: i64, body: &[u8], header: &str) -> bool {
    let expected = mac(secret.expose(), id, timestamp, body);
    let mut verified = false;
    for part in header.split(' ').take(MAX_SIGNATURES) {
        let Some(encoded) = part.strip_prefix("v1,") else {
            continue;
        };
        if let Ok(presented) = STANDARD.decode(encoded) {
            verified |= bool::from(presented.as_slice().ct_eq(&expected));
        }
    }
    verified
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reference vector of the Standard Webhooks specification (and
    /// its libraries' test suites).
    #[test]
    fn signatures_verify_against_the_standard_webhooks_reference_vector() {
        let secret = SigningSecret::parse("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw").unwrap();
        let (id, timestamp, body) = (
            "msg_p5jXN8AQM9LWM0D4loKWxJek",
            1_614_265_330,
            br#"{"test": 2432232314}"#.as_slice(),
        );
        assert_eq!(
            signature(&secret, id, timestamp, body),
            "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
        );
        let header = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=";
        assert!(verify(&secret, id, timestamp, body, header));
        assert!(!verify(&secret, id, timestamp + 1, body, header));
        assert!(!verify(&secret, "msg_other", timestamp, body, header));
        assert!(!verify(&secret, id, timestamp, b"{\"test\": 1}", header));
    }

    #[test]
    fn a_rotation_header_carries_both_signatures_and_either_verifies() {
        let current = SigningSecret::from_bytes(vec![1; 32]);
        let previous = SigningSecret::from_bytes(vec![2; 32]);
        let header = header(&[&current, &previous], "msg_1", 7, b"{}");
        let parts: Vec<&str> = header.split(' ').collect();
        assert_eq!(parts.len(), 2);
        assert!(parts.iter().all(|p| p.starts_with("v1,")));
        assert!(verify(&current, "msg_1", 7, b"{}", &header));
        assert!(verify(&previous, "msg_1", 7, b"{}", &header));
        let stranger = SigningSecret::from_bytes(vec![3; 32]);
        assert!(!verify(&stranger, "msg_1", 7, b"{}", &header));
    }

    #[test]
    fn secrets_round_trip_and_never_show_in_debug() {
        struct Counting;
        impl Random for Counting {
            fn fill(&self, bytes: &mut [u8]) -> Result<(), Failure> {
                for (i, b) in bytes.iter_mut().enumerate() {
                    *b = i as u8;
                }
                Ok(())
            }
        }
        let secret = SigningSecret::generate(&Counting).unwrap();
        let text = secret.reveal();
        assert!(text.starts_with("whsec_") && text.len() == 6 + 44);
        assert_eq!(SigningSecret::parse(&text), Some(secret.clone()));
        assert_eq!(format!("{secret:?}"), "SigningSecret(<redacted>)");
        assert!(SigningSecret::parse("whsec_!!").is_none());
        assert!(SigningSecret::parse("whsec_").is_none());
        let id = Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
        assert_eq!(webhook_id(id), "msg_0123456789abcdef0123456789abcdef");
    }
}
