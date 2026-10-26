//! Signing secrets at rest (NT §5.5): AES-256-GCM under
//! `k_wh = HKDF-SHA256(secret_key, info = "merezha-notify/webhook-secret/v1")`,
//! a random 96-bit nonce per seal and the endpoint id as associated data,
//! so a ciphertext copied onto another endpoint does not open. Opening
//! tries `secret_key`, then `secret_key_previous`.
use super::signing::SigningSecret;
use crate::{config::KeyMaterial, keys};
use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use merezha_domain::Failure;
use merezha_platform::Random;
use uuid::Uuid;

/// HKDF info of the webhook secret key.
pub const WEBHOOK_SECRET_INFO: &[u8] = b"merezha-notify/webhook-secret/v1";
/// A stored secret neither key opens (`secret_key` changed without the old
/// one as `secret_key_previous`, or a damaged row).
pub const SECRET_UNREADABLE: &str = "secret_unreadable";
/// Nonce bytes.
pub const NONCE_BYTES: usize = 12;

/// A sealed secret: ciphertext and tag, and its nonce.
#[derive(Clone, PartialEq, Eq)]
pub struct Sealed {
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
}
impl std::fmt::Debug for Sealed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Sealed(..)")
    }
}

/// Seals with `k_wh` and opens with it or its previous value. `Debug`
/// never shows the keys.
#[derive(Clone)]
pub struct SecretBox {
    current: Aes256Gcm,
    previous: Option<Aes256Gcm>,
}
impl std::fmt::Debug for SecretBox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretBox")
            .field("previous", &self.previous.is_some())
            .finish_non_exhaustive()
    }
}
fn cipher(material: &KeyMaterial) -> Aes256Gcm {
    let key = keys::hkdf(material.expose(), WEBHOOK_SECRET_INFO);
    Aes256Gcm::new_from_slice(key.expose())
        .unwrap_or_else(|_| unreachable!("a derived key is 32 bytes"))
}
impl SecretBox {
    pub fn new(secret_key: &KeyMaterial, previous: Option<&KeyMaterial>) -> Self {
        Self {
            current: cipher(secret_key),
            previous: previous.map(cipher),
        }
    }
    /// Whether a previous key is configured, so rows may still be sealed
    /// under it.
    pub fn has_previous(&self) -> bool {
        self.previous.is_some()
    }
    /// `secret` sealed for `endpoint` under the current key.
    pub fn seal(
        &self,
        endpoint: Uuid,
        secret: &SigningSecret,
        random: &dyn Random,
    ) -> Result<Sealed, Failure> {
        let mut nonce = [0u8; NONCE_BYTES];
        random.fill(&mut nonce)?;
        let ciphertext = self
            .current
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: secret.expose(),
                    aad: endpoint.as_bytes(),
                },
            )
            .map_err(|_| Failure::transient("internal"))?;
        Ok(Sealed {
            ciphertext,
            nonce: nonce.to_vec(),
        })
    }
    /// The secret of `endpoint`, and whether only the previous key opened
    /// it. Fails [`SECRET_UNREADABLE`].
    pub fn open(
        &self,
        endpoint: Uuid,
        ciphertext: &[u8],
        nonce: &[u8],
    ) -> Result<(SigningSecret, bool), Failure> {
        if nonce.len() != NONCE_BYTES {
            return Err(Failure::permanent(SECRET_UNREADABLE));
        }
        let payload = || Payload {
            msg: ciphertext,
            aad: endpoint.as_bytes(),
        };
        let nonce = Nonce::from_slice(nonce);
        if let Ok(bytes) = self.current.decrypt(nonce, payload()) {
            return Ok((SigningSecret::from_bytes(bytes), false));
        }
        match &self.previous {
            Some(previous) => previous
                .decrypt(nonce, payload())
                .map(|bytes| (SigningSecret::from_bytes(bytes), true))
                .map_err(|_| Failure::permanent(SECRET_UNREADABLE)),
            None => Err(Failure::permanent(SECRET_UNREADABLE)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(u8);
    impl Random for Fixed {
        fn fill(&self, bytes: &mut [u8]) -> Result<(), Failure> {
            bytes.fill(self.0);
            Ok(())
        }
    }
    fn key(byte: u8) -> KeyMaterial {
        KeyMaterial::for_tests(vec![byte; 32])
    }

    #[test]
    fn a_sealed_secret_opens_for_its_endpoint_under_either_key_only() {
        let secret = SigningSecret::from_bytes(vec![9; 32]);
        let (a, b) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let old = SecretBox::new(&key(1), None);
        let sealed = old.seal(a, &secret, &Fixed(7)).unwrap();
        assert_eq!(sealed.nonce.len(), 12);
        assert_eq!(sealed.ciphertext.len(), 32 + 16);
        assert!(!sealed.ciphertext.windows(32).any(|w| w == secret.expose()));
        assert_eq!(
            old.open(a, &sealed.ciphertext, &sealed.nonce).unwrap(),
            (secret.clone(), false)
        );
        // The endpoint id is the associated data.
        assert_eq!(
            old.open(b, &sealed.ciphertext, &sealed.nonce).unwrap_err(),
            Failure::permanent(SECRET_UNREADABLE)
        );
        // After a `secret_key` rotation the previous key still opens it.
        let rotated = SecretBox::new(&key(2), Some(&key(1)));
        assert_eq!(
            rotated.open(a, &sealed.ciphertext, &sealed.nonce).unwrap(),
            (secret.clone(), true)
        );
        let forgotten = SecretBox::new(&key(2), None);
        assert!(
            forgotten
                .open(a, &sealed.ciphertext, &sealed.nonce)
                .is_err()
        );
        assert_eq!(format!("{rotated:?}"), "SecretBox { previous: true, .. }");
    }
}
