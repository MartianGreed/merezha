//! Signing secrets at rest (NT §5.5): AES-256-GCM under
//! `k_wh = HKDF-SHA256(secret_key, info = "merezha-notify/webhook-secret/v1")`,
//! a random 96-bit nonce per seal and the endpoint id as associated data,
//! so a ciphertext copied onto another endpoint does not open. Opening
//! tries the key the row names first, then `secret_key` and
//! `secret_key_previous`.
//!
//! Each sealed secret records the [`KeyId`] of its `k_wh`: the first 8
//! bytes of SHA-256 over [`KEY_ID_LABEL`] and the key. It names the key
//! without revealing it, so the `retention` task reseals and counts the
//! rows still under `secret_key_previous`.
use super::signing::SigningSecret;
use crate::{config::KeyMaterial, keys};
use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use merezha_domain::Failure;
use merezha_platform::Random;
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// HKDF info of the webhook secret key.
pub const WEBHOOK_SECRET_INFO: &[u8] = b"merezha-notify/webhook-secret/v1";
/// Hashed before the key into its [`KeyId`].
pub const KEY_ID_LABEL: &[u8] = b"merezha-notify/webhook-key-id/v1";
/// A stored secret neither key opens (`secret_key` changed without the old
/// one as `secret_key_previous`, or a damaged row).
pub const SECRET_UNREADABLE: &str = "secret_unreadable";
/// Nonce bytes.
pub const NONCE_BYTES: usize = 12;

/// The 8-byte name of a `k_wh`.
pub type KeyId = [u8; 8];
/// A secret as a row stores it: ciphertext and tag, nonce, and key id.
pub type StoredSecret = (Vec<u8>, Vec<u8>, Option<Vec<u8>>);

/// A sealed secret: ciphertext and tag, its nonce, and the key it is
/// sealed under.
#[derive(Clone, PartialEq, Eq)]
pub struct Sealed {
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
    pub key_id: Vec<u8>,
}
impl std::fmt::Debug for Sealed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Sealed(..)")
    }
}

/// One `k_wh` and its id.
#[derive(Clone)]
struct Sealer {
    cipher: Aes256Gcm,
    id: KeyId,
}
impl Sealer {
    fn new(material: &KeyMaterial) -> Self {
        let key = keys::hkdf(material.expose(), WEBHOOK_SECRET_INFO);
        let digest = Sha256::new()
            .chain_update(KEY_ID_LABEL)
            .chain_update(key.expose())
            .finalize();
        let mut id = [0u8; 8];
        id.copy_from_slice(&digest[..8]);
        Self {
            cipher: Aes256Gcm::new_from_slice(key.expose())
                .unwrap_or_else(|_| unreachable!("a derived key is 32 bytes")),
            id,
        }
    }
    fn open(&self, endpoint: Uuid, ciphertext: &[u8], nonce: &[u8]) -> Option<SigningSecret> {
        self.cipher
            .decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: ciphertext,
                    aad: endpoint.as_bytes(),
                },
            )
            .ok()
            .map(SigningSecret::from_bytes)
    }
}

/// Seals with `k_wh` and opens with it or its previous value. `Debug`
/// never shows the keys.
#[derive(Clone)]
pub struct SecretBox {
    current: Sealer,
    previous: Option<Sealer>,
}
impl std::fmt::Debug for SecretBox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretBox")
            .field("previous", &self.previous.is_some())
            .finish_non_exhaustive()
    }
}
impl SecretBox {
    pub fn new(secret_key: &KeyMaterial, previous: Option<&KeyMaterial>) -> Self {
        Self {
            current: Sealer::new(secret_key),
            previous: previous.map(Sealer::new),
        }
    }
    /// The id of the current key, which every new seal records.
    pub fn key_id(&self) -> KeyId {
        self.current.id
    }
    /// The ids of the current and the previous key (8 zero bytes without
    /// one): what an unreadable row was tried with.
    pub fn key_pair(&self) -> [u8; 16] {
        let mut pair = [0u8; 16];
        pair[..8].copy_from_slice(&self.current.id);
        if let Some(previous) = &self.previous {
            pair[8..].copy_from_slice(&previous.id);
        }
        pair
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
            .cipher
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
            key_id: self.current.id.to_vec(),
        })
    }
    /// The secret of `endpoint`, and whether only the previous key opened
    /// it. `key_id` (the row's) picks the key to try first. Fails
    /// [`SECRET_UNREADABLE`].
    pub fn open(
        &self,
        endpoint: Uuid,
        ciphertext: &[u8],
        nonce: &[u8],
        key_id: Option<&[u8]>,
    ) -> Result<(SigningSecret, bool), Failure> {
        if nonce.len() != NONCE_BYTES {
            return Err(Failure::permanent(SECRET_UNREADABLE));
        }
        let previous_first = self
            .previous
            .as_ref()
            .is_some_and(|p| key_id == Some(p.id.as_slice()));
        let mut order: Vec<(&Sealer, bool)> = vec![(&self.current, false)];
        if let Some(previous) = &self.previous {
            order.push((previous, true));
        }
        if previous_first {
            order.reverse();
        }
        order
            .into_iter()
            .find_map(|(sealer, old)| sealer.open(endpoint, ciphertext, nonce).map(|s| (s, old)))
            .ok_or_else(|| Failure::permanent(SECRET_UNREADABLE))
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
        assert_eq!(sealed.key_id, old.key_id().to_vec());
        assert!(!sealed.ciphertext.windows(32).any(|w| w == secret.expose()));
        let open = |b: &SecretBox, endpoint| {
            b.open(
                endpoint,
                &sealed.ciphertext,
                &sealed.nonce,
                Some(&sealed.key_id),
            )
        };
        assert_eq!(open(&old, a).unwrap(), (secret.clone(), false));
        // The endpoint id is the associated data.
        assert_eq!(
            open(&old, b).unwrap_err(),
            Failure::permanent(SECRET_UNREADABLE)
        );
        // After a `secret_key` rotation the previous key still opens it,
        // and the key ids tell the two keys apart.
        let rotated = SecretBox::new(&key(2), Some(&key(1)));
        assert_ne!(rotated.key_id(), old.key_id());
        assert_eq!(open(&rotated, a).unwrap(), (secret.clone(), true));
        assert_eq!(
            rotated
                .open(a, &sealed.ciphertext, &sealed.nonce, None)
                .unwrap(),
            (secret.clone(), true)
        );
        let forgotten = SecretBox::new(&key(2), None);
        assert!(open(&forgotten, a).is_err());
        assert_eq!(format!("{rotated:?}"), "SecretBox { previous: true, .. }");
        assert_eq!(SecretBox::new(&key(1), None).key_id(), old.key_id());
    }
}
