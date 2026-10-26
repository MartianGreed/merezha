//! Prefixed identifiers (section 2): the database stores UUIDv7 values and
//! the API renders them as `<prefix>_<32 lowercase hex>`, such as
//! `usr_0192f5c4e1b07a3c9d2e4f6a8b0c1d2e`. Every rendered id is a valid
//! [`merezha_domain::Identity`].
use merezha_domain::{Failure, Identity};
use merezha_platform::Random;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Users.
pub const USER: &str = "usr";
/// Sessions.
pub const SESSION: &str = "ses";
/// Organizations.
pub const ORGANIZATION: &str = "org";
/// Memberships.
pub const MEMBERSHIP: &str = "mem";
/// Invitations.
pub const INVITATION: &str = "inv";
/// Roles (built-in, declared and custom).
pub const ROLE: &str = "rol";
/// Role assignments, tenant and application.
pub const ASSIGNMENT: &str = "asg";
/// Service accounts.
pub const SERVICE_ACCOUNT: &str = "svc";
/// API keys.
pub const API_KEY: &str = "key";
/// Passkeys.
pub const PASSKEY: &str = "pky";
/// OAuth identities.
pub const OAUTH_IDENTITY: &str = "oid";
/// WebAuthn ceremonies (their ids are random, not time ordered).
pub const CEREMONY: &str = "cer";

/// `<prefix>_<hex>`.
pub fn render(prefix: &str, id: Uuid) -> String {
    format!("{prefix}_{}", id.simple())
}

/// The rendered id as an [`Identity`].
pub fn identity(prefix: &str, id: Uuid) -> Identity {
    // The rendered form is ASCII letters, digits and `_`, at most 36 bytes,
    // which `Identity` always accepts.
    Identity::new(render(prefix, id)).unwrap_or_else(|_| unreachable!("prefixed ids are valid"))
}

/// Parse `<prefix>_<32 lowercase hex>`. Anything else, an id of another
/// type included, fails permanent `not_found`, so ids of one type are never
/// accepted where another is expected.
pub fn parse(prefix: &str, text: &str) -> Result<Uuid, Failure> {
    let not_found = || Failure::permanent("not_found");
    let hex = text
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('_'))
        .ok_or_else(not_found)?;
    if hex.len() != 32 || !hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(not_found());
    }
    Uuid::try_parse(hex).map_err(|_| not_found())
}

/// A new UUIDv7 at `now` with random bits from the platform CSPRNG.
pub fn new_v7(now: SystemTime, random: &dyn Random) -> Result<Uuid, Failure> {
    let millis = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut bits = [0u8; 10];
    random.fill(&mut bits)?;
    Ok(uuid::Builder::from_unix_timestamp_millis(millis, &bits).into_uuid())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_round_trip_and_refuse_other_prefixes() {
        let id = new_v7(SystemTime::now(), &merezha_platform::OsRandom).unwrap();
        assert_eq!(id.get_version_num(), 7);
        let text = render(USER, id);
        assert_eq!(text.len(), 36);
        assert!(text.starts_with("usr_"));
        assert_eq!(parse(USER, &text).unwrap(), id);
        assert_eq!(identity(USER, id).as_str(), text);
        for bad in [
            render(SESSION, id),
            text.to_uppercase(),
            format!("{text}0"),
            text.replace("usr_", "usr"),
            id.hyphenated().to_string(),
            String::new(),
        ] {
            assert_eq!(parse(USER, &bad).unwrap_err().code, "not_found", "{bad}");
        }
    }
}
