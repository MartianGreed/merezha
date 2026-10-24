//! Password normalization and policy (section 7, owner answer ID2): NFKC,
//! a minimum length (12 by default, never below 8), a maximum of 256
//! characters and 1024 bytes, a breached-password blocklist and context
//! words. No composition rules and no expiry (R15).
use crate::{blocklist, normalize::Email};
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashSet;
use unicode_normalization::UnicodeNormalization;

/// Largest password after NFKC, so Argon2 input is bounded.
pub const MAX_PASSWORD_BYTES: usize = 1024;
/// `min_length` default and floor.
pub const DEFAULT_MIN_LENGTH: usize = 12;
pub const MIN_LENGTH_FLOOR: usize = 8;
/// `max_length` default and ceiling.
pub const MAX_LENGTH_CEILING: usize = 256;
/// Context words shorter than this are ignored: they would refuse half of
/// all passwords without making any of them stronger.
pub const MIN_CONTEXT_WORD: usize = 4;

/// A password after NFKC. `Debug` prints `[redacted]`; there is no
/// `Serialize`, and the value is zeroized on drop.
pub struct Password(SecretString);
impl Password {
    /// NFKC-normalize `input`. Fails `too_long` past
    /// [`MAX_PASSWORD_BYTES`] and `invalid` when empty.
    pub fn new(input: &str) -> Result<Self, &'static str> {
        if input.len() > MAX_PASSWORD_BYTES * 4 {
            return Err("too_long");
        }
        let normalized: String = input.nfkc().collect();
        if normalized.is_empty() {
            return Err("invalid");
        }
        if normalized.len() > MAX_PASSWORD_BYTES {
            return Err("too_long");
        }
        Ok(Self(SecretString::from(normalized)))
    }
    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }
    /// Length in Unicode scalar values.
    pub fn chars(&self) -> usize {
        self.expose().chars().count()
    }
}
impl std::fmt::Debug for Password {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[redacted]")
    }
}

/// Why a password was refused; the HTTP codes of the PR 1 table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyError {
    TooShort,
    TooLong,
    Blocklisted,
    Contextual,
}
impl PolicyError {
    pub fn code(self) -> &'static str {
        match self {
            PolicyError::TooShort => "password_too_short",
            PolicyError::TooLong => "password_too_long",
            PolicyError::Blocklisted => "password_blocklisted",
            PolicyError::Contextual => "password_contextual",
        }
    }
}

/// The configured policy.
#[derive(Clone)]
pub struct PasswordPolicy {
    pub min_length: usize,
    pub max_length: usize,
    /// The operator's extra entries, NFKC and lowercase.
    extra: HashSet<String>,
    /// The application name, NFKC and lowercase.
    app_words: Vec<String>,
}
impl std::fmt::Debug for PasswordPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PasswordPolicy")
            .field("min_length", &self.min_length)
            .field("max_length", &self.max_length)
            .field("extra_entries", &self.extra.len())
            .finish_non_exhaustive()
    }
}
impl PasswordPolicy {
    pub fn new(min_length: usize, max_length: usize, app_name: Option<&str>) -> Self {
        let app_words = app_name
            .map(|name| {
                fold(name)
                    .split(|c: char| !c.is_alphanumeric())
                    .filter(|w| w.chars().count() >= MIN_CONTEXT_WORD)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        Self {
            min_length,
            max_length,
            extra: HashSet::new(),
            app_words,
        }
    }
    /// Add the operator's blocklist: one password per line.
    pub fn with_extra(mut self, text: &str) -> Self {
        self.extra = text
            .lines()
            .map(fold)
            .filter(|line| !line.is_empty())
            .collect();
        self
    }
    /// Check `password` for the account `email`, in order: length, the
    /// blocklists, then context words (the email local part, the email
    /// domain labels, the application name) contained in the password,
    /// all compared after NFKC and lowercasing.
    pub fn check(&self, password: &Password, email: &Email) -> Result<(), PolicyError> {
        let length = password.chars();
        if length < self.min_length {
            return Err(PolicyError::TooShort);
        }
        if length > self.max_length {
            return Err(PolicyError::TooLong);
        }
        let folded = fold(password.expose());
        if blocklist::contains(&folded) || self.extra.contains(&folded) {
            return Err(PolicyError::Blocklisted);
        }
        let local = fold(email.local());
        let mut words: Vec<&str> = vec![local.as_str()];
        let labels: Vec<String> = email.domain_labels().into_iter().map(fold).collect();
        words.extend(labels.iter().map(String::as_str));
        words.extend(self.app_words.iter().map(String::as_str));
        if words
            .iter()
            .filter(|w| w.chars().count() >= MIN_CONTEXT_WORD)
            .any(|w| folded.contains(w))
        {
            return Err(PolicyError::Contextual);
        }
        Ok(())
    }
}

/// NFKC then lowercase, the comparison form of every rule.
pub fn fold(text: &str) -> String {
    text.nfkc().collect::<String>().to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn email(text: &str) -> Email {
        crate::normalize::email(text).unwrap()
    }

    #[test]
    fn passwords_are_nfkc_and_bounded_and_never_printed() {
        // Fullwidth letters fold to ASCII under NFKC.
        let p = Password::new("ｐａｓｓｗｏｒｄ").unwrap();
        assert_eq!(p.expose(), "password");
        assert_eq!(Password::new("").unwrap_err(), "invalid");
        assert_eq!(
            Password::new(&"a".repeat(MAX_PASSWORD_BYTES + 1)).unwrap_err(),
            "too_long"
        );
        assert_eq!(Password::new(&"é".repeat(512)).unwrap().chars(), 512);
        assert_eq!(Password::new(&"é".repeat(513)).unwrap_err(), "too_long");
        let secret = Password::new("correct horse battery").unwrap();
        assert_eq!(format!("{secret:?}"), "[redacted]");
    }

    #[test]
    fn policy_rules_in_order() {
        let policy = PasswordPolicy::new(12, 256, Some("Tasks Board"));
        let ada = email("ada.lovelace@analytical.example");
        let check = |text: &str| policy.check(&Password::new(text).unwrap(), &ada);
        assert_eq!(check("short"), Err(PolicyError::TooShort));
        assert_eq!(check("elevenchars"), Err(PolicyError::TooShort));
        assert_eq!(check(&"x".repeat(257)), Err(PolicyError::TooLong));
        // Common breached passwords, whatever their case or width.
        assert_eq!(check("password1234"), Err(PolicyError::Blocklisted));
        assert_eq!(check("PASSWORD1234"), Err(PolicyError::Blocklisted));
        assert_eq!(
            check("ｐａｓｓｗｏｒｄ１２３４"),
            Err(PolicyError::Blocklisted)
        );
        assert_eq!(check("qwertyuiop123"), Err(PolicyError::Blocklisted));
        // Context words: the local part, a domain label, the app name.
        assert_eq!(check("my ada.lovelace pass"), Err(PolicyError::Contextual));
        assert_eq!(
            check("Analytical-Engine-1843"),
            Err(PolicyError::Contextual)
        );
        assert_eq!(check("my tasks are endless"), Err(PolicyError::Contextual));
        assert_eq!(check("BOARDing pass 1843!"), Err(PolicyError::Contextual));
        // The TLD and short words are not context.
        assert_eq!(check("example of a phrase"), Ok(()));
        assert_eq!(check("correct horse battery staple"), Ok(()));
        // No composition rules: twelve lowercase letters pass.
        assert_eq!(check("zqvdkrnmwplh"), Ok(()));
        // The floor of 8 and the operator's own list.
        let relaxed = PasswordPolicy::new(8, 256, None).with_extra("Zqvdkrnm\n\n");
        let check = |text: &str| relaxed.check(&Password::new(text).unwrap(), &ada);
        assert_eq!(check("zqvdkrnm"), Err(PolicyError::Blocklisted));
        assert_eq!(check("zqvdkrnx"), Ok(()));
        assert_eq!(check("1234567"), Err(PolicyError::TooShort));
        assert_eq!(PolicyError::Contextual.code(), "password_contextual");
    }
}
