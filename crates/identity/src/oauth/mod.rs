//! OAuth 2.0 and OpenID Connect sign-in (section 6): the configured
//! providers, their cached discovery documents and key sets, the
//! authorization code flow with PKCE, `state`, a browser binding cookie and
//! the OIDC `nonce`, and the linking rules of [`crate::link`].
//!
//! [`adapter`] is the only module that touches `openidconnect`, `oauth2` and
//! their `chrono` types (R14); everything it hands back is plain data.
pub mod adapter;
pub mod flows;
pub mod routes;

use crate::config::{ProviderKind, ProviderSettings};
use std::{collections::BTreeMap, sync::Arc};

/// Google's OIDC issuer, where its discovery document lives.
pub const GOOGLE_ISSUER: &str = "https://accounts.google.com";
/// GitHub's authorization endpoint.
pub const GITHUB_AUTHORIZE: &str = "https://github.com/login/oauth/authorize";
/// GitHub's token endpoint.
pub const GITHUB_TOKEN: &str = "https://github.com/login/oauth/access_token";
/// GitHub's REST API (`/user`, `/user/emails`).
pub const GITHUB_API: &str = "https://api.github.com";
/// Largest provider response read (discovery, key set, token, profile).
pub const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
/// How long a discovery document and its key set are reused.
pub const DISCOVERY_TTL: std::time::Duration = std::time::Duration::from_secs(3600);
/// Least time between two key set refetches forced by an unknown `kid`.
pub const JWKS_REFETCH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
/// Skew allowed on an ID token's `iat` (section 6, 7).
pub const IAT_SKEW: std::time::Duration = std::time::Duration::from_secs(300);
/// Longest provider `subject` stored.
pub const MAX_SUBJECT_BYTES: usize = 255;

/// `[a-z][a-z0-9_]{0,31}`.
pub fn valid_provider_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && name.len() <= 32
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// Endpoints that replace a provider's built-in ones: a Google issuer, or
/// GitHub's authorization, token and API bases (GitHub Enterprise Server,
/// and the mock providers of the tests). Set in code by the embedding
/// binary, never from request input.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProviderEndpoints {
    pub issuer: Option<String>,
    pub authorization: Option<String>,
    pub token: Option<String>,
    pub api: Option<String>,
}

/// What a provider asserted about the person, after verification. The
/// provider's access and refresh tokens are already discarded.
#[derive(Clone, PartialEq, Eq)]
pub struct Profile {
    /// The provider's stable id (`sub`, GitHub's numeric `id`).
    pub subject: String,
    /// The address the provider asserts, if any.
    pub email: Option<String>,
    /// Whether the provider asserted it as verified.
    pub email_verified: bool,
    pub display_name: Option<String>,
}
impl std::fmt::Debug for Profile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Profile")
            .field("email_verified", &self.email_verified)
            .finish_non_exhaustive()
    }
}

/// Why a provider exchange failed: unreachable (`provider_unavailable`) or a
/// protocol, signature or claim problem (`oauth_failed`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderError {
    Unavailable,
    Failed,
}
impl ProviderError {
    pub fn code(self) -> &'static str {
        match self {
            ProviderError::Unavailable => "provider_unavailable",
            ProviderError::Failed => "oauth_failed",
        }
    }
}

/// One configured provider and its cached metadata.
pub struct Provider {
    pub settings: ProviderSettings,
    pub endpoints: ProviderEndpoints,
    pub(crate) cache: adapter::Cache,
}
impl std::fmt::Debug for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Provider")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}
impl Provider {
    pub fn name(&self) -> &str {
        &self.settings.name
    }
    pub fn kind(&self) -> ProviderKind {
        self.settings.kind
    }
    /// The issuer ID tokens and `iss` must name: the override, else the
    /// configured or built-in one. `None` for GitHub.
    pub fn issuer(&self) -> Option<&str> {
        match self.settings.kind {
            ProviderKind::Github => None,
            _ => self
                .endpoints
                .issuer
                .as_deref()
                .or(self.settings.issuer.as_deref()),
        }
    }
}

/// The configured providers by name.
#[derive(Clone, Debug, Default)]
pub struct Providers {
    by_name: BTreeMap<String, Arc<Provider>>,
}
impl Providers {
    pub fn new(
        settings: &BTreeMap<String, ProviderSettings>,
        overrides: &BTreeMap<String, ProviderEndpoints>,
    ) -> Self {
        Self {
            by_name: settings
                .iter()
                .map(|(name, settings)| {
                    (
                        name.clone(),
                        Arc::new(Provider {
                            settings: settings.clone(),
                            endpoints: overrides.get(name).cloned().unwrap_or_default(),
                            cache: adapter::Cache::default(),
                        }),
                    )
                })
                .collect(),
        }
    }
    pub fn get(&self, name: &str) -> Option<&Arc<Provider>> {
        self.by_name.get(name)
    }
    pub fn iter(&self) -> impl Iterator<Item = &Arc<Provider>> {
        self.by_name.values()
    }
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }
    /// Whether `name` is a configured provider (identities of a provider
    /// removed from configuration cannot sign in).
    pub fn contains(&self, name: &str) -> bool {
        self.by_name.contains_key(name)
    }
}

/// The redirect URI of `provider`: exact and per provider, the RFC 9700
/// mix-up defense (section 6).
pub fn redirect_uri(api_base_url: &str, provider: &str) -> String {
    format!(
        "{}/v1/identity/oauth/{provider}/callback",
        api_base_url.trim_end_matches('/')
    )
}

/// GitHub's `/user/emails` choice: the primary address, and only when it
/// is verified (section 6). Unverified primaries and secondary addresses,
/// verified or not, are never used.
pub fn github_email(entries: &[(String, bool, bool)]) -> Option<String> {
    entries
        .iter()
        .find(|(_, primary, verified)| *primary && *verified)
        .map(|(email, _, _)| email.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_names_follow_the_grammar() {
        for good in ["google", "github", "acme_sso", "a", "x1"] {
            assert!(valid_provider_name(good), "{good}");
        }
        for bad in [
            "",
            "Google",
            "1abc",
            "acme-sso",
            "acme.sso",
            &"a".repeat(33),
        ] {
            assert!(!valid_provider_name(bad), "{bad}");
        }
    }

    #[test]
    fn redirect_uris_are_exact_and_per_provider() {
        assert_eq!(
            redirect_uri("https://api.example.com", "google"),
            "https://api.example.com/v1/identity/oauth/google/callback"
        );
        assert_eq!(
            redirect_uri("https://api.example.com/", "acme_sso"),
            "https://api.example.com/v1/identity/oauth/acme_sso/callback"
        );
        assert_ne!(
            redirect_uri("https://api.example.com", "google"),
            redirect_uri("https://api.example.com", "github")
        );
    }

    #[test]
    fn github_uses_the_verified_primary_only() {
        let e = |email: &str, primary, verified| (email.to_owned(), primary, verified);
        assert_eq!(
            github_email(&[
                e("old@x.example", false, true),
                e("ada@x.example", true, true)
            ]),
            Some("ada@x.example".to_owned())
        );
        // An unverified primary is not used, nor a verified secondary.
        assert_eq!(
            github_email(&[
                e("ada@x.example", true, false),
                e("alt@x.example", false, true)
            ]),
            None
        );
        assert_eq!(github_email(&[]), None);
    }
}
