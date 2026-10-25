//! The identity block (`docs/design/identity.md`).
//!
//! PR 1: accounts with email and password, email verification, password
//! sign-in, server-side sessions (cookie and bearer), CSRF and Origin
//! checks, reauthentication, magic links, password reset and change, email
//! change, account deletion, PostgreSQL rate limits, audit events, the
//! cleanup job and the session
//! [`Authenticator`](merezha_platform::ports::Authenticator).
//!
//! PR 2: organizations, memberships, invitations, custom and declared
//! roles, tenant and application role assignments, service accounts, API
//! keys in the `Authenticator`, version-cached grant snapshots, the RBAC
//! [`RbacAuthorizer`](rbac::RbacAuthorizer), the
//! [`IdentityDirectory`](directory::IdentityDirectory) and the
//! `platform_admin` grant of the CLI.
//!
//! PR 3: OAuth 2.0 and OIDC sign-in, sign-up and linking (Google, GitHub,
//! generic OIDC; [`oauth`], [`link`]) and, behind the `passkeys` cargo
//! feature, WebAuthn passkeys (`passkeys`).
pub mod access;
pub mod admin_http;
pub mod apikey;
pub mod appscope;
pub mod benchmark;
pub mod blocklist;
pub mod catalog;
pub mod cleanup;
pub mod config;
pub mod core;
pub mod directory;
pub mod flows;
pub mod gcra;
pub mod grants;
pub mod hasher;
pub mod http;
pub mod ids;
pub mod link;
pub mod links;
pub mod machines;
pub mod metrics;
pub mod module;
pub mod normalize;
pub mod oauth;
#[cfg(feature = "passkeys")]
pub mod passkeys;
pub mod password;
pub mod rbac;
pub mod secrets;
pub mod session;
pub mod sql;
pub mod tenancy;

pub use catalog::Catalog;
pub use config::{HostFacts, Settings};
pub use core::{NAME, event, topic};
pub use module::{ERRORS, IdentityHandle, IdentityModule, MIGRATIONS};
pub use session::SessionAuthenticator;

/// The passkey routes, when the feature is compiled in.
pub(crate) fn passkey_routes() -> axum::Router<std::sync::Arc<core::Core>> {
    #[cfg(feature = "passkeys")]
    {
        passkeys::routes::routes()
    }
    #[cfg(not(feature = "passkeys"))]
    {
        axum::Router::new()
    }
}
