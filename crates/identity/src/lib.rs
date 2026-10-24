//! The identity block (`docs/design/identity.md`), PR 1: accounts with
//! email and password, email verification, password sign-in, server-side
//! sessions (cookie and bearer), CSRF and Origin checks, reauthentication,
//! magic links, password reset and change, email change, account deletion,
//! PostgreSQL rate limits, audit events, the cleanup job and the session
//! [`Authenticator`](merezha_platform::ports::Authenticator).
//!
//! Organizations, RBAC, the `Authorizer`, API keys and the `Directory`
//! arrive with ID PR 2; OAuth, OIDC and passkeys with PR 3. Until then the
//! principal carries an empty grant snapshot and the application keeps the
//! loopback `AllowAll` policy.
pub mod benchmark;
pub mod blocklist;
pub mod catalog;
pub mod cleanup;
pub mod config;
pub mod core;
pub mod flows;
pub mod gcra;
pub mod hasher;
pub mod http;
pub mod ids;
pub mod link;
pub mod links;
pub mod metrics;
pub mod module;
pub mod normalize;
pub mod password;
pub mod secrets;
pub mod session;
pub mod sql;

pub use config::{HostFacts, Settings};
pub use core::{NAME, event, topic};
pub use module::{ERRORS, IdentityHandle, IdentityModule, MIGRATIONS};
pub use session::SessionAuthenticator;
