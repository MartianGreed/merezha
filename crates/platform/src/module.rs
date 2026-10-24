//! The contract every block implements.
use crate::{
    BoxFuture,
    config::{ConfigIssue, ModuleSection},
    context::AppContext,
    errors::ErrorCode,
};
use axum::Router;
use merezha_domain::Failure;
use merezha_postgres::Migration;
use std::time::Instant;

/// Names of route prefixes owned by the host; modules cannot use them.
/// `api` and `application` are also the kernel-only outbox and notifier
/// sources.
pub const RESERVED_NAMES: [&str; 6] = [
    "application",
    "api",
    "services",
    "pipelines",
    "reload",
    "health",
];

/// Integration events a module publishes and consumes through the outbox.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ModuleEvents {
    pub publishes: &'static [&'static str],
    pub consumes: &'static [&'static str],
}

/// A bounded context hosted in the process: its configuration section,
/// schema migrations, routes, background work and readiness.
///
/// Lifecycle, driven by the host in this order: [`Module::configure`] once
/// with `[modules.<name>]`, migrations verified or applied, [`Module::start`],
/// [`Module::routes`] mounted under `/v1/<name>`, then on shutdown
/// [`Module::shutdown`] after the listener has drained. A module is enabled
/// exactly when its section is present.
pub trait Module: Send + Sync + 'static {
    /// `[a-z][a-z0-9_]*`, at most 64 bytes, unique, not in [`RESERVED_NAMES`].
    /// It names the configuration section, the route prefix and the
    /// migration ledger key, so it must never change.
    fn name(&self) -> &'static str;
    /// Validate and keep the module's own settings. Report every problem at
    /// once with keys from [`ModuleSection::key`]. The default accepts only
    /// an empty section.
    fn configure(&mut self, section: &ModuleSection) -> Result<(), Vec<ConfigIssue>> {
        let issues = section.unknown_keys(&[]);
        if issues.is_empty() {
            Ok(())
        } else {
            Err(issues)
        }
    }
    /// Ordered forward-only migrations for this module's own schema.
    fn migrations(&self) -> &[Migration] {
        &[]
    }
    /// Error codes this module returns with their HTTP statuses, registered
    /// at startup (R8). A code another module or the kernel registered with
    /// a different status stops startup with `error_code_conflict`.
    /// Canonical kernel codes need not be listed.
    fn errors(&self) -> &[ErrorCode] {
        &[]
    }
    /// Integration events, for the catalog and later wiring checks.
    fn events(&self) -> ModuleEvents {
        ModuleEvents::default()
    }
    /// Start background tasks. Bounded by the host's start timeout; a
    /// failure aborts startup. `ctx` is bound to this module: its outbox and
    /// notifier handles can only be bound to [`Module::name`].
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        let _ = ctx;
        Box::pin(async { Ok(()) })
    }
    /// Routes, nested under `/v1/<name>`. Called once after `start`. Errors
    /// should use [`crate::edge::error_response`].
    fn routes(&self, ctx: &AppContext) -> Router {
        let _ = ctx;
        Router::new()
    }
    /// Readiness contribution; must answer within one second and must not
    /// block on external services beyond that.
    fn ready(&self) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async { Ok(()) })
    }
    /// Stop background work and drain it before `deadline`. Called once,
    /// after the listener stopped admitting requests.
    fn shutdown(&self, deadline: Instant) -> BoxFuture<'_, ()> {
        let _ = deadline;
        Box::pin(async {})
    }
}
