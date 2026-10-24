//! The notifications block (`docs/design/notifications.md`): email and SMS
//! through Brevo, Web Push, an in-app inbox with SSE and signed webhooks.
//!
//! This crate is slice NT-N1: the [`Notifications`] module with the full
//! `[modules.notifications]` parsing and validation ([`config`]), the
//! [`topics`], the [`templates`] registry with its filters and startup
//! checks, the preview routes, the capture [`drivers`] and the core tables of
//! migration 1 ([`MIGRATIONS`]). `merezha notifications check` runs the same
//! checks from the CLI.
//!
//! Seams for the next slices: the `Notifier` implementation and expansion
//! (NT-N2) bind through [`Notifications::checked`] and replace the kernel's
//! capture fallback in the `AppContext` builder; delivery workers (NT-N2 to
//! NT-N4) render with [`templates::TemplateRegistry::render`] and send
//! through [`drivers::EmailDriver`] and [`drivers::SmsDriver`]. The module
//! already claims the `api` source its HTTP send route writes as.
pub mod config;
pub mod drivers;
mod routes;
pub mod templates;
pub mod topics;

pub use config::{HostFacts, Settings};
pub use routes::{PREVIEW_CSP, PREVIEW_PERMISSION, loopback_host_header};
pub use templates::TemplateRegistry;

use merezha_platform::{
    AppContext, ErrorCode, Migration, Module, ModuleEvents, ModuleSection, config::ConfigIssue,
};
use std::sync::{Arc, OnceLock};

/// The module name: section `[modules.notifications]`, routes under
/// `/v1/notifications`, schema `merezha_notify`.
pub const NAME: &str = "notifications";
/// The PostgreSQL schema the block owns.
pub const SCHEMA: &str = "merezha_notify";

/// Migration 1, the core tables (NT §2.4). Later slices append their own
/// migrations and never edit this one.
pub const MIGRATIONS: [Migration; 1] = [Migration::new(
    1,
    "core",
    include_str!("../migrations/0001_core.sql"),
)];

/// The codes this slice's routes return, with their statuses (R8). They are
/// canonical kernel codes, registered again with the same status.
pub const ERRORS: [ErrorCode; 11] = [
    ErrorCode::new("invalid_input", 400),
    ErrorCode::new("unauthenticated", 401),
    ErrorCode::new("unauthorized", 403),
    ErrorCode::new("not_found", 404),
    ErrorCode::new("validation_failed", 422),
    ErrorCode::new("invalid_locale", 422),
    ErrorCode::new("unknown_template", 422),
    ErrorCode::new("channel_not_allowed", 422),
    ErrorCode::new("reserved_data_key", 422),
    ErrorCode::new("data_too_large", 422),
    ErrorCode::new("render_failed", 422),
];

/// Kernel sources the block writes as besides its name: `api`, for the
/// HTTP send route (NT §3.1, R4a).
pub const EXTRA_SOURCES: [&str; 1] = ["api"];

/// A validated configuration: the settings and the compiled templates.
#[derive(Debug)]
pub struct Checked {
    pub settings: Arc<Settings>,
    pub registry: Arc<TemplateRegistry>,
}

/// Every startup check of NT §4 and §7.2 on one section, all problems at
/// once: the settings first, then the templates against them.
pub fn check(section: &ModuleSection, facts: &HostFacts) -> Result<Checked, Vec<ConfigIssue>> {
    let (settings, mut issues) = config::parse_all(section, facts);
    match TemplateRegistry::load(&settings) {
        Ok(registry) if issues.is_empty() => Ok(Checked {
            settings: Arc::new(settings),
            registry: Arc::new(registry),
        }),
        Ok(_) => Err(issues),
        Err(found) => {
            issues.extend(found);
            Err(issues)
        }
    }
}

/// The notifications block as a host [`Module`].
#[derive(Debug, Default)]
pub struct Notifications {
    facts: HostFacts,
    checked: Arc<OnceLock<Arc<Checked>>>,
}
impl Notifications {
    /// The block with the host facts its cross-section checks need; see
    /// [`HostFacts::load`].
    pub fn new(facts: HostFacts) -> Self {
        Self {
            facts,
            checked: Arc::new(OnceLock::new()),
        }
    }
    /// The validated configuration, once [`Module::configure`] succeeded.
    pub fn checked(&self) -> Option<Arc<Checked>> {
        self.checked.get().cloned()
    }
    /// A handle that sees the configuration after the host took the module.
    pub fn handle(&self) -> NotificationsHandle {
        NotificationsHandle(self.checked.clone())
    }
}
/// See [`Notifications::handle`].
#[derive(Clone, Debug)]
pub struct NotificationsHandle(Arc<OnceLock<Arc<Checked>>>);
impl NotificationsHandle {
    pub fn checked(&self) -> Option<Arc<Checked>> {
        self.0.get().cloned()
    }
}

impl Module for Notifications {
    fn name(&self) -> &'static str {
        NAME
    }
    fn configure(&mut self, section: &ModuleSection) -> Result<(), Vec<ConfigIssue>> {
        let checked = check(section, &self.facts)?;
        // A module is configured once; a second call keeps the first result.
        let _ = self.checked.set(Arc::new(checked));
        Ok(())
    }
    fn migrations(&self) -> &[Migration] {
        &MIGRATIONS
    }
    fn errors(&self) -> &[ErrorCode] {
        &ERRORS
    }
    fn extra_sources(&self) -> &[&'static str] {
        &EXTRA_SOURCES
    }
    fn events(&self) -> ModuleEvents {
        ModuleEvents::default()
    }
    fn routes(&self, ctx: &AppContext) -> axum::Router {
        let Some(checked) = self.checked() else {
            return axum::Router::new();
        };
        let captures = checked.settings.capture_active().then(|| {
            drivers::CaptureDriver::new(ctx.database().clone(), checked.settings.retention.captures)
        });
        routes::router(routes::RouteState {
            settings: checked.settings.clone(),
            registry: checked.registry.clone(),
            authorizer: ctx.authorizer().clone(),
            captures,
        })
    }
}
