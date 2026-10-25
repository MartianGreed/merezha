//! The notifications block (`docs/design/notifications.md`): email and SMS
//! through Brevo, Web Push, an in-app inbox with SSE and signed webhooks.
//!
//! Slice NT-N1 brought the [`Notifications`] module with the full
//! `[modules.notifications]` parsing and validation ([`config`]), the
//! [`topics`], the [`templates`] registry, the preview routes and the
//! capture [`drivers`]. Slice NT-N2 adds the `Notifier` port
//! ([`notifier`]: `validate`, `request`, `emit_webhook`, provided to every
//! context through [`Module::notifier`]), the expansion job, the email and
//! SMS delivery worker over the capture drivers, preferences, suppressions
//! and signed unsubscribe ([`keys`]), the inbox REST routes, `send`,
//! `status` and `redeliver`, and the `settle_notifications`, `purge_data`
//! and `inbox_expire` tasks, with migration 2 ([`MIGRATIONS`]).
//!
//! Seams for the next slices: the Brevo drivers (NT-N3, NT-N4) implement
//! [`drivers::EmailDriver`] and [`drivers::SmsDriver`] and are installed in
//! [`notifier::Core`]; until then a `brevo` driver leaves its delivery jobs
//! queued and unleased. The SSE stream (NT-N5) listens on
//! [`sql::INBOX_CHANNEL`], which every inbox change notifies. Push (NT-N6)
//! replaces the `no_subscription` skip of push targets; webhook fan-out
//! (NT-N7) handles the `notify.webhook_event` jobs `emit_webhook` queues.
pub mod config;
pub mod deliver;
pub mod drivers;
pub mod expand;
pub mod faults;
pub mod handler;
mod http;
pub mod ids;
mod inbox;
pub mod keys;
pub mod maintenance;
pub mod metrics;
pub mod notifier;
pub mod payload;
pub mod policy;
mod routes;
pub mod sql;
pub mod templates;
pub mod topics;

pub use config::{HostFacts, Settings};
pub use http::{ADMIN_PERMISSION, SEND_PERMISSION, STATUS_PERMISSION, SUPPRESSIONS_PERMISSION};
pub use notifier::{BlockNotifiers, BoundNotifier};
pub use routes::{PREVIEW_CSP, PREVIEW_PERMISSION, loopback_host_header};
pub use templates::TemplateRegistry;

use merezha_domain::{Failure, Identity};
use merezha_platform::{
    AppContext, BoxFuture, ErrorCode, Migration, Module, ModuleEvents, ModuleSection,
    config::ConfigIssue,
    ports::{Channel, KindOptions, NotifierFactory, ScheduleDeclaration, Source},
};
use std::sync::{Arc, OnceLock};

/// The module name: section `[modules.notifications]`, routes under
/// `/v1/notifications`, schema `merezha_notify`.
pub const NAME: &str = "notifications";
/// The PostgreSQL schema the block owns.
pub const SCHEMA: &str = "merezha_notify";
/// The instrumentation scope of the block's instruments (TE §1.3).
pub const METER_SCOPE: &str = "merezha.notifications";

/// Migration 1, the core tables, and migration 2, the inbox (NT §2.4).
/// Later slices append their own migrations and never edit these.
pub const MIGRATIONS: [Migration; 2] = [
    Migration::new(1, "core", include_str!("../migrations/0001_core.sql")),
    Migration::new(2, "inbox", include_str!("../migrations/0002_inbox.sql")),
];

/// The permission catalog of NT §7.1 this block checks so far.
pub const PERMISSIONS: [&str; 5] = [
    PREVIEW_PERMISSION,
    SEND_PERMISSION,
    STATUS_PERMISSION,
    ADMIN_PERMISSION,
    SUPPRESSIONS_PERMISSION,
];

/// The codes this block's routes and port return, with their statuses
/// (R8). Canonical kernel codes are listed again with the same status.
pub const ERRORS: [ErrorCode; 30] = [
    ErrorCode::new("invalid_input", 400),
    ErrorCode::new("invalid_cursor", 400),
    ErrorCode::new("token_invalid", 400),
    ErrorCode::new("unauthenticated", 401),
    ErrorCode::new("unauthorized", 403),
    ErrorCode::new("not_found", 404),
    ErrorCode::new("not_redeliverable", 409),
    ErrorCode::new("secret_purged", 409),
    ErrorCode::new("data_purged", 410),
    ErrorCode::new("validation_failed", 422),
    ErrorCode::new("invalid_locale", 422),
    ErrorCode::new("unknown_topic", 422),
    ErrorCode::new("unknown_template", 422),
    ErrorCode::new("channel_not_allowed", 422),
    ErrorCode::new("too_many_recipients", 422),
    ErrorCode::new("invalid_recipient", 422),
    ErrorCode::new("recipient_not_in_tenant", 422),
    ErrorCode::new("reserved_data_key", 422),
    ErrorCode::new("data_too_large", 422),
    ErrorCode::new("render_failed", 422),
    ErrorCode::new("unknown_event_type", 422),
    ErrorCode::new("webhooks_disabled", 422),
    ErrorCode::new("invalid_dedupe_key", 422),
    ErrorCode::new("topic_not_configurable", 422),
    ErrorCode::new("invalid_timezone", 422),
    ErrorCode::new("unavailable", 503),
    ErrorCode::new("database_unavailable", 503),
    ErrorCode::new("database_timeout", 503),
    ErrorCode::new("commit_outcome_unknown", 503),
    ErrorCode::new("overloaded", 503),
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
impl Checked {
    /// Startup signals that are not errors, as `(key, code)`: with
    /// `email.driver = "disabled"` the built-in security mail (identity's
    /// token and notice mail) is recorded as `skipped` / `channel_disabled`
    /// and never sent (`security_mail_disabled`).
    pub fn warnings(&self) -> Vec<(String, &'static str)> {
        let settings = &self.settings;
        let mut warnings = Vec::new();
        if !settings.channel_available(Channel::Email)
            && settings.topics.values().any(|t| {
                t.category == topics::Category::Security && t.channels.contains(&Channel::Email)
            })
        {
            warnings.push((
                "modules.notifications.email.driver".to_owned(),
                "security_mail_disabled",
            ));
        }
        warnings
    }
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
    core: notifier::Slot,
    faults: faults::Faults,
}
impl Notifications {
    /// The block with the host facts its cross-section checks need; see
    /// [`HostFacts::load`].
    pub fn new(facts: HostFacts) -> Self {
        Self {
            facts,
            ..Self::default()
        }
    }
    /// The block with armed crash points (feature `testkit`).
    #[cfg(feature = "testkit")]
    pub fn with_faults(mut self, faults: faults::Faults) -> Self {
        self.faults = faults;
        self
    }
    /// The validated configuration, once [`Module::configure`] succeeded.
    pub fn checked(&self) -> Option<Arc<Checked>> {
        self.checked.get().cloned()
    }
    /// A handle that sees the configuration after the host took the module.
    pub fn handle(&self) -> NotificationsHandle {
        NotificationsHandle(self.checked.clone())
    }
    /// The shared core, once [`Module::start`] ran: what the handlers and
    /// routes use, for embedding and tests.
    pub fn core(&self) -> Option<Arc<notifier::Core>> {
        self.core.get().cloned()
    }
    /// The block's notifier factory, the one [`Module::notifier`] provides.
    pub fn notifiers(&self) -> BlockNotifiers {
        BlockNotifiers::new(self.core.clone(), self.checked.clone())
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

/// The job kind of each delivering channel whose driver this slice has.
fn delivering(settings: &Settings) -> Vec<Channel> {
    let mut channels = Vec::new();
    if settings.email.driver == config::Driver::Capture {
        channels.push(Channel::Email);
    }
    if settings.sms.driver == config::Driver::Capture {
        channels.push(Channel::Sms);
    }
    channels
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
    fn notifier(&self) -> Option<Arc<dyn NotifierFactory>> {
        self.checked()
            .map(|_| Arc::new(self.notifiers()) as Arc<dyn NotifierFactory>)
    }
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async move {
            let checked = self
                .checked()
                .ok_or_else(|| Failure::permanent("invalid_config"))?;
            let settings = checked.settings.clone();
            let capture =
                || drivers::CaptureDriver::new(ctx.database().clone(), settings.retention.captures);
            let email: Option<Arc<dyn drivers::EmailDriver>> = (settings.email.driver
                == config::Driver::Capture)
                .then(|| Arc::new(capture()) as _);
            let sms: Option<Arc<dyn drivers::SmsDriver>> =
                (settings.sms.driver == config::Driver::Capture).then(|| Arc::new(capture()) as _);
            let core = notifier::Core {
                settings: settings.clone(),
                registry: checked.registry.clone(),
                keys: keys::Keys::new(&settings.secret_key, settings.secret_key_previous.as_ref()),
                database: ctx.database().clone(),
                outbox: ctx.outbox(NAME)?,
                directory: ctx.directory().clone(),
                clock: clock(&ctx),
                random: random(&ctx),
                metrics: metrics::Metrics::new(&ctx.telemetry().meter(METER_SCOPE)),
                faults: self.faults.clone(),
                email,
                sms,
            };
            let jobs = ctx.jobs(NAME)?;
            jobs.register(
                notifier::EXPAND_KIND,
                notifier::EXPAND_QUEUE,
                Arc::new(handler::Expand(self.core.clone())),
                KindOptions::default(),
            )?;
            for channel in delivering(&settings) {
                if let Some((queue, kind)) = expand::delivery_queue(channel) {
                    jobs.register(
                        kind,
                        queue,
                        Arc::new(handler::Deliver(self.core.clone(), channel)),
                        KindOptions::default(),
                    )?;
                }
            }
            for task in maintenance::Task::ALL {
                let kind = task.kind();
                jobs.register(
                    &kind,
                    maintenance::QUEUE,
                    Arc::new(handler::Maintenance(self.core.clone(), task)),
                    KindOptions::default(),
                )?;
                jobs.schedule(ScheduleDeclaration {
                    name: kind.clone(),
                    cron: task.cron().to_owned(),
                    timezone: "UTC".to_owned(),
                    queue: Identity::new(maintenance::QUEUE)?,
                    kind: Identity::new(kind)?,
                    payload: serde_json::json!({}),
                })?;
            }
            let _ = self.core.set(Arc::new(core));
            Ok(())
        })
    }
    fn routes(&self, ctx: &AppContext) -> axum::Router {
        let Some(checked) = self.checked() else {
            return axum::Router::new();
        };
        let captures = checked.settings.capture_active().then(|| {
            drivers::CaptureDriver::new(ctx.database().clone(), checked.settings.retention.captures)
        });
        let previews = routes::router(routes::RouteState {
            settings: checked.settings.clone(),
            registry: checked.registry.clone(),
            authorizer: ctx.authorizer().clone(),
            captures,
        });
        let api = self.notifiers().notifier(
            Source::new(EXTRA_SOURCES[0]).unwrap_or_else(|_| unreachable!("`api` is a source")),
        );
        previews.merge(http::router(http::Ctx {
            slot: self.core.clone(),
            checked,
            authorizer: ctx.authorizer().clone(),
            api,
            directory: ctx.directory().clone(),
        }))
    }
}

/// The context's clock and randomness as shared handles.
struct ContextClock(AppContext);
impl merezha_platform::Clock for ContextClock {
    fn now(&self) -> std::time::SystemTime {
        self.0.clock().now()
    }
    fn monotonic(&self) -> std::time::Instant {
        self.0.clock().monotonic()
    }
}
struct ContextRandom(AppContext);
impl merezha_platform::Random for ContextRandom {
    fn fill(&self, bytes: &mut [u8]) -> Result<(), Failure> {
        self.0.random().fill(bytes)
    }
}
fn clock(ctx: &AppContext) -> Arc<dyn merezha_platform::Clock> {
    Arc::new(ContextClock(ctx.clone()))
}
fn random(ctx: &AppContext) -> Arc<dyn merezha_platform::Random> {
    Arc::new(ContextRandom(ctx.clone()))
}
