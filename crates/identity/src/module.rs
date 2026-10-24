//! The identity block as a platform [`Module`].
use crate::{
    cleanup::{self, CleanupJob},
    config::{HostFacts, Settings},
    core::{Core, NAME, event},
    hasher::{HashCounts, Hasher},
    metrics::Metrics,
    password::PasswordPolicy,
    secrets::Keys,
    session::SessionAuthenticator,
};
use merezha_domain::Failure;
use merezha_platform::{
    AppContext, BoxFuture, ErrorCode, Migration, Module, ModuleEvents, ModuleSection,
    config::ConfigIssue,
    ports::{Authenticator, KindOptions, ScheduleDeclaration},
};
use std::sync::{Arc, OnceLock};

/// Migration 1 (section 3), ledger key `identity`.
pub const MIGRATIONS: &[Migration] = &[Migration::new(
    1,
    "identity_core",
    include_str!("../migrations/0001_identity_core.sql"),
)];

/// The codes identity returns, with their statuses (section 4, R8). The
/// canonical kernel codes are the kernel's.
pub const ERRORS: &[ErrorCode] = &[
    ErrorCode::new("ambiguous_credentials", 400),
    ErrorCode::new("invalid_credentials", 401),
    ErrorCode::new("invalid_token", 401),
    ErrorCode::new("reauthentication_required", 401),
    ErrorCode::new("sign_in_required", 401),
    ErrorCode::new("origin_rejected", 403),
    ErrorCode::new("csrf_failed", 403),
    ErrorCode::new("email_unverified", 403),
    ErrorCode::new("account_disabled", 403),
    ErrorCode::new("registration_closed", 403),
    ErrorCode::new("email_taken", 409),
    ErrorCode::new("sole_owner", 409),
    ErrorCode::new("password_too_short", 422),
    ErrorCode::new("password_too_long", 422),
    ErrorCode::new("password_blocklisted", 422),
    ErrorCode::new("password_contextual", 422),
    ErrorCode::new("notifications_required", 422),
    ErrorCode::new("blocklist_unavailable", 422),
];

/// What a started block exposes to embedding code and tests.
#[derive(Clone)]
pub struct IdentityHandle {
    core: Arc<OnceLock<Arc<Core>>>,
    counts: Arc<OnceLock<Arc<HashCounts>>>,
    metrics: Arc<OnceLock<Metrics>>,
}
impl std::fmt::Debug for IdentityHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityHandle")
            .field("started", &self.core.get().is_some())
            .finish()
    }
}
impl IdentityHandle {
    /// The session authenticator to install in the host
    /// (`HostOptions::authenticator`). It answers `unavailable` for
    /// credentials until the block started.
    pub fn authenticator(&self) -> Arc<dyn Authenticator> {
        Arc::new(SessionAuthenticator::new(self.core.clone()))
    }
    /// Record the identity metrics on `meter`; call before the host starts.
    /// The first meter set wins.
    pub fn set_meter(&self, meter: &opentelemetry::metrics::Meter) {
        let _ = self.metrics.set(Metrics::new(meter));
    }
    /// Argon2id hashes and verifications since start.
    pub fn hash_counts(&self) -> Option<(u64, u64)> {
        let counts = self.counts.get()?;
        Some((
            counts.hashes.load(std::sync::atomic::Ordering::SeqCst),
            counts
                .verifications
                .load(std::sync::atomic::Ordering::SeqCst),
        ))
    }
    /// One cleanup pass now, as the scheduled job runs it.
    pub async fn cleanup_now(&self) -> Result<cleanup::CleanupReport, Failure> {
        let core = self
            .core
            .get()
            .ok_or_else(|| Failure::transient("unavailable"))?;
        core.cleanup().await
    }
}

/// The identity block: `[modules.identity]`, schema `merezha_identity`,
/// routes `/v1/identity/` and the session [`Authenticator`].
pub struct IdentityModule {
    facts: Option<HostFacts>,
    settings: Option<Settings>,
    metrics: Arc<OnceLock<Metrics>>,
    core: Arc<OnceLock<Arc<Core>>>,
    counts: Arc<OnceLock<Arc<HashCounts>>>,
}
impl std::fmt::Debug for IdentityModule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityModule")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}
impl IdentityModule {
    /// A module for a host described by `facts` (`None` when the kernel
    /// configuration is invalid; startup then fails on the kernel's issues).
    pub fn new(facts: Option<HostFacts>) -> Self {
        Self {
            facts,
            settings: None,
            metrics: Arc::new(OnceLock::new()),
            core: Arc::new(OnceLock::new()),
            counts: Arc::new(OnceLock::new()),
        }
    }
    /// Record the identity metrics on `meter`.
    pub fn with_meter(self, meter: &opentelemetry::metrics::Meter) -> Self {
        let _ = self.metrics.set(Metrics::new(meter));
        self
    }
    /// The session authenticator to install in the host. It answers
    /// `unavailable` for credentials until the block started.
    pub fn authenticator(&self) -> Arc<dyn Authenticator> {
        Arc::new(SessionAuthenticator::new(self.core.clone()))
    }
    pub fn handle(&self) -> IdentityHandle {
        IdentityHandle {
            core: self.core.clone(),
            counts: self.counts.clone(),
            metrics: self.metrics.clone(),
        }
    }
    /// The validated settings, after [`Module::configure`].
    pub fn settings(&self) -> Option<&Settings> {
        self.settings.as_ref()
    }
}

impl IdentityModule {
    /// The meter set through the handle, else the host's
    /// (`AppContext::telemetry`, scope `merezha.identity`).
    fn metrics(&self, ctx: &AppContext) -> Metrics {
        self.metrics
            .get_or_init(|| Metrics::new(&ctx.telemetry().meter(METER_SCOPE)))
            .clone()
    }
}

/// The instrumentation scope of identity's instruments.
pub const METER_SCOPE: &str = "merezha.identity";

impl Module for IdentityModule {
    fn name(&self) -> &'static str {
        NAME
    }
    fn configure(&mut self, section: &ModuleSection) -> Result<(), Vec<ConfigIssue>> {
        self.settings = Some(Settings::parse(section, self.facts.as_ref())?);
        Ok(())
    }
    fn migrations(&self) -> &[Migration] {
        MIGRATIONS
    }
    fn errors(&self) -> &[ErrorCode] {
        ERRORS
    }
    fn events(&self) -> ModuleEvents {
        ModuleEvents {
            publishes: &event::ALL,
            consumes: &[],
        }
    }
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async move {
            let settings = self
                .settings
                .clone()
                .ok_or_else(|| Failure::permanent("invalid_config"))?;
            // Section 8: without a delivery path, verification and reset
            // mail would be dropped; only loopback development prints links.
            if !ctx.notifications_deliver() && !settings.dev_print_links {
                return Err(Failure::permanent("notifications_required"));
            }
            crate::blocklist::warm();
            if crate::blocklist::len() == 0 {
                return Err(Failure::permanent("blocklist_unavailable"));
            }
            let (hasher, policy) = match &settings.password {
                Some(password) => {
                    let password = password.clone();
                    let random = ctx.clone();
                    let metrics = self.metrics(&ctx);
                    let hasher = merezha_telemetry::spawn_blocking_in_span(move || {
                        Hasher::new(&password, random.random(), metrics)
                    })
                    .await
                    .map_err(|_| Failure::transient("internal"))??;
                    let mut policy = PasswordPolicy::new(
                        settings.password.as_ref().map_or(12, |p| p.min_length),
                        settings.password.as_ref().map_or(256, |p| p.max_length),
                        settings.app_name.as_deref(),
                    );
                    if let Some(extra) = settings
                        .password
                        .as_ref()
                        .and_then(|p| p.blocklist.as_deref())
                    {
                        policy = policy.with_extra(extra);
                    }
                    (Some(hasher), Some(policy))
                }
                None => (None, None),
            };
            if let Some(hasher) = &hasher {
                let _ = self.counts.set(hasher.counts().clone());
            }
            let keys = Keys::derive(
                settings.secret_key.expose().as_bytes(),
                settings
                    .secret_key_previous
                    .as_ref()
                    .map(|s| s.expose().as_bytes()),
            );
            let jobs = ctx.jobs(NAME)?;
            jobs.register(
                cleanup::KIND,
                cleanup::QUEUE,
                Arc::new(CleanupJob {
                    core: self.core.clone(),
                }),
                KindOptions::default(),
            )?;
            jobs.register(
                crate::links::KIND,
                cleanup::QUEUE,
                Arc::new(crate::links::LinkRequestJob {
                    core: self.core.clone(),
                }),
                KindOptions::default(),
            )?;
            jobs.schedule(ScheduleDeclaration {
                name: cleanup::KIND.to_owned(),
                cron: settings.cleanup_cron.clone(),
                timezone: "UTC".to_owned(),
                queue: merezha_domain::Identity::new(cleanup::QUEUE)?,
                kind: merezha_domain::Identity::new(cleanup::KIND)?,
                payload: serde_json::json!({}),
            })?;
            let core = Core {
                notifier: ctx.notifier(NAME)?,
                outbox: ctx.outbox(NAME)?,
                keys,
                hasher,
                policy,
                metrics: self.metrics(&ctx),
                settings,
                ctx,
            };
            self.core
                .set(Arc::new(core))
                .map_err(|_| Failure::permanent("module_already_started"))?;
            Ok(())
        })
    }
    fn routes(&self, _: &AppContext) -> axum::Router {
        match self.core.get() {
            Some(core) => crate::http::router(core.clone()),
            None => axum::Router::new(),
        }
    }
}
