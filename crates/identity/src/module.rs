//! The identity block as a platform [`Module`].
use crate::{
    catalog::Catalog,
    directory::IdentityDirectory,
    rbac::{GrantCache, Once, Policy, RbacAuthorizer},
};
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
use merezha_platform::ports::{Authorizer, Directory};
use merezha_platform::{
    AppContext, BoxFuture, ErrorCode, Migration, Module, ModuleEvents, ModuleSection,
    config::ConfigIssue,
    ports::{Authenticator, KindOptions, ScheduleDeclaration},
};
use std::{
    collections::BTreeSet,
    sync::{Arc, OnceLock},
};

/// Migrations 1 to 3 (section 3), ledger key `identity`.
pub const MIGRATIONS: &[Migration] = &[
    Migration::new(
        1,
        "identity_core",
        include_str!("../migrations/0001_identity_core.sql"),
    ),
    Migration::new(
        2,
        "organizations_rbac_api_keys",
        include_str!("../migrations/0002_organizations_rbac_api_keys.sql"),
    ),
    Migration::new(
        3,
        "oauth_passkeys",
        include_str!("../migrations/0003_oauth_passkeys.sql"),
    ),
];

/// The codes identity returns, with their statuses (section 4, R8). The
/// canonical kernel codes are the kernel's.
pub const ERRORS: &[ErrorCode] = &[
    ErrorCode::new("ambiguous_credentials", 400),
    ErrorCode::new("invalid_credentials", 401),
    ErrorCode::new("invalid_token", 401),
    ErrorCode::new("invalid_ceremony", 401),
    ErrorCode::new("reauthentication_required", 401),
    ErrorCode::new("sign_in_required", 401),
    ErrorCode::new("origin_rejected", 403),
    ErrorCode::new("csrf_failed", 403),
    ErrorCode::new("email_unverified", 403),
    ErrorCode::new("account_disabled", 403),
    ErrorCode::new("registration_closed", 403),
    ErrorCode::new("permission_escalation", 403),
    ErrorCode::new("email_mismatch", 403),
    ErrorCode::new("builtin_role", 403),
    ErrorCode::new("email_taken", 409),
    ErrorCode::new("sole_owner", 409),
    ErrorCode::new("slug_taken", 409),
    ErrorCode::new("slug_reserved", 409),
    ErrorCode::new("role_key_taken", 409),
    ErrorCode::new("role_in_use", 409),
    ErrorCode::new("already_member", 409),
    ErrorCode::new("invitation_stale", 409),
    ErrorCode::new("last_credential", 409),
    ErrorCode::new("identity_already_linked", 409),
    ErrorCode::new("passkey_exists", 409),
    ErrorCode::new("unknown_permission", 422),
    ErrorCode::new("unknown_resource_kind", 422),
    ErrorCode::new("resource_kind_tenant_only", 422),
    ErrorCode::new("organization_limit", 422),
    ErrorCode::new("role_limit", 422),
    ErrorCode::new("assignment_limit", 422),
    ErrorCode::new("service_account_limit", 422),
    ErrorCode::new("api_key_limit", 422),
    ErrorCode::new("passkey_limit", 422),
    ErrorCode::new("passkey_rejected", 422),
    ErrorCode::new("passkeys_config", 422),
    ErrorCode::new("declared_role_in_use", 422),
    ErrorCode::new("password_too_short", 422),
    ErrorCode::new("password_too_long", 422),
    ErrorCode::new("password_blocklisted", 422),
    ErrorCode::new("password_contextual", 422),
    ErrorCode::new("notifications_required", 422),
    ErrorCode::new("blocklist_unavailable", 422),
    ErrorCode::new("provider_unavailable", 503),
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
    /// The RBAC `Authorizer` to install in the host (`HostParts::authorizer`).
    pub fn authorizer(&self) -> Arc<dyn Authorizer> {
        Arc::new(RbacAuthorizer::new(self.core.clone()))
    }
    /// The `Directory` to install in the host (`HostOptions::directory`).
    /// It answers `unavailable` until the block started.
    pub fn directory(&self) -> Arc<dyn Directory> {
        Arc::new(IdentityDirectory::new(self.core.clone()))
    }
    /// `merezha identity platform-admin grant|revoke <email>` on a started
    /// block: `Ok((change, user id))`; `not_found` for an unknown address,
    /// `email_unverified` to grant an unverified or disabled account.
    pub async fn platform_admin(
        &self,
        email: &crate::normalize::Email,
        grant: bool,
    ) -> Result<(crate::appscope::AdminChange, uuid::Uuid), Failure> {
        let core = self
            .core
            .get()
            .ok_or_else(|| Failure::transient("unavailable"))?;
        core.platform_admin(email, grant)
            .await
            .map_err(|e| e.failure)
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
    catalog: Catalog,
    resource_kinds: BTreeSet<String>,
    endpoints: std::collections::BTreeMap<String, crate::oauth::ProviderEndpoints>,
    tenant_resource_kinds: BTreeSet<String>,
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
            catalog: Catalog::builtin(),
            resource_kinds: BTreeSet::new(),
            endpoints: std::collections::BTreeMap::new(),
            tenant_resource_kinds: BTreeSet::new(),
            settings: None,
            metrics: Arc::new(OnceLock::new()),
            core: Arc::new(OnceLock::new()),
            counts: Arc::new(OnceLock::new()),
        }
    }
    /// Add a block's permissions to the catalog (section 5), such as
    /// `("content", &merezha_content::CONTENT_PERMISSIONS)`. Each must start
    /// with `<block>.`; fails with the first that does not.
    pub fn add_block_permissions(
        &mut self,
        block: &'static str,
        names: &[&str],
    ) -> Result<(), String> {
        self.catalog.add_block(block, names)
    }
    /// Add the application's `[[permissions]]` to the catalog (AP-C2 reads
    /// them from `application.toml`). Fails with `invalid_permission` or
    /// `block_prefix` and the offending name.
    pub fn add_application_permissions<'a>(
        &mut self,
        permissions: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<(), (&'static str, String)> {
        for (name, description) in permissions {
            self.catalog
                .add_application(name, description)
                .map_err(|code| (code, name.to_owned()))?;
        }
        Ok(())
    }
    /// Resource kinds role assignments may target: those a block declares
    /// (content's `content_type`) and those routes name in `resource`.
    pub fn add_resource_kinds(&mut self, kinds: &[&str]) {
        self.resource_kinds
            .extend(kinds.iter().map(|k| (*k).to_owned()));
    }
    /// Replace provider `name`'s built-in endpoints: a Google issuer, or
    /// GitHub's authorization, token and API bases (GitHub Enterprise
    /// Server, and the mock providers of tests). Code only, never request
    /// input or configuration.
    pub fn override_provider_endpoints(
        &mut self,
        name: &str,
        endpoints: crate::oauth::ProviderEndpoints,
    ) {
        self.endpoints.insert(name.to_owned(), endpoints);
    }
    /// [`IdentityModule::add_resource_kinds`] for kinds whose resources
    /// always belong to a tenant, such as the application's route
    /// `resource` kinds (AP-C2, checked only at tenant scope): tenant role
    /// assignments may target them, application-level ones fail
    /// `resource_kind_tenant_only`, since they could never take effect.
    pub fn add_tenant_resource_kinds(&mut self, kinds: &[&str]) {
        self.add_resource_kinds(kinds);
        self.tenant_resource_kinds
            .extend(kinds.iter().map(|k| (*k).to_owned()));
    }
    /// The permission catalog this module checks grants against.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
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
    /// Open the block for an administration command (`merezha identity
    /// platform-admin`) on `ctx` after [`Module::configure`]: no listener,
    /// no jobs, no password hasher, no notification delivery path and no
    /// declared role sync, so a command run with another configuration
    /// never rewrites roles. Only the handle's administration calls work.
    pub async fn open_admin(&self, ctx: AppContext) -> Result<(), Failure> {
        let settings = self
            .settings
            .clone()
            .ok_or_else(|| Failure::permanent("invalid_config"))?;
        let keys = Keys::derive(
            settings.secret_key.expose().as_bytes(),
            settings
                .secret_key_previous
                .as_ref()
                .map(|s| s.expose().as_bytes()),
        );
        let core = self.core_with(ctx, settings, keys, None, None)?;
        self.core
            .set(Arc::new(core))
            .map_err(|_| Failure::permanent("module_already_started"))
    }
    fn core_with(
        &self,
        ctx: AppContext,
        settings: Settings,
        keys: Keys,
        hasher: Option<Hasher>,
        policy: Option<PasswordPolicy>,
    ) -> Result<Core, Failure> {
        let rbac = Policy::new(self.catalog.clone(), &settings.rbac, 1);
        Ok(Core {
            notifier: ctx.notifier(NAME)?,
            outbox: ctx.outbox(NAME)?,
            keys,
            hasher,
            policy,
            metrics: self.metrics(&ctx),
            grants: GrantCache::new(settings.rbac.cache_entries),
            audited: Once::default(),
            audit_slots: Arc::new(tokio::sync::Semaphore::new(crate::core::AUDIT_SLOTS)),
            break_glass: Once::default(),
            break_glass_pending: Once::default(),
            break_glass_slots: Arc::new(tokio::sync::Semaphore::new(
                crate::core::BREAK_GLASS_SLOTS,
            )),
            resource_kinds: self.resource_kinds.clone(),
            oauth: crate::oauth::Providers::new(&settings.oauth.providers, &self.endpoints),
            #[cfg(feature = "passkeys")]
            webauthn: match &settings.passkeys {
                Some(passkeys) => Some(Arc::new(crate::passkeys::relying_party(passkeys)?)),
                None => None,
            },
            tenant_resource_kinds: self.tenant_resource_kinds.clone(),
            rbac,
            settings,
            ctx,
        })
    }
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
        self.settings = Some(Settings::parse(
            section,
            self.facts.as_ref(),
            &self.catalog,
        )?);
        Ok(())
    }
    fn migrations(&self) -> &[Migration] {
        MIGRATIONS
    }
    fn errors(&self) -> &[ErrorCode] {
        ERRORS
    }
    fn body_limits(&self) -> Vec<merezha_platform::RouteBodyLimit> {
        #[cfg(feature = "passkeys")]
        if self.settings.as_ref().is_some_and(|s| s.passkeys.is_some()) {
            return crate::passkeys::routes::body_limits();
        }
        Vec::new()
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
            let core = self.core_with(ctx, settings, keys, hasher, policy)?;
            core.sync_declared_roles().await?;
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
