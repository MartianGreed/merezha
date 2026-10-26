//! The application on PostgreSQL as a host-provided module of
//! `merezha run`: the host applies or verifies its `merezha_app`
//! migrations with every other module's under the ledger key
//! `application`, starts it once the pool is proven, nests its routes under
//! `/v1/application` and includes it in readiness and shutdown.
//!
//! `application` is a reserved name, so no block can register it; only
//! `run` adds this module, after module configuration. Its background
//! [`Scheduler`] starts in [`Module::start`] and stops in
//! [`Module::shutdown`]; [`Module::ready`] holds readiness until this
//! instance's projection revisions are warm (`warm_readiness`, R10).
//!
//! Reactors (AP §C.5) write through [`PlatformEffects`], the module's
//! notifier, outbox and directory bound to the source `application`; when
//! a reactor may request commands, the module registers the
//! `application.command` job handler before the jobs runtime starts.
use crate::reactions::{CommandEffects, PlatformEffects};
use merezha_application::{
    Application, Guest,
    config::{Config, EffectKind},
    postgres::{
        COMMAND_KIND, COMMAND_QUEUE, MIGRATIONS, MODULE, PostgresOptions, PostgresStore, Scheduler,
    },
};
use merezha_domain::Failure;
use merezha_platform::{
    AppContext, BoxFuture, Migration, Module,
    ports::{Authorizer, KindOptions},
};
use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

/// Time [`Module::start`] gives the store's connect: within the host's
/// module start bound (10 s).
const CONNECT_BUDGET: Duration = Duration::from_secs(8);

/// The statements [`ApplicationModule`] runs: the application store's.
const STATEMENTS: &[&[merezha_postgres::Statement]] = &[merezha_application::postgres::STATEMENTS];

pub struct ApplicationModule {
    config: Config,
    /// How often the lag gauges are sampled while metrics are exported.
    sample_interval: Duration,
    sampler: OnceLock<tokio::task::AbortHandle>,
    guest: Arc<dyn Guest>,
    policy: Arc<dyn Authorizer>,
    options: PostgresOptions,
    /// Connections the jobs runtime needs from the pool (AP §B.10).
    jobs_connections: usize,
    application: Arc<OnceLock<Application>>,
    scheduler: OnceLock<Scheduler>,
    failure: Arc<OnceLock<Failure>>,
}
impl ApplicationModule {
    /// `policy` decides route permissions and projection controls, as it
    /// does for memory and SQLite applications.
    pub fn new(
        config: Config,
        guest: Arc<dyn Guest>,
        policy: Arc<dyn Authorizer>,
        options: PostgresOptions,
        jobs_connections: usize,
    ) -> Self {
        Self {
            config,
            sample_interval: Duration::from_secs(15),
            sampler: OnceLock::new(),
            guest,
            policy,
            options,
            jobs_connections,
            application: Arc::new(OnceLock::new()),
            scheduler: OnceLock::new(),
            failure: Arc::new(OnceLock::new()),
        }
    }
    /// Sample the projection lag every `interval` while metrics are
    /// exported (`telemetry.metrics.sample_interval_ms`, 15 s by default).
    pub fn sample_interval(mut self, interval: Duration) -> Self {
        self.sample_interval = interval;
        self
    }
    /// Why [`Module::start`] failed, such as `storage_configuration` for
    /// another application's database. The host only reports
    /// `module_start_failed`.
    pub fn failure(&self) -> Arc<OnceLock<Failure>> {
        self.failure.clone()
    }
    /// The application once [`Module::start`] opened it, for the
    /// `application` section of `/v1/admin/status`.
    pub fn application(&self) -> Arc<OnceLock<Application>> {
        self.application.clone()
    }
    /// The pool-size guidance of AP §A.8: HTTP admission, background runs,
    /// the jobs runtime and two spare connections. Below it requests wait
    /// for connections, so startup warns (`db.pool_undersized`).
    fn check_pool(&self, ctx: &AppContext) {
        let pool_size = ctx.database().status().max_size;
        let recommended = self.config.limits.max_concurrent
            + self.options.background.max_concurrent
            + self.jobs_connections
            + 2;
        if pool_size < recommended {
            merezha_telemetry::event!(WARN, "db.pool_undersized", pool_size, recommended);
        }
    }
    /// Whether a configured reactor may request command effects.
    fn commands(&self) -> bool {
        self.config
            .reactors
            .iter()
            .flatten()
            .any(|r| r.effects.contains(&EffectKind::Command))
    }
    async fn open(&self, ctx: &AppContext) -> Result<Application, Failure> {
        let deadline = Instant::now() + CONNECT_BUDGET;
        let mut options = self.options.clone();
        if self.config.reactors.as_ref().is_some_and(|r| !r.is_empty()) {
            options.effects = Some(Arc::new(PlatformEffects::new(ctx)?));
        }
        let store = PostgresStore::connect(ctx.database(), &self.config, options, deadline).await?;
        // Route permissions are enforced through the HTTP adapter's caller
        // and `policy` before any guest call (AP §C.4); the payload hook
        // adds nothing on the platform host.
        let policy: Arc<dyn merezha_application::Authorizer> =
            Arc::new(merezha_application::AllowAll);
        // Construction asks every version 2 guest `describe`: guest calls
        // run on a blocking thread.
        let (config, guest) = (self.config.clone(), self.guest.clone());
        let meters = ctx.telemetry().clone();
        merezha_telemetry::spawn_blocking_in_span(move || {
            Application::with_postgres(config, guest, policy, Arc::new(store))
                .map(|application| application.with_meters(&meters))
        })
        .await
        .map_err(|_| Failure::transient("internal"))?
    }
}
impl Module for ApplicationModule {
    fn name(&self) -> &'static str {
        MODULE
    }
    fn migrations(&self) -> &[Migration] {
        MIGRATIONS
    }
    fn statements(&self) -> &[&'static [merezha_postgres::Statement]] {
        STATEMENTS
    }
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async move {
            // Command effects run as jobs; the handler must be registered
            // before the jobs runtime starts, after every module.
            if self.commands() {
                let handler =
                    CommandEffects::new(self.application.clone(), self.policy.allows_all());
                let registered = ctx.jobs(MODULE).and_then(|jobs| {
                    jobs.register(
                        COMMAND_KIND,
                        COMMAND_QUEUE,
                        Arc::new(handler),
                        KindOptions::default(),
                    )
                });
                if let Err(failure) = registered {
                    let _ = self.failure.set(failure.clone());
                    return Err(failure);
                }
            }
            let opened = self.open(&ctx).await;
            let started = opened.and_then(|application| {
                let scheduler = Scheduler::start(&application)?;
                Ok((application, scheduler))
            });
            match started {
                Ok((application, scheduler)) => {
                    self.check_pool(&ctx);
                    if ctx.telemetry().enabled()
                        && let Ok(sampler) = application.spawn_lag_sampler(self.sample_interval)
                    {
                        let _ = self.sampler.set(sampler);
                    }
                    let _ = self.application.set(application);
                    let _ = self.scheduler.set(scheduler);
                    Ok(())
                }
                Err(failure) => {
                    let _ = self.failure.set(failure.clone());
                    Err(failure)
                }
            }
        })
    }
    fn routes(&self, _: &AppContext) -> axum::Router {
        match self.application.get() {
            Some(application) => {
                crate::application_http::postgres_routes(application.clone(), self.policy.clone())
            }
            None => axum::Router::new(),
        }
    }
    /// Not ready while a configured projection revision warms up.
    fn ready(&self) -> BoxFuture<'_, Result<(), Failure>> {
        let ready = self.scheduler.get().is_some_and(Scheduler::ready);
        Box::pin(async move {
            if ready {
                Ok(())
            } else {
                Err(Failure::transient("projection_warming"))
            }
        })
    }
    /// Stop the runners: in-flight run steps are cancelled, so their lock
    /// sessions close.
    fn shutdown(&self, deadline: Instant) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if let Some(sampler) = self.sampler.get() {
                sampler.abort();
            }
            if let Some(scheduler) = self.scheduler.get() {
                scheduler.shutdown(deadline).await;
            }
        })
    }
}
