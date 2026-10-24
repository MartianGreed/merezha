//! The application on PostgreSQL as a host-provided module of
//! `merezha run`: the host applies or verifies its `merezha_app`
//! migrations with every other module's under the ledger key
//! `application`, starts it once the pool is proven, nests its routes under
//! `/v1/application` and includes it in readiness and shutdown.
//!
//! `application` is a reserved name, so no block can register it; only
//! `run` adds this module, after module configuration. Background runners
//! (AP-A3) start and stop in [`Module::start`] and [`Module::shutdown`].
use merezha_application::{
    Application, Guest,
    config::Config,
    postgres::{MIGRATIONS, MODULE, PostgresOptions, PostgresStore},
};
use merezha_domain::Failure;
use merezha_platform::{AppContext, BoxFuture, Migration, Module, ports::Authorizer};
use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

/// Time [`Module::start`] gives the store's connect: within the host's
/// module start bound (10 s).
const CONNECT_BUDGET: Duration = Duration::from_secs(8);

pub struct ApplicationModule {
    config: Config,
    guest: Arc<dyn Guest>,
    policy: Arc<dyn Authorizer>,
    options: PostgresOptions,
    application: OnceLock<Application>,
    failure: Arc<OnceLock<Failure>>,
}
impl ApplicationModule {
    /// `policy` authorizes application messages and projection controls,
    /// as it does for memory and SQLite applications.
    pub fn new(
        config: Config,
        guest: Arc<dyn Guest>,
        policy: Arc<dyn Authorizer>,
        options: PostgresOptions,
    ) -> Self {
        Self {
            config,
            guest,
            policy,
            options,
            application: OnceLock::new(),
            failure: Arc::new(OnceLock::new()),
        }
    }
    /// Why [`Module::start`] failed, such as `storage_configuration` for
    /// another application's database. The host only reports
    /// `module_start_failed`.
    pub fn failure(&self) -> Arc<OnceLock<Failure>> {
        self.failure.clone()
    }
    async fn open(&self, ctx: &AppContext) -> Result<Application, Failure> {
        let deadline = Instant::now() + CONNECT_BUDGET;
        let store =
            PostgresStore::connect(ctx.database(), &self.config, self.options.clone(), deadline)
                .await?;
        let policy: Arc<dyn merezha_application::Authorizer> = self.policy.clone();
        Application::with_postgres(
            self.config.clone(),
            self.guest.clone(),
            policy,
            Arc::new(store),
        )
    }
}
impl Module for ApplicationModule {
    fn name(&self) -> &'static str {
        MODULE
    }
    fn migrations(&self) -> &[Migration] {
        MIGRATIONS
    }
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async move {
            let opened = self.open(&ctx).await;
            match opened {
                Ok(application) => {
                    let _ = self.application.set(application);
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
                crate::application_http::routes(application.clone(), Some(self.policy.clone()))
            }
            None => axum::Router::new(),
        }
    }
}
