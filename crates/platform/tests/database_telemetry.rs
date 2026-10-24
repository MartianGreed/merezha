//! The host builds the PostgreSQL instruments from the meters it is given
//! and hands each module a pool scoped to its name (TE §1.3, T4), against
//! real PostgreSQL.
use axum::Router;
use merezha_domain::Failure;
use merezha_platform::{
    AppContext, BoxFuture, Host, HostOptions, HostParts, Migration, Module, ProjectConfig,
    Timeouts, ports::LoopbackAllowAll,
};
use merezha_postgres::{Statement, TestDatabase, classify};
use merezha_telemetry::testing::Harness;
use std::{sync::Arc, time::Duration};
use tracing::Instrument;

const MIGRATIONS: [Migration; 1] = [Migration::new(
    1,
    "create",
    "CREATE SCHEMA probe; CREATE TABLE probe.items (id int);",
)];
const COUNT: Statement = Statement::new(
    "probe.items.count",
    "SELECT probe.items",
    "SELECT count(*) FROM probe.items",
);

/// Runs one registered statement through its context's pool at start.
struct Probe;
impl Module for Probe {
    fn name(&self) -> &'static str {
        "probe"
    }
    fn migrations(&self) -> &[Migration] {
        &MIGRATIONS
    }
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async move {
            let db = ctx.database();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let client = db.acquire(deadline).await?;
            db.query_one(&client, &COUNT, &[])
                .await
                .map_err(|e| classify(&e))?;
            Ok(())
        })
    }
    fn routes(&self, _ctx: &AppContext) -> Router {
        Router::new()
    }
}

fn config(test: &TestDatabase, migrate: bool) -> ProjectConfig {
    ProjectConfig::parse(
        &format!(
            "[app]\nid = \"probe-app\"\n[database]\nurl = {{ env = \"PROBE_DATABASE_URL\" }}\nmigrate = {migrate}\npool_size = 2\n[runtime]\nmanifest = \"runtime.toml\"\n[http]\nlisten = \"127.0.0.1:0\"\n"
        ),
        std::env::temp_dir(),
        &[("PROBE_DATABASE_URL".into(), test.url().to_owned())],
        &[],
    )
    .unwrap()
}
fn parts() -> HostParts {
    HostParts {
        routes: Router::new(),
        authorizer: Arc::new(LoopbackAllowAll::new("127.0.0.1:0".parse().unwrap()).unwrap()),
        timeouts: Timeouts::default(),
        jobs: None,
    }
}

#[tokio::test]
async fn the_host_instruments_its_pool_and_scopes_it_to_each_module() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let meters = harness.telemetry().meters();
    let startup = tracing::info_span!(target: "merezha_test", "startup");
    let host = Host::start_with(
        config(&test, true),
        vec![Box::new(Probe)],
        parts(),
        HostOptions {
            meters,
            ..HostOptions::default()
        },
    )
    .instrument(startup)
    .await
    .unwrap();
    // The migration ran under the startup span, labelled with its module.
    let migrate = harness.span("migrate probe");
    assert_eq!(
        migrate.parent_span_id,
        harness.span("startup").span_context.span_id()
    );
    let pending = harness.metric("merezha.db.migrations.pending");
    assert_eq!(pending.value(&[("merezha.module", "probe")]), Some(0.0));
    // The module's own statement carries its name.
    let duration = harness.metric("db.client.operation.duration");
    assert_eq!(
        duration.value(&[
            ("db.collection.name", "probe.items"),
            ("merezha.module", "probe"),
        ]),
        Some(1.0)
    );
    // The host's own probe is the kernel's.
    assert!(
        duration
            .value(&[
                ("db.operation.name", "SELECT"),
                ("merezha.module", "kernel")
            ])
            .is_some()
    );
    assert_eq!(
        harness
            .metric("db.client.connection.max")
            .value(&[("db.client.connection.pool.name", "merezha")]),
        Some(2.0)
    );
    drop(host);
    harness.assert_catalogued();
}
