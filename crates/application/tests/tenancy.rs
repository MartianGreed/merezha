//! Tenant streams and partitioned projections on PostgreSQL (AP §A.6,
//! §A.7, §C.4, slice C2 tests 3 to 5): tenants never cross in streams,
//! receipts, partitions or queries, concurrent tenants stay independent,
//! a partitioned rebuild swaps generations atomically and sweeps the old
//! ones, and a partitioned batch losing its CAS writes nothing. Each test
//! creates its own database through `MEREZHA_TEST_DATABASE_URL` and skips
//! when it is unset.
use merezha_application::{
    AllowAll, Application, Guest, GuestSession,
    config::Config,
    postgres::{
        BackgroundOptions, CatchUp, MIGRATIONS, MODULE, Pause, PostgresOptions, PostgresStore,
        RunHooks, Scheduler, verify,
    },
};
use merezha_domain::{
    Command, Event, Failure, Identity, Metadata, Operation, PROTOCOL_V1, PROTOCOL_V2, Principal,
    PrincipalKind, Query, Receipt, Reply, StoredEvent,
};
use merezha_postgres::{
    Database, MigrationMode, ModuleMigrations, TestDatabase, run_migrations, tokio_postgres::Row,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn config_with(revision: u32, partition: &str, limits: &str) -> Config {
    Config::parse(&format!(
        r#"version = 2
{limits}
[storage]
backend = "postgres"
application = "tenancy-test"
[[commands]]
tag = "Add"
service = "domain"
aggregate = "Task"
access = "authenticated"
scope = "tenant"
[[commands]]
tag = "Note"
service = "domain"
aggregate = "Note"
access = "public"
[[queries]]
tag = "Mine"
service = "view"
projection = "tasks"
access = "authenticated"
scope = "{scope}"{opt_in}
[[queries]]
tag = "Totals"
service = "view"
projection = "totals"
access = "public"
# Every tenant's events, on purpose (AP §C.4).
cross_tenant = true
[[projections]]
name = "tasks"
service = "view"
initial = {{ n = 0 }}
revision = {revision}
partition = "{partition}"
[[projections]]
name = "totals"
service = "view"
initial = {{ n = 0 }}
"#,
        scope = if partition == "tenant" {
            "tenant"
        } else {
            "application"
        },
        // Unpartitioned, `Mine` reads every tenant's events.
        opt_in = if partition == "tenant" {
            ""
        } else {
            "\ncross_tenant = true"
        },
    ))
    .unwrap()
}
fn config() -> Config {
    config_with(1, "tenant", "[limits]\ntimeout_ms = 30000\nbatch_size = 64")
}
fn member(subject: &str, tenant: &str) -> Principal {
    Principal::new(id(subject), PrincipalKind::User, Some(id(tenant)), vec![]).unwrap()
}
fn metadata(actor: Option<&str>) -> Metadata {
    Metadata {
        message_id: id("m"),
        correlation_id: id("c"),
        causation_id: None,
        actor: actor.map(id),
        principal: None,
    }
}
fn add(stream: &str, key: Option<&str>) -> Command {
    Command {
        tag: id("Add"),
        stream: id(stream),
        payload: json!({}),
        metadata: metadata(None),
        idempotency_key: key.map(id),
        expected_version: None,
    }
}
fn note(key: Option<&str>, actor: Option<&str>) -> Command {
    Command {
        tag: id("Note"),
        stream: id("n-1"),
        payload: json!({}),
        metadata: metadata(actor),
        idempotency_key: key.map(id),
        expected_version: None,
    }
}
fn query(tag: &str) -> Query {
    Query {
        tag: id(tag),
        payload: json!({}),
        metadata: metadata(None),
        min_position: None,
    }
}

/// One recorded `project` call: projection, partition, the events' streams.
type ProjectCall = (String, Option<String>, Vec<String>);

/// The guest: `domain` (version 1) decides one `Added` event; `view`
/// (version 2, so it sees `partition`) counts events into `{n, by}` and
/// records every call's partition and its events' streams.
#[derive(Default)]
struct Probe {
    by: &'static str,
    projects: Mutex<Vec<ProjectCall>>,
}
#[derive(Clone)]
struct Fake(Arc<Probe>);
impl Guest for Fake {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["domain".into(), "view".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
impl GuestSession for Fake {
    fn protocol(&self, service: &str) -> u16 {
        if service == "view" {
            PROTOCOL_V2
        } else {
            PROTOCOL_V1
        }
    }
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        Ok(match operation {
            Operation::Describe {} => Reply::Described { protocol: 2 },
            Operation::Validate { payload, .. } => Reply::Validated { payload },
            Operation::Decide { .. } => Reply::Decided {
                events: vec![Event {
                    tag: id("Added"),
                    schema_version: 1,
                    payload: json!({}),
                }],
            },
            Operation::Project {
                projection,
                state,
                events,
                partition,
                ..
            } => {
                self.0.projects.lock().unwrap().push((
                    projection.as_str().to_owned(),
                    partition.map(|p| p.as_str().to_owned()),
                    events
                        .iter()
                        .map(|e: &StoredEvent| e.stream.as_str().to_owned())
                        .collect(),
                ));
                Reply::Projected {
                    state: json!({
                        "n": state["n"].as_u64().unwrap() + events.len() as u64,
                        "by": self.0.by,
                    }),
                }
            }
            Operation::Query { state, .. } => Reply::Queried { value: state },
        })
    }
}

struct Pg {
    runtime: tokio::runtime::Runtime,
    admin: Database,
    test: TestDatabase,
}
struct Instance {
    app: Application,
    scheduler: Option<Scheduler>,
}
impl Pg {
    fn new() -> Option<Self> {
        let test = TestDatabase::create()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let admin = Database::connect(&test.config()).unwrap();
        runtime.block_on(async {
            let modules = [ModuleMigrations {
                module: MODULE,
                migrations: MIGRATIONS,
            }];
            run_migrations(&admin, &modules, MigrationMode::Apply, deadline())
                .await
                .unwrap();
        });
        Some(Self {
            runtime,
            admin,
            test,
        })
    }
    fn background(catch_up: CatchUp, hooks: RunHooks) -> BackgroundOptions {
        BackgroundOptions {
            catch_up,
            interval: Duration::from_millis(200),
            run_timeout: Duration::from_secs(10),
            instance_id: "tenancy".into(),
            hooks,
            ..BackgroundOptions::default()
        }
    }
    fn open(
        &self,
        config: Config,
        probe: &Arc<Probe>,
        background: BackgroundOptions,
    ) -> Result<Instance, Failure> {
        let mut db_config = self.test.config();
        db_config.pool_size = 8;
        db_config.acquire_timeout = Duration::from_secs(5);
        let db = Database::connect(&db_config).unwrap();
        let store = Arc::new(self.runtime.block_on(PostgresStore::connect(
            &db,
            &config,
            PostgresOptions {
                background,
                ..PostgresOptions::default()
            },
            deadline(),
        ))?);
        let app = Application::with_postgres(
            config,
            Arc::new(Fake(probe.clone())),
            Arc::new(AllowAll),
            store,
        )?;
        Ok(Instance {
            app,
            scheduler: None,
        })
    }
    fn manual(&self, probe: &Arc<Probe>) -> Instance {
        self.open(
            config(),
            probe,
            Self::background(CatchUp::Manual, RunHooks::default()),
        )
        .unwrap()
    }
    fn start(&self, probe: &Arc<Probe>) -> Instance {
        let mut instance = self
            .open(
                config(),
                probe,
                Self::background(CatchUp::Auto, RunHooks::default()),
            )
            .unwrap();
        let _guard = self.runtime.enter();
        instance.scheduler = Some(Scheduler::start(&instance.app).unwrap());
        instance
    }
    fn stop(&self, instance: &Instance) {
        if let Some(scheduler) = &instance.scheduler {
            self.runtime.block_on(scheduler.shutdown(deadline()));
        }
    }
    fn rows(&self, statement: &str) -> Vec<Row> {
        self.runtime.block_on(async {
            let client = self.admin.acquire(deadline()).await.unwrap();
            client.query(statement, &[]).await.unwrap()
        })
    }
    fn count(&self, statement: &str) -> i64 {
        self.rows(statement)[0].get(0)
    }
    fn execute(&self, statement: &str) {
        self.runtime.block_on(async {
            let client = self.admin.acquire(deadline()).await.unwrap();
            client.batch_execute(statement).await.unwrap();
        });
    }
    fn verified(&self) -> bool {
        self.runtime
            .block_on(verify(&self.admin, deadline()))
            .unwrap()
            .ok
    }
    /// Every partition of `tasks` at its served generation: tenant to n.
    fn partitions(&self) -> BTreeMap<String, u64> {
        self.rows(
            "SELECT pp.tenant, (pp.state->>'n')::bigint FROM merezha_app.projection_partitions pp \
             JOIN merezha_app.projections p USING (name, revision, generation) WHERE p.name = 'tasks'",
        )
        .iter()
        .map(|row| (row.get(0), row.get::<_, i64>(1) as u64))
        .collect()
    }
}
fn mine(app: &Application, principal: &Principal) -> Value {
    app.query_as(principal.clone(), query("Mine"))
        .unwrap()
        .value
}

/// AP-C2 test 3: a tenant A principal cannot read or write tenant B: the
/// same local stream id and key give distinct histories and receipts,
/// naming the other tenant's stream nests it under one's own, partitions
/// are per tenant, and queries see only their own partition.
#[test]
fn tenants_never_cross_streams_receipts_partitions_or_queries() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe {
        by: "a",
        ..Probe::default()
    });
    let instance = pg.manual(&probe);
    let app = &instance.app;
    let (ada, bob, cyd) = (
        member("usr_ada", "org_a"),
        member("usr_bob", "org_b"),
        member("usr_cyd", "org_c"),
    );
    let first: Receipt = app.command_as(ada.clone(), add("t-1", Some("k"))).unwrap();
    assert_eq!(
        (first.stream.as_str(), first.version),
        ("org_a:Task/t-1", 1)
    );
    let second = app.command_as(ada.clone(), add("t-1", None)).unwrap();
    assert_eq!(second.version, 2);
    // Same stream id and key in tenant B: its own stream and receipt.
    let other = app.command_as(bob.clone(), add("t-1", Some("k"))).unwrap();
    assert_eq!(
        (other.stream.as_str(), other.version),
        ("org_b:Task/t-1", 1)
    );
    assert_eq!(
        app.command_as(ada.clone(), add("t-1", Some("k"))).unwrap(),
        first,
        "each tenant replays its own receipt"
    );
    // Naming A's stream from B nests it under B; A's history is untouched.
    let named = app
        .command_as(bob.clone(), add("org_a:Task/t-1", None))
        .unwrap();
    assert_eq!(named.stream.as_str(), "org_b:Task/org_a:Task/t-1");
    // A tenantless application event and the anonymous-actor seam: an
    // anonymous caller naming Ada's subject never replays her receipt.
    let signed = app.command_as(ada.clone(), note(Some("n"), None)).unwrap();
    let forged = app.command(note(Some("n"), Some("usr_ada"))).unwrap();
    assert_ne!(signed, forged);
    assert_eq!(forged.version, signed.version + 1);
    assert_eq!(
        app.command(note(Some("n"), Some("usr_ada"))).unwrap(),
        forged
    );
    // Rows: streams and events carry their tenant; receipts their scope.
    let streams: Vec<(String, Option<String>)> = pg
        .rows("SELECT stream, tenant FROM merezha_app.streams ORDER BY stream")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        streams,
        [
            ("Note/n-1".to_owned(), None),
            ("org_a:Task/t-1".to_owned(), Some("org_a".to_owned())),
            (
                "org_b:Task/org_a:Task/t-1".to_owned(),
                Some("org_b".to_owned())
            ),
            ("org_b:Task/t-1".to_owned(), Some("org_b".to_owned())),
        ]
    );
    let receipts: Vec<(String, String, String)> = pg
        .rows("SELECT tag, tenant, actor FROM merezha_app.receipts ORDER BY tag, tenant, actor")
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(
        receipts,
        [
            ("Add".to_owned(), "org_a".to_owned(), "@usr_ada".to_owned()),
            ("Add".to_owned(), "org_b".to_owned(), "@usr_bob".to_owned()),
            ("Note".to_owned(), String::new(), "@usr_ada".to_owned()),
            ("Note".to_owned(), String::new(), "usr_ada".to_owned()),
        ]
    );
    // Partitions: one guest call per tenant group, with its partition; the
    // tenantless events only advance the checkpoint.
    let caught = app.catch_up("tasks").unwrap();
    assert_eq!(caught.state, Value::Null, "a partitioned pair has no state");
    assert_eq!(caught.checkpoint, 6);
    let calls: Vec<_> = probe
        .projects
        .lock()
        .unwrap()
        .iter()
        .filter(|(projection, _, _)| projection == "tasks")
        .cloned()
        .collect();
    assert!(!calls.is_empty());
    for (_, partition, streams) in &calls {
        let partition = partition.as_deref().expect("a partition on every call");
        assert!(
            streams
                .iter()
                .all(|s| s.starts_with(&format!("{partition}:"))),
            "{partition}: {streams:?}"
        );
    }
    assert_eq!(
        pg.partitions(),
        BTreeMap::from([("org_a".into(), 2), ("org_b".into(), 2)])
    );
    assert_eq!(mine(app, &ada), json!({"n": 2, "by": "a"}));
    assert_eq!(mine(app, &bob), json!({"n": 2, "by": "a"}));
    assert_eq!(mine(app, &cyd), json!({"n": 0}), "no partition: initial");
    // The unpartitioned projection still folds every event.
    app.catch_up("totals").unwrap();
    assert_eq!(app.query(query("Totals")).unwrap().value["n"], 6);
    // A tenant query needs a tenant.
    let roaming = Principal::new(id("usr_dee"), PrincipalKind::User, None, vec![]).unwrap();
    assert_eq!(
        app.query_as(roaming, query("Mine")).unwrap_err().code,
        "tenant_required"
    );
    assert!(pg.verified());
}

/// AP-C2 test 5: a partitioned rebuild builds the next generation beside
/// the served one, swaps it in atomically and deletes the old generation
/// in chunks of 1000; tenant queries looping through the swap and the
/// deletion never see the initial state for a tenant that has a
/// partition. Partitions a previous rebuild left at the target generation
/// are cleared before the first batch.
#[test]
fn a_partitioned_rebuild_swaps_generations_atomically_and_sweeps_the_old() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe {
        by: "a",
        ..Probe::default()
    });
    let instance = pg.manual(&probe);
    let app = &instance.app;
    const TENANTS: usize = 1100;
    for tenant in 0..TENANTS {
        let principal = member("usr_a", &format!("org_{tenant}"));
        app.command_as(principal, add("t-1", None)).unwrap();
    }
    app.catch_up("tasks").unwrap();
    assert_eq!(pg.partitions().len(), TENANTS);
    assert_eq!(
        pg.count("SELECT count(*) FROM merezha_app.projection_partitions"),
        TENANTS as i64
    );
    // An abandoned rebuild's leftover at the next generation.
    pg.execute(
        "INSERT INTO merezha_app.projection_partitions (name, revision, generation, tenant, state) \
         VALUES ('tasks', 1, 2, 'org_7', '{\"n\":999}'), ('tasks', 1, 2, 'org_ghost', '{\"n\":5}')",
    );
    let done = Arc::new(AtomicBool::new(false));
    let seen_initial = std::thread::scope(|scope| {
        let looping = scope.spawn(|| {
            let mut queries = 0usize;
            let mut initial = Vec::new();
            while !done.load(Ordering::SeqCst) || queries < 50 {
                let tenant = format!("org_{}", queries % TENANTS);
                let value = mine(app, &member("usr_q", &tenant));
                if value["n"] != 1 {
                    initial.push((tenant, value));
                }
                queries += 1;
            }
            initial
        });
        let rebuilt = app.rebuild("tasks").unwrap();
        assert_eq!(rebuilt.checkpoint, TENANTS as u64);
        // Through the sweep too: it ran inside the rebuild call.
        done.store(true, Ordering::SeqCst);
        looping.join().unwrap()
    });
    assert_eq!(seen_initial, Vec::<(String, Value)>::new());
    let generation: i32 =
        pg.rows("SELECT generation FROM merezha_app.projections WHERE name = 'tasks'")[0].get(0);
    assert_eq!(generation, 2);
    let generations: Vec<i32> = pg
        .rows("SELECT DISTINCT generation FROM merezha_app.projection_partitions")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(generations, [2], "the old generation is gone");
    let partitions = pg.partitions();
    assert_eq!(partitions.len(), TENANTS, "the leftover ghost was cleared");
    assert!(
        partitions.values().all(|n| *n == 1),
        "org_7 rebuilt from events"
    );
    assert_eq!(
        mine(app, &member("usr_q", "org_7")),
        json!({"n": 1, "by": "a"})
    );
    // Live catch-up continues on the new generation.
    app.command_as(member("usr_a", "org_7"), add("t-1", None))
        .unwrap();
    app.catch_up("tasks").unwrap();
    assert_eq!(mine(app, &member("usr_q", "org_7"))["n"], 2);
    assert!(pg.verified());
}

/// AP §A.6: the `partitioned` flag of an existing revision cannot change;
/// a new revision can, and a tenant query then needs the partitioned one.
#[test]
fn changing_partition_needs_a_new_revision() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    drop(pg.manual(&probe));
    let background = || Pg::background(CatchUp::Manual, RunHooks::default());
    assert_eq!(
        pg.open(config_with(1, "none", ""), &probe, background())
            .err()
            .unwrap()
            .code,
        "storage_configuration"
    );
    let revised = pg
        .open(config_with(2, "none", ""), &probe, background())
        .unwrap();
    revised.app.catch_up("tasks").unwrap();
    let partitioned: Vec<(i32, bool)> = pg
        .rows("SELECT revision, partitioned FROM merezha_app.projections WHERE name = 'tasks' ORDER BY revision")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(partitioned, [(1, true), (2, false)]);
    // The other direction: an unpartitioned revision reopened as
    // partitioned.
    assert_eq!(
        pg.open(config_with(2, "tenant", ""), &probe, background())
            .err()
            .unwrap()
            .code,
        "storage_configuration"
    );
    assert!(
        pg.open(config_with(3, "tenant", ""), &probe, background())
            .is_ok()
    );
}

/// Tests that race a deadline or a pause; CI runs them alone
/// (`timing::`).
mod timing {
    use super::*;

    /// AP-C2 test 4: two instances under concurrent load from two tenants keep
    /// every partition exact, and `verify` passes.
    #[test]
    fn two_tenants_under_concurrent_load_keep_their_partitions_apart() {
        let Some(pg) = Pg::new() else { return };
        let probe = Arc::new(Probe {
            by: "x",
            ..Probe::default()
        });
        let one = pg.start(&probe);
        let two = pg.start(&probe);
        const PER_THREAD: usize = 40;
        let tenants = ["org_a", "org_b"];
        std::thread::scope(|scope| {
            for (n, instance) in [&one, &two].into_iter().enumerate() {
                for tenant in tenants {
                    for thread in 0..2 {
                        let app = instance.app.clone();
                        let principal = member(&format!("usr_{n}_{thread}"), tenant);
                        scope.spawn(move || {
                            for i in 0..PER_THREAD {
                                let stream = format!("t-{}", (i + thread) % 5);
                                loop {
                                    match app.command_as(principal.clone(), add(&stream, None)) {
                                        Ok(_) => break,
                                        // Racing writers on one stream retry.
                                        Err(f) if f.code == "concurrency_conflict" => continue,
                                        Err(f) => panic!("{f:?}"),
                                    }
                                }
                            }
                        });
                    }
                }
            }
        });
        let per_tenant = (2 * 2 * PER_THREAD) as u64;
        let head = 2 * per_tenant;
        let started = Instant::now();
        loop {
            let reached = tenants.iter().all(|tenant| {
                let value = one
                    .app
                    .query_as(member("usr_q", tenant), query("Mine"))
                    .map(|r| (r.value, r.checkpoint));
                matches!(value, Ok((v, checkpoint)) if v["n"] == per_tenant && checkpoint == head)
            });
            if reached {
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(20), "convergence");
            std::thread::sleep(Duration::from_millis(50));
        }
        assert_eq!(
            pg.partitions(),
            BTreeMap::from([("org_a".into(), per_tenant), ("org_b".into(), per_tenant)])
        );
        for (_, partition, streams) in probe.projects.lock().unwrap().iter() {
            if let Some(partition) = partition {
                assert!(
                    streams
                        .iter()
                        .all(|s| s.starts_with(&format!("{partition}:")))
                );
            }
        }
        pg.stop(&one);
        pg.stop(&two);
        assert!(pg.verified());
    }

    /// AP-A3 delivery note, owed by C2: a partitioned batch losing its CAS
    /// writes no partition. Two runs without the lock race one batch; the
    /// first pauses after its CAS and writes, the second computes the same
    /// batch with its own guest, waits on the row lock and loses, and every
    /// partition holds the winner's state.
    #[test]
    fn a_partitioned_batch_losing_its_cas_writes_none() {
        let Some(pg) = Pg::new() else { return };
        let pause = Arc::new(Pause::default());
        let winner = Arc::new(Probe {
            by: "winner",
            ..Probe::default()
        });
        let loser = Arc::new(Probe {
            by: "loser",
            ..Probe::default()
        });
        let hooks = |pause: Option<Arc<Pause>>| RunHooks {
            skip_lock: true,
            pause,
            ..RunHooks::default()
        };
        let first = pg
            .open(
                config(),
                &winner,
                Pg::background(CatchUp::Manual, hooks(Some(pause.clone()))),
            )
            .unwrap();
        let second = pg
            .open(
                config(),
                &loser,
                Pg::background(CatchUp::Manual, hooks(None)),
            )
            .unwrap();
        for tenant in ["org_a", "org_b", "org_c"] {
            first
                .app
                .command_as(member("usr_a", tenant), add("t-1", None))
                .unwrap();
        }
        pause.arm("tasks", Duration::from_millis(1500));
        std::thread::scope(|scope| {
            let paused = scope.spawn(|| first.app.catch_up("tasks").unwrap());
            while !pause.entered() {
                std::thread::sleep(Duration::from_millis(5));
            }
            let lost = second.app.catch_up("tasks").unwrap();
            let won = paused.join().unwrap();
            assert_eq!((won.checkpoint, lost.checkpoint), (3, 3));
        });
        let loser_calls = loser
            .projects
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _, _)| p == "tasks")
            .count();
        assert_eq!(loser_calls, 3, "the losing run projected the batch");
        let states: Vec<Value> = pg
            .rows("SELECT state::text FROM merezha_app.projection_partitions ORDER BY tenant")
            .iter()
            .map(|r| serde_json::from_str(r.get::<_, &str>(0)).unwrap())
            .collect();
        assert_eq!(states, vec![json!({"n": 1, "by": "winner"}); 3]);
    }

    /// PR #58 review: a partitioned rebuild abandoned and requested again
    /// while its run still holds the `b:` lock is another candidate. The
    /// run pauses inside its first candidate commit; meanwhile the rebuild
    /// is abandoned (waiting on that commit) and requested again, and the
    /// run's next batch then sees the new request. It must not continue it
    /// over the old request's partitions: every tenant ends with exactly
    /// its own events.
    #[test]
    fn a_rebuild_replaced_under_its_running_lock_never_double_counts() {
        let Some(pg) = Pg::new() else { return };
        let commit = Arc::new(Pause::default());
        let batch = Arc::new(Pause::default());
        let probe = Arc::new(Probe {
            by: "a",
            ..Probe::default()
        });
        let instance = pg
            .open(
                config_with(1, "tenant", "[limits]\ntimeout_ms = 30000\nbatch_size = 16"),
                &probe,
                Pg::background(
                    CatchUp::Manual,
                    RunHooks {
                        pause: Some(commit.clone()),
                        rebuild_batch: Some(batch.clone()),
                        ..RunHooks::default()
                    },
                ),
            )
            .unwrap();
        let app = &instance.app;
        for i in 0..100 {
            let principal = member("usr_a", &format!("org_{}", i % 10));
            app.command_as(principal, add("t-1", None)).unwrap();
        }
        app.catch_up("tasks").unwrap();
        commit.arm("tasks", Duration::from_millis(1500));
        std::thread::scope(|scope| {
            let rebuilding = scope.spawn(|| app.rebuild("tasks").unwrap());
            while !commit.entered() {
                std::thread::sleep(Duration::from_millis(5));
            }
            // The run's next batch waits while the request is replaced.
            batch.arm("tasks", Duration::from_millis(2500));
            app.admit().unwrap().abandon_rebuild("tasks").unwrap();
            app.admit()
                .unwrap()
                .request_rebuild("tasks", false, None)
                .unwrap();
            let rebuilt = rebuilding.join().unwrap();
            assert_eq!(rebuilt.checkpoint, 100);
        });
        assert!(batch.entered(), "the run reached its next batch");
        let partitions = pg.partitions();
        assert_eq!(partitions.len(), 10);
        assert!(
            partitions.values().all(|n| *n == 10),
            "no tenant counted twice: {partitions:?}"
        );
        assert_eq!(
            pg.count("SELECT count(*) FROM merezha_app.projection_partitions"),
            10,
            "only the served generation remains"
        );
    }
}
