//! Reactors on PostgreSQL (AP §C.5, slice C3, and NT-N8's reaction part):
//! the `react` runner, the two reply tiers, the checkpoint transaction,
//! failure semantics, tenant safety and the admin operations, against a
//! test [`Effects`] port that writes its effects into a table inside the
//! checkpoint transaction, as the platform's notifier and outbox do. Each
//! test creates its own database through `MEREZHA_TEST_DATABASE_URL` and
//! skips when it is unset.
use merezha_application::{
    AllowAll, Application, Guest, GuestSession,
    config::Config,
    postgres::{
        BackgroundOptions, CommandJob, EffectBody, EffectRequest, Effects, FailOnce, MIGRATIONS,
        MODULE, PostgresOptions, PostgresStore, ReactorOutcome, RunHooks, Scheduler,
    },
    status::SkipTo,
};
use merezha_domain::{
    Command, Event, Failure, Identity, Metadata, Operation, PROTOCOL_V1, PROTOCOL_V2, Principal,
    PrincipalKind, Reaction, Reply, StoredEvent,
};
use merezha_postgres::{
    BoxFuture, Database, MigrationMode, ModuleMigrations, TestDatabase, Tx, run_migrations,
    tokio_postgres::Row,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
/// Wait until `ready` holds, within `within`.
fn until(within: Duration, what: &str, mut ready: impl FnMut() -> bool) {
    let started = Instant::now();
    while !ready() {
        assert!(started.elapsed() < within, "{what} within {within:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

const ROUTES: &str = r#"version = 2
[storage]
backend = "postgres"
application = "reactor-test"
[limits]
batch_size = 16
[[permissions]]
name = "tasks.archive"
[[commands]]
tag = "Emit"
service = "domain"
aggregate = "Task"
access = "public"
[[commands]]
tag = "TenantEmit"
service = "domain"
aggregate = "Task"
access = "authenticated"
scope = "tenant"
[[commands]]
tag = "ArchiveTask"
service = "domain"
aggregate = "Task"
permission = "tasks.archive"
scope = "tenant"
[[commands]]
tag = "Unarchive"
service = "domain"
aggregate = "Task"
permission = "tasks.unarchive_all"
[[permissions]]
name = "tasks.unarchive_all"
[[queries]]
tag = "Count"
service = "view"
projection = "count"
access = "public"
cross_tenant = true
[[projections]]
name = "count"
service = "view"
initial = 0
"#;
/// The routes plus `reactors`, each `(name, extra lines)`.
fn config(reactors: &[(&str, &str)]) -> Config {
    let mut text = ROUTES.to_owned();
    for (name, extra) in reactors {
        text.push_str(&format!(
            "[[reactors]]\nname = \"{name}\"\nservice = \"reactor\"\nevents = [\"TaskCompleted\"]\n\
             effects = [\"notify\", \"webhook\", \"command\"]\ncommands = [\"ArchiveTask\"]\n\
             permissions = [\"tasks.archive\"]\nstart = \"beginning\"\n{extra}\n"
        ));
    }
    Config::parse(&text).unwrap()
}
fn notifier() -> Config {
    config(&[("notifier", "")])
}

/// The guest: `domain` (version 1) decides one event of the command
/// payload's `tag` with its `payload`; `ArchiveTask` decides
/// `TaskArchived`. `view` counts events. `reactor` (version 2) answers each
/// event with the effects its payload lists under `effects`, and can be
/// made to fail on `poison` events, to fail transiently, or to break the
/// reply contract on `malformed` events.
#[derive(Default)]
struct Probe {
    /// Positions of every react call, in call order.
    reacts: Mutex<Vec<Vec<u64>>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    /// Fail permanently on events whose payload has `"poison": true`.
    poison: AtomicBool,
    /// Fail every react call with transient `overloaded`.
    transient: AtomicBool,
    /// Answer `malformed` events with a reaction for another event.
    malformed: AtomicBool,
    delay_ms: std::sync::atomic::AtomicU64,
}
impl Probe {
    fn reacted(&self) -> Vec<u64> {
        self.reacts.lock().unwrap().concat()
    }
}
#[derive(Clone)]
struct Fake(Arc<Probe>);
impl Guest for Fake {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["domain".into(), "view".into(), "reactor".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
struct InFlight<'a>(&'a Probe);
impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}
impl GuestSession for Fake {
    fn protocol(&self, service: &str) -> u16 {
        if service == "reactor" {
            PROTOCOL_V2
        } else {
            PROTOCOL_V1
        }
    }
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        match operation {
            Operation::Describe {} => Ok(Reply::Described { protocol: 2 }),
            Operation::Validate { payload, .. } => Ok(Reply::Validated { payload }),
            Operation::Decide { tag, payload, .. } => {
                let (tag, payload) = if tag.as_str() == "ArchiveTask" {
                    (id("TaskArchived"), payload)
                } else {
                    (
                        id(payload["tag"].as_str().unwrap_or("TaskCompleted")),
                        payload["payload"].clone(),
                    )
                };
                Ok(Reply::Decided {
                    events: vec![Event {
                        tag,
                        schema_version: 1,
                        payload,
                    }],
                })
            }
            Operation::Project { state, events, .. } => Ok(Reply::Projected {
                state: json!(state.as_u64().unwrap() + events.len() as u64),
            }),
            Operation::Query { state, .. } => Ok(Reply::Queried { value: state }),
            Operation::React { events, .. } => self.react(events),
        }
    }
}
impl Fake {
    fn react(&self, events: Vec<StoredEvent>) -> Result<Reply, Failure> {
        let probe = &self.0;
        let now = probe.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        let _in_flight = InFlight(probe);
        probe.max_in_flight.fetch_max(now, Ordering::SeqCst);
        probe
            .reacts
            .lock()
            .unwrap()
            .push(events.iter().map(|e| e.position).collect());
        let delay = probe.delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            std::thread::sleep(Duration::from_millis(delay));
        }
        if probe.transient.load(Ordering::SeqCst) {
            return Err(Failure::transient("overloaded"));
        }
        let flagged = |key: &str| events.iter().any(|e| e.event.payload[key] == json!(true));
        if probe.poison.load(Ordering::SeqCst) && flagged("poison") {
            return Err(Failure::permanent("poisoned"));
        }
        let malformed = probe.malformed.load(Ordering::SeqCst) && flagged("malformed");
        let reactions = events
            .iter()
            .map(|event| Reaction {
                event_id: if malformed {
                    id("event/0")
                } else {
                    event.event_id.clone()
                },
                effects: serde_json::from_value(event.event.payload["effects"].clone())
                    .unwrap_or_default(),
            })
            .collect();
        Ok(Reply::Reacted { reactions })
    }
}

/// The test port: notify, webhook and command effects become rows of
/// `reactor_test.effects` written in the checkpoint transaction. `check`
/// refuses topic `unknown.topic` (`unknown_topic`, as the notifications
/// block's `validate`); `write` refuses a `user` recipient missing from
/// `members` on a tenant event (`recipient_not_in_tenant`, as
/// `Notifier::request`). `is_member` answers from `members` and records a
/// call made while a checkpoint transaction is open.
struct TestEffects {
    db: Database,
    members: Mutex<BTreeSet<(String, String)>>,
    directory_down: AtomicBool,
    unavailable: AtomicBool,
    /// `is_member` calls made while a reactor session was in a transaction.
    tripped: AtomicUsize,
    asked: AtomicUsize,
}
impl TestEffects {
    fn new(db: Database) -> Arc<Self> {
        Arc::new(Self {
            db,
            members: Mutex::default(),
            directory_down: AtomicBool::new(false),
            unavailable: AtomicBool::new(false),
            tripped: AtomicUsize::new(0),
            asked: AtomicUsize::new(0),
        })
    }
    fn member(&self, user: &str, tenant: &str) {
        self.members
            .lock()
            .unwrap()
            .insert((user.into(), tenant.into()));
    }
}
impl Effects for TestEffects {
    fn check(&self, request: &EffectRequest) -> Result<(), Failure> {
        match &request.body {
            EffectBody::Notify { topic, .. } if topic.as_str() == "unknown.topic" => {
                Err(Failure::permanent("unknown_topic"))
            }
            _ => Ok(()),
        }
    }
    fn is_member<'a>(
        &'a self,
        user: &'a Identity,
        tenant: &'a Identity,
    ) -> BoxFuture<'a, Result<bool, Failure>> {
        Box::pin(async move {
            self.asked.fetch_add(1, Ordering::SeqCst);
            // A checkpoint transaction runs on a session named after its
            // reactor; none may be open while the Directory is asked.
            let client = self.db.acquire(deadline()).await?;
            let open: i64 = client
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'merezha r:%' \
                     AND xact_start IS NOT NULL AND datname = current_database()",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if open > 0 {
                self.tripped.fetch_add(1, Ordering::SeqCst);
            }
            if self.directory_down.load(Ordering::SeqCst) {
                return Err(Failure::transient("directory_unavailable"));
            }
            Ok(self
                .members
                .lock()
                .unwrap()
                .contains(&(user.as_str().into(), tenant.as_str().into())))
        })
    }
    fn write<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        request: &'a EffectRequest,
    ) -> BoxFuture<'a, Result<(), Failure>> {
        Box::pin(async move {
            if self.unavailable.load(Ordering::SeqCst) {
                return Err(Failure::transient("unavailable"));
            }
            let body = match &request.body {
                EffectBody::Notify {
                    topic,
                    recipients,
                    members,
                    delay_ms,
                    ..
                } => {
                    if request.tenant.is_some()
                        && recipients.iter().any(|r| {
                            matches!(r, merezha_domain::Recipient::User { id } if !members.contains(id))
                        })
                    {
                        return Err(Failure::permanent("recipient_not_in_tenant"));
                    }
                    json!({"topic": topic, "members": members, "delay_ms": delay_ms})
                }
                EffectBody::Webhook { event_type, .. } => json!({"event_type": event_type}),
                EffectBody::Command { job, delay_ms } => {
                    json!({"job": job, "delay_ms": delay_ms})
                }
            };
            tx.execute(
                "INSERT INTO reactor_test.effects (dedupe_key, kind, tenant, body) \
                 VALUES ($1, $2, $3, $4::text::json) ON CONFLICT DO NOTHING",
                &[
                    &request.dedupe_key,
                    &request.body.kind(),
                    &request.tenant.as_ref().map(Identity::as_str),
                    &body.to_string(),
                ],
            )
            .await
            .map_err(|e| merezha_postgres::classify(&e))?;
            Ok(())
        })
    }
}

/// A fresh migrated database with the test port's table, and the runtime
/// the stores block on.
struct Pg {
    runtime: tokio::runtime::Runtime,
    admin: Database,
    test: TestDatabase,
}
struct Instance {
    app: Application,
    effects: Arc<TestEffects>,
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
            let client = admin.acquire(deadline()).await.unwrap();
            client
                .batch_execute(
                    "CREATE SCHEMA reactor_test; CREATE TABLE reactor_test.effects (\
                     dedupe_key text PRIMARY KEY, kind text NOT NULL, tenant text NULL, body json NOT NULL)",
                )
                .await
                .unwrap();
        });
        Some(Self {
            runtime,
            admin,
            test,
        })
    }
    fn background(hooks: RunHooks) -> BackgroundOptions {
        BackgroundOptions {
            interval: Duration::from_millis(200),
            reactor_interval: Duration::from_millis(200),
            // Long enough that no machine reaches a retry time between two
            // calls of a test; tests that want a retry move `retry_at`.
            reactor_retry: (Duration::from_secs(2), Duration::from_secs(8)),
            run_timeout: Duration::from_secs(5),
            instance_id: "test".into(),
            hooks,
            ..BackgroundOptions::default()
        }
    }
    /// An instance with its own pool, as another host.
    fn open(
        &self,
        config: Config,
        probe: &Arc<Probe>,
        background: BackgroundOptions,
    ) -> Result<Instance, Failure> {
        let mut db_config = self.test.config();
        db_config.pool_size = 6;
        db_config.acquire_timeout = Duration::from_secs(5);
        let db = Database::connect(&db_config).unwrap();
        let effects = TestEffects::new(Database::connect(&self.test.config()).unwrap());
        let options = PostgresOptions {
            background,
            effects: Some(effects.clone() as Arc<dyn Effects>),
            ..PostgresOptions::default()
        };
        let store = Arc::new(self.runtime.block_on(PostgresStore::connect(
            &db,
            &config,
            options,
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
            effects,
            scheduler: None,
        })
    }
    fn start(&self, config: Config, probe: &Arc<Probe>, background: BackgroundOptions) -> Instance {
        let mut instance = self.open(config, probe, background).unwrap();
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
    /// `(checkpoint, status, halted_position, failures)` of a reactor.
    fn reactor(&self, name: &str) -> (i64, String, Option<i64>, i32) {
        let row = &self.rows(&format!(
            "SELECT checkpoint, status, halted_position, failures FROM merezha_app.reactors \
             WHERE name = '{name}'"
        ))[0];
        (row.get(0), row.get(1), row.get(2), row.get(3))
    }
    /// The test port's effect rows, `dedupe_key kind tenant`, by key.
    fn effects(&self) -> Vec<String> {
        self.rows(
            "SELECT dedupe_key || ' ' || kind || ' ' || coalesce(tenant, '-') \
             FROM reactor_test.effects ORDER BY dedupe_key",
        )
        .iter()
        .map(|r| r.get(0))
        .collect()
    }
    /// The body of the one effect row of `kind`.
    fn body(&self, kind: &str) -> Value {
        let text: String = self.rows(&format!(
            "SELECT body::text FROM reactor_test.effects WHERE kind = '{kind}'"
        ))[0]
            .get(0);
        serde_json::from_str(&text).unwrap()
    }
    /// `position.index effect code` of every rejection of `name`.
    fn rejections(&self, name: &str) -> Vec<String> {
        self.rows(&format!(
            "SELECT position || '.' || index || ' ' || effect || ' ' || code \
             FROM merezha_app.reactor_rejections WHERE name = '{name}' ORDER BY position, index"
        ))
        .iter()
        .map(|r| r.get(0))
        .collect()
    }
}
fn metadata() -> Metadata {
    Metadata {
        message_id: id("m-1"),
        correlation_id: id("c-1"),
        causation_id: None,
        actor: None,
        principal: None,
    }
}
/// An `Emit` command deciding one `tag` event on `stream` whose payload
/// is `payload`.
fn emit(stream: &str, tag: &str, payload: Value) -> Command {
    Command {
        tag: id("Emit"),
        stream: id(stream),
        payload: json!({"tag": tag, "payload": payload}),
        metadata: metadata(),
        idempotency_key: None,
        expected_version: None,
    }
}
/// A task completion whose reaction lists `effects`.
fn completed(stream: &str, effects: Value) -> Command {
    emit(stream, "TaskCompleted", json!({"effects": effects}))
}
fn member(user: &str, tenant: &str) -> Principal {
    Principal::new(id(user), PrincipalKind::User, Some(id(tenant)), vec![]).unwrap()
}
/// A tenant task completion by `principal`.
fn tenant_completed(instance: &Instance, principal: &Principal, stream: &str, effects: Value) {
    let mut command = completed(stream, effects);
    command.tag = id("TenantEmit");
    instance.app.command_as(principal.clone(), command).unwrap();
}
fn notify(user: &str) -> Value {
    json!({"effect": "notify", "topic": "tasks.completed",
           "recipients": [{"user": {"id": user}}], "channels": ["inbox", "email"],
           "data": {"title": "Write docs"}})
}
fn archive(stream: &str, delay_ms: u64) -> Value {
    json!({"effect": "command", "tag": "ArchiveTask", "stream": stream, "payload": {},
           "delay_ms": delay_ms})
}
fn react(instance: &Instance, name: &str) -> ReactorOutcome {
    instance.app.react_now(name).unwrap()
}

/// C3 test 1: completing a task gives exactly one notification request and
/// one delayed command, both keyed `reaction:{reactor}:{event_id}:{index}`
/// with the event's tenant and the source `application` dedupe namespace
/// derived by the host, never by the guest.
#[test]
fn completing_a_task_gives_one_notification_and_one_delayed_command() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let instance = pg
        .open(notifier(), &probe, Pg::background(RunHooks::default()))
        .unwrap();
    instance.effects.member("usr_ada", "org_1");
    let ada = member("usr_ada", "org_1");
    tenant_completed(
        &instance,
        &ada,
        "t-1",
        json!([notify("usr_ada"), archive("t-1", 604_800_000)]),
    );
    // An event the filter does not keep, and a completion without effects.
    instance
        .app
        .command(emit(
            "t-2",
            "TaskRenamed",
            json!({"effects": [notify("usr_ada")]}),
        ))
        .unwrap();
    instance.app.command(completed("t-3", json!([]))).unwrap();
    assert_eq!(
        react(&instance, "notifier"),
        ReactorOutcome::Finished { behind: false }
    );
    assert_eq!(
        pg.effects(),
        [
            "reaction:notifier:event/1:0 notify org_1",
            "reaction:notifier:event/1:1 command org_1",
        ]
    );
    // The guest saw only the kept events, the filtered one never.
    assert_eq!(probe.reacted(), [1, 3]);
    assert_eq!(pg.reactor("notifier"), (3, "active".into(), None, 0));
    // The confirmed member and the delay reach the port; the command job
    // carries what its handler needs, and its key is the receipt's.
    let body = pg.body("command");
    assert_eq!(body["delay_ms"], json!(604_800_000));
    let job: CommandJob = serde_json::from_value(body["job"].clone()).unwrap();
    assert_eq!(
        (
            job.reactor.as_str(),
            job.position,
            job.index,
            job.tenant.as_ref().map(Identity::as_str)
        ),
        ("notifier", 1, 1, Some("org_1"))
    );
    assert_eq!(job.idempotency_key().unwrap().as_str(), "rx:notifier:1:1");
    let notify = pg.body("notify");
    assert_eq!(notify["members"], json!(["usr_ada"]));
    // Reacting again finds nothing new.
    react(&instance, "notifier");
    assert_eq!(pg.effects().len(), 2);
}

/// C3 test 9: tenant safety. A user recipient outside the event's tenant
/// is rejected (`recipient_not_in_tenant`), a `tenant` recipient must name
/// the event's tenant, a command effect runs as `sys_reactor_{name}` and
/// composes the event's tenant, and a webhook on an event without tenant
/// goes out without one (application-level endpoints).
#[test]
fn effects_stay_inside_the_events_tenant() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let instance = pg
        .open(notifier(), &probe, Pg::background(RunHooks::default()))
        .unwrap();
    instance.effects.member("usr_ada", "org_1");
    // usr_bob belongs to org_2 only.
    instance.effects.member("usr_bob", "org_2");
    let ada = member("usr_ada", "org_1");
    tenant_completed(
        &instance,
        &ada,
        "t-1",
        json!([
            notify("usr_bob"),
            {"effect": "notify", "topic": "tasks.completed", "recipients": [{"tenant": {"id": "org_2"}}]},
            {"effect": "notify", "topic": "tasks.completed", "recipients": [{"tenant": {"id": "org_1", "roles": ["admin"]}}]},
            archive("t-1", 0),
        ]),
    );
    instance
        .app
        .command(completed(
            "t-9",
            json!([
                {"effect": "webhook", "event_type": "tasks.task_completed", "data": {"task_id": "t-9"}},
                {"effect": "notify", "topic": "tasks.completed", "recipients": [{"tenant": {"id": "org_1"}}]},
            ]),
        ))
        .unwrap();
    react(&instance, "notifier");
    assert_eq!(
        pg.rejections("notifier"),
        [
            "1.0 notify recipient_not_in_tenant",
            "1.1 notify invalid_recipient",
            "2.1 notify tenant_required",
        ]
    );
    assert_eq!(
        pg.effects(),
        [
            "reaction:notifier:event/1:2 notify org_1",
            "reaction:notifier:event/1:3 command org_1",
            "reaction:notifier:event/2:0 webhook -",
        ]
    );
    // The command effect, as the application.command handler runs it.
    let body = pg.body("command");
    let job: CommandJob = serde_json::from_value(body["job"].clone()).unwrap();
    let receipt = instance.app.command_effect(&job, false).unwrap();
    assert_eq!(receipt.stream.as_str(), "org_1:Task/t-1");
    let stored: String = pg.rows(&format!(
        "SELECT metadata::text FROM merezha_app.events WHERE position = {}",
        receipt.position
    ))[0]
        .get(0);
    let stored: Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(
        stored["principal"],
        json!({"subject": "sys_reactor_notifier", "kind": "system", "tenant": "org_1"})
    );
    assert_eq!(stored["causation_id"], json!("event/1"));
    assert_eq!(stored["message_id"], json!("rx:notifier:1:3"));
    // A duplicate delivery replays the receipt: one append.
    assert_eq!(instance.app.command_effect(&job, false).unwrap(), receipt);
    assert_eq!(
        pg.count("SELECT count(*) FROM merezha_app.events WHERE tag = 'TaskArchived'"),
        1
    );
    // The reactor holds only its permissions, and only its command tags.
    let mut other = job.clone();
    other.tag = id("Unarchive");
    assert_eq!(
        instance.app.command_effect(&other, false).unwrap_err().code,
        "command_not_allowed"
    );
    let mut gone = job.clone();
    gone.reactor = id("removed");
    assert_eq!(
        instance.app.command_effect(&gone, false).unwrap_err().code,
        "reactor_not_found"
    );
    let reactor = &instance.app_config_reactor("notifier");
    let caller =
        merezha_application::ReactorCaller::new(reactor, Some(id("org_1")), false).unwrap();
    assert_eq!(caller.subject().as_str(), "sys_reactor_notifier");
    let unarchive = Command {
        tag: id("Unarchive"),
        stream: id("t-1"),
        payload: json!({}),
        metadata: metadata(),
        idempotency_key: None,
        expected_version: None,
    };
    assert_eq!(
        instance.app.command_as(caller, unarchive).unwrap_err().code,
        "unauthorized"
    );
}

impl Instance {
    fn app_config_reactor(&self, name: &str) -> merezha_application::config::Reactor {
        notifier()
            .reactors
            .unwrap()
            .into_iter()
            .find(|r| r.name.as_str() == name)
            .unwrap()
    }
}

/// C3 test 6 and NT-N8: a rejected effect (an unknown topic, an address
/// recipient without `allow_addresses`, a command tag outside `commands`,
/// a delay past 30 days, an effect type the reactor does not allow) is
/// listed in `rejections` with its code, its sibling effects are written
/// and the checkpoint advances.
#[test]
fn rejected_effects_are_listed_their_siblings_written_and_the_checkpoint_advances() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let config = config(&[("notifier", ""), ("quiet", "")]);
    let mut config = config;
    // `quiet` allows notify effects only.
    config.reactors.as_mut().unwrap()[1].effects =
        vec![merezha_application::config::EffectKind::Notify];
    let instance = pg
        .open(config, &probe, Pg::background(RunHooks::default()))
        .unwrap();
    instance
        .app
        .command(completed(
            "t-1",
            json!([
                {"effect": "notify", "topic": "unknown.topic", "recipients": [{"user": {"id": "usr_ada"}}]},
                {"effect": "notify", "topic": "tasks.completed", "recipients": [{"address": {"email": "ada@example.com"}}]},
                {"effect": "command", "tag": "Unarchive", "stream": "t-1", "payload": {}},
                {"effect": "command", "tag": "ArchiveTask", "stream": "t-1", "payload": {}, "delay_ms": 2_592_000_001_u64},
                notify("usr_ada"),
                archive("t-1", 0),
            ]),
        ))
        .unwrap();
    react(&instance, "notifier");
    react(&instance, "quiet");
    assert_eq!(
        pg.rejections("notifier"),
        [
            "1.0 notify unknown_topic",
            "1.1 notify address_not_allowed",
            "1.2 command command_not_allowed",
            "1.3 command invalid_delay",
        ]
    );
    assert_eq!(
        pg.rejections("quiet"),
        [
            "1.0 notify unknown_topic",
            "1.1 notify address_not_allowed",
            "1.2 command effect_not_allowed",
            "1.3 command effect_not_allowed",
            "1.5 command effect_not_allowed",
        ]
    );
    assert_eq!(
        pg.effects(),
        [
            "reaction:notifier:event/1:4 notify -",
            "reaction:notifier:event/1:5 command -",
            "reaction:quiet:event/1:4 notify -",
        ]
    );
    assert_eq!(pg.reactor("notifier").0, 1);
    assert_eq!(pg.reactor("quiet").0, 1);
    // Listed by the admin API, oldest first, one page at a time.
    let page = instance
        .app
        .admit()
        .unwrap()
        .rejections("notifier", 3, None)
        .unwrap();
    assert_eq!(page.rejections.len(), 3);
    assert_eq!(page.next.as_deref(), Some("1.2"));
    let rest = instance
        .app
        .admit()
        .unwrap()
        .rejections("notifier", 3, page.next.as_deref())
        .unwrap();
    assert_eq!(
        rest.rejections
            .iter()
            .map(|r| (r.index, r.code.as_str()))
            .collect::<Vec<_>>(),
        [(3, "invalid_delay")]
    );
    assert_eq!(rest.next, None);
    let listed = instance.app.admit().unwrap().reactors().unwrap();
    assert_eq!(
        listed
            .reactors
            .iter()
            .map(|r| (r.name.as_str(), r.rejected, r.lag))
            .collect::<Vec<_>>(),
        [("notifier", 4, 0), ("quiet", 5, 0)]
    );
    for bad in ["x", "1", "1.x", "-1.0"] {
        assert_eq!(
            instance
                .app
                .admit()
                .unwrap()
                .rejections("notifier", 3, Some(bad))
                .unwrap_err()
                .code,
            "invalid_input",
            "{bad}"
        );
    }
    assert_eq!(
        instance
            .app
            .admit()
            .unwrap()
            .rejections("nobody", 3, None)
            .unwrap_err()
            .code,
        "reactor_not_found"
    );
}

/// C3 test 2 (forced rollback): a checkpoint transaction rolled back after
/// its writes leaves no effect row and no rejection, the checkpoint stays,
/// and the batch re-runs once with the same keys, so nothing is duplicated.
#[test]
fn a_rolled_back_checkpoint_transaction_writes_nothing_and_the_rerun_writes_once() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let fail = Arc::new(FailOnce::default());
    let hooks = RunHooks {
        reactor_fail: Some(fail.clone()),
        ..RunHooks::default()
    };
    let instance = pg.open(notifier(), &probe, Pg::background(hooks)).unwrap();
    instance
        .app
        .command(completed(
            "t-1",
            json!([notify("usr_ada"), {"effect": "notify", "topic": "unknown.topic", "recipients": [{"user": {"id": "u"}}]}]),
        ))
        .unwrap();
    fail.arm("notifier");
    assert!(matches!(
        react(&instance, "notifier"),
        ReactorOutcome::Waiting(Some(_))
    ));
    assert!(fail.fired());
    assert_eq!(pg.effects(), Vec::<String>::new());
    assert_eq!(pg.rejections("notifier"), Vec::<String>::new());
    let (checkpoint, status, _, failures) = pg.reactor("notifier");
    assert_eq!((checkpoint, status.as_str(), failures), (0, "active", 1));
    pg.execute("UPDATE merezha_app.reactors SET retry_at = NULL");
    react(&instance, "notifier");
    assert_eq!(pg.effects(), ["reaction:notifier:event/1:0 notify -"]);
    assert_eq!(pg.rejections("notifier"), ["1.1 notify unknown_topic"]);
    assert_eq!(pg.reactor("notifier"), (1, "active".into(), None, 0));
}

/// C3 test 4: rebuilding a projection never re-fires a reaction. After
/// 100 reactions, a rebuild leaves the effects and the reactor checkpoint
/// unchanged.
#[test]
fn rebuilding_a_projection_never_refires_a_reaction() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let instance = pg
        .open(notifier(), &probe, Pg::background(RunHooks::default()))
        .unwrap();
    for i in 0..100 {
        instance
            .app
            .command(completed(&format!("t-{i}"), json!([notify("usr_ada")])))
            .unwrap();
    }
    react(&instance, "notifier");
    assert_eq!(pg.count("SELECT count(*) FROM reactor_test.effects"), 100);
    assert_eq!(pg.reactor("notifier").0, 100);
    let calls = probe.reacted().len();
    let rebuilt = instance.app.rebuild("count").unwrap();
    assert_eq!((rebuilt.state, rebuilt.checkpoint), (json!(100), 100));
    instance.app.catch_up("count").unwrap();
    react(&instance, "notifier");
    assert_eq!(pg.count("SELECT count(*) FROM reactor_test.effects"), 100);
    assert_eq!(pg.reactor("notifier").0, 100);
    assert_eq!(probe.reacted().len(), calls, "no event reacted to twice");
}

/// C3 test 5: a guest failing one event of a batch of 16 permanently is
/// isolated one event at a time; the reactor halts at exactly that
/// position (`reactor.halted`), its effects before it committed. It
/// resumes by itself once the guest is fixed and the retry time passes,
/// and `skip` also resumes it.
#[test]
fn a_poison_event_is_isolated_halts_the_reactor_and_a_fix_or_skip_resumes_it() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    probe.poison.store(true, Ordering::SeqCst);
    let instance = pg
        .open(notifier(), &probe, Pg::background(RunHooks::default()))
        .unwrap();
    for i in 1..=16 {
        let mut payload = json!({"effects": [notify("usr_ada")]});
        if i == 11 {
            payload["poison"] = json!(true);
        }
        instance
            .app
            .command(emit(&format!("t-{i}"), "TaskCompleted", payload))
            .unwrap();
    }
    assert!(matches!(
        react(&instance, "notifier"),
        ReactorOutcome::Waiting(Some(_))
    ));
    // The batch of 16 failed, then single events up to the poison one.
    let calls = probe.reacts.lock().unwrap().clone();
    assert_eq!(calls[0], (1..=16).collect::<Vec<u64>>());
    assert!(calls[1..].iter().all(|batch| batch.len() == 1));
    assert_eq!(
        pg.reactor("notifier"),
        (10, "halted".into(), Some(11), 1),
        "halted at exactly the poison event"
    );
    assert_eq!(pg.count("SELECT count(*) FROM reactor_test.effects"), 10);
    // Still poisoned: the retry halts again at the same place, counting.
    pg.execute("UPDATE merezha_app.reactors SET retry_at = NULL");
    react(&instance, "notifier");
    assert_eq!(pg.reactor("notifier"), (10, "halted".into(), Some(11), 2));
    // A fixed guest (a reload) resumes it at its retry time.
    probe.poison.store(false, Ordering::SeqCst);
    let waited = react(&instance, "notifier");
    assert!(
        matches!(waited, ReactorOutcome::Waiting(Some(_))),
        "{waited:?}"
    );
    pg.execute("UPDATE merezha_app.reactors SET retry_at = now()");
    react(&instance, "notifier");
    assert_eq!(pg.reactor("notifier"), (16, "active".into(), None, 0));
    assert_eq!(pg.count("SELECT count(*) FROM reactor_test.effects"), 16);

    // `skip` resumes a halted reactor past exactly the stuck event.
    probe.poison.store(true, Ordering::SeqCst);
    let mut poisoned = completed("t-17", json!([notify("usr_ada")]));
    poisoned.payload["payload"]["poison"] = json!(true);
    instance.app.command(poisoned).unwrap();
    instance
        .app
        .command(completed("t-18", json!([notify("usr_ada")])))
        .unwrap();
    react(&instance, "notifier");
    assert_eq!(pg.reactor("notifier"), (16, "halted".into(), Some(17), 1));
    let admin = || instance.app.admit().unwrap();
    assert_eq!(
        admin()
            .skip_reactor("notifier", SkipTo::Position(18), Some("usr_ops"))
            .unwrap_err()
            .code,
        "invalid_skip"
    );
    let skipped = admin()
        .skip_reactor("notifier", SkipTo::Position(17), Some("usr_ops"))
        .unwrap();
    assert_eq!(skipped.checkpoint, 17);
    assert_eq!(pg.reactor("notifier"), (17, "active".into(), None, 0));
    assert_eq!(
        pg.count(
            "SELECT count(*) FROM merezha_app.reactor_skips WHERE name = 'notifier' \
             AND position = 17 AND reason = 'admin' AND principal = 'usr_ops'"
        ),
        1
    );
    react(&instance, "notifier");
    assert_eq!(pg.reactor("notifier").0, 18);
    assert_eq!(pg.count("SELECT count(*) FROM reactor_test.effects"), 17);
}

/// C3 test 5: `on_error = "skip"` records the skip of a permanently failing
/// event and advances past it in the checkpoint transaction; a malformed
/// reply halts the reactor under either setting.
#[test]
fn on_error_skip_records_the_skip_and_a_malformed_reply_halts_under_either_setting() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    probe.poison.store(true, Ordering::SeqCst);
    probe.malformed.store(true, Ordering::SeqCst);
    let config = config(&[("halting", ""), ("skipping", "on_error = \"skip\"")]);
    let instance = pg
        .open(config, &probe, Pg::background(RunHooks::default()))
        .unwrap();
    let poisoned = |stream: &str| {
        let mut command = completed(stream, json!([notify("usr_ada")]));
        command.payload["payload"]["poison"] = json!(true);
        command
    };
    instance
        .app
        .command(completed("t-1", json!([notify("usr_ada")])))
        .unwrap();
    instance.app.command(poisoned("t-2")).unwrap();
    instance
        .app
        .command(completed("t-3", json!([notify("usr_ada")])))
        .unwrap();
    react(&instance, "skipping");
    assert_eq!(pg.reactor("skipping"), (3, "active".into(), None, 0));
    assert_eq!(
        pg.count(
            "SELECT count(*) FROM merezha_app.reactor_skips WHERE name = 'skipping' \
             AND position = 2 AND reason = 'on_error_skip' AND failure_code = 'poisoned'"
        ),
        1
    );
    react(&instance, "halting");
    assert_eq!(pg.reactor("halting"), (1, "halted".into(), Some(2), 1));
    // A malformed reply (a reaction for another event) halts both.
    let mut malformed = completed("t-4", json!([notify("usr_ada")]));
    malformed.payload["payload"]["malformed"] = json!(true);
    instance.app.command(malformed).unwrap();
    react(&instance, "skipping");
    assert_eq!(pg.reactor("skipping"), (3, "halted".into(), Some(4), 1));
    let failure: String =
        pg.rows("SELECT failure_code FROM merezha_app.reactors WHERE name = 'skipping'")[0].get(0);
    assert_eq!(failure, "invalid_response");
    // `skip` to the head resumes it, recorded as such.
    let skipped = instance
        .app
        .admit()
        .unwrap()
        .skip_reactor("skipping", SkipTo::Head, None)
        .unwrap();
    assert_eq!(skipped.checkpoint, 4);
    assert_eq!(
        pg.count(
            "SELECT count(*) FROM merezha_app.reactor_skips WHERE name = 'skipping' \
             AND reason = 'skip_to_head' AND position = 4"
        ),
        1
    );
    assert_eq!(pg.reactor("skipping"), (4, "active".into(), None, 0));
}

/// C3 test 6: a transient failure (the guest overloaded, the port's
/// outbox unavailable) retries with backoff without advancing.
#[test]
fn a_transient_failure_retries_with_backoff_without_advancing() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let instance = pg
        .open(notifier(), &probe, Pg::background(RunHooks::default()))
        .unwrap();
    instance
        .app
        .command(completed("t-1", json!([notify("usr_ada")])))
        .unwrap();
    probe.transient.store(true, Ordering::SeqCst);
    let first = react(&instance, "notifier");
    let ReactorOutcome::Waiting(Some(wait)) = first else {
        panic!("{first:?}");
    };
    // Equal jitter: half the base to the base for the first failure.
    assert!(
        wait >= Duration::from_secs(1) && wait <= Duration::from_secs(2),
        "{wait:?}"
    );
    let (checkpoint, status, halted, failures) = pg.reactor("notifier");
    assert_eq!(
        (checkpoint, status.as_str(), halted, failures),
        (0, "active", None, 1)
    );
    // Not due before its retry time.
    assert!(matches!(
        react(&instance, "notifier"),
        ReactorOutcome::Waiting(Some(_))
    ));
    assert_eq!(probe.reacted().len(), 1);
    // Its retry time has come.
    pg.execute("UPDATE merezha_app.reactors SET retry_at = now()");
    react(&instance, "notifier");
    assert_eq!(pg.reactor("notifier").3, 2, "backoff grows with failures");
    probe.transient.store(false, Ordering::SeqCst);
    instance.effects.unavailable.store(true, Ordering::SeqCst);
    pg.execute("UPDATE merezha_app.reactors SET retry_at = NULL");
    react(&instance, "notifier");
    assert_eq!(pg.reactor("notifier"), (0, "active".into(), None, 3));
    assert_eq!(pg.effects(), Vec::<String>::new());
    instance.effects.unavailable.store(false, Ordering::SeqCst);
    // `retry` makes it due now.
    instance
        .app
        .admit()
        .unwrap()
        .retry_reactor("notifier")
        .unwrap();
    react(&instance, "notifier");
    assert_eq!(pg.reactor("notifier"), (1, "active".into(), None, 0));
    assert_eq!(pg.effects().len(), 1);
}

/// C3 test 8: `start = "head"` (the default) reacts to nothing from before
/// the reactor existed; `start = "beginning"` reacts to everything. A
/// restart keeps the row's checkpoint.
#[test]
fn a_new_reactor_starts_at_the_head_unless_it_opts_into_the_beginning() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let before = pg
        .open(config(&[]), &probe, Pg::background(RunHooks::default()))
        .unwrap();
    for i in 0..3 {
        before
            .app
            .command(completed(&format!("t-{i}"), json!([notify("usr_ada")])))
            .unwrap();
    }
    drop(before);
    let mut both = config(&[("from-head", ""), ("from-start", "")]);
    both.reactors.as_mut().unwrap()[0].start = merezha_application::config::ReactorStart::Head;
    let instance = pg
        .open(both.clone(), &probe, Pg::background(RunHooks::default()))
        .unwrap();
    assert_eq!(pg.reactor("from-head").0, 3);
    assert_eq!(pg.reactor("from-start").0, 0);
    instance
        .app
        .command(completed("t-9", json!([notify("usr_ada")])))
        .unwrap();
    react(&instance, "from-head");
    react(&instance, "from-start");
    let effects = pg.effects();
    assert_eq!(
        effects
            .iter()
            .filter(|e| e.contains(":from-head:"))
            .collect::<Vec<_>>(),
        ["reaction:from-head:event/4:0 notify -"]
    );
    assert_eq!(
        effects
            .iter()
            .filter(|e| e.contains(":from-start:"))
            .count(),
        4
    );
    // A reactor removed and re-added resumes from its row: the gap shows.
    drop(instance);
    let without = pg
        .open(config(&[]), &probe, Pg::background(RunHooks::default()))
        .unwrap();
    without
        .app
        .command(completed("t-10", json!([notify("usr_ada")])))
        .unwrap();
    drop(without);
    let again = pg
        .open(both, &probe, Pg::background(RunHooks::default()))
        .unwrap();
    let listed = again.app.admit().unwrap().reactors().unwrap();
    assert_eq!(listed.head, 5);
    assert_eq!(
        listed
            .reactors
            .iter()
            .map(|r| (r.name.as_str(), r.lag))
            .collect::<Vec<_>>(),
        [("from-head", 1), ("from-start", 1)]
    );
}

/// C3 test 11: the Directory is asked before the checkpoint transaction
/// opens, never while it is open, once per `(user, tenant)` pair of a
/// batch; an outage retries the batch without advancing.
#[test]
fn the_directory_is_never_asked_inside_a_checkpoint_transaction_and_an_outage_retries() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let instance = pg
        .open(notifier(), &probe, Pg::background(RunHooks::default()))
        .unwrap();
    instance.effects.member("usr_ada", "org_1");
    let ada = member("usr_ada", "org_1");
    instance
        .effects
        .directory_down
        .store(true, Ordering::SeqCst);
    for stream in ["t-1", "t-2"] {
        tenant_completed(
            &instance,
            &ada,
            stream,
            json!([notify("usr_ada"), notify("usr_ada"), notify("usr_bob")]),
        );
    }
    assert!(matches!(
        react(&instance, "notifier"),
        ReactorOutcome::Waiting(Some(_))
    ));
    let failure: String = pg.rows("SELECT failure_code FROM merezha_app.reactors")[0].get(0);
    assert_eq!(failure, "directory_unavailable");
    assert_eq!(pg.reactor("notifier"), (0, "active".into(), None, 1));
    assert_eq!(pg.effects(), Vec::<String>::new());
    instance
        .effects
        .directory_down
        .store(false, Ordering::SeqCst);
    instance.effects.asked.store(0, Ordering::SeqCst);
    pg.execute("UPDATE merezha_app.reactors SET retry_at = NULL");
    react(&instance, "notifier");
    assert_eq!(pg.reactor("notifier").0, 2);
    // Two pairs for the whole batch: (ada, org_1) and (bob, org_1).
    assert_eq!(instance.effects.asked.load(Ordering::SeqCst), 2);
    assert_eq!(instance.effects.tripped.load(Ordering::SeqCst), 0);
    assert_eq!(pg.effects().len(), 4);
    assert_eq!(pg.rejections("notifier").len(), 2);
}

/// NT-N8: two reactors on one event give two notifications, each under its
/// own key.
#[test]
fn two_reactors_on_one_event_give_two_notifications() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let instance = pg
        .open(
            config(&[("first", ""), ("second", "")]),
            &probe,
            Pg::background(RunHooks::default()),
        )
        .unwrap();
    instance
        .app
        .command(completed("t-1", json!([notify("usr_ada")])))
        .unwrap();
    react(&instance, "first");
    react(&instance, "second");
    assert_eq!(
        pg.effects(),
        [
            "reaction:first:event/1:0 notify -",
            "reaction:second:event/1:0 notify -",
        ]
    );
}

/// C3 test 10 and AP §C.1: memory and SQLite refuse reactors, and a
/// reactor's service must speak protocol 2.
#[test]
fn reactors_need_postgres_and_a_version_2_service() {
    let text = format!(
        "{}[[reactors]]\nname = \"r\"\nservice = \"reactor\"\n",
        ROUTES
    );
    let memory = text.replace(
        "[storage]\nbackend = \"postgres\"\napplication = \"reactor-test\"\n",
        "",
    );
    let guest = Arc::new(Fake(Arc::default()));
    assert_eq!(
        Application::open(
            Config::decode(&memory).unwrap(),
            guest.clone(),
            Arc::new(AllowAll)
        )
        .err()
        .unwrap()
        .code,
        "reactors_require_postgres"
    );
    let dir = tempfile::tempdir().unwrap();
    let sqlite = text.replace(
        "[storage]\nbackend = \"postgres\"\napplication = \"reactor-test\"\n",
        &format!(
            "[storage]\nbackend = \"sqlite\"\npath = \"{}\"\napplication = \"reactor-test\"\ninitialize = true\n",
            dir.path().join("app.db").display()
        ),
    );
    assert_eq!(
        Application::open(Config::decode(&sqlite).unwrap(), guest, Arc::new(AllowAll))
            .err()
            .unwrap()
            .code,
        "reactors_require_postgres"
    );
    let Some(pg) = Pg::new() else { return };
    let mut wrong = notifier();
    wrong.reactors.as_mut().unwrap()[0].service = id("domain");
    let probe = Arc::new(Probe::default());
    assert_eq!(
        pg.open(wrong, &probe, Pg::background(RunHooks::default()))
            .err()
            .unwrap()
            .code,
        "reactor_requires_protocol_2"
    );
    // Without effect ports a store with reactors does not open.
    let db = Database::connect(&pg.test.config()).unwrap();
    let refused = pg.runtime.block_on(PostgresStore::connect(
        &db,
        &notifier(),
        PostgresOptions::default(),
        deadline(),
    ));
    assert_eq!(refused.unwrap_err().code, "reactor_effects_unavailable");
}

/// The admin operations: pause stops the reactor (a paused batch's CAS
/// fails), resume makes it due, and the status document lists reactors.
#[test]
fn pause_resume_and_status() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let instance = pg
        .open(notifier(), &probe, Pg::background(RunHooks::default()))
        .unwrap();
    let admin = || instance.app.admit().unwrap();
    assert_eq!(
        admin()
            .set_reactor_active("notifier", false)
            .unwrap()
            .status,
        "paused"
    );
    instance
        .app
        .command(completed("t-1", json!([notify("usr_ada")])))
        .unwrap();
    assert_eq!(react(&instance, "notifier"), ReactorOutcome::Waiting(None));
    assert_eq!(pg.reactor("notifier").0, 0);
    let status = instance.app.status().unwrap();
    assert_eq!(
        serde_json::to_value(&status.reactors).unwrap(),
        json!([{"name": "notifier", "status": "paused", "lag_events": 1}])
    );
    assert_eq!(
        admin().set_reactor_active("notifier", true).unwrap().status,
        "active"
    );
    react(&instance, "notifier");
    assert_eq!(pg.reactor("notifier"), (1, "active".into(), None, 0));
    for missing in ["nobody", "bad name", "x:y"] {
        assert_eq!(
            admin().set_reactor_active(missing, false).unwrap_err().code,
            "reactor_not_found"
        );
        assert_eq!(
            admin().retry_reactor(missing).unwrap_err().code,
            "reactor_not_found"
        );
        assert_eq!(
            admin()
                .skip_reactor(missing, SkipTo::Head, None)
                .unwrap_err()
                .code,
            "reactor_not_found"
        );
    }
    // Nothing left to skip: the checkpoint stays.
    assert_eq!(
        admin()
            .skip_reactor("notifier", SkipTo::Head, None)
            .unwrap()
            .checkpoint,
        1
    );
    assert_eq!(
        pg.count("SELECT count(*) FROM merezha_app.reactor_skips"),
        0
    );
}

/// Tests that wait on background runs or race them (issue #37): CI runs
/// them alone, one at a time (`cargo test -- --test-threads=1 timing::`).
mod timing {
    use super::*;

    /// The scheduler reacts within a second of a local commit, and a
    /// halted reactor resumes by itself at its retry time once the guest
    /// is fixed.
    #[test]
    fn the_scheduler_reacts_after_a_commit_and_resumes_a_halted_reactor_by_itself() {
        let Some(pg) = Pg::new() else { return };
        let probe = Arc::new(Probe::default());
        let instance = pg.start(notifier(), &probe, Pg::background(RunHooks::default()));
        let started = Instant::now();
        instance
            .app
            .command(completed("t-1", json!([notify("usr_ada")])))
            .unwrap();
        until(Duration::from_secs(2), "the reaction", || {
            pg.effects().len() == 1
        });
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        probe.poison.store(true, Ordering::SeqCst);
        let mut poisoned = completed("t-2", json!([notify("usr_ada")]));
        poisoned.payload["payload"]["poison"] = json!(true);
        instance.app.command(poisoned).unwrap();
        until(Duration::from_secs(5), "the halt", || {
            pg.reactor("notifier").1 == "halted"
        });
        probe.poison.store(false, Ordering::SeqCst);
        until(Duration::from_secs(5), "the resume", || {
            pg.reactor("notifier") == (2, "active".into(), None, 0)
        });
        assert_eq!(pg.effects().len(), 2);
        pg.stop(&instance);
    }

    /// C3 test 7: two instances never run the same reactor at once, and
    /// every event is reacted to once.
    #[test]
    fn two_instances_never_run_the_same_reactor_at_once() {
        let Some(pg) = Pg::new() else { return };
        let probe = Arc::new(Probe::default());
        probe.delay_ms.store(3, Ordering::SeqCst);
        let mut c = notifier();
        c.limits.batch_size = 4;
        let instances = [
            pg.start(c.clone(), &probe, Pg::background(RunHooks::default())),
            pg.start(c, &probe, Pg::background(RunHooks::default())),
        ];
        for i in 0..120 {
            instances[i % 2]
                .app
                .command(completed(&format!("t-{i}"), json!([notify("usr_ada")])))
                .unwrap();
        }
        until(Duration::from_secs(20), "every reaction", || {
            pg.reactor("notifier").0 == 120
        });
        assert_eq!(
            probe.max_in_flight.load(Ordering::SeqCst),
            1,
            "one runner at a time"
        );
        assert_eq!(pg.count("SELECT count(*) FROM reactor_test.effects"), 120);
        let mut reacted = probe.reacted();
        assert_eq!(reacted.len(), 120, "each event reacted to once");
        reacted.sort_unstable();
        reacted.dedup();
        assert_eq!(reacted.len(), 120);
        for instance in &instances {
            pg.stop(instance);
        }
    }
}
