//! Reactors through the platform host (AP §C.5, slice C3, and NT-N8's
//! reaction part): the application module's effect ports over the real
//! notifications block, the outbox and the jobs runtime, the
//! `application.command` handler, and the reactor admin routes. In process
//! against real PostgreSQL; each test creates its own database through
//! `MEREZHA_TEST_DATABASE_URL` and skips when it is unset.
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use merezha_application::{
    Application, Guest, GuestSession,
    config::Config,
    postgres::{BackgroundOptions, CommandJob, FailOnce, PostgresOptions, RunHooks},
};
use merezha_cli::{application_module::ApplicationModule, reactions::CommandEffects};
use merezha_domain::{
    Command, Event, Failure, Identity, Metadata, Operation, PROTOCOL_V1, PROTOCOL_V2, Principal,
    PrincipalKind, Reaction, Reply, StoredEvent,
};
use merezha_notifications::{HostFacts, Notifications};
use merezha_platform::{
    BoxFuture, ConfigInput, Host, HostOptions, HostParts, Module, Timeouts,
    edge::Authenticated,
    host,
    ports::{
        Contact, Directory, GrantSnapshot, Grants, JobContext, JobHandler, LoopbackAllowAll, Page,
        PageRequest, PermissionSet, RoleKey, TenantId, UserId,
    },
};
use merezha_postgres::{Database, TestDatabase};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};
use tower::ServiceExt;

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
/// base64 of 32 bytes.
const KEY: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";

/// The application: `domain` (version 1) decides one event of the
/// command payload's `tag`; `ArchiveTask` decides `TaskArchived`; `view`
/// counts; `reactor` (version 2) answers each event with the effects its
/// payload lists.
#[derive(Clone, Default)]
struct Fake;
impl Guest for Fake {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["domain".into(), "view".into(), "reactor".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
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
        Ok(match operation {
            Operation::Describe {} => Reply::Described { protocol: 2 },
            Operation::Validate { payload, .. } => Reply::Validated { payload },
            Operation::Decide { tag, payload, .. } => Reply::Decided {
                events: vec![if tag.as_str() == "ArchiveTask" {
                    Event {
                        tag: id("TaskArchived"),
                        schema_version: 1,
                        payload,
                    }
                } else {
                    Event {
                        tag: id(payload["tag"].as_str().unwrap_or("TaskCompleted")),
                        schema_version: 1,
                        payload: payload["payload"].clone(),
                    }
                }],
            },
            Operation::Project { state, events, .. } => Reply::Projected {
                state: json!(state.as_u64().unwrap_or(0) + events.len() as u64),
            },
            Operation::Query { state, .. } => Reply::Queried { value: state },
            Operation::React { events, .. } => Reply::Reacted {
                reactions: events
                    .iter()
                    .map(|event: &StoredEvent| Reaction {
                        event_id: event.event_id.clone(),
                        effects: serde_json::from_value(event.event.payload["effects"].clone())
                            .unwrap_or_default(),
                    })
                    .collect(),
            },
        })
    }
}

/// Members and verified contacts, as identity's Directory answers.
#[derive(Default)]
struct People {
    members: Mutex<BTreeSet<(String, String)>>,
}
impl People {
    fn member(&self, user: &str, tenant: &str) {
        self.members
            .lock()
            .unwrap()
            .insert((user.into(), tenant.into()));
    }
}
impl Directory for People {
    fn users<'a>(&'a self, ids: &'a [UserId]) -> BoxFuture<'a, Result<Vec<Contact>, Failure>> {
        let contacts = ids
            .iter()
            .map(|user| Contact {
                user: user.clone(),
                email: format!("{}@example.com", user.as_str()),
                display_name: Some(user.as_str().to_owned()),
                locale: None,
            })
            .collect();
        Box::pin(async move { Ok(contacts) })
    }
    fn tenant_members<'a>(
        &'a self,
        _: &'a TenantId,
        _: Option<&'a [RoleKey]>,
        _: PageRequest,
    ) -> BoxFuture<'a, Result<Page<Contact>, Failure>> {
        Box::pin(async { Ok(Page::empty()) })
    }
    fn application_members<'a>(
        &'a self,
        _: Option<&'a [RoleKey]>,
        _: PageRequest,
    ) -> BoxFuture<'a, Result<Page<Contact>, Failure>> {
        Box::pin(async { Ok(Page::empty()) })
    }
    fn is_member<'a>(
        &'a self,
        user: &'a UserId,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<bool, Failure>> {
        let member = self
            .members
            .lock()
            .unwrap()
            .contains(&(user.as_str().into(), tenant.as_str().into()));
        Box::pin(async move { Ok(member) })
    }
}

const APPLICATION: &str = r#"version = 2
[storage]
backend = "postgres"
application = "reactions"
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
[[projections]]
name = "count"
service = "view"
initial = 0
[[reactors]]
name = "task-notifier"
service = "reactor"
events = ["TaskCompleted"]
effects = ["notify", "webhook", "command"]
commands = ["ArchiveTask"]
permissions = ["tasks.archive"]
start = "beginning"
[[reactors]]
name = "auditor"
service = "reactor"
events = ["TaskCompleted"]
effects = ["notify"]
start = "beginning"
"#;

/// The project: the notifications block with topic `tasks.completed`
/// (inbox and email, capture driver), webhooks with the event type
/// `tasks.task_completed`, and the queues.
fn project(dir: &Path, url: &str) -> ConfigInput {
    let templates = dir.join("templates/tasks.completed");
    std::fs::create_dir_all(templates.join("en")).unwrap();
    std::fs::write(
        templates.join("template.toml"),
        "description = \"Done\"\n[preview]\ntitle = \"T\"\n",
    )
    .unwrap();
    std::fs::write(templates.join("en/subject.txt"), "Done: {{ title }}").unwrap();
    std::fs::write(
        templates.join("en/body.txt"),
        "{{ title }} for {{ recipient.name }}",
    )
    .unwrap();
    std::fs::write(
        templates.join("en/inbox.toml"),
        "title = \"{{ title }}\"\nbody = \"Done\"\n",
    )
    .unwrap();
    let path = dir.join("merezha.toml");
    std::fs::write(
        &path,
        format!(
            "[app]\nid = \"reactions\"\n[database]\nurl = {{ env = \"APP_DATABASE_URL\" }}\nmigrate = true\n\
             pool_size = 16\n[runtime]\nmanifest = \"runtime.toml\"\n\
             [jobs]\npoll_interval_ms = 100\n[jobs.relay]\ninterval_ms = 100\n\
             [jobs.queues.application]\nconcurrency = 2\nbackoff_base_ms = 100\n\
             [jobs.queues.\"notify.webhook\"]\nbackoff_max_ms = 86400000\n\
             [modules.notifications]\nsecret_key = {{ env = \"NOTIFY_KEY\" }}\n\
             [modules.notifications.templates]\ndir = \"{}\"\n\
             [modules.notifications.webhooks]\n\
             event_types = {{ \"tasks.task_completed\" = {{ description = \"A task was completed\" }} }}\n\
             [modules.notifications.topics.\"tasks.completed\"]\ncategory = \"transactional\"\n\
             channels = [\"inbox\", \"email\"]\ndefault_channels = [\"inbox\"]\n",
            dir.join("templates").display()
        ),
    )
    .unwrap();
    ConfigInput {
        path,
        env: vec![
            ("APP_DATABASE_URL".into(), url.into()),
            ("NOTIFY_KEY".into(), KEY.into()),
        ],
        overrides: Vec::new(),
    }
}

struct World {
    _dir: tempfile::TempDir,
    test: TestDatabase,
    host: Host,
    application: Arc<OnceLock<Application>>,
    people: Arc<People>,
    db: Database,
}
impl World {
    async fn start(hooks: RunHooks) -> Option<Self> {
        let test = TestDatabase::create()?;
        let dir = tempfile::tempdir().unwrap();
        let input = project(dir.path(), test.url());
        let notifications: Vec<Box<dyn Module>> =
            vec![Box::new(Notifications::new(HostFacts::load(&input)))];
        let (config, mut modules) = host::configure(&input, notifications).unwrap();
        let authorizer =
            Arc::new(LoopbackAllowAll::new("127.0.0.1:8080".parse().unwrap()).unwrap());
        let module = ApplicationModule::new(
            Config::parse(APPLICATION).unwrap(),
            Arc::new(Fake),
            authorizer.clone(),
            PostgresOptions {
                background: BackgroundOptions {
                    interval: Duration::from_millis(200),
                    reactor_interval: Duration::from_millis(200),
                    reactor_retry: (Duration::from_millis(100), Duration::from_millis(200)),
                    hooks,
                    ..BackgroundOptions::default()
                },
                ..PostgresOptions::default()
            },
            2,
        );
        let application = module.application();
        modules.insert(0, Box::new(module));
        let jobs = Arc::new(merezha_jobs::PgJobs::new(&config.jobs));
        let people = Arc::new(People::default());
        let host = Host::start_with(
            config,
            modules,
            HostParts {
                routes: axum::Router::new(),
                authorizer,
                timeouts: Timeouts::default(),
                jobs: Some(jobs),
            },
            HostOptions {
                directory: Some(people.clone()),
                ..HostOptions::default()
            },
        )
        .await
        .unwrap();
        let db = Database::connect(&test.config()).unwrap();
        Some(Self {
            _dir: dir,
            test,
            host,
            application,
            people,
            db,
        })
    }
    fn app(&self) -> Application {
        self.application.get().unwrap().clone()
    }
    /// Run `work` on a blocking thread, as every dispatch must.
    async fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce(Application) -> T + Send + 'static,
    ) -> T {
        let app = self.app();
        merezha_telemetry::spawn_blocking_in_span(move || work(app))
            .await
            .unwrap()
    }
    async fn texts(&self, sql: &str) -> Vec<String> {
        let client = self.db.acquire(deadline()).await.unwrap();
        client
            .query(sql, &[])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect()
    }
    async fn count(&self, sql: &str) -> i64 {
        let client = self.db.acquire(deadline()).await.unwrap();
        client.query_one(sql, &[]).await.unwrap().get(0)
    }
    /// Wait until `sql` counts `expected`.
    async fn until(&self, sql: &str, expected: i64) {
        let started = Instant::now();
        loop {
            let n = self.count(sql).await;
            if n == expected {
                return;
            }
            if started.elapsed() >= Duration::from_secs(15) {
                let reactors = self
                    .texts(
                        "SELECT name || ' ' || checkpoint || ' ' || status || ' ' \
                         || coalesce(failure_code, '-') || ' ' || failures FROM merezha_app.reactors",
                    )
                    .await;
                panic!("{sql}: {n}, expected {expected}; reactors {reactors:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    async fn checkpoint(&self, reactor: &str) -> i64 {
        self.count(&format!(
            "SELECT checkpoint FROM merezha_app.reactors WHERE name = '{reactor}'"
        ))
        .await
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
/// A tenant task completion whose reaction lists `effects`.
fn completed(stream: &str, effects: Value) -> Command {
    Command {
        tag: id("TenantEmit"),
        stream: id(stream),
        payload: json!({"tag": "TaskCompleted", "payload": {"effects": effects}}),
        metadata: metadata(),
        idempotency_key: None,
        expected_version: None,
    }
}
fn ada() -> Principal {
    Principal::new(
        id("usr_ada"),
        PrincipalKind::User,
        Some(id("org_1")),
        vec![],
    )
    .unwrap()
}
fn notify(user: &str, topic: &str) -> Value {
    json!({"effect": "notify", "topic": topic, "recipients": [{"user": {"id": user}}],
           "data": {"title": "Write docs"}})
}

/// C3 test 1, NT-N8 and C3 test 9 through the real notifications block:
/// a completion gives one notification request per reactor (two reactors,
/// two notifications), keyed `reaction:{reactor}:{event_id}:{index}` with
/// source `application`, an inbox item for the member, a delayed command
/// job `application`/`application.command` and a webhook event; an unknown
/// topic and a user outside the tenant are rejected and the checkpoint
/// advances.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reactions_reach_the_notifications_block_and_the_outbox() {
    let Some(world) = World::start(RunHooks::default()).await else {
        return;
    };
    world.people.member("usr_ada", "org_1");
    world
        .blocking(|app| {
            app.command_as(
                ada(),
                completed(
                    "t-1",
                    json!([
                        notify("usr_ada", "tasks.completed"),
                        {"effect": "command", "tag": "ArchiveTask", "stream": "t-1", "payload": {},
                         "delay_ms": 604_800_000_u64},
                        notify("usr_ada", "tasks.unknown"),
                        notify("usr_bob", "tasks.completed"),
                        {"effect": "webhook", "event_type": "tasks.task_completed", "data": {"task_id": "t-1"}},
                    ]),
                ),
            )
        })
        .await
        .unwrap();
    // Both reactors notify: the auditor sees the notify effects only.
    world
        .until(
            "SELECT count(*) FROM merezha_notify.notifications WHERE source = 'application'",
            2,
        )
        .await;
    assert_eq!(
        world
            .texts(
                "SELECT dedupe_key FROM merezha_notify.notifications WHERE source = 'application' \
                 ORDER BY dedupe_key"
            )
            .await,
        [
            "reaction:auditor:event/1:0",
            "reaction:task-notifier:event/1:0",
        ]
    );
    world
        .until(
            "SELECT count(*) FROM merezha_notify.inbox_items i JOIN merezha_notify.notifications n \
             ON n.id = i.notification_id WHERE n.source = 'application'",
            2,
        )
        .await;
    assert_eq!(world.checkpoint("task-notifier").await, 1);
    assert_eq!(world.checkpoint("auditor").await, 1);
    assert_eq!(
        world
            .texts(
                "SELECT name || ' ' || position || '.' || index || ' ' || effect || ' ' || code \
                 FROM merezha_app.reactor_rejections ORDER BY name, index"
            )
            .await,
        [
            "auditor 1.1 command effect_not_allowed",
            "auditor 1.2 notify unknown_topic",
            "auditor 1.3 notify recipient_not_in_tenant",
            "auditor 1.4 webhook effect_not_allowed",
            "task-notifier 1.2 notify unknown_topic",
            "task-notifier 1.3 notify recipient_not_in_tenant",
        ]
    );
    // The command effect: one outbox record, relayed into one delayed job.
    assert_eq!(
        world
            .texts(
                "SELECT source || ' ' || dedupe_key || ' ' || queue || ' ' || job_kind || ' ' || tenant \
                 FROM merezha_jobs.outbox WHERE source = 'application'"
            )
            .await,
        ["application reaction:task-notifier:event/1:1 application application.command org_1"]
    );
    world
        .until(
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'application.command' \
             AND state = 'available' AND run_at > now() + interval '6 days'",
            1,
        )
        .await;
    // The webhook effect, keyed by the reaction, for the event's tenant.
    assert_eq!(
        world
            .count(
                "SELECT count(*) FROM merezha_jobs.outbox WHERE source = 'notifications' \
                 AND job_kind = 'notify.webhook_event' AND tenant = 'org_1' \
                 AND payload->>'source_event_id' = 'reaction:task-notifier:event/1:4'"
            )
            .await,
        1
    );
    drop(world.host);
}

/// C3 test 9: a webhook effect on an event without tenant goes to the
/// application-level endpoints (no tenant), and C3 test 2: a forced
/// rollback of the checkpoint transaction leaves no notifications row and
/// no outbox record; the rerun writes each once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolled_back_checkpoint_leaves_no_notification_and_tenantless_webhooks_stay_application_level()
 {
    let fail = Arc::new(FailOnce::default());
    fail.arm("task-notifier");
    let hooks = RunHooks {
        reactor_fail: Some(fail.clone()),
        ..RunHooks::default()
    };
    let Some(world) = World::start(hooks).await else {
        return;
    };
    world
        .blocking(|app| {
            app.command(Command {
                tag: id("Emit"),
                stream: id("t-9"),
                payload: json!({"tag": "TaskCompleted", "payload": {"effects": [
                    {"effect": "webhook", "event_type": "tasks.task_completed", "data": {}},
                    {"effect": "notify", "topic": "tasks.completed",
                     "recipients": [{"address": {"email": "ops@example.com"}}]},
                ]}}),
                metadata: metadata(),
                idempotency_key: None,
                expected_version: None,
            })
        })
        .await
        .unwrap();
    world
        .until(
            "SELECT count(*) FROM merezha_app.reactors WHERE name = 'task-notifier' AND checkpoint = 1",
            1,
        )
        .await;
    assert!(fail.fired(), "the first checkpoint transaction rolled back");
    assert_eq!(
        world
            .count(
                "SELECT count(*) FROM merezha_jobs.outbox WHERE payload->>'source_event_id' \
                 = 'reaction:task-notifier:event/1:0'"
            )
            .await,
        1,
        "written once, by the rerun"
    );
    assert_eq!(
        world
            .count(
                "SELECT count(*) FROM merezha_jobs.outbox WHERE job_kind = 'notify.webhook_event' \
                 AND tenant IS NULL AND payload->>'tenant' IS NULL"
            )
            .await,
        1,
        "application-level: no tenant"
    );
    // An address recipient needs `allow_addresses`.
    assert_eq!(
        world
            .texts("SELECT code FROM merezha_app.reactor_rejections WHERE name = 'task-notifier'")
            .await,
        ["address_not_allowed"]
    );
    assert_eq!(
        world
            .count("SELECT count(*) FROM merezha_notify.notifications WHERE source = 'application'")
            .await,
        0
    );
}

/// C3 test 3: the `application.command` handler runs the command as
/// `sys_reactor_task-notifier` on the event's tenant; a duplicate delivery
/// of the same job gives one append and replays the receipt. A due job
/// runs through the jobs runtime the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn command_effects_run_once_as_the_reactor_on_the_events_tenant() {
    let Some(world) = World::start(RunHooks::default()).await else {
        return;
    };
    world.people.member("usr_ada", "org_1");
    world
        .blocking(|app| {
            app.command_as(
                ada(),
                completed(
                    "t-1",
                    json!([{"effect": "command", "tag": "ArchiveTask", "stream": "t-1", "payload": {}}]),
                ),
            )
        })
        .await
        .unwrap();
    // Due now: the jobs runtime runs it.
    world
        .until(
            "SELECT count(*) FROM merezha_app.events WHERE tag = 'TaskArchived' \
             AND stream = 'org_1:Task/t-1' \
             AND metadata->'principal'->>'subject' = 'sys_reactor_task-notifier' \
             AND metadata->'principal'->>'kind' = 'system' \
             AND metadata->>'causation_id' = 'event/1'",
            1,
        )
        .await;
    world
        .until(
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'application.command' \
             AND state = 'succeeded'",
            1,
        )
        .await;
    // Duplicate deliveries of the job, at once and again.
    let payload: String = world
        .texts("SELECT payload::text FROM merezha_jobs.outbox WHERE source = 'application'")
        .await
        .remove(0);
    let job: CommandJob = serde_json::from_str(&payload).unwrap();
    let handler = Arc::new(CommandEffects::new(world.application.clone(), false));
    let context = || {
        let (_tx, cancel) = tokio::sync::watch::channel(false);
        JobContext {
            id: 1,
            kind: id("application.command"),
            attempt: 2,
            max_attempts: 10,
            idempotency_key: "application:reaction:task-notifier:event/1:0".into(),
            deadline: tokio::time::Instant::now() + Duration::from_secs(10),
            cancel,
            correlation_id: id("c-1"),
            causation_id: Some(id("event/1")),
            tenant: Some(id("org_1")),
        }
    };
    let value = serde_json::to_value(&job).unwrap();
    let (first, second) = tokio::join!(
        handler.handle(context(), value.clone()),
        handler.handle(context(), value.clone())
    );
    first.unwrap();
    second.unwrap();
    handler.handle(context(), value).await.unwrap();
    assert_eq!(
        world
            .count("SELECT count(*) FROM merezha_app.events WHERE tag = 'TaskArchived'")
            .await,
        1,
        "one append; every duplicate replayed the receipt"
    );
    // A malformed or unknown job dead-letters (permanent).
    let refused = handler
        .handle(context(), json!({"nope": 1}))
        .await
        .unwrap_err();
    assert_eq!(refused.code, "invalid_job_payload");
    let mut other = job.clone();
    other.tag = id("TenantEmit");
    let refused = handler
        .handle(context(), serde_json::to_value(&other).unwrap())
        .await
        .unwrap_err();
    assert_eq!(
        (refused.code.as_str(), refused.is_retryable()),
        ("command_not_allowed", false)
    );
}

/// Allows every message and decides permissions from the principal's
/// grant snapshot, as the identity block's authorizer does.
struct GrantPolicy;
impl merezha_application::Authorizer for GrantPolicy {
    fn authorize(
        &self,
        _: merezha_domain::MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &Metadata,
    ) -> Result<(), Failure> {
        Ok(())
    }
}
impl merezha_platform::ports::Authorizer for GrantPolicy {
    fn check(
        &self,
        principal: Option<&merezha_platform::Principal>,
        permission: &Identity,
        scope: &merezha_platform::ResourceScope,
    ) -> Result<merezha_platform::ports::Allowed, merezha_platform::ports::AccessDenied> {
        use merezha_platform::ports::{AccessDenied, DenyReason};
        match principal {
            Some(principal) => principal.check(permission, scope),
            None => Err(AccessDenied::new(DenyReason::Unauthenticated)),
        }
    }
}
fn operator(permissions: &[&str]) -> Option<Arc<merezha_platform::Principal>> {
    let set: PermissionSet = permissions.iter().map(|p| id(p)).collect();
    Some(Arc::new(merezha_platform::Principal {
        subject: id("usr_ops"),
        kind: merezha_platform::ports::PrincipalKind::User,
        user: Some(id("usr_ops")),
        service_account: None,
        tenant: None,
        grants: Arc::new(
            GrantSnapshot::empty().with_application(Grants::new(vec![id("operator")], set)),
        ),
        session: None,
    }))
}

/// The reactor admin routes (AP §D.1): reads need
/// `application.admin.read`, writes `application.admin.write`, at
/// application scope; their documents and error codes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_reactor_admin_routes_check_permissions_and_answer_their_documents() {
    let Some(world) = World::start(RunHooks::default()).await else {
        return;
    };
    world
        .blocking(|app| {
            app.command_as(
                ada(),
                completed("t-1", json!([notify("usr_bob", "tasks.unknown")])),
            )
        })
        .await
        .unwrap();
    world
        .until(
            "SELECT count(*) FROM merezha_app.reactors WHERE checkpoint = 1",
            2,
        )
        .await;
    let router = merezha_cli::application_http::postgres_routes(world.app(), Arc::new(GrantPolicy));
    let call = |principal: Option<Arc<merezha_platform::Principal>>,
                method: &'static str,
                path: String,
                body: &'static str| {
        let router = router.clone();
        async move {
            let response = router
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header("content-type", "application/json")
                        .extension(Authenticated(principal))
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status().as_u16();
            let body = to_bytes(response.into_body(), 65_536).await.unwrap();
            (
                status,
                serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null),
            )
        }
    };
    let read = || operator(&["application.admin.read"]);
    let write = || operator(&["application.admin.write"]);
    let (status, listed) = call(read(), "GET", "/reactors".into(), "").await;
    assert_eq!(status, 200, "{listed}");
    assert_eq!(listed["head"], 1);
    assert_eq!(
        listed["reactors"][1],
        json!({"name": "task-notifier", "checkpoint": 1, "lag": 0, "status": "active",
               "halted_position": null, "failure_code": null, "failures": 0, "retry_at": null,
               "rejected": 1})
    );
    let (status, page) = call(
        read(),
        "GET",
        "/reactors/task-notifier/rejections?limit=1".into(),
        "",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(page["rejections"][0]["code"], "unknown_topic");
    assert_eq!(page["rejections"][0]["effect"], "notify");
    assert!(page.get("next").is_none(), "{page}");
    // Writes need the write permission, reads the read one.
    for (method, path, body) in [
        ("POST", "/reactors/task-notifier/pause", ""),
        ("POST", "/reactors/task-notifier/resume", ""),
        ("POST", "/reactors/task-notifier/retry", ""),
        ("POST", "/reactors/task-notifier/skip", r#"{"to":"head"}"#),
    ] {
        let (status, _) = call(read(), method, path.into(), body).await;
        assert_eq!(status, 403, "{path}");
        let (status, _) = call(None, method, path.into(), body).await;
        assert_eq!(status, 401, "{path}");
    }
    for path in ["/reactors", "/reactors/task-notifier/rejections"] {
        let (status, _) = call(write(), "GET", path.into(), "").await;
        assert_eq!(status, 403, "{path}");
    }
    let (status, paused) = call(write(), "POST", "/reactors/task-notifier/pause".into(), "").await;
    assert_eq!(
        (status, paused),
        (200, json!({"name": "task-notifier", "status": "paused"}))
    );
    let (status, resumed) =
        call(write(), "POST", "/reactors/task-notifier/resume".into(), "").await;
    assert_eq!((status, resumed["status"].clone()), (200, json!("active")));
    let (status, retried) = call(write(), "POST", "/reactors/task-notifier/retry".into(), "").await;
    assert_eq!((status, retried["status"].clone()), (200, json!("active")));
    let (status, error) = call(
        write(),
        "POST",
        "/reactors/task-notifier/skip".into(),
        r#"{"position":5}"#,
    )
    .await;
    assert_eq!(
        (status, error["error"]["code"].clone()),
        (409, json!("invalid_skip"))
    );
    let (status, skipped) = call(
        write(),
        "POST",
        "/reactors/task-notifier/skip".into(),
        r#"{"to":"head"}"#,
    )
    .await;
    assert_eq!(
        (status, skipped),
        (200, json!({"name": "task-notifier", "checkpoint": 1}))
    );
    for body in [
        "",
        "{}",
        r#"{"to":"tail"}"#,
        r#"{"position":"x"}"#,
        r#"{"position":1,"to":"head"}"#,
    ] {
        let (status, error) =
            call(write(), "POST", "/reactors/task-notifier/skip".into(), body).await;
        assert_eq!(
            (status, error["error"]["code"].clone()),
            (400, json!("invalid_input")),
            "{body}"
        );
    }
    for query in ["limit=0", "limit=201", "after=x", "other=1"] {
        let (status, _) = call(
            read(),
            "GET",
            format!("/reactors/task-notifier/rejections?{query}"),
            "",
        )
        .await;
        assert_eq!(status, 400, "{query}");
    }
    let (status, missing) = call(write(), "POST", "/reactors/nobody/pause".into(), "").await;
    assert_eq!(
        (status, missing["error"]["code"].clone()),
        (404, json!("reactor_not_found"))
    );
    let (status, missing) = call(read(), "GET", "/reactors/nobody/rejections".into(), "").await;
    assert_eq!(status, 404, "{missing}");
    let (status, document) = call(read(), "GET", "/status".into(), "").await;
    assert_eq!(status, 200);
    assert_eq!(
        document["reactors"],
        json!([{"name": "auditor", "status": "active", "lag_events": 0},
               {"name": "task-notifier", "status": "active", "lag_events": 0}])
    );
    let _ = &world.test;
}
