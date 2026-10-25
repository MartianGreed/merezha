use merezha_application::{
    AllowAll, Application, Authorizer, Guest, GuestSession,
    config::{Config, Limits},
    store::{EventStore, MemoryEventStore},
};
use merezha_domain::{
    Command, Event, Failure, Identity, MessageKind, Metadata, Operation, Query, Reply,
    validate_payload,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn metadata(actor: &str) -> Metadata {
    Metadata {
        message_id: id("message-1"),
        correlation_id: id("correlation-1"),
        causation_id: Some(id("cause-1")),
        actor: Some(id(actor)),
        principal: None,
    }
}
fn command(amount: i64, key: Option<&str>) -> Command {
    Command {
        tag: id("Add"),
        stream: id("one"),
        payload: json!({"amount":amount}),
        metadata: metadata("alice"),
        idempotency_key: key.map(id),
        expected_version: None,
    }
}
fn query() -> Query {
    Query {
        tag: id("Total"),
        payload: json!({}),
        metadata: metadata("alice"),
        min_position: None,
    }
}
fn config() -> Config {
    Config::parse(
        r#"version=1
[[commands]]
tag="Add"
service="business"
aggregate="Counter"
[[queries]]
tag="Total"
service="view"
projection="total"
[[projections]]
name="total"
service="view"
initial=0
"#,
    )
    .unwrap()
}
// A one-shot rendezvous with a deadline. Unlike a cyclic barrier, a duplicate
// decision cannot enter a second cycle and leave the test process hung.
struct Gate {
    arrived: Mutex<usize>,
    changed: Condvar,
}
impl Gate {
    fn new() -> Self {
        Self {
            arrived: Mutex::new(0),
            changed: Condvar::new(),
        }
    }
    fn wait(&self) {
        let mut arrived = self.arrived.lock().unwrap();
        *arrived += 1;
        self.changed.notify_all();
        let (arrived, _) = self
            .changed
            .wait_timeout_while(arrived, std::time::Duration::from_secs(2), |count| {
                *count < 2
            })
            .unwrap();
        assert!(*arrived >= 2, "concurrency fixture rendezvous timed out");
    }
}
#[derive(Default)]
struct Control {
    decisions: AtomicUsize,
    fail: AtomicBool,
    no_events: AtomicBool,
    fail_projection: AtomicUsize,
    lives: Mutex<Vec<bool>>,
    gate: Option<Arc<Gate>>,
    release: Option<Arc<Gate>>,
}
#[derive(Clone)]
struct Fake(Arc<Control>);
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Add {
    amount: i64,
    #[serde(default)]
    note: Option<String>,
}
impl Guest for Fake {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["business".into(), "view".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
impl GuestSession for Fake {
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        match operation {
            Operation::Validate {
                kind: MessageKind::Command,
                payload,
                ..
            } => validate_payload::<Add>(payload),
            Operation::Validate { payload, .. } => Ok(Reply::Validated { payload }),
            Operation::Decide {
                payload, history, ..
            } => {
                self.0.decisions.fetch_add(1, Ordering::SeqCst);
                if let Some(gate) = &self.0.gate {
                    gate.wait();
                }
                if let Some(release) = &self.0.release {
                    release.wait();
                }
                if self.0.fail.load(Ordering::SeqCst) {
                    return Err(Failure::permanent("invariant_violation"));
                }
                let amount = payload["amount"].as_i64().unwrap();
                let total: i64 = history
                    .iter()
                    .map(|e| e.event.payload["amount"].as_i64().unwrap())
                    .sum();
                if total + amount < 0 {
                    return Err(Failure::permanent("invariant_violation"));
                }
                if self.0.no_events.load(Ordering::SeqCst) {
                    return Ok(Reply::Decided { events: Vec::new() });
                }
                Ok(Reply::Decided {
                    events: vec![Event {
                        tag: id("Added"),
                        schema_version: 1,
                        payload: json!({"amount":amount}),
                    }],
                })
            }
            Operation::Project {
                state,
                events,
                live,
                ..
            } => {
                self.0.lives.lock().unwrap().push(live);
                if events
                    .iter()
                    .any(|e| e.position == self.0.fail_projection.load(Ordering::SeqCst) as u64)
                {
                    return Err(Failure::transient("projection_failed"));
                }
                let added: i64 = events
                    .iter()
                    .map(|e| e.event.payload["amount"].as_i64().unwrap())
                    .sum();
                Ok(Reply::Projected {
                    state: json!(state.as_i64().unwrap() + added),
                })
            }
            Operation::Describe {} => unreachable!("a version 1 fake is never described"),
            Operation::Query { state, .. } => Ok(Reply::Queried { value: state }),
        }
    }
}
fn setup(config: Config, control: Arc<Control>) -> (Application, Arc<MemoryEventStore>) {
    let store = Arc::new(MemoryEventStore::new(config.limits.clone()).unwrap());
    let app = Application::new(
        config,
        Arc::new(Fake(control)),
        Arc::new(AllowAll),
        store.clone(),
    )
    .unwrap();
    (app, store)
}
#[test]
fn restart_retains_receipt_and_projection() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("application.db");
    let control = Arc::new(Control::default());
    let open = |initialize| {
        let mut config = config();
        config.storage = merezha_application::config::StorageConfig::Sqlite {
            path: path.clone(),
            application: id("counter-v1"),
            initialize,
        };
        Application::open(config, Arc::new(Fake(control.clone())), Arc::new(AllowAll)).unwrap()
    };
    let app = open(true);
    let receipt = app.command(command(3, Some("restart"))).unwrap();
    app.catch_up("total").unwrap();
    drop(app);
    let reopened = open(false);
    assert_eq!(reopened.query(query()).unwrap().value, json!(3));
    assert_eq!(
        reopened.command(command(3, Some("restart"))).unwrap(),
        receipt
    );
    assert_eq!(control.decisions.load(Ordering::SeqCst), 1);
}
fn sqlite(
    mut config: Config,
    path: &std::path::Path,
    initialize: bool,
    control: Arc<Control>,
    authorizer: Arc<dyn Authorizer>,
) -> Result<Application, Failure> {
    config.storage = merezha_application::config::StorageConfig::Sqlite {
        path: path.into(),
        application: id("counter-v1"),
        initialize,
    };
    Application::open(config, Arc::new(Fake(control)), authorizer)
}
#[test]
fn sqlite_replays_canonical_scoped_receipts_but_still_validates_and_authorizes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let control = Arc::new(Control::default());
    let app = sqlite(config(), &path, true, control.clone(), Arc::new(AllowAll)).unwrap();
    let first = app.command(command(3, Some("k"))).unwrap();
    let mut bob = command(3, Some("k"));
    bob.metadata.actor = Some(id("bob"));
    assert_eq!(app.command(bob.clone()).unwrap().version, 2);
    control.fail.store(true, Ordering::SeqCst);
    assert_eq!(
        app.command(command(2, Some("retry"))).unwrap_err().code,
        "invariant_violation"
    );
    control.fail.store(false, Ordering::SeqCst);
    assert_eq!(app.command(command(2, Some("retry"))).unwrap().version, 3);
    drop(app);
    let app = sqlite(config(), &path, false, control.clone(), Arc::new(AllowAll)).unwrap();
    let mut equivalent = command(3, Some("k"));
    equivalent.payload = json!({"note":null,"amount":3});
    let before = control.decisions.load(Ordering::SeqCst);
    assert_eq!(app.command(equivalent).unwrap(), first);
    assert_eq!(app.command(bob).unwrap().version, 2);
    assert_eq!(control.decisions.load(Ordering::SeqCst), before);
    for changed in [
        command(4, Some("k")),
        {
            let mut c = command(3, Some("k"));
            c.stream = id("other");
            c
        },
        {
            let mut c = command(3, Some("k"));
            c.expected_version = Some(0);
            c
        },
    ] {
        assert_eq!(
            app.command(changed).unwrap_err().code,
            "idempotency_mismatch"
        );
    }
    let mut invalid = command(3, Some("k"));
    invalid.payload = json!({"amount":"bad"});
    assert_eq!(app.command(invalid).unwrap_err().code, "validation_failed");
    drop(app);
    struct Deny;
    impl Authorizer for Deny {
        fn authorize(
            &self,
            _: MessageKind,
            _: &str,
            _: Option<&str>,
            _: &Value,
            _: &Metadata,
        ) -> Result<(), Failure> {
            Err(Failure::permanent("forbidden"))
        }
    }
    let app = sqlite(config(), &path, false, control, Arc::new(Deny)).unwrap();
    assert_eq!(
        app.command(command(3, Some("k"))).unwrap_err().code,
        "forbidden"
    );
}
#[test]
fn sqlite_zero_event_receipts_and_capacity_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let control = Arc::new(Control::default());
    control.no_events.store(true, Ordering::SeqCst);
    let mut c = config();
    c.limits.max_idempotency = 1;
    let app = sqlite(c.clone(), &path, true, control.clone(), Arc::new(AllowAll)).unwrap();
    let receipt = app.command(command(3, Some("zero"))).unwrap();
    assert_eq!(
        (receipt.version, receipt.position, receipt.events),
        (0, 0, 0)
    );
    drop(app);
    let app = sqlite(c, &path, false, control.clone(), Arc::new(AllowAll)).unwrap();
    control.no_events.store(false, Ordering::SeqCst);
    assert_eq!(app.command(command(3, Some("zero"))).unwrap(), receipt);
    assert_eq!(
        app.command(command(3, Some("another"))).unwrap_err().code,
        "idempotency_capacity"
    );
    assert_eq!(app.command(command(3, None)).unwrap().version, 1);
    assert_eq!(control.decisions.load(Ordering::SeqCst), 2);
}
#[test]
fn sqlite_clones_share_pending_keys_and_only_one_racing_stream_write_commits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("key-db");
    let gate = Arc::new(Gate::new());
    let release = Arc::new(Gate::new());
    let control = Arc::new(Control {
        gate: Some(gate.clone()),
        release: Some(release.clone()),
        ..Control::default()
    });
    let app = sqlite(config(), &path, true, control.clone(), Arc::new(AllowAll)).unwrap();
    let worker = app.clone();
    let thread = std::thread::spawn(move || worker.command(command(1, Some("same"))));
    gate.wait();
    assert_eq!(
        app.command(command(1, Some("same"))).unwrap_err().code,
        "idempotency_in_flight"
    );
    assert_eq!(
        app.command(command(2, Some("same"))).unwrap_err().code,
        "idempotency_mismatch"
    );
    release.wait();
    let receipt = thread.join().unwrap().unwrap();
    drop(app);
    let app = sqlite(config(), &path, false, control.clone(), Arc::new(AllowAll)).unwrap();
    assert_eq!(app.command(command(1, Some("same"))).unwrap(), receipt);
    assert_eq!(control.decisions.load(Ordering::SeqCst), 1);
    drop(app);
    let path = dir.path().join("stream-db");
    let gate = Arc::new(Gate::new());
    let release = Arc::new(Gate::new());
    let control = Arc::new(Control {
        gate: Some(gate.clone()),
        release: Some(release),
        ..Control::default()
    });
    let app = sqlite(config(), &path, true, control.clone(), Arc::new(AllowAll)).unwrap();
    let worker = app.clone();
    let thread = std::thread::spawn(move || worker.command(command(1, None)));
    // Ensure both decisions use the same expected version before either commits.
    gate.wait();
    let second = app.command(command(1, None));
    let first = thread.join().unwrap();
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert_eq!(
        first.err().or(second.err()).unwrap().code,
        "concurrency_conflict"
    );
    drop(app);
    let app = sqlite(config(), &path, false, control, Arc::new(AllowAll)).unwrap();
    assert_eq!(app.catch_up("total").unwrap().checkpoint, 1);
}
#[test]
fn sqlite_failed_catchup_and_rebuild_preserve_the_last_persisted_pair() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let control = Arc::new(Control::default());
    let mut c = config();
    c.limits.batch_size = 1;
    let app = sqlite(c.clone(), &path, true, control.clone(), Arc::new(AllowAll)).unwrap();
    for i in 1..=3 {
        app.command(command(i, Some(&format!("k{i}")))).unwrap();
    }
    control.fail_projection.store(2, Ordering::SeqCst);
    assert_eq!(app.catch_up("total").unwrap_err().code, "projection_failed");
    drop(app);
    let app = sqlite(c.clone(), &path, false, control.clone(), Arc::new(AllowAll)).unwrap();
    let pair = app.query(query()).unwrap();
    assert_eq!((pair.value, pair.checkpoint), (json!(1), 1));
    control.fail_projection.store(0, Ordering::SeqCst);
    assert_eq!(app.catch_up("total").unwrap().checkpoint, 3);
    control.fail_projection.store(2, Ordering::SeqCst);
    assert_eq!(app.rebuild("total").unwrap_err().code, "projection_failed");
    drop(app);
    let app = sqlite(c, &path, false, control.clone(), Arc::new(AllowAll)).unwrap();
    let pair = app.query(query()).unwrap();
    assert_eq!((pair.value, pair.checkpoint), (json!(6), 3));
    control.fail_projection.store(0, Ordering::SeqCst);
    assert_eq!(app.rebuild("total").unwrap().state, json!(6));
    assert!(control.lives.lock().unwrap().contains(&false));
}
#[test]
fn sqlite_event_stream_byte_and_state_limits_remain_finite_across_restart() {
    for mode in ["events", "streams", "bytes", "state"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let control = Arc::new(Control::default());
        let mut c = config();
        match mode {
            "events" => c.limits.max_events = 1,
            "streams" => c.limits.max_streams = 1,
            "bytes" => c.limits.max_store_bytes = 1,
            "state" => c.limits.max_state_bytes = 1,
            _ => unreachable!(),
        }
        let app = sqlite(c.clone(), &path, true, control.clone(), Arc::new(AllowAll)).unwrap();
        if mode == "bytes" {
            assert_eq!(
                app.command(command(1, Some("k"))).unwrap_err().code,
                "store_capacity"
            );
        } else {
            app.command(command(9, Some("k"))).unwrap();
        }
        drop(app);
        let app = sqlite(c, &path, false, control, Arc::new(AllowAll)).unwrap();
        let mut next = command(2, Some("next"));
        if mode == "streams" {
            next.stream = id("two");
        }
        let result = app.command(next);
        if mode == "state" {
            result.unwrap();
            assert_eq!(app.catch_up("total").unwrap_err().code, "payload_limit");
            assert_eq!(app.query(query()).unwrap().checkpoint, 0);
        } else {
            assert_eq!(result.unwrap_err().code, "store_capacity");
        }
    }
}
#[test]
fn commands_queries_projection_catchup_and_rebuild_have_separate_effects() {
    let control = Arc::new(Control::default());
    let (app, store) = setup(config(), control.clone());
    let first = app.command(command(3, Some("first"))).unwrap();
    assert_eq!((first.version, first.position), (1, 1));
    assert_eq!(app.query(query()).unwrap().value, json!(0));
    let projected = app.catch_up("total").unwrap();
    assert_eq!((projected.state, projected.checkpoint), (json!(3), 1));
    assert_eq!(app.query(query()).unwrap().value, json!(3));
    assert_eq!(app.catch_up("total").unwrap().checkpoint, 1);
    assert_eq!(
        app.command(command(-4, None)).unwrap_err().code,
        "invariant_violation"
    );
    app.command(command(2, None)).unwrap();
    let rebuilt = app.rebuild("total").unwrap();
    assert_eq!((rebuilt.state, rebuilt.checkpoint), (json!(5), 2));
    assert_eq!(*control.lives.lock().unwrap(), vec![true, false]);
    let history = store.load(&id("Counter/one")).unwrap();
    assert_eq!(history[0].metadata, metadata("alice"));
    assert!(history[0].accepted_unix_ms > 0);
    assert_eq!(store.head().unwrap(), 2);
}
#[test]
fn idempotency_uses_validated_payload_actor_tag_target_and_precondition() {
    let control = Arc::new(Control::default());
    let (app, store) = setup(config(), control.clone());
    let original = app.command(command(3, Some("key"))).unwrap();
    let mut equivalent = command(3, Some("key"));
    equivalent.payload = json!({"note":null,"amount":3});
    assert_eq!(app.command(equivalent).unwrap(), original);
    assert_eq!(control.decisions.load(Ordering::SeqCst), 1);
    assert_eq!(
        app.command(command(4, Some("key"))).unwrap_err().code,
        "idempotency_mismatch"
    );
    let mut different = command(3, Some("key"));
    different.stream = id("two");
    assert_eq!(
        app.command(different).unwrap_err().code,
        "idempotency_mismatch"
    );
    let mut different = command(3, Some("key"));
    different.expected_version = Some(0);
    assert_eq!(
        app.command(different).unwrap_err().code,
        "idempotency_mismatch"
    );
    let mut different_actor = command(3, Some("key"));
    different_actor.metadata.actor = Some(id("bob"));
    app.command(different_actor).unwrap();
    assert_eq!(store.head().unwrap(), 2);
}
#[test]
fn failed_commands_release_claims_and_validation_precedes_decisions() {
    let control = Arc::new(Control::default());
    let (app, store) = setup(config(), control.clone());
    control.fail.store(true, Ordering::SeqCst);
    assert!(app.command(command(1, Some("retry"))).is_err());
    assert_eq!(store.head().unwrap(), 0);
    control.fail.store(false, Ordering::SeqCst);
    app.command(command(1, Some("retry"))).unwrap();
    let mut invalid = command(2, None);
    invalid.payload = json!({"amount":"wrong"});
    assert_eq!(app.command(invalid).unwrap_err().code, "validation_failed");
    assert_eq!(control.decisions.load(Ordering::SeqCst), 2);
}
#[test]
fn concurrent_same_key_is_in_flight_then_replays() {
    let gate = Arc::new(Gate::new());
    let release = Arc::new(Gate::new());
    let control = Arc::new(Control {
        gate: Some(gate.clone()),
        release: Some(release.clone()),
        ..Control::default()
    });
    let (app, store) = setup(config(), control.clone());
    let worker = app.clone();
    let thread = std::thread::spawn(move || worker.command(command(1, Some("same"))));
    gate.wait();
    assert_eq!(
        app.command(command(1, Some("same"))).unwrap_err().code,
        "idempotency_in_flight"
    );
    assert_eq!(
        app.command(command(2, Some("same"))).unwrap_err().code,
        "idempotency_mismatch"
    );
    release.wait();
    let receipt = thread.join().unwrap().unwrap();
    assert_eq!(app.command(command(1, Some("same"))).unwrap(), receipt);
    assert_eq!(store.head().unwrap(), 1);
}
#[test]
fn concurrent_aggregate_commands_cannot_overwrite_one_another() {
    let control = Arc::new(Control {
        gate: Some(Arc::new(Gate::new())),
        ..Control::default()
    });
    let (app, store) = setup(config(), control);
    let other = app.clone();
    let first = std::thread::spawn(move || other.command(command(1, None)));
    let second = app.command(command(1, None));
    let first = first.join().unwrap();
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let failure = first.err().or(second.err()).unwrap();
    assert_eq!(failure.code, "concurrency_conflict");
    assert_eq!(store.head().unwrap(), 1);
}
#[test]
fn projection_failure_retries_without_advancing_failed_batch_and_rebuild_is_atomic() {
    let control = Arc::new(Control::default());
    let mut config = config();
    config.limits.batch_size = 1;
    let (app, store) = setup(config, control.clone());
    app.command(command(1, None)).unwrap();
    app.command(command(2, None)).unwrap();
    control.fail_projection.store(2, Ordering::SeqCst);
    assert!(app.catch_up("total").is_err());
    let snapshot = app.query(query()).unwrap();
    assert_eq!((snapshot.value, snapshot.checkpoint), (json!(1), 1));
    control.fail_projection.store(0, Ordering::SeqCst);
    assert_eq!(app.catch_up("total").unwrap().state, json!(3));
    control.fail_projection.store(2, Ordering::SeqCst);
    assert!(app.rebuild("total").is_err());
    let snapshot = app.query(query()).unwrap();
    assert_eq!((snapshot.value, snapshot.checkpoint), (json!(3), 2));
    control.fail_projection.store(0, Ordering::SeqCst);
    assert_eq!(app.rebuild("total").unwrap().state, json!(3));
    assert_eq!(store.head().unwrap(), 2);
}
#[test]
fn atomic_store_rejects_conflicts_invalid_batches_and_capacity_without_partial_history() {
    let limits = Limits {
        max_events: 2,
        max_streams: 1,
        ..Limits::default()
    };
    let store = MemoryEventStore::new(limits).unwrap();
    let event = Event {
        tag: id("Added"),
        schema_version: 1,
        payload: json!({"amount":1}),
    };
    let mut invalid = event.clone();
    invalid.schema_version = 0;
    assert!(
        store
            .append(
                &id("one"),
                0,
                vec![event.clone(), invalid],
                &metadata("alice")
            )
            .is_err()
    );
    assert_eq!(store.head().unwrap(), 0);
    store
        .append(&id("one"), 0, vec![event.clone()], &metadata("alice"))
        .unwrap();
    assert_eq!(
        store
            .append(&id("one"), 0, vec![event.clone()], &metadata("alice"))
            .unwrap_err()
            .code,
        "concurrency_conflict"
    );
    assert!(
        store
            .append(&id("two"), 0, vec![event.clone()], &metadata("alice"))
            .is_err()
    );
    assert!(
        store
            .append(
                &id("one"),
                1,
                vec![event.clone(), event.clone()],
                &metadata("alice")
            )
            .is_err()
    );
    store
        .append(&id("one"), 1, vec![event], &metadata("alice"))
        .unwrap();
    assert_eq!(
        store
            .read_all(0, 64)
            .unwrap()
            .iter()
            .map(|e| (e.version, e.position))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 2)]
    );
}
struct Policy(AtomicBool);
impl Authorizer for Policy {
    fn authorize(
        &self,
        _: MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &Metadata,
    ) -> Result<(), Failure> {
        if self.0.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(Failure::permanent("unauthorized"))
        }
    }
}
#[test]
fn policy_runs_on_cached_commands_and_queries_and_capacity_never_evicts_success() {
    let mut config = config();
    config.limits.max_idempotency = 1;
    let policy = Arc::new(Policy(AtomicBool::new(true)));
    let store = Arc::new(MemoryEventStore::new(config.limits.clone()).unwrap());
    let app = Application::new(
        config,
        Arc::new(Fake(Arc::new(Control::default()))),
        policy.clone(),
        store.clone(),
    )
    .unwrap();
    app.command(command(1, Some("key"))).unwrap();
    assert_eq!(
        app.command(command(1, Some("other"))).unwrap_err().code,
        "idempotency_capacity"
    );
    policy.0.store(false, Ordering::SeqCst);
    assert_eq!(
        app.command(command(1, Some("key"))).unwrap_err().code,
        "unauthorized"
    );
    assert_eq!(app.query(query()).unwrap_err().code, "unauthorized");
    assert_eq!(store.head().unwrap(), 1);
}
#[test]
fn configuration_routes_and_dispatch_are_strict_and_admission_is_finite() {
    assert!(Config::parse("version=1\nunknown=true").is_err());
    let mut c = config();
    c.commands.push(c.commands[0].clone());
    assert!(c.validate().is_err());
    let mut c = config();
    c.queries[0].projection = id("missing");
    assert!(c.validate().is_err());
    let mut c = config();
    c.commands[0].service = id("missing");
    assert!(
        Application::in_memory(
            c,
            Arc::new(Fake(Arc::new(Control::default()))),
            Arc::new(AllowAll)
        )
        .is_err()
    );
    let mut c = config();
    c.limits.max_concurrent = 1;
    let (app, _) = setup(c, Arc::new(Control::default()));
    let permit = app.admit().unwrap();
    assert!(app.admit().is_err());
    drop(permit);
    assert!(app.admit().is_ok());
    let mut unknown = command(1, None);
    unknown.tag = id("Unknown");
    assert_eq!(app.command(unknown).unwrap_err().code, "handler_not_found");
}

#[test]
fn expired_admission_and_store_byte_capacity_fail_without_history_or_lost_claims() {
    let mut c = config();
    c.limits.timeout_ms = 1;
    let (app, store) = setup(c, Arc::new(Control::default()));
    let admission = app.admit().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert_eq!(
        admission
            .command(command(1, Some("expired")))
            .unwrap_err()
            .code,
        "timeout"
    );
    assert_eq!(store.head().unwrap(), 0);
    let mut c = config();
    c.limits.max_store_bytes = 1;
    let (app, store) = setup(c, Arc::new(Control::default()));
    for _ in 0..2 {
        assert_eq!(
            app.command(command(1, Some("retry-capacity")))
                .unwrap_err()
                .code,
            "store_capacity"
        );
    }
    assert_eq!(store.head().unwrap(), 0);
}

#[test]
fn aggregate_namespace_cannot_alias_another_aggregate_stream_pair() {
    // "Counter/child" + "one" would collide with "Counter" + "child/one".
    let mut c = config();
    c.commands[0].aggregate = id("Counter/child");
    assert!(c.validate().is_err());
}

#[test]
fn explicit_stale_precondition_without_idempotency_cannot_append() {
    let (app, store) = setup(config(), Arc::new(Control::default()));
    app.command(command(1, None)).unwrap();
    let mut stale = command(2, None);
    stale.expected_version = Some(0);
    assert_eq!(app.command(stale).unwrap_err().code, "concurrency_conflict");
    assert_eq!(store.head().unwrap(), 1);
    let mut current = command(2, None);
    current.expected_version = Some(1);
    assert_eq!(app.command(current).unwrap().version, 2);
}

#[test]
fn multiple_events_have_individual_versions_and_allow_follow_up_append() {
    let store = MemoryEventStore::new(Limits::default()).unwrap();
    let event = |amount| Event {
        tag: id("Added"),
        schema_version: 1,
        payload: json!({"amount":amount}),
    };
    let receipt = store
        .append(
            &id("one"),
            0,
            vec![event(1), event(2), event(3)],
            &metadata("alice"),
        )
        .unwrap();
    assert_eq!(
        (receipt.version, receipt.position, receipt.events),
        (3, 3, 3)
    );
    let history = store.load(&id("one")).unwrap();
    assert_eq!(
        history
            .iter()
            .map(|e| (e.version, e.position))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 2), (3, 3)]
    );
    let expected = history.last().unwrap().version;
    assert_eq!(
        store
            .append(&id("one"), expected, vec![event(4)], &metadata("alice"))
            .unwrap()
            .version,
        4
    );
    let other = store
        .append(&id("two"), 0, vec![event(5)], &metadata("alice"))
        .unwrap();
    assert_eq!((other.version, other.position), (1, 5));
    assert_eq!(
        store
            .read_all(0, 16)
            .unwrap()
            .iter()
            .map(|e| e.position)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
}

struct GappedFeed(Arc<MemoryEventStore>);
impl EventStore for GappedFeed {
    fn load(&self, stream: &Identity) -> Result<Vec<merezha_domain::StoredEvent>, Failure> {
        self.0.load(stream)
    }
    fn head(&self) -> Result<u64, Failure> {
        self.0.head()
    }
    fn read_all(
        &self,
        after: u64,
        limit: usize,
    ) -> Result<Vec<merezha_domain::StoredEvent>, Failure> {
        let mut events = self.0.read_all(after, limit)?;
        if after == 0 && !events.is_empty() {
            events.remove(0);
        }
        Ok(events)
    }
    fn append(
        &self,
        stream: &Identity,
        expected: u64,
        events: Vec<Event>,
        metadata: &Metadata,
    ) -> Result<merezha_domain::Receipt, Failure> {
        self.0.append(stream, expected, events, metadata)
    }
}
#[test]
fn faulty_feed_cannot_advance_checkpoint_past_an_unprojected_event() {
    let config = config();
    let store = Arc::new(MemoryEventStore::new(config.limits.clone()).unwrap());
    let app = Application::new(
        config,
        Arc::new(Fake(Arc::new(Control::default()))),
        Arc::new(AllowAll),
        Arc::new(GappedFeed(store.clone())),
    )
    .unwrap();
    app.command(command(1, None)).unwrap();
    app.command(command(2, None)).unwrap();
    assert_eq!(
        app.catch_up("total").unwrap_err().code,
        "invalid_event_feed"
    );
    let model = app.query(query()).unwrap();
    assert_eq!((model.value, model.checkpoint), (json!(0), 0));
    assert_eq!(store.head().unwrap(), 2);
}
