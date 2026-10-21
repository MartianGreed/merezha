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
    Arc, Barrier, Mutex,
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
#[derive(Default)]
struct Control {
    decisions: AtomicUsize,
    fail: AtomicBool,
    fail_projection: AtomicUsize,
    lives: Mutex<Vec<bool>>,
    gate: Option<Arc<Barrier>>,
    release: Option<Arc<Barrier>>,
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
    let gate = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
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
        gate: Some(Arc::new(Barrier::new(2))),
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
