//! The read model's contract through its `handle` export, using the
//! version 1 application protocol exactly as the host sends it.

use merezha_domain::{
    Event, Failure, Identity, MessageKind, Metadata, Operation, Reply, Request, Response,
    StoredEvent,
};
use serde_json::{Value, json};
use task_view::handle;

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}

fn call(operation: Operation) -> Result<Reply, Failure> {
    let input = serde_json::to_string(&Request::new(operation)).unwrap();
    Response::decode(&handle(&input).expect("the ABI call itself succeeds"))
}

fn metadata() -> Metadata {
    Metadata {
        message_id: id("msg-1"),
        correlation_id: id("corr-1"),
        causation_id: None,
        actor: None,
    }
}

/// A recorded event at `version` of the task's stream and global `position`.
fn stored(position: u64, version: u64, tag: &str, payload: Value) -> StoredEvent {
    let task_id = payload["task_id"].as_str().unwrap().to_owned();
    StoredEvent {
        event_id: id(&format!("evt-{position}")),
        stream: id(&task_id),
        version,
        position,
        event: Event {
            tag: id(tag),
            schema_version: 1,
            payload,
        },
        metadata: metadata(),
        accepted_unix_ms: 1_700_000_000_000,
    }
}

fn created(position: u64, task: &str, title: &str) -> StoredEvent {
    stored(
        position,
        1,
        "TaskCreated",
        json!({ "task_id": task, "title": title }),
    )
}

fn renamed(position: u64, version: u64, task: &str, title: &str) -> StoredEvent {
    stored(
        position,
        version,
        "TaskRenamed",
        json!({ "task_id": task, "title": title }),
    )
}

fn completed(position: u64, version: u64, task: &str) -> StoredEvent {
    stored(
        position,
        version,
        "TaskCompleted",
        json!({ "task_id": task }),
    )
}

fn project(state: Value, events: Vec<StoredEvent>) -> Result<Value, Failure> {
    match call(Operation::Project {
        projection: id("tasks"),
        state,
        events,
        live: true,
    })? {
        Reply::Projected { state } => Ok(state),
        other => panic!("unexpected reply {other:?}"),
    }
}

fn query(state: &Value, tag: &str, payload: Value) -> Result<Value, Failure> {
    match call(Operation::Query {
        tag: id(tag),
        payload,
        state: state.clone(),
        metadata: metadata(),
    })? {
        Reply::Queried { value } => Ok(value),
        other => panic!("unexpected reply {other:?}"),
    }
}

fn validate(tag: &str, payload: Value) -> Result<Value, Failure> {
    match call(Operation::Validate {
        kind: MessageKind::Query,
        tag: id(tag),
        payload,
    })? {
        Reply::Validated { payload } => Ok(payload),
        other => panic!("unexpected reply {other:?}"),
    }
}

fn empty() -> Value {
    json!({ "tasks": {} })
}

/// Three tasks: task-1 renamed and completed, task-2 open, task-3 open.
fn board() -> Value {
    let first = project(
        empty(),
        vec![
            created(1, "task-1", "Write docs"),
            created(2, "task-2", "Review PR"),
        ],
    )
    .unwrap();
    project(
        first,
        vec![completed(3, 2, "task-1"), created(4, "task-3", "Ship")],
    )
    .unwrap()
}

#[cfg(not(feature = "faulty"))]
#[test]
fn batches_fold_into_the_read_model() {
    let state = project(
        empty(),
        vec![
            created(1, "task-1", "Write docs"),
            renamed(2, 2, "task-1", "Write the docs"),
            created(3, "task-2", "Review PR"),
            completed(4, 3, "task-1"),
        ],
    )
    .unwrap();
    assert_eq!(
        state,
        json!({ "tasks": {
            "task-1": { "title": "Write the docs", "status": "completed", "version": 3 },
            "task-2": { "title": "Review PR", "status": "open", "version": 1 },
        }})
    );
}

#[cfg(not(feature = "faulty"))]
#[test]
fn a_redelivered_batch_changes_nothing() {
    let batch = vec![
        created(1, "task-1", "Write docs"),
        renamed(2, 2, "task-1", "Write the docs"),
    ];
    let once = project(empty(), batch.clone()).unwrap();
    assert_eq!(project(once.clone(), batch), Ok(once));
}

#[cfg(not(feature = "faulty"))]
#[test]
fn rebuild_reproduces_the_same_state_in_one_batch_or_many() {
    let events = vec![
        created(1, "task-1", "Write docs"),
        created(2, "task-2", "Review PR"),
        renamed(3, 2, "task-2", "Review the PR"),
        completed(4, 2, "task-1"),
    ];
    let whole = project(empty(), events.clone()).unwrap();
    let mut stepwise = empty();
    for event in events {
        stepwise = project(stepwise, vec![event]).unwrap();
    }
    assert_eq!(whole, stepwise);
}

#[test]
fn gaps_and_impossible_transitions_fail_the_whole_batch() {
    let gap = Err(Failure::permanent("projection_gap"));
    assert_eq!(project(empty(), vec![completed(1, 2, "task-1")]), gap);
    let open = project(empty(), vec![created(1, "task-1", "Write docs")]).unwrap();
    assert_eq!(
        project(
            open.clone(),
            vec![created(2, "task-2", "x"), completed(3, 3, "task-1")]
        ),
        gap
    );
    let done = project(open, vec![completed(2, 2, "task-1")]).unwrap();
    assert_eq!(project(done, vec![completed(3, 3, "task-1")]), gap);
}

#[test]
fn unknown_events_and_states_are_rejected() {
    let mut archived = created(1, "task-1", "x");
    archived.event.tag = id("TaskArchived");
    assert_eq!(
        project(empty(), vec![archived]),
        Err(Failure::permanent("unknown_event"))
    );
    let mut untrimmed = created(1, "task-1", "x");
    untrimmed.event.payload = json!({ "task_id": "task-1", "title": " x" });
    assert_eq!(
        project(empty(), vec![untrimmed]),
        Err(Failure::permanent("invalid_event"))
    );
    assert_eq!(
        project(json!({ "tasks": {}, "extra": 1 }), vec![]),
        Err(Failure::permanent("invalid_state"))
    );
    assert_eq!(
        query(&json!([]), "TaskSummary", json!({})),
        Err(Failure::permanent("invalid_state"))
    );
    let other = call(Operation::Project {
        projection: id("people"),
        state: empty(),
        events: vec![],
        live: true,
    });
    assert_eq!(other, Err(Failure::permanent("unknown_projection")));
}

#[test]
fn queries_read_the_state() {
    let state = board();
    assert_eq!(
        query(&state, "GetTask", json!({ "task_id": "task-1" })),
        Ok(
            json!({ "task_id": "task-1", "title": "Write docs", "status": "completed", "version": 2 })
        )
    );
    assert_eq!(
        query(&state, "GetTask", json!({ "task_id": "task-9" })),
        Err(Failure::permanent("task_not_found"))
    );
    assert_eq!(
        query(&state, "TaskSummary", json!({})),
        Ok(json!({ "total": 3, "open": 2, "completed": 1 }))
    );
    let open = query(&state, "ListTasks", json!({ "status": "open" })).unwrap();
    assert_eq!(open["tasks"].as_array().unwrap().len(), 2);
    assert_eq!(open["tasks"][0]["task_id"], "task-2");
    assert!(open.get("next").is_none());
}

#[test]
fn listing_pages_in_id_order() {
    let state = board();
    let first = query(&state, "ListTasks", json!({ "limit": 2 })).unwrap();
    let ids = |page: &Value| -> Vec<String> {
        page["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|task| task["task_id"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(ids(&first), ["task-1", "task-2"]);
    assert_eq!(first["next"], "task-2");
    let second = query(
        &state,
        "ListTasks",
        json!({ "limit": 2, "after": "task-2" }),
    )
    .unwrap();
    assert_eq!(ids(&second), ["task-3"]);
    assert!(second.get("next").is_none());
    let exact = query(&state, "ListTasks", json!({ "limit": 3 })).unwrap();
    assert!(exact.get("next").is_none());
}

#[test]
fn query_validation_is_typed_and_canonical() {
    assert_eq!(validate("ListTasks", json!({})), Ok(json!({ "limit": 50 })));
    assert_eq!(
        validate("ListTasks", json!({ "status": "open", "limit": 100 })),
        Ok(json!({ "status": "open", "limit": 100 }))
    );
    let invalid = |code| Err(Failure::permanent(code));
    assert_eq!(
        validate("ListTasks", json!({ "limit": 0 })),
        invalid("invalid_limit")
    );
    assert_eq!(
        validate("ListTasks", json!({ "limit": 101 })),
        invalid("invalid_limit")
    );
    assert_eq!(
        validate("ListTasks", json!({ "limit": 1.5 })),
        invalid("validation_failed")
    );
    assert_eq!(
        validate("ListTasks", json!({ "status": "late" })),
        invalid("validation_failed")
    );
    assert_eq!(
        validate("GetTask", json!({ "task_id": "bad id" })),
        invalid("validation_failed")
    );
    assert_eq!(
        validate("TaskSummary", json!({ "x": 1 })),
        invalid("validation_failed")
    );
    assert_eq!(validate("CreateTask", json!({})), invalid("unknown_query"));
    let command = call(Operation::Validate {
        kind: MessageKind::Command,
        tag: id("ListTasks"),
        payload: json!({}),
    });
    assert_eq!(command, Err(Failure::permanent("unknown_query")));
}

#[test]
fn decisions_are_not_this_component() {
    let decide = call(Operation::Decide {
        tag: id("CreateTask"),
        stream: id("task-1"),
        payload: json!({ "title": "x" }),
        history: vec![],
        metadata: metadata(),
    });
    assert_eq!(decide, Err(Failure::permanent("unsupported_operation")));
}

#[cfg(feature = "faulty")]
#[test]
fn the_faulty_revision_fails_batches_with_a_rename() {
    let open = project(empty(), vec![created(1, "task-1", "Write docs")]).unwrap();
    assert_eq!(
        project(
            open.clone(),
            vec![created(2, "task-2", "x"), renamed(3, 2, "task-1", "y")]
        ),
        Err(Failure::transient("projection_failed"))
    );
    assert!(project(open, vec![completed(2, 2, "task-1")]).is_ok());
}
