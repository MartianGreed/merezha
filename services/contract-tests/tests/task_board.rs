//! Native composition of the task board components through the version 1
//! application protocol, with a minimal in-test event log standing in for
//! the host: validate, decide, append with consecutive versions and global
//! positions, then project in batches and query. The fixture and expected
//! catch-up are the files the real-host scenario uses.

use merezha_domain::{
    Event, Failure, Identity, MessageKind, Metadata, Operation, Reply, Request, Response,
    StoredEvent,
};
use serde_json::{Value, json};

const TASKS: &str = include_str!("../../../examples/task-board/fixtures/tasks.json");
const CAUGHT_UP: &str = include_str!("../../../examples/task-board/expected/catch-up-created.json");

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}

fn call(
    handle: fn(&str) -> Result<String, String>,
    operation: Operation,
) -> Result<Reply, Failure> {
    let input = serde_json::to_string(&Request::new(operation)).unwrap();
    Response::decode(&handle(&input).unwrap())
}

fn metadata() -> Metadata {
    Metadata {
        message_id: id("msg"),
        correlation_id: id("task-board"),
        causation_id: None,
        actor: Some(id("ada")),
    }
}

/// The host's part of the protocol, reduced to what the guests observe.
#[derive(Default)]
struct Log {
    events: Vec<StoredEvent>,
}

impl Log {
    fn command(&mut self, tag: &str, task: &str, payload: Value) -> Result<u64, Failure> {
        let stream = id(&format!("Task/{task}"));
        let Reply::Validated { payload } = call(
            task_domain::handle,
            Operation::Validate {
                kind: MessageKind::Command,
                tag: id(tag),
                payload,
            },
        )?
        else {
            panic!("validate must answer validated");
        };
        let history: Vec<StoredEvent> = self
            .events
            .iter()
            .filter(|event| event.stream == stream)
            .cloned()
            .collect();
        let Reply::Decided { events } = call(
            task_domain::handle,
            Operation::Decide {
                tag: id(tag),
                stream: stream.clone(),
                payload,
                history: history.clone(),
                metadata: metadata(),
            },
        )?
        else {
            panic!("decide must answer decided");
        };
        let mut version = history.len() as u64;
        for event in events {
            version += 1;
            self.append(&stream, version, event);
        }
        Ok(version)
    }

    fn append(&mut self, stream: &Identity, version: u64, event: Event) {
        let position = self.events.len() as u64 + 1;
        self.events.push(StoredEvent {
            event_id: id(&format!("evt-{position}")),
            stream: stream.clone(),
            version,
            position,
            event,
            metadata: metadata(),
            accepted_unix_ms: 1_700_000_000_000,
        });
    }
}

/// Folds `log` from `checkpoint` in batches of `batch`, as catch-up does.
fn catch_up(
    log: &Log,
    mut state: Value,
    checkpoint: usize,
    batch: usize,
) -> Result<Value, Failure> {
    for events in log.events[checkpoint..].chunks(batch) {
        let Reply::Projected { state: next } = call(
            task_view::handle,
            Operation::Project {
                projection: id("tasks"),
                state,
                events: events.to_vec(),
                live: true,
            },
        )?
        else {
            panic!("project must answer projected");
        };
        state = next;
    }
    Ok(state)
}

fn query(state: &Value, tag: &str, payload: Value) -> Result<Value, Failure> {
    match call(
        task_view::handle,
        Operation::Query {
            tag: id(tag),
            payload,
            state: state.clone(),
            metadata: metadata(),
        },
    )? {
        Reply::Queried { value } => Ok(value),
        other => panic!("unexpected reply {other:?}"),
    }
}

fn seeded() -> Log {
    let mut log = Log::default();
    let tasks: Vec<Value> = serde_json::from_str(TASKS).unwrap();
    for task in tasks {
        let created = log.command(
            "CreateTask",
            task["stream"].as_str().unwrap(),
            json!({ "title": task["title"] }),
        );
        assert_eq!(created, Ok(1));
    }
    log
}

#[test]
fn the_fixture_catches_up_to_the_expected_read_model() {
    let log = seeded();
    let expected: Value = serde_json::from_str(CAUGHT_UP).unwrap();
    let state = catch_up(&log, json!({ "tasks": {} }), 0, 16).unwrap();
    assert_eq!(state, expected["state"]);
    assert_eq!(json!(log.events.len()), expected["checkpoint"]);
}

#[test]
fn rejected_commands_leave_the_log_unchanged() {
    let mut log = seeded();
    log.command("CompleteTask", "task-2", json!({})).unwrap();
    let before = log.events.len();
    assert_eq!(
        log.command("CompleteTask", "task-2", json!({})),
        Err(Failure::permanent("task_already_completed"))
    );
    assert_eq!(
        log.command("RenameTask", "task-2", json!({ "title": "Late" })),
        Err(Failure::permanent("task_completed"))
    );
    assert_eq!(log.events.len(), before);
}

#[test]
fn rebuild_in_any_batch_size_matches_incremental_catch_up() {
    let mut log = seeded();
    log.command(
        "RenameTask",
        "task-1",
        json!({ "title": "Write the user guide" }),
    )
    .unwrap();
    log.command("CompleteTask", "task-2", json!({})).unwrap();
    let first = catch_up(&log, json!({ "tasks": {} }), 0, 16).unwrap();
    log.command(
        "RenameTask",
        "task-3",
        json!({ "title": "Plan the live demo" }),
    )
    .unwrap();
    let incremental = catch_up(&log, first, 5, 16).unwrap();
    for batch in [1, 2, 16] {
        assert_eq!(
            catch_up(&log, json!({ "tasks": {} }), 0, batch),
            Ok(incremental.clone())
        );
    }
    assert_eq!(
        query(&incremental, "TaskSummary", json!({})),
        Ok(json!({ "total": 3, "open": 2, "completed": 1 }))
    );
    assert_eq!(
        query(&incremental, "GetTask", json!({ "task_id": "Task/task-3" }))
            .map(|task| task["title"].clone()),
        Ok(json!("Plan the live demo"))
    );
}
