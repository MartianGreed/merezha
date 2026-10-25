//! The decider's contract through its `handle` export, using the version 1
//! application protocol exactly as the host sends it.

use merezha_domain::{
    Classification, Event, Failure, Identity, Metadata, Operation, Reply, Request, Response,
    StoredEvent,
};
use serde_json::{Value, json};
use task_domain::handle;

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}

fn call(operation: Operation) -> Result<Reply, Failure> {
    let input = serde_json::to_string(&Request::new(operation)).unwrap();
    Response::decode(&handle(&input).expect("the ABI call itself succeeds"))
}

fn validate(tag: &str, payload: Value) -> Result<Value, Failure> {
    match call(Operation::Validate {
        kind: merezha_domain::MessageKind::Command,
        tag: id(tag),
        payload,
    })? {
        Reply::Validated { payload } => Ok(payload),
        other => panic!("unexpected reply {other:?}"),
    }
}

fn metadata() -> Metadata {
    Metadata {
        message_id: id("msg-1"),
        correlation_id: id("corr-1"),
        causation_id: None,
        actor: Some(id("ada")),
        principal: None,
    }
}

/// Records `events` the way the host does: consecutive stream versions.
fn store(stream: &str, history: &mut Vec<StoredEvent>, events: Vec<Event>) {
    for event in events {
        let version = history.len() as u64 + 1;
        history.push(StoredEvent {
            event_id: id(&format!("evt-{version}")),
            stream: id(stream),
            version,
            position: version,
            event,
            metadata: metadata(),
            accepted_unix_ms: 1_700_000_000_000,
        });
    }
}

fn decide(
    stream: &str,
    history: &[StoredEvent],
    tag: &str,
    payload: Value,
) -> Result<Vec<Event>, Failure> {
    match call(Operation::Decide {
        tag: id(tag),
        stream: id(stream),
        payload,
        history: history.to_vec(),
        metadata: metadata(),
    })? {
        Reply::Decided { events } => Ok(events),
        other => panic!("unexpected reply {other:?}"),
    }
}

fn rejected(code: &str) -> Result<Vec<Event>, Failure> {
    Err(Failure::permanent(code))
}

/// Runs `commands` in order on one stream, recording accepted events.
fn history(stream: &str, commands: &[(&str, Value)]) -> Vec<StoredEvent> {
    let mut history = Vec::new();
    for (tag, payload) in commands {
        let events = decide(stream, &history, tag, payload.clone()).unwrap();
        store(stream, &mut history, events);
    }
    history
}

#[test]
fn validation_returns_the_canonical_payload() {
    assert_eq!(
        validate("CreateTask", json!({ "title": "  Write docs \n" })),
        Ok(json!({ "title": "Write docs" }))
    );
    assert_eq!(validate("CompleteTask", json!({})), Ok(json!({})));
}

#[test]
fn validation_rejects_bad_shapes_and_titles() {
    let invalid = |code| Err(Failure::permanent(code));
    assert_eq!(
        validate("CreateTask", json!({})),
        invalid("validation_failed")
    );
    assert_eq!(
        validate("CreateTask", json!({ "title": "a", "owner": "ada" })),
        invalid("validation_failed")
    );
    assert_eq!(
        validate("CreateTask", json!({ "title": 7 })),
        invalid("validation_failed")
    );
    assert_eq!(
        validate("CompleteTask", json!({ "now": true })),
        invalid("validation_failed")
    );
    assert_eq!(
        validate("CreateTask", json!({ "title": "   " })),
        invalid("invalid_title")
    );
    assert_eq!(
        validate("RenameTask", json!({ "title": "a\u{7}b" })),
        invalid("invalid_title")
    );
    assert_eq!(
        validate("CreateTask", json!({ "title": "é".repeat(121) })),
        invalid("invalid_title")
    );
    assert!(validate("CreateTask", json!({ "title": "é".repeat(120) })).is_ok());
    assert_eq!(
        validate("DeleteTask", json!({})),
        invalid("unknown_command")
    );
}

#[test]
fn queries_and_projections_are_not_this_component() {
    let query = call(Operation::Validate {
        kind: merezha_domain::MessageKind::Query,
        tag: id("GetTask"),
        payload: json!({}),
    });
    assert_eq!(query, Err(Failure::permanent("unknown_command")));
    let project = call(Operation::Project {
        projection: id("tasks"),
        state: json!({}),
        events: vec![],
        live: true,
        partition: None,
    });
    assert_eq!(project, Err(Failure::permanent("unsupported_operation")));
}

#[test]
fn creating_a_task_records_its_id_and_canonical_title() {
    let events = decide(
        "task-1",
        &[],
        "CreateTask",
        json!({ "title": " Write docs " }),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&events).unwrap(),
        json!([{
            "tag": "TaskCreated",
            "schema_version": 1,
            "payload": { "task_id": "task-1", "title": "Write docs" },
        }])
    );
}

#[test]
fn an_open_task_can_be_renamed_and_completed() {
    let history = history(
        "task-1",
        &[
            ("CreateTask", json!({ "title": "Write docs" })),
            ("RenameTask", json!({ "title": "Write the docs" })),
            ("CompleteTask", json!({})),
        ],
    );
    let tags: Vec<&str> = history.iter().map(|e| e.event.tag.as_str()).collect();
    assert_eq!(tags, ["TaskCreated", "TaskRenamed", "TaskCompleted"]);
    assert_eq!(
        history[1].event.payload,
        json!({ "task_id": "task-1", "title": "Write the docs" })
    );
}

#[test]
fn invariants_reject_commands_without_events() {
    let open = history(
        "task-1",
        &[("CreateTask", json!({ "title": "Write docs" }))],
    );
    let done = history(
        "task-1",
        &[
            ("CreateTask", json!({ "title": "Write docs" })),
            ("CompleteTask", json!({})),
        ],
    );
    let rename = || json!({ "title": "Other" });
    assert_eq!(
        decide("task-1", &[], "RenameTask", rename()),
        rejected("task_not_found")
    );
    assert_eq!(
        decide("task-1", &[], "CompleteTask", json!({})),
        rejected("task_not_found")
    );
    assert_eq!(
        decide("task-1", &open, "CreateTask", json!({ "title": "Again" })),
        rejected("task_exists")
    );
    assert_eq!(
        decide(
            "task-1",
            &open,
            "RenameTask",
            json!({ "title": " Write docs" })
        ),
        rejected("title_unchanged")
    );
    assert_eq!(
        decide("task-1", &done, "CompleteTask", json!({})),
        rejected("task_already_completed")
    );
    assert_eq!(
        decide("task-1", &done, "RenameTask", rename()),
        rejected("task_completed")
    );
    assert_eq!(
        decide("task-1", &done, "CreateTask", json!({ "title": "Again" })),
        rejected("task_exists")
    );
}

#[test]
fn decide_validates_the_payload_again() {
    assert_eq!(
        decide("task-1", &[], "CreateTask", json!({ "title": "" })),
        rejected("invalid_title")
    );
    assert_eq!(
        decide("task-1", &[], "CreateTask", json!({ "name": "x" })),
        rejected("validation_failed")
    );
}

#[test]
fn replay_is_deterministic() {
    let history = history(
        "task-1",
        &[
            ("CreateTask", json!({ "title": "Write docs" })),
            ("RenameTask", json!({ "title": "Write the docs" })),
        ],
    );
    let first = decide(
        "task-1",
        &history,
        "RenameTask",
        json!({ "title": "Final" }),
    );
    let second = decide(
        "task-1",
        &history,
        "RenameTask",
        json!({ "title": "Final" }),
    );
    assert_eq!(first, second);
    assert_eq!(
        decide(
            "task-1",
            &history,
            "RenameTask",
            json!({ "title": "Write the docs" })
        ),
        rejected("title_unchanged")
    );
}

#[test]
fn corrupt_history_is_rejected() {
    let good = history(
        "task-1",
        &[
            ("CreateTask", json!({ "title": "Write docs" })),
            ("CompleteTask", json!({})),
        ],
    );
    let complete =
        |history: &[StoredEvent]| decide("task-1", history, "RenameTask", json!({ "title": "x" }));

    let mut gap = good.clone();
    gap[1].version = 3;
    assert_eq!(complete(&gap), rejected("corrupt_history"));

    let mut other_stream = good.clone();
    other_stream[0].stream = id("task-2");
    assert_eq!(complete(&other_stream), rejected("corrupt_history"));

    let mut foreign_payload = good.clone();
    foreign_payload[0].event.payload = json!({ "task_id": "task-2", "title": "Write docs" });
    assert_eq!(complete(&foreign_payload), rejected("corrupt_history"));

    let out_of_order = vec![good[1].clone()];
    let mut out_of_order = out_of_order;
    out_of_order[0].version = 1;
    assert_eq!(complete(&out_of_order), rejected("corrupt_history"));

    let mut unknown = good.clone();
    unknown[1].event.tag = id("TaskArchived");
    assert_eq!(complete(&unknown), rejected("unknown_event"));

    let mut newer = good.clone();
    // Version 2 is the owned `TaskCreated`; 3 is newer than this schema.
    newer[0].event.schema_version = 3;
    assert_eq!(complete(&newer), rejected("unsupported_schema_version"));

    let mut malformed = good;
    malformed[0].event.payload = json!({ "task_id": "task-1" });
    assert_eq!(complete(&malformed), rejected("invalid_event"));
}

#[test]
fn protocol_errors_are_classified_responses() {
    let response = |input: &str| Response::decode(&handle(input).unwrap());
    assert_eq!(
        response(
            r#"{"version":2,"body":{"op":"validate","kind":"command","tag":"CreateTask","payload":{}}}"#
        ),
        Err(Failure::permanent("protocol_version"))
    );
    assert_eq!(
        response("not json"),
        Err(Failure::permanent("validation_failed"))
    );
    let failure = response(r#"{"version":1,"body":{"op":"archive"}}"#).unwrap_err();
    assert_eq!(failure.classification, Classification::Permanent);
}
