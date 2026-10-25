//! Version 1 wire golden: these strings were produced by the SDK before
//! protocol version 2 existed, so v1 guests see byte-identical requests.
use merezha_domain::{
    Event, Failure, Identity, MessageKind, Metadata, Operation, Reply, Request, Response,
    StoredEvent,
};
use serde_json::json;

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn metadata() -> Metadata {
    serde_json::from_value(json!({
        "message_id": "m-1",
        "correlation_id": "c-1",
        "causation_id": "event/1",
        "actor": "usr_1"
    }))
    .unwrap()
}
fn stored() -> StoredEvent {
    StoredEvent {
        event_id: id("event/1"),
        stream: id("Task/t-1"),
        version: 1,
        position: 1,
        event: Event {
            tag: id("TaskCreated"),
            schema_version: 1,
            payload: json!({"task_id":"t-1","title":"Write docs"}),
        },
        metadata: metadata(),
        accepted_unix_ms: 1_700_000_000_000,
    }
}
fn v1(body: Operation) -> String {
    serde_json::to_string(&Request::new(body)).unwrap()
}

const DECIDE: &str = r#"{"version":1,"body":{"op":"decide","tag":"CreateTask","stream":"Task/t-1","payload":{"title":"Write docs"},"history":[{"event_id":"event/1","stream":"Task/t-1","version":1,"position":1,"event":{"tag":"TaskCreated","schema_version":1,"payload":{"task_id":"t-1","title":"Write docs"}},"metadata":{"message_id":"m-1","correlation_id":"c-1","causation_id":"event/1","actor":"usr_1"},"accepted_unix_ms":1700000000000}],"metadata":{"message_id":"m-1","correlation_id":"c-1","causation_id":"event/1","actor":"usr_1"}}}"#;
const PROJECT: &str = r#"{"version":1,"body":{"op":"project","projection":"tasks","state":{"tasks":{}},"events":[{"event_id":"event/1","stream":"Task/t-1","version":1,"position":1,"event":{"tag":"TaskCreated","schema_version":1,"payload":{"task_id":"t-1","title":"Write docs"}},"metadata":{"message_id":"m-1","correlation_id":"c-1","causation_id":"event/1","actor":"usr_1"},"accepted_unix_ms":1700000000000}],"live":true}}"#;
const QUERY: &str = r#"{"version":1,"body":{"op":"query","tag":"ListTasks","payload":{},"state":{"tasks":{}},"metadata":{"message_id":"m-1","correlation_id":"c-1","causation_id":"event/1","actor":"usr_1"}}}"#;
const VALIDATE: &str = r#"{"version":1,"body":{"op":"validate","kind":"command","tag":"CreateTask","payload":{"title":"Write docs"}}}"#;
const OK: &str =
    r#"{"version":1,"outcome":{"status":"ok","reply":{"kind":"projected","state":{"tasks":{}}}}}"#;
const ERR: &str = r#"{"version":1,"outcome":{"status":"err","error":{"code":"task_exists","classification":"permanent"}}}"#;

#[test]
fn version_1_requests_and_responses_are_byte_identical() {
    let decide = Operation::Decide {
        tag: id("CreateTask"),
        stream: id("Task/t-1"),
        payload: json!({"title":"Write docs"}),
        history: vec![stored()],
        metadata: metadata(),
    };
    assert_eq!(v1(decide), DECIDE);
    let project: Operation = serde_json::from_value(json!({
        "op": "project", "projection": "tasks", "state": {"tasks":{}},
        "events": [stored()], "live": true
    }))
    .unwrap();
    assert_eq!(v1(project), PROJECT);
    let query = Operation::Query {
        tag: id("ListTasks"),
        payload: json!({}),
        state: json!({"tasks":{}}),
        metadata: metadata(),
    };
    assert_eq!(v1(query), QUERY);
    let validate = Operation::Validate {
        kind: MessageKind::Command,
        tag: id("CreateTask"),
        payload: json!({"title":"Write docs"}),
    };
    assert_eq!(v1(validate), VALIDATE);
    assert_eq!(
        Response::encode(Ok(Reply::Projected {
            state: json!({"tasks":{}})
        }))
        .unwrap(),
        OK
    );
    assert_eq!(
        Response::encode(Err(Failure::permanent("task_exists"))).unwrap(),
        ERR
    );
}

#[test]
fn version_1_golden_requests_decode_and_round_trip() {
    for golden in [DECIDE, PROJECT, QUERY, VALIDATE] {
        let request = Request::decode(golden).unwrap();
        assert_eq!(request.version, 1);
        assert_eq!(serde_json::to_string(&request).unwrap(), golden);
    }
    assert!(Response::decode(OK).is_ok());
    assert_eq!(
        Response::decode(ERR),
        Err(Failure::permanent("task_exists"))
    );
}
