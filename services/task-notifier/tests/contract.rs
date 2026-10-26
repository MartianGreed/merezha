//! The reactor's contract through its `handle` export, using application
//! protocol version 2 exactly as the host sends `react` (AP §C.2).

use merezha_domain::{
    Effect, Event, Failure, Identity, Metadata, Operation, PROTOCOL_V2, Principal, PrincipalKind,
    Reply, Request, Response, StoredEvent,
};
use serde_json::{Value, json};
use task_notifier::{ARCHIVE_DELAY_MS, handle};

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn stored(
    position: u64,
    stream: &str,
    tag: &str,
    payload: Value,
    user: Option<&str>,
) -> StoredEvent {
    StoredEvent {
        event_id: id(&format!("event/{position}")),
        stream: id(stream),
        version: 1,
        position,
        event: Event {
            tag: id(tag),
            schema_version: 1,
            payload,
        },
        metadata: Metadata {
            message_id: id("m-1"),
            correlation_id: id("c-1"),
            causation_id: None,
            actor: user.map(id),
            principal: user.map(|subject| {
                Principal::new(id(subject), PrincipalKind::User, Some(id("org_1")), vec![]).unwrap()
            }),
        },
        accepted_unix_ms: 1_700_000_000_000,
    }
}
fn react(events: Vec<StoredEvent>) -> Result<Reply, Failure> {
    let request = Request::with_version(
        PROTOCOL_V2,
        Operation::React {
            reactor: id("task-notifier"),
            events,
        },
    );
    let output = handle(&serde_json::to_string(&request).unwrap()).expect("the ABI call succeeds");
    Response::decode_version(&output, PROTOCOL_V2)
}

#[test]
fn a_completed_task_notifies_who_completed_it_and_schedules_its_archiving() {
    let reply = react(vec![
        stored(
            41,
            "org_1:Task/t-1",
            "TaskCompleted",
            json!({"task_id": "org_1:Task/t-1"}),
            Some("usr_ada"),
        ),
        stored(
            42,
            "Task/t-2",
            "TaskRenamed",
            json!({"task_id": "Task/t-2", "title": "New"}),
            Some("usr_ada"),
        ),
        // Version 1 history: no principal, so nobody to notify. A local
        // id may hold `/`.
        stored(
            43,
            "Task/t-3/a",
            "TaskCompleted",
            json!({"task_id": "Task/t-3/a"}),
            None,
        ),
    ])
    .unwrap();
    // The exact reply the host checks: one reaction per event, in order.
    assert_eq!(
        serde_json::to_value(&reply).unwrap(),
        json!({"kind": "reacted", "reactions": [
            {"event_id": "event/41", "effects": [
                {"effect": "notify", "topic": "tasks.completed",
                 "recipients": [{"user": {"id": "usr_ada"}}], "channels": ["inbox", "email"],
                 "data": {"task_id": "org_1:Task/t-1"}},
                {"effect": "command", "tag": "ArchiveTask", "stream": "t-1", "payload": {},
                 "delay_ms": 604_800_000_u64},
            ]},
            {"event_id": "event/42", "effects": []},
            {"event_id": "event/43", "effects": [
                {"effect": "command", "tag": "ArchiveTask", "stream": "t-3/a", "payload": {},
                 "delay_ms": 604_800_000_u64},
            ]},
        ]})
    );
    assert_eq!(ARCHIVE_DELAY_MS, 7 * 86_400_000);
    // Guests never name a dedupe key, a tenant or a URL.
    let text = serde_json::to_string(&reply).unwrap();
    for host_only in ["dedupe", "tenant", "url", "source_event"] {
        assert!(!text.contains(host_only), "{host_only} in {text}");
    }
}

#[test]
fn a_system_principal_is_not_notified() {
    let mut event = stored(
        1,
        "Task/t-1",
        "TaskCompleted",
        json!({"task_id": "Task/t-1"}),
        None,
    );
    event.metadata.principal = Some(
        Principal::new(
            id("sys_reactor_task-notifier"),
            PrincipalKind::System,
            None,
            vec![],
        )
        .unwrap(),
    );
    let Reply::Reacted { reactions } = react(vec![event]).unwrap() else {
        panic!("reacted");
    };
    assert!(matches!(
        reactions[0].effects.as_slice(),
        [Effect::Command { .. }]
    ));
}

#[test]
fn an_undecodable_completion_fails_and_other_operations_are_refused() {
    assert_eq!(
        react(vec![stored(
            1,
            "Task/t-1",
            "TaskCompleted",
            json!({"task_id": "Task/t-1", "extra": true}),
            None
        )]),
        Err(Failure::permanent("invalid_event"))
    );
    let validate = Request::with_version(
        PROTOCOL_V2,
        Operation::Validate {
            kind: merezha_domain::MessageKind::Command,
            tag: id("ArchiveTask"),
            payload: json!({}),
        },
    );
    let output = handle(&serde_json::to_string(&validate).unwrap()).unwrap();
    assert_eq!(
        Response::decode_version(&output, PROTOCOL_V2),
        Err(Failure::permanent("unsupported_operation"))
    );
    // `describe` is answered by the SDK; version 1 cannot carry `react`.
    let described = handle(r#"{"version":2,"body":{"op":"describe"}}"#).unwrap();
    assert_eq!(
        Response::decode_version(&described, PROTOCOL_V2),
        Ok(Reply::Described { protocol: 2 })
    );
    let v1 = serde_json::to_string(&Request::new(Operation::React {
        reactor: id("task-notifier"),
        events: vec![],
    }))
    .unwrap();
    assert_eq!(
        Response::decode(&handle(&v1).unwrap()),
        Err(Failure::permanent("protocol_version"))
    );
}
