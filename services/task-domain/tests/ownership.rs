//! Protocol version 2: ownership from the host-established principal, the
//! automatic `describe`, and version 1 history without an owner.
use merezha_domain::{
    Event, Failure, Identity, Metadata, Operation, PROTOCOL_V2, Principal, PrincipalKind, Reply,
    Request, Response, StoredEvent,
};
use serde_json::{Value, json};
use task_domain::handle;

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn user(subject: &str, roles: &[&str]) -> Principal {
    Principal::new(
        id(subject),
        PrincipalKind::User,
        Some(id("org_1")),
        roles.iter().map(|r| id(r)).collect(),
    )
    .unwrap()
}
fn metadata(principal: Option<Principal>) -> Metadata {
    Metadata {
        message_id: id("msg-1"),
        correlation_id: id("corr-1"),
        causation_id: None,
        actor: principal.as_ref().map(|p| p.subject.clone()),
        principal,
    }
}
fn v2(operation: Operation) -> Result<Reply, Failure> {
    let input = serde_json::to_string(&Request::with_version(PROTOCOL_V2, operation)).unwrap();
    Response::decode_version(&handle(&input).unwrap(), PROTOCOL_V2)
}
/// Decide as `principal` and record accepted events as the host does, with
/// the stored principal (no roles).
fn decide(
    history: &mut Vec<StoredEvent>,
    principal: Option<Principal>,
    tag: &str,
    payload: Value,
) -> Result<Vec<Event>, Failure> {
    let reply = v2(Operation::Decide {
        tag: id(tag),
        stream: id("task-1"),
        payload,
        history: history.clone(),
        metadata: metadata(principal.clone()),
    })?;
    let Reply::Decided { events } = reply else {
        panic!("unexpected reply {reply:?}");
    };
    let mut stored = metadata(principal);
    if let Some(p) = &mut stored.principal {
        p.roles.clear();
    }
    for event in &events {
        let version = history.len() as u64 + 1;
        history.push(StoredEvent {
            event_id: id(&format!("event/{version}")),
            stream: id("task-1"),
            version,
            position: version,
            event: event.clone(),
            metadata: stored.clone(),
            accepted_unix_ms: 1_700_000_000_000,
        });
    }
    Ok(events)
}

#[test]
fn describe_is_answered_with_protocol_2() {
    assert_eq!(
        v2(Operation::Describe {}),
        Ok(Reply::Described { protocol: 2 })
    );
}

#[test]
fn create_task_records_the_principal_as_owner() {
    let mut history = Vec::new();
    let events = decide(
        &mut history,
        Some(user("usr_ada", &["member"])),
        "CreateTask",
        json!({"title":"Write docs"}),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&events).unwrap(),
        json!([{
            "tag": "TaskCreated",
            "schema_version": 2,
            "payload": {"task_id":"task-1","title":"Write docs","owner":"usr_ada"},
        }])
    );
}

#[test]
fn only_the_owner_or_an_admin_changes_an_owned_task() {
    let mut history = Vec::new();
    decide(
        &mut history,
        Some(user("usr_ada", &[])),
        "CreateTask",
        json!({"title":"Write docs"}),
    )
    .unwrap();
    let rename = json!({"title":"Write the docs"});
    let not_owner = Err(Failure::permanent("not_owner"));
    assert_eq!(
        decide(
            &mut history.clone(),
            Some(user("usr_bob", &["member"])),
            "RenameTask",
            rename.clone()
        ),
        not_owner
    );
    assert_eq!(
        decide(&mut history.clone(), None, "CompleteTask", json!({})),
        not_owner,
        "an anonymous caller is not the owner"
    );
    decide(
        &mut history,
        Some(user("usr_ada", &[])),
        "RenameTask",
        rename,
    )
    .unwrap();
    // An admin completes it; the event names the owner, not the admin, so
    // a reactor can notify the owner (schema version 2).
    let completed = decide(
        &mut history,
        Some(user("usr_root", &["admin"])),
        "CompleteTask",
        json!({}),
    )
    .unwrap();
    assert_eq!(completed[0].schema_version, 2);
    assert_eq!(
        completed[0].payload,
        json!({"task_id":"task-1","owner":"usr_ada"})
    );
    // A completed owned task still refuses others first.
    assert_eq!(
        decide(
            &mut history.clone(),
            Some(user("usr_bob", &[])),
            "RenameTask",
            json!({"title":"x"})
        ),
        not_owner
    );
    assert_eq!(
        decide(
            &mut history,
            Some(user("usr_ada", &[])),
            "RenameTask",
            json!({"title":"x"})
        ),
        Err(Failure::permanent("task_completed"))
    );
}

#[test]
fn unowned_tasks_stay_open_to_every_caller() {
    // Created through version 1 (no principal): schema version 1, no owner.
    let mut history = Vec::new();
    let events = decide(&mut history, None, "CreateTask", json!({"title":"Old"})).unwrap();
    assert_eq!(events[0].schema_version, 1);
    assert_eq!(events[0].payload, json!({"task_id":"task-1","title":"Old"}));
    decide(
        &mut history,
        Some(user("usr_bob", &["member"])),
        "RenameTask",
        json!({"title":"New"}),
    )
    .unwrap();
    let completed = decide(&mut history, None, "CompleteTask", json!({})).unwrap();
    assert_eq!(completed[0].schema_version, 1);
    assert_eq!(completed[0].payload, json!({"task_id":"task-1"}));
}

#[test]
fn owned_history_is_checked_against_its_schema_version() {
    let mut history = Vec::new();
    decide(
        &mut history,
        Some(user("usr_ada", &[])),
        "CreateTask",
        json!({"title":"Write docs"}),
    )
    .unwrap();
    let complete = |history: &[StoredEvent]| {
        decide(
            &mut history.to_vec(),
            Some(user("usr_ada", &[])),
            "CompleteTask",
            json!({}),
        )
    };
    let mut downgraded = history.clone();
    downgraded[0].event.schema_version = 1;
    assert_eq!(
        complete(&downgraded),
        Err(Failure::permanent("invalid_event"))
    );
    let mut ownerless = history.clone();
    ownerless[0].event.payload = json!({"task_id":"task-1","title":"Write docs"});
    assert_eq!(
        complete(&ownerless),
        Err(Failure::permanent("invalid_event"))
    );
    assert!(complete(&history).is_ok());
}
